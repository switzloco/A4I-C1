"""
K-12 School Matching Algorithm
Finds and ranks schools based on student profile and preferences
"""
from typing import Dict, Any, List, Optional
import math
from ..config import MATCHING_WEIGHTS, MATCH_CATEGORIES


def match_schools(
    student_profile: Dict[str, Any],
    project_id: str,
    dataset: str = "education_data",
    limit: int = 20
) -> Dict[str, Any]:
    """
    Find schools matching student profile by querying BigQuery.
    
    Args:
        student_profile: Student profile from parse_student_documents
        project_id: Google Cloud project ID
        dataset: BigQuery dataset name
        limit: Maximum number of schools to return
        
    Returns:
        Dictionary with matched schools and metadata
    """
    try:
        from google.cloud import bigquery
        import google.auth
        import subprocess
        
        # Get BigQuery client - don't pass project_id, let it default to environment
        try:
            result = subprocess.run(
                ['gcloud', 'auth', 'print-access-token'],
                capture_output=True,
                text=True,
                check=True
            )
            access_token = result.stdout.strip()
            from google.auth.transport.requests import Request
            from google.oauth2.credentials import Credentials
            creds = Credentials(token=access_token)
            client = bigquery.Client(credentials=creds)
        except:
            credentials, _ = google.auth.default()
            client = bigquery.Client(credentials=credentials)
        
        # Build query based on student profile
        query = _build_matching_query(
            student_profile=student_profile,
            project_id=project_id,
            dataset=dataset,
            limit=limit
        )
        
        # Execute query - save full query for debugging
        with open('/tmp/school_match_query.sql', 'w') as f:
            f.write(query)
        print(f"\n=== Query saved to /tmp/school_match_query.sql (length: {len(query)} chars) ===")
        
        query_job = client.query(query)
        results = query_job.result()
        
        # Convert to list of dicts and deduplicate by ncessch
        seen_schools = {}
        for row in results:
            school = dict(row)
            ncessch = school.get('ncessch')
            # Keep first occurrence of each school
            if ncessch and ncessch not in seen_schools:
                seen_schools[ncessch] = school
        
        schools = list(seen_schools.values())
        
        if not schools:
            return {
                "status": "no_matches",
                "message": "No schools found matching criteria",
                "schools": [],
                "query": query
            }
        
        return {
            "status": "success",
            "schools": schools,
            "count": len(schools),
            "query": query,
            "student_profile": student_profile
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error matching schools: {str(e)}",
            "schools": []
        }


def _build_matching_query(
    student_profile: Dict[str, Any],
    project_id: str,
    dataset: str,
    limit: int
) -> str:
    """
    Build BigQuery SQL query based on student profile.
    
    Args:
        student_profile: Student profile data
        project_id: GCP project ID
        dataset: BigQuery dataset
        limit: Max results
        
    Returns:
        SQL query string
    """
    # Extract key info from profile
    school_level = student_profile.get("school_level", 0)
    location = student_profile.get("location", {})
    city = location.get("city", "").upper() if location else ""
    needs = student_profile.get("needs_categories", {})
    interests = student_profile.get("interest_categories", {})
    
    # Query with real data from joined tables
    query = f"""
    WITH school_data AS (
        SELECT
            c.ncessch,
            c.school_name,
            c.lea_name as district_name,
            c.leaid,
            c.city_location,
            c.state_location,
            c.county_code,
            c.school_level,
            c.enrollment,
            c.teachers_fte,
            c.charter,
            c.latitude,
            c.longitude,
            ROUND(SAFE_CAST(c.free_lunch AS FLOAT64) / NULLIF(SAFE_CAST(c.enrollment AS FLOAT64), 0) * 100, 1) as low_income_pct,
            ROUND(SAFE_CAST(c.enrollment AS FLOAT64) / NULLIF(SAFE_CAST(c.teachers_fte AS FLOAT64), 0), 1) as student_teacher_ratio,

            -- Graduation data (for high schools) - use REAL data
            g.grad_rate_midpt as graduation_rate,

            -- STEM programs - use REAL data (only for high schools, otherwise NULL)
            CASE
                WHEN c.school_level = 3 THEN GREATEST(COALESCE(SAFE_CAST(ap.SCH_APCOURSES AS INT64), 0), 0)
                ELSE NULL
            END as ap_courses,
            CASE
                WHEN c.school_level = 3 THEN GREATEST(COALESCE(SAFE_CAST(ap.TOT_APENR_M AS INT64), 0) + COALESCE(SAFE_CAST(ap.TOT_APENR_F AS INT64), 0), 0)
                ELSE NULL
            END as ap_enrollment,

            -- Gifted & Talented - use REAL data
            IF(gt.COMBOKEY IS NOT NULL, 1, 0) as has_gifted_program,

            -- District finance - use REAL data
            f.per_pupil_total,
            f.per_pupil_instruction

        FROM `{dataset}.ccd_directory` c
        LEFT JOIN `{dataset}.graduation_rates` g
            ON c.ncessch = g.ncessch
            AND SAFE_CAST(g.race AS INT64) = 99
            AND SAFE_CAST(g.disability AS INT64) = 99
            AND SAFE_CAST(g.econ_disadvantaged AS INT64) = 99
        LEFT JOIN `{dataset}.stem_advanced_placement` ap
            ON CONCAT(c.leaid, SAFE_CAST(c.school_id AS STRING)) = ap.COMBOKEY
        LEFT JOIN `{dataset}.stem_gifted_and_talented` gt
            ON CONCAT(c.leaid, SAFE_CAST(c.school_id AS STRING)) = gt.COMBOKEY
        LEFT JOIN `{dataset}.district_finance` f
            ON c.leaid = f.LEAID
        WHERE SAFE_CAST(c.enrollment AS INT64) >= 50
          AND SAFE_CAST(c.teachers_fte AS FLOAT64) > 0
          AND c.school_name IS NOT NULL
    """
    
    # Filter by school level
    if school_level > 0:
        query += f"  AND school_level = {school_level}\n"
    
    # Filter by location if provided
    if city:
        query += f"  AND UPPER(city_location) = '{city}'\n"
    
    query += """
        LIMIT 100
    ),
    scored_schools AS (
        SELECT 
            *,
            -- Calculate match scores
            (
                -- 1. School Quality (30%)
                (
                    CASE 
                        WHEN graduation_rate >= 90 THEN 1.0
                        WHEN graduation_rate >= 80 THEN 0.8
                        WHEN graduation_rate >= 70 THEN 0.6
                        WHEN graduation_rate IS NULL AND school_level < 3 THEN 0.7  -- Elementary/Middle (no grad data)
                        ELSE 0.4
                    END * 0.5 +  -- Graduation rate weight
                    CASE 
                        WHEN student_teacher_ratio <= 15 THEN 1.0
                        WHEN student_teacher_ratio <= 20 THEN 0.8
                        WHEN student_teacher_ratio <= 25 THEN 0.6
                        ELSE 0.4
                    END * 0.5  -- Class size weight
                ) * 0.30 +
                
                -- 2. Programs & Services (25%)
                (
                    CASE WHEN ap_courses >= 10 THEN 1.0
                         WHEN ap_courses >= 5 THEN 0.7
                         WHEN ap_courses > 0 THEN 0.5
                         WHEN school_level < 3 THEN 0.7  -- Elementary/Middle (no AP expected)
                         ELSE 0.3
                    END * 0.6 +  -- STEM programs
                    CASE WHEN has_gifted_program = 1 THEN 1.0 ELSE 0.5 END * 0.4  -- Gifted programs
                ) * 0.25 +
                
                -- 3. School Environment (20%)
                (
                    CASE 
                        WHEN enrollment BETWEEN 200 AND 800 THEN 1.0  -- Ideal size
                        WHEN enrollment BETWEEN 100 AND 1500 THEN 0.7
                        ELSE 0.5
                    END * 0.5 +
                    CASE 
                        WHEN student_teacher_ratio <= 18 THEN 1.0
                        WHEN student_teacher_ratio <= 22 THEN 0.7
                        ELSE 0.5
                    END * 0.5
                ) * 0.20 +
                
                -- 4. Location (15%) - will calculate distance in Python
                0.15 * 0.8 +  -- Placeholder, adjust based on distance
                
                -- 5. Admission Fit (10%)
                (
                    CASE WHEN charter = 0 THEN 1.0  -- Public schools (easier admission)
                         ELSE 0.7  -- Charter (lottery)
                    END
                ) * 0.10
            ) as base_match_score
            
        FROM school_data
    )
    SELECT 
        ncessch,
        school_name,
        district_name,
        leaid,
        city_location,
        county_code,
        school_level,
        enrollment,
        student_teacher_ratio,
        low_income_pct,
        graduation_rate,
        ap_courses,
        ap_enrollment,
        has_gifted_program,
        charter,
        latitude,
        longitude,
        per_pupil_total,
        per_pupil_instruction,
        base_match_score,
        CASE 
            WHEN base_match_score >= 0.85 THEN 'Excellent Match'
            WHEN base_match_score >= 0.70 THEN 'Good Match'
            WHEN base_match_score >= 0.50 THEN 'Fair Match'
            ELSE 'Consider'
        END as match_category
    FROM scored_schools
    WHERE base_match_score >= 0.40  -- Minimum threshold
    ORDER BY base_match_score DESC, graduation_rate DESC
    LIMIT """ + str(limit) + """
    """
    
    return query


def rank_schools(
    schools: List[Dict[str, Any]],
    student_profile: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Rank and score schools based on student profile.
    
    Adds personalized scoring and reasoning for each school.
    
    Args:
        schools: List of matched schools from BigQuery
        student_profile: Student profile data
        
    Returns:
        Ranked list of schools with scores and reasoning
    """
    try:
        location = student_profile.get("location", {})
        home_city = location.get("city", "").upper() if location else ""
        
        ranked_schools = []
        
        for school in schools:
            # Calculate distance-based score (if location provided)
            location_score = 1.0  # Default if no location
            if home_city and school.get("city_location"):
                if school["city_location"].upper() == home_city:
                    location_score = 1.0  # Same city
                else:
                    location_score = 0.5  # Different city
            
            # Adjust base score with location
            base_score = school.get("base_match_score", 0.5)
            adjusted_score = base_score * 0.85 + location_score * 0.15
            
            # Generate match reasoning
            reasoning = _generate_match_reasoning(school, student_profile, adjusted_score)
            
            # Determine admission type
            admission_type = "Charter School (Lottery)" if school.get("charter") == 1 else "Public School (Enrollment)"
            
            # Add enriched data
            enriched_school = school.copy()
            enriched_school["match_score"] = round(adjusted_score * 100, 1)  # Convert to percentage
            enriched_school["match_reasoning"] = reasoning
            enriched_school["admission_type"] = admission_type
            enriched_school["distance_score"] = location_score
            
            ranked_schools.append(enriched_school)
        
        # Sort by adjusted score
        ranked_schools.sort(key=lambda x: x["match_score"], reverse=True)
        
        # Add rank
        for i, school in enumerate(ranked_schools, 1):
            school["rank"] = i
        
        return ranked_schools
        
    except Exception as e:
        print(f"Error ranking schools: {e}")
        return schools


def _generate_match_reasoning(
    school: Dict[str, Any],
    student_profile: Dict[str, Any],
    match_score: float
) -> List[str]:
    """
    Generate human-readable reasoning for why this school matches.
    
    Args:
        school: School data
        student_profile: Student profile
        match_score: Calculated match score
        
    Returns:
        List of reason strings
    """
    reasons = []
    
    # Academic quality
    grad_rate = school.get("graduation_rate")
    if grad_rate and grad_rate >= 85:
        reasons.append(f"✓ High graduation rate ({grad_rate}%)")
    elif grad_rate and grad_rate >= 75:
        reasons.append(f"✓ Good graduation rate ({grad_rate}%)")
    
    # Class size
    str_ratio = school.get("student_teacher_ratio")
    if str_ratio and str_ratio <= 18:
        reasons.append(f"✓ Small class sizes ({str_ratio}:1 student-teacher ratio)")
    elif str_ratio and str_ratio <= 22:
        reasons.append(f"✓ Reasonable class sizes ({str_ratio}:1 ratio)")
    
    # STEM programs
    ap_courses = school.get("ap_courses", 0)
    interests = student_profile.get("interest_categories", {})
    if ap_courses and ap_courses >= 5 and interests.get("stem"):
        reasons.append(f"✓ Strong STEM programs ({ap_courses} AP courses)")
    elif ap_courses and ap_courses > 0:
        reasons.append(f"✓ Offers AP courses ({ap_courses} available)")
    
    # Gifted program
    needs = student_profile.get("needs_categories", {})
    if school.get("has_gifted_program") and needs.get("gifted"):
        reasons.append("✓ Has Gifted & Talented program")
    
    # Location
    location = student_profile.get("location", {})
    home_city = location.get("city", "").upper() if location else ""
    if home_city and school.get("city_location", "").upper() == home_city:
        reasons.append(f"✓ Located in {school['city_location']}")
    
    # School size
    enrollment = school.get("enrollment")
    if enrollment and 200 <= enrollment <= 800:
        reasons.append(f"✓ Medium-sized school ({int(enrollment)} students)")
    
    # Admission type
    if school.get("charter") == 0:
        reasons.append("✓ Public school (neighborhood enrollment)")
    else:
        reasons.append("⚠ Charter school (lottery application required)")
    
    # If few reasons, add generic positive
    if len(reasons) < 3:
        reasons.append("✓ Meets basic quality standards")
    
    return reasons


def generate_school_recommendations(
    ranked_schools: List[Dict[str, Any]],
    student_profile: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate final recommendations with application strategy.
    
    Args:
        ranked_schools: Ranked list of schools
        student_profile: Student profile
        
    Returns:
        Complete recommendations with strategy
    """
    try:
        if not ranked_schools:
            return {
                "status": "no_recommendations",
                "message": "No schools matched the criteria",
                "recommendations": []
            }
        
        # Categorize schools
        excellent = [s for s in ranked_schools if s["match_score"] >= 85]
        good = [s for s in ranked_schools if 70 <= s["match_score"] < 85]
        fair = [s for s in ranked_schools if 50 <= s["match_score"] < 70]
        
        # Generate summary
        summary = f"""
Found {len(ranked_schools)} schools matching your child's profile:
• {len(excellent)} Excellent matches (85%+ fit)
• {len(good)} Good matches (70-85% fit)
• {len(fair)} Fair matches (50-70% fit)
"""
        
        # Application strategy
        strategy = _generate_application_strategy(ranked_schools, student_profile)
        
        return {
            "status": "success",
            "total_matches": len(ranked_schools),
            "top_10": ranked_schools[:10],
            "by_category": {
                "excellent": excellent[:5],
                "good": good[:5],
                "fair": fair[:3]
            },
            "summary": summary.strip(),
            "application_strategy": strategy,
            "student_profile": student_profile
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error generating recommendations: {str(e)}",
            "recommendations": []
        }


def _generate_application_strategy(
    ranked_schools: List[Dict[str, Any]],
    student_profile: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Generate personalized application strategy.
    
    Args:
        ranked_schools: Ranked schools
        student_profile: Student profile
        
    Returns:
        Application strategy dict
    """
    # Identify school types
    public_schools = [s for s in ranked_schools if s.get("charter") == 0]
    charter_schools = [s for s in ranked_schools if s.get("charter") == 1]
    
    # Top picks
    top_choice = ranked_schools[0] if ranked_schools else None
    neighborhood_school = public_schools[0] if public_schools else None
    
    strategy = {
        "recommended_approach": "",
        "top_choice": top_choice,
        "neighborhood_option": neighborhood_school,
        "lottery_schools": charter_schools[:3],
        "next_steps": []
    }
    
    # Generate recommendations
    if top_choice:
        if top_choice.get("charter") == 0:
            strategy["recommended_approach"] = f"Your top match ({top_choice['school_name']}) is a public school. You likely have guaranteed enrollment if you live in the attendance zone."
            strategy["next_steps"].append("1. Verify you live in the school's attendance boundary")
            strategy["next_steps"].append("2. Complete enrollment forms by district deadline")
            strategy["next_steps"].append("3. Schedule a school tour to visit")
        else:
            strategy["recommended_approach"] = f"Your top match ({top_choice['school_name']}) is a charter school requiring lottery application."
            strategy["next_steps"].append("1. Submit lottery application before deadline (typically Jan-Feb)")
            strategy["next_steps"].append("2. Have a backup plan (neighborhood school)")
            strategy["next_steps"].append("3. Monitor lottery results (typically March-April)")
    
    if charter_schools:
        strategy["next_steps"].append(f"4. Apply to {len(charter_schools[:3])} charter schools to maximize options")
    
    return strategy

