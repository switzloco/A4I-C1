"""
BigQuery Tool Functions for Education Data
Uses real tables: ccd_directory, graduation_rates, district_finance, stem_*
"""
from typing import Dict, Any, List, Optional
from google.cloud import bigquery
from google.adk.tools import ToolContext
import google.auth
import subprocess
import os


def _get_bigquery_client(project_id: str = None):
    """
    Get BigQuery client with proper authentication.
    Falls back to multiple auth methods to ensure it works.
    If project_id is not provided, uses the default project from the environment.
    """
    try:
        # Try using gcloud access token (works with active gcloud auth)
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
            # Don't pass project_id - let it default to environment's project
            return bigquery.Client(credentials=creds)
        except:
            # Fallback to default credentials
            credentials, project = google.auth.default()
            # Don't pass project_id - let it default to environment's project
            return bigquery.Client(credentials=credentials)
    except Exception as e:
        # Last resort - try without explicit credentials or project
        return bigquery.Client()


def query_bigquery(
    sql_query: str, 
    tool_context: ToolContext
) -> Dict[str, Any]:
    """
    Execute a SQL query against BigQuery and return results.
    
    Args:
        sql_query: A valid BigQuery SQL query string
        tool_context: ADK tool context for state management
        
    Returns:
        Dictionary with 'status', 'data' (list of dicts), and 'row_count'
    """
    try:
        project_id = tool_context.state.get("project_id")
        if not project_id:
            return {
                "status": "error",
                "message": "Project ID not found in context",
                "data": []
            }
        
        # Get authenticated client
        client = _get_bigquery_client(project_id)
        query_job = client.query(sql_query)
        results = query_job.result()
        
        # Convert to list of dicts
        rows = [dict(row) for row in results]
        
        # Store in state
        tool_context.state["last_bq_query"] = sql_query
        tool_context.state["last_bq_row_count"] = len(rows)
        
        return {
            "status": "success",
            "data": rows,
            "row_count": len(rows),
            "query": sql_query
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"BigQuery error: {str(e)}",
            "data": [],
            "query": sql_query
        }


def get_school_data(
    state: Optional[str] = "CA",
    county: Optional[str] = None,
    school_level: Optional[int] = None,
    limit: int = 100,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Retrieve school information from CCD directory.
    
    Args:
        state: State location (default: 'CA')
        county: Optional county code filter
        school_level: Optional school level (1=Elementary, 2=Middle, 3=High, 4=Other)
        limit: Maximum number of schools to return
        tool_context: ADK tool context
        
    Returns:
        Dictionary with school data
    """
    try:
        project_id = tool_context.state.get("project_id")
        dataset = tool_context.state.get("bigquery_dataset", "education_data")
        
        query = f"""
        SELECT
            ncessch,
            school_name,
            leaid,
            lea_name,
            city_location,
            state_location,
            county_code,
            school_level,
            enrollment,
            teachers_fte,
            free_lunch,
            charter,
            latitude,
            longitude,
            ROUND(free_lunch / NULLIF(enrollment, 0) * 100, 1) as low_income_pct,
            ROUND(enrollment / NULLIF(teachers_fte, 0), 1) as student_teacher_ratio
        FROM `{dataset}.ccd_directory`
        WHERE state_location = '{state}'
          AND enrollment > 0
        """
        
        if county:
            query += f" AND county_code = '{county}'"
        if school_level:
            query += f" AND school_level = {school_level}"
            
        query += f" ORDER BY enrollment DESC LIMIT {limit}"
        
        return query_bigquery(query, tool_context)
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error retrieving school data: {str(e)}",
            "data": []
        }


def get_graduation_data(
    min_graduation_rate: Optional[float] = None,
    limit: int = 100,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Retrieve graduation rate data for California high schools.
    
    Args:
        min_graduation_rate: Optional minimum graduation rate filter
        limit: Maximum number of records to return
        tool_context: ADK tool context
        
    Returns:
        Dictionary with graduation data
    """
    try:
        project_id = tool_context.state.get("project_id")
        dataset = tool_context.state.get("bigquery_dataset", "education_data")
        
        query = f"""
        SELECT
            ncessch,
            school_name,
            leaid,
            lea_name,
            grad_rate_midpt,
            grad_rate_low,
            grad_rate_high,
            cohort_num
        FROM `{dataset}.graduation_rates`
        WHERE race = 99  -- Overall (not by subgroup)
          AND disability = 99
          AND econ_disadvantaged = 99
          AND lep = 99
          AND homeless = 99
          AND foster_care = 99
          AND grad_rate_midpt > 0  -- Exclude suppressed data
        """
        
        if min_graduation_rate:
            query += f" AND grad_rate_midpt >= {min_graduation_rate}"
            
        query += f" ORDER BY grad_rate_midpt DESC LIMIT {limit}"
        
        return query_bigquery(query, tool_context)
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error retrieving graduation data: {str(e)}",
            "data": []
        }


def get_district_finance(
    leaid: Optional[str] = None,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Retrieve district-level finance data.
    
    Args:
        leaid: Optional specific district ID to retrieve
        tool_context: ADK tool context
        
    Returns:
        Dictionary with finance data
    """
    try:
        project_id = tool_context.state.get("project_id")
        dataset = tool_context.state.get("bigquery_dataset", "education_data")
        
        query = f"""
        SELECT
            LEAID,
            NAME as district_name,
            MEMBERSCH as district_enrollment,
            per_pupil_total,
            per_pupil_instruction,
            per_pupil_support,
            TOTALEXP as total_expenditure,
            TCURINST as current_instruction_spending,
            TCURSSVC as support_services_spending
        FROM `{dataset}.district_finance`
        WHERE per_pupil_total > 0
        """
        
        if leaid:
            query += f" AND LEAID = '{leaid}'"
        else:
            query += " LIMIT 100"
            
        return query_bigquery(query, tool_context)
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error retrieving finance data: {str(e)}",
            "data": []
        }


def get_state_averages(
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Get California state averages for comparison.
    
    Returns:
        Dictionary with state-wide average metrics
    """
    try:
        project_id = tool_context.state.get("project_id")
        dataset = tool_context.state.get("bigquery_dataset", "education_data")
        
        query = f"""
        SELECT
            AVG(ROUND(c.free_lunch / NULLIF(c.enrollment, 0) * 100, 1)) as avg_low_income_pct,
            AVG(f.per_pupil_total) as avg_per_pupil_spending,
            AVG(g.grad_rate_midpt) as avg_graduation_rate,
            AVG(ROUND(c.enrollment / NULLIF(c.teachers_fte, 0), 1)) as avg_student_teacher_ratio
        FROM `{dataset}.ccd_directory` c
        LEFT JOIN `{dataset}.district_finance` f ON c.leaid = f.LEAID
        LEFT JOIN `{dataset}.graduation_rates` g ON c.ncessch = g.ncessch
            AND g.race = 99 AND g.disability = 99 AND g.econ_disadvantaged = 99
        WHERE c.enrollment >= 100
        """
        
        result = query_bigquery(query, tool_context)
        if result.get("status") == "success" and result.get("data"):
            return result["data"][0]
        return {}
    except Exception as e:
        print(f"Error getting state averages: {e}")
        return {}


def find_high_need_low_tech_spending(
    county: Optional[str] = None,
    limit: int = 5,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    RESEARCH QUESTION 1:
    Find schools with highest low-income rate and lowest per-pupil spending.
    
    This identifies schools that need priority grant funding.
    
    Args:
        county: Optional county code to filter by
        limit: Number of schools to return (default: 5)
        tool_context: ADK tool context
        
    Returns:
        Dictionary with prioritized schools for grant funding
    """
    try:
        project_id = tool_context.state.get("project_id")
        dataset = tool_context.state.get("bigquery_dataset", "education_data")
        
        # Get state averages for comparison
        state_avg = get_state_averages(tool_context)
        
        query = f"""
        SELECT
            c.school_name,
            c.lea_name,
            c.city_location,
            c.county_code,
            c.enrollment,
            c.free_lunch,
            c.latitude,
            c.longitude,
            ROUND(c.free_lunch / NULLIF(c.enrollment, 0) * 100, 1) as low_income_pct,
            ROUND(c.enrollment / NULLIF(c.teachers_fte, 0), 1) as student_teacher_ratio,
            COALESCE(f.per_pupil_total, 0) as per_pupil_total,
            COALESCE(f.per_pupil_instruction, 0) as per_pupil_instruction,
            -- Priority score: prioritize high low-income %
            (c.free_lunch / NULLIF(c.enrollment, 0) * 100) as priority_score
        FROM `{dataset}.ccd_directory` c
        LEFT JOIN `{dataset}.district_finance` f
          ON c.leaid = f.LEAID
        WHERE c.enrollment >= 100
          AND c.free_lunch > 0
          AND (c.free_lunch / NULLIF(c.enrollment, 0) * 100) >= 50
        """
        
        if county:
            query += f" AND c.county_code = '{county}'"
            
        query += f"""
        ORDER BY priority_score DESC
        LIMIT {limit}
        """
        
        result = query_bigquery(query, tool_context)
        
        if result.get("status") == "error" or result.get("row_count", 0) == 0:
            return {
                "status": "no_data",
                "message": "No schools found with complete finance and demographic data.",
                "data": [],
                "suggestion": "Try removing county filter or check if finance data exists for this area."
            }
        
        # Generate summary
        summary_lines = [f"**Found {result['row_count']} schools with high need:**\n"]
        for i, school in enumerate(result['data'][:5], 1):
            summary_lines.append(
                f"{i}. **{school['school_name']}** - {school['lea_name']}\n"
                f"   📍 {school['city_location']}, County: {school['county_code']}\n"
                f"   👥 Enrollment: {int(school['enrollment'])}\n"
                f"   💰 Low-Income: {school['low_income_pct']}%\n"
                f"   💵 Per-Pupil Spending: ${int(school['per_pupil_total'])}\n"
            )
        
        result['summary'] = '\n'.join(summary_lines)
        result['state_averages'] = state_avg
        return result
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error finding high-need schools: {str(e)}",
            "data": []
        }


def find_high_graduation_low_funding(
    min_graduation_rate: float = 75.0,  # Lowered from 85 to 75
    min_low_income_pct: float = 50.0,   # Changed from max to min, looking for high-need schools
    limit: int = 10,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    RESEARCH QUESTION 2:
    Find schools with high graduation rates despite high poverty (low funding proxy).
    
    These are efficient schools whose models can be replicated.
    
    Args:
        min_graduation_rate: Minimum graduation rate threshold (default: 85%)
        max_low_income_pct: Minimum low-income % to indicate high need (default: 70%)
        limit: Number of schools to return
        tool_context: ADK tool context
        
    Returns:
        Dictionary with high-performing, high-need schools
    """
    try:
        project_id = tool_context.state.get("project_id")
        dataset = tool_context.state.get("bigquery_dataset", "education_data")
        
        # Get state averages for comparison
        state_avg = get_state_averages(tool_context)
        
        query = f"""
        SELECT
            c.school_name,
            c.lea_name,
            c.city_location,
            c.enrollment,
            c.latitude,
            c.longitude,
            ROUND(c.free_lunch / NULLIF(c.enrollment, 0) * 100, 1) as low_income_pct,
            g.grad_rate_midpt as graduation_rate,
            g.cohort_num,
            ROUND(c.enrollment / NULLIF(c.teachers_fte, 0), 1) as student_teacher_ratio,
            c.charter,
            f.per_pupil_total,
            f.per_pupil_instruction
        FROM `{dataset}.ccd_directory` c
        INNER JOIN `{dataset}.graduation_rates` g
          ON c.ncessch = g.ncessch
        LEFT JOIN `{dataset}.district_finance` f
          ON c.leaid = f.LEAID
        WHERE g.grad_rate_midpt >= {min_graduation_rate}
          AND (c.free_lunch / NULLIF(c.enrollment, 0) * 100) >= {min_low_income_pct}
          AND c.enrollment >= 100
          AND g.race = 99
          AND g.disability = 99
          AND g.econ_disadvantaged = 99
          AND g.lep = 99
          AND g.homeless = 99
          AND g.foster_care = 99
        ORDER BY g.grad_rate_midpt DESC, low_income_pct DESC
        LIMIT {limit}
        """
        
        result = query_bigquery(query, tool_context)
        
        if result.get("status") == "error" or result.get("row_count", 0) == 0:
            return {
                "status": "no_data",
                "message": f"No schools found with graduation ≥{min_graduation_rate}% and low-income ≥{min_low_income_pct}%.",
                "data": [],
                "suggestion": "Try lowering thresholds or expanding search criteria."
            }
        
        # Generate summary
        summary_lines = [f"**Found {result['row_count']} high-performing high-need schools:**\n"]
        for i, school in enumerate(result['data'][:10], 1):
            summary_lines.append(
                f"{i}. **{school['school_name']}** - {school['lea_name']}\n"
                f"   📍 {school['city_location']}\n"
                f"   🎓 Graduation Rate: {school['graduation_rate']}%\n"
                f"   💰 Low-Income: {school['low_income_pct']}%\n"
                f"   👥 Enrollment: {int(school['enrollment'])}\n"
            )
        
        result['summary'] = '\n'.join(summary_lines)
        result['state_averages'] = state_avg
        return result
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error finding high-performing schools: {str(e)}",
            "data": []
        }


def find_strong_stem_low_class_size(
    max_student_teacher_ratio: int = 25,  # Increased from 20 to 25
    school_level: int = 3,  # High schools
    limit: int = 10,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    RESEARCH QUESTION 3:
    Find schools with strong STEM programs and low class sizes.
    
    Args:
        max_student_teacher_ratio: Maximum student-teacher ratio (default: 20)
        school_level: School level filter (3=High School)
        limit: Number of schools to return
        tool_context: ADK tool context
        
    Returns:
        Dictionary with STEM-strong schools with favorable class sizes
    """
    try:
        project_id = tool_context.state.get("project_id")
        dataset = tool_context.state.get("bigquery_dataset", "education_data")
        
        # Get state averages for comparison
        state_avg = get_state_averages(tool_context)
        
        # Query with STEM data joined (using AP courses as STEM indicator)
        query = f"""
        SELECT
            c.school_name,
            c.lea_name,
            c.city_location,
            c.enrollment,
            c.latitude,
            c.longitude,
            ROUND(c.enrollment / NULLIF(c.teachers_fte, 0), 1) as student_teacher_ratio,
            c.school_level,
            c.charter,
            ROUND(c.free_lunch / NULLIF(c.enrollment, 0) * 100, 1) as low_income_pct,
            ap.SCH_APCOURSES as ap_courses,
            COALESCE(ap.TOT_APENR_M, 0) + COALESCE(ap.TOT_APENR_F, 0) as total_ap_enrollment
        FROM `{dataset}.ccd_directory` c
        LEFT JOIN `{dataset}.stem_advanced_placement` ap
          ON CONCAT(c.leaid, c.school_id) = ap.COMBOKEY
        WHERE c.enrollment >= 100
          AND c.teachers_fte > 0
          AND (c.enrollment / c.teachers_fte) <= {max_student_teacher_ratio}
          AND c.school_level = {school_level}
          AND COALESCE(ap.SCH_APCOURSES, 0) >= 0  -- Show all schools, prefer those with AP
        ORDER BY COALESCE(ap.SCH_APCOURSES, 0) DESC, student_teacher_ratio ASC
        LIMIT {limit}
        """
        
        result = query_bigquery(query, tool_context)
        
        if result.get("status") == "error" or result.get("row_count", 0) == 0:
            return {
                "status": "no_data",
                "message": f"No schools found with student-teacher ratio ≤{max_student_teacher_ratio} and STEM programs.",
                "data": [],
                "suggestion": "Try increasing max_student_teacher_ratio or removing STEM requirement."
            }
        
        # Generate summary
        summary_lines = [f"**Found {result['row_count']} schools with STEM programs and favorable class sizes:**\n"]
        for i, school in enumerate(result['data'][:10], 1):
            ap_courses = school.get('ap_courses', 0) or 0
            summary_lines.append(
                f"{i}. **{school['school_name']}** - {school['lea_name']}\n"
                f"   📍 {school['city_location']}\n"
                f"   📚 Student-Teacher Ratio: {school['student_teacher_ratio']}\n"
                f"   🔬 AP Courses Offered: {ap_courses}\n"
                f"   👥 Enrollment: {int(school['enrollment'])}\n"
            )
        
        result['summary'] = '\n'.join(summary_lines)
        result['state_averages'] = state_avg
        return result
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error finding STEM schools: {str(e)}",
            "data": []
        }


def search_schools_with_stem(
    stem_course: str = "ap",  # Options: ap, calculus, physics, chemistry, biology
    min_enrollment: int = 10,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Search for schools offering specific STEM courses.
    
    Args:
        stem_course: Type of STEM course (ap, calculus, physics, chemistry, biology)
        min_enrollment: Minimum student enrollment in the course
        tool_context: ADK tool context
        
    Returns:
        Dictionary with schools offering the specified STEM course
    """
    try:
        project_id = tool_context.state.get("project_id")
        dataset = tool_context.state.get("bigquery_dataset", "education_data")
        
        # Map course name to table
        course_tables = {
            "ap": "stem_advanced_placement",
            "calculus": "stem_calculus",
            "physics": "stem_physics",
            "chemistry": "stem_chemistry",
            "biology": "stem_biology"
        }
        
        if stem_course not in course_tables:
            return {
                "status": "error",
                "message": f"Invalid STEM course. Choose from: {list(course_tables.keys())}",
                "data": []
            }
        
        table_name = course_tables[stem_course]
        
        # Different enrollment columns for different courses
        if stem_course == "ap":
            enrollment_col = "TOT_APENR_M + TOT_APENR_F"
        elif stem_course == "calculus":
            enrollment_col = "SCH_ENRL_CALC_M + SCH_ENRL_CALC_F"
        elif stem_course == "physics":
            enrollment_col = "SCH_ENR_PHYS_M + SCH_ENR_PHYS_F"
        elif stem_course == "chemistry":
            enrollment_col = "SCH_ENR_CHEM_M + SCH_ENR_CHEM_F"
        elif stem_course == "biology":
            enrollment_col = "SCH_ENR_BIO_M + SCH_ENR_BIO_F"
        
        query = f"""
        SELECT
            c.school_name,
            c.lea_name,
            c.city_location,
            c.enrollment as school_enrollment,
            c.latitude,
            c.longitude,
            ({enrollment_col}) as course_enrollment,
            ROUND(c.enrollment / NULLIF(c.teachers_fte, 0), 1) as student_teacher_ratio
        FROM `{dataset}.ccd_directory` c
        INNER JOIN `{dataset}.{table_name}` stem
          ON CONCAT(c.leaid, c.school_id) = stem.COMBOKEY
        WHERE ({enrollment_col}) >= {min_enrollment}
        ORDER BY course_enrollment DESC
        LIMIT 50
        """
        
        return query_bigquery(query, tool_context)
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error searching STEM courses: {str(e)}",
            "data": []
        }
