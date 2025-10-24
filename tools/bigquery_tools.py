"""
ADK-Compatible BigQuery Tool Functions
Uses ToolContext for state management and authentication

Primary data source: school_analytics_view (consolidated view with all school data)
"""
from typing import Dict, Any, List, Optional
from google.cloud import bigquery
from google.adk.tools import ToolContext
import pandas as pd


def query_bigquery(
    sql_query: str, 
    tool_context: ToolContext
) -> Dict[str, Any]:
    """
    Execute a SQL query against BigQuery and return results.
    
    Use this tool to retrieve education data from BigQuery datasets.
    The query should be a valid BigQuery SQL statement.
    
    Args:
        sql_query: A valid BigQuery SQL query string
        
    Returns:
        Dictionary with 'status', 'data' (list of dicts), and 'row_count'
        
    Examples:
        - "SELECT * FROM schools WHERE state = 'CA' LIMIT 10"
        - "SELECT district, AVG(score) as avg_score FROM test_scores GROUP BY district"
    """
    try:
        # Get project info from context state
        project_id = tool_context.state.get("project_id")
        if not project_id:
            return {
                "status": "error",
                "message": "Project ID not found in context",
                "data": []
            }
        
        # Initialize BigQuery client
        client = bigquery.Client(project=project_id)
        
        # Execute query
        query_job = client.query(sql_query)
        results = query_job.result()
        
        # Convert to list of dicts
        rows = [dict(row) for row in results]
        
        # Store last query in state
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


def query_school_analytics(
    filters: Optional[str] = None,
    order_by: Optional[str] = None,
    limit: int = 100,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Query the consolidated school_analytics_view with all school data.

    This view contains all the data you need: school info, demographics, test scores,
    funding, and performance metrics in one place.

    Args:
        filters: Optional WHERE clause filters (e.g., "State = 'CA' AND enrollment > 500")
        order_by: Optional ORDER BY clause (e.g., "proficiency_rate DESC")
        limit: Maximum number of records to return (default: 100)

    Returns:
        Dictionary with comprehensive school data from the consolidated view

    Example:
        query_school_analytics(
            filters="State = 'CA' AND low_income_pct > 50",
            order_by="proficiency_rate ASC",
            limit=10
        )
    """
    try:
        project_id = tool_context.state.get("project_id")
        dataset = tool_context.state.get("bigquery_dataset", "school_data")

        # Build query using the consolidated view
        query = f"""
        SELECT *
        FROM `{project_id}.{dataset}.school_analytics_view`
        """

        if filters:
            query += f" WHERE {filters}"

        if order_by:
            query += f" ORDER BY {order_by}"

        query += f" LIMIT {limit}"

        # Use the query_bigquery function
        return query_bigquery(query, tool_context)

    except Exception as e:
        return {
            "status": "error",
            "message": f"Error querying school analytics: {str(e)}",
            "data": []
        }


def get_school_data(
    state: Optional[str] = None,
    district: Optional[str] = None,
    limit: int = 100,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Retrieve school information from the consolidated analytics view.

    Use this tool to get basic school data. You can filter by state and/or district.

    Args:
        state: Optional 2-letter state code (e.g., 'CA', 'NY')
        district: Optional district name
        limit: Maximum number of schools to return (default: 100)

    Returns:
        Dictionary with school data including name, location, enrollment, etc.
    """
    try:
        # Build filters
        filters = []
        if state:
            filters.append(f"State = '{state}'")
        if district:
            filters.append(f"District_Name = '{district}'")

        filter_string = " AND ".join(filters) if filters else None

        # Use the consolidated view
        return query_school_analytics(
            filters=filter_string,
            limit=limit,
            tool_context=tool_context
        )

    except Exception as e:
        return {
            "status": "error",
            "message": f"Error retrieving school data: {str(e)}",
            "data": []
        }


def get_test_scores(
    state: Optional[str] = None,
    subject: Optional[str] = None,
    year: Optional[int] = None,
    limit: int = 100,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Retrieve standardized test score data.
    
    Use this tool to get test performance metrics. Filter by state, subject, or year.
    
    Args:
        state: Optional 2-letter state code
        subject: Optional subject name (e.g., 'Math', 'Reading')
        year: Optional year (e.g., 2023)
        limit: Maximum number of records to return
        
    Returns:
        Dictionary with test score data including school, scores, and demographics
    """
    try:
        project_id = tool_context.state.get("project_id")
        dataset = tool_context.state.get("bigquery_dataset", "education_data")
        
        query = f"""
        SELECT 
            School_Code as school_code,
            Test_Year as year,
            Grade as grade,
            Test_ID as subject,
            Mean_Scale_Score as avg_score,
            Percentage_Standard_Met_and_Above as proficiency_rate,
            Total_Tested_with_Scores_at_Reporting_Level as num_students
        FROM `{project_id}.{dataset}.CAASP_Test_Scores`
        WHERE 1=1
        """
        
        if state:
            query += f" AND state = '{state}'"
        if subject:
            query += f" AND subject = '{subject}'"
        if year:
            query += f" AND year = {year}"
            
        query += f" LIMIT {limit}"
        
        return query_bigquery(query, tool_context)
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error retrieving test scores: {str(e)}",
            "data": []
        }


def get_demographics(
    state: Optional[str] = None,
    limit: int = 100,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Retrieve demographic information for schools.
    
    Use this tool to get data on student demographics including ethnicity,
    economic status, and special programs.
    
    Args:
        state: Optional 2-letter state code
        limit: Maximum number of records to return
        
    Returns:
        Dictionary with demographic data
    """
    try:
        project_id = tool_context.state.get("project_id")
        dataset = tool_context.state.get("bigquery_dataset", "education_data")
        
        query = f"""
        SELECT 
            School_Name as school_name,
            District_Name as district,
            State as state,
            Enroll_Total as total_students,
            Socioeconomically_Disadvantaged_Pct as percent_disadvantaged,
            English_Learner_Pct as percent_ell,
            Students_with_Disabilities_Pct as percent_special_ed,
            Free_Reduced_Meal_Eligible_Pct as percent_free_reduced
        FROM `{project_id}.{dataset}.schooltable`
        WHERE 1=1
        """
        
        if state:
            query += f" AND state = '{state}'"
            
        query += f" LIMIT {limit}"
        
        return query_bigquery(query, tool_context)
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error retrieving demographics: {str(e)}",
            "data": []
        }


def find_high_need_low_tech_spending(
    county: Optional[str] = None,
    state: Optional[str] = None,
    limit: int = 5,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Find schools with highest rate of low-income students and lowest per-pupil technology spending.
    
    This is useful for identifying schools that need priority grant funding for technology.
    
    Args:
        county: Optional county name to filter by
        state: Optional state code to filter by
        limit: Number of schools to return (default: 5)
        
    Returns:
        Dictionary with schools ranked by need + tech spending gap
    """
    try:
        project_id = tool_context.state.get("project_id")
        dataset = tool_context.state.get("bigquery_dataset", "education_data")
        
        # Try comprehensive table first (has funding data)
        query = f"""
        SELECT 
            school_name,
            district,
            state,
            county,
            low_income_pct,
            tech_spending_per_pupil,
            total_enrollment,
            (low_income_pct * 100 - tech_spending_per_pupil) as need_score
        FROM `{project_id}.{dataset}.synthetic_school_demo_fund_perf`
        WHERE low_income_pct IS NOT NULL 
          AND tech_spending_per_pupil IS NOT NULL
        """
        
        if county:
            query += f" AND LOWER(county) = LOWER('{county}')"
        if state:
            query += f" AND state = '{state}'"
            
        query += f"""
        ORDER BY low_income_pct DESC, tech_spending_per_pupil ASC
        LIMIT {limit}
        """
        
        result = query_bigquery(query, tool_context)
        
        # If no data found, return helpful message
        if result["status"] == "error" or result["row_count"] == 0:
            return {
                "status": "partial",
                "message": "Technology spending data not available in BigQuery. Using demographic data only.",
                "recommendation": "I can show you schools with high low-income populations. For technology spending data, please provide it or I can search for external sources.",
                "data": [],
                "fallback_query": "high poverty schools technology funding gap"
            }
        
        return result
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error finding high-need low-tech schools: {str(e)}",
            "data": [],
            "fallback_query": "schools high poverty low technology spending"
        }


def find_high_graduation_low_funding(
    state: Optional[str] = None,
    min_graduation_rate: float = 85.0,
    limit: int = 10,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Find schools with high graduation rates despite below-average funding.
    
    These schools are doing more with less - their program models can be replicated.
    
    Args:
        state: Optional state code to filter by
        min_graduation_rate: Minimum graduation rate threshold (default: 85%)
        limit: Number of schools to return
        
    Returns:
        Dictionary with high-performing, efficiently-run schools
    """
    try:
        project_id = tool_context.state.get("project_id")
        dataset = tool_context.state.get("bigquery_dataset", "education_data")
        
        # Try comprehensive table (has both funding and performance)
        query = f"""
        WITH avg_funding AS (
            SELECT AVG(per_pupil_spending) as avg_spending
            FROM `{project_id}.{dataset}.synthetic_school_demo_fund_perf`
            WHERE per_pupil_spending IS NOT NULL
        )
        SELECT 
            s.school_name,
            s.district,
            s.state,
            s.graduation_rate,
            s.per_pupil_spending,
            a.avg_spending,
            (s.per_pupil_spending / a.avg_spending) as funding_ratio,
            s.total_enrollment
        FROM `{project_id}.{dataset}.synthetic_school_demo_fund_perf` s
        CROSS JOIN avg_funding a
        WHERE s.graduation_rate >= {min_graduation_rate}
          AND s.per_pupil_spending < a.avg_spending
          AND s.graduation_rate IS NOT NULL
          AND s.per_pupil_spending IS NOT NULL
        """
        
        if state:
            query += f" AND s.state = '{state}'"
            
        query += f"""
        ORDER BY s.graduation_rate DESC, s.per_pupil_spending ASC
        LIMIT {limit}
        """
        
        result = query_bigquery(query, tool_context)
        
        # If no data, provide fallback
        if result["status"] == "error" or result["row_count"] == 0:
            return {
                "status": "partial",
                "message": "Graduation rate and funding data not available in BigQuery.",
                "recommendation": "Please provide graduation rates and per-pupil funding data, or I can search for schools with efficient program models.",
                "data": [],
                "fallback_query": "high graduation rate schools low funding best practices"
            }
        
        return result
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error finding efficient high-performing schools: {str(e)}",
            "data": [],
            "fallback_query": "schools high graduation low funding"
        }


def find_strong_stem_low_class_size(
    state: Optional[str] = None,
    max_class_size: int = 20,
    limit: int = 10,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Find schools with strong STEM programs and low class sizes.
    
    Useful for identifying effective STEM education models.
    
    Args:
        state: Optional state code to filter by
        max_class_size: Maximum average class size (default: 20)
        limit: Number of schools to return
        
    Returns:
        Dictionary with STEM-strong schools with favorable class sizes
    """
    try:
        project_id = tool_context.state.get("project_id")
        dataset = tool_context.state.get("bigquery_dataset", "education_data")
        
        # Try comprehensive table
        query = f"""
        SELECT 
            school_name,
            district,
            state,
            stem_program_score,
            avg_class_size,
            student_teacher_ratio,
            total_enrollment,
            stem_courses_offered
        FROM `{project_id}.{dataset}.synthetic_school_demo_fund_perf`
        WHERE avg_class_size <= {max_class_size}
          AND stem_program_score IS NOT NULL
          AND avg_class_size IS NOT NULL
        """
        
        if state:
            query += f" AND state = '{state}'"
            
        query += f"""
        ORDER BY stem_program_score DESC, avg_class_size ASC
        LIMIT {limit}
        """
        
        result = query_bigquery(query, tool_context)
        
        # If no data, provide fallback
        if result["status"] == "error" or result["row_count"] == 0:
            return {
                "status": "partial",
                "message": "STEM program and class size data not available in BigQuery.",
                "recommendation": "Please provide information about STEM programs and class sizes, or I can search for schools with strong STEM reputations.",
                "data": [],
                "fallback_query": "schools strong STEM programs small class sizes"
            }
        
        return result
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error finding STEM schools with low class sizes: {str(e)}",
            "data": [],
            "fallback_query": "schools STEM programs low student teacher ratio"
        }


def search_education_data(
    query: str,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Search for education data when BigQuery doesn't have the needed information.
    
    This tool provides general guidance and asks the user for specific data.
    
    Args:
        query: Description of what data is needed
        
    Returns:
        Dictionary with guidance on how to provide the data
    """
    return {
        "status": "needs_input",
        "message": f"I don't have data for: {query}",
        "request_to_user": f"To help you with '{query}', I need additional information. Could you provide:\n"
                          f"- Specific school names or districts you're interested in\n"
                          f"- Any data you have about {query}\n"
                          f"- Or I can search online sources for general trends and best practices in this area.\n\n"
                          f"Would you like me to provide general guidance based on education research instead?",
        "general_guidance": True
    }

