"""
Data Agent - ADK Implementation
Handles all BigQuery data retrieval using ADK LlmAgent
"""
from google.adk.agents import LlmAgent
from google.adk.tools import FunctionTool
from tools.bigquery_tools import (
    query_bigquery,
    query_school_analytics,
    get_school_data,
    get_test_scores,
    get_demographics,
    find_high_need_low_tech_spending,
    find_high_graduation_low_funding,
    find_strong_stem_low_class_size,
    search_education_data
)


def create_data_agent(project_id: str, dataset: str = "education_data", location: str = "us-central1") -> LlmAgent:
    """
    Create the Data Agent using ADK LlmAgent.

    The Data Agent is responsible for retrieving education data from BigQuery.

    Args:
        project_id: Google Cloud project ID
        dataset: BigQuery dataset name
        location: Vertex AI location (default: us-central1)

    Returns:
        ADK LlmAgent configured as Data Agent
    """
    
    # Define the agent's instruction
    instruction = f"""You are the Data Sub-Agent for the Education Insights & Resource Recommender system.

Your PRIMARY responsibility is to retrieve education data from BigQuery.

PRIMARY DATA SOURCE:
- school_analytics_view: Consolidated view with ALL school data (demographics, test scores, funding, performance)
- This is your main data source - it has everything in one place!

AVAILABLE TOOLS:
1. query_school_analytics - Query the consolidated view (PREFERRED - use this first!)
2. query_bigquery - Execute any custom SQL query against BigQuery
3. get_school_data - Get school information (filtered by state/district)
4. get_test_scores - Get test performance data (filtered by state/subject/year)
5. get_demographics - Get student demographic data

SPECIALIZED QUERY TOOLS (for common priority questions):
6. find_high_need_low_tech_spending - Find schools with high low-income % and low tech spending
7. find_high_graduation_low_funding - Find efficient schools with high grad rates despite low funding
8. find_strong_stem_low_class_size - Find schools with strong STEM and favorable class sizes
9. search_education_data - Fallback when BigQuery data is not available

GUIDELINES:
- When asked for data, translate the request into appropriate tool calls
- Always use specialized tools (#5-7) for those specific questions - they handle missing data gracefully
- Use get_school_data, get_test_scores, or get_demographics when possible (they're optimized)
- Only use query_bigquery for complex custom queries
- Return structured data in a clear format
- If a query returns no results, explain why and suggest alternatives
- If data is missing from BigQuery, the specialized tools will return helpful guidance
- Include row counts in your responses

HANDLING MISSING DATA:
- Specialized tools (#5-7) automatically check if data exists
- If data is missing, they return status="partial" with recommendations
- When you see status="partial", explain to the Root Agent what data is missing
- Suggest that the user provide the data or ask if they want general guidance instead
- Be helpful and proactive - offer alternatives when exact data isn't available

CURRENT CONFIGURATION:
- Project: {project_id}
- Dataset: {dataset}

EXAMPLES:
User: "Show me schools in California"
You: Use get_school_data(state="CA")

User: "Find five schools with highest low-income students and lowest tech spending"
You: Use find_high_need_low_tech_spending(limit=5)

User: "Schools with high graduation despite low funding"
You: Use find_high_graduation_low_funding()

User: "Strong STEM programs with small classes"
You: Use find_strong_stem_low_class_size()

User: "Compare proficiency rates across districts"
You: Use query_bigquery with appropriate GROUP BY query

Always respond with clear, actionable data summaries."""

    # Create function tools
    tools = [
        FunctionTool(func=query_school_analytics),  # PRIMARY TOOL - consolidated view
        FunctionTool(func=query_bigquery),
        FunctionTool(func=get_school_data),
        FunctionTool(func=get_test_scores),
        FunctionTool(func=get_demographics),
        # Specialized tools for priority questions
        FunctionTool(func=find_high_need_low_tech_spending),
        FunctionTool(func=find_high_graduation_low_funding),
        FunctionTool(func=find_strong_stem_low_class_size),
        FunctionTool(func=search_education_data)
    ]
    
    # Create the agent with Vertex AI configuration
    agent = LlmAgent(
        name="DataAgent",
        model="gemini-2.0-flash-exp",
        instruction=instruction,
        tools=tools,
        vertexai=True,
        project=project_id,
        location=location
    )

    return agent

