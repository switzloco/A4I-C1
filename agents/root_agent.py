"""
Root Agent - ADK Implementation
Top-level orchestrator using agent tools (function wrappers)
"""
import os
from google.adk.agents import LlmAgent
from agents.data_agent_tool import query_education_data
from agents.insights_agent_tool import get_insights_and_recommendations
from agents.config import ROOT_AGENT_PROMPT


def create_root_agent(project_id: str, dataset: str = "education_data", location: str = "us-central1") -> LlmAgent:
    """
    Create the Root Agent using ADK LlmAgent.

    The Root Agent uses function tools to access data and insights capabilities.
    This pattern allows agent orchestration within ADK's validation requirements.

    Args:
        project_id: Google Cloud project ID
        dataset: BigQuery dataset name
        location: Vertex AI location (default: us-central1)

    Returns:
        ADK LlmAgent configured as Root Agent
    """

    # Use persona-aware prompt from config and add tool descriptions
    instruction = ROOT_AGENT_PROMPT + f"""

**YOUR TOOLS:**

1. **query_education_data(query_type, state, district, school_name, limit)**
   - Retrieve data from BigQuery
   - query_type: "schools", "test_scores", or "demographics"
   - Use this to get the data you need for analysis

2. **get_insights_and_recommendations(query, user_type)**
   - Generate analysis and recommendations
   - Orchestrates recommend-critique-revise loop
   - Returns validated, high-quality recommendations

**WORKFLOW:**
1. Identify user type (parent/educator/official)
2. Use query_education_data() to retrieve relevant data
3. Use get_insights_and_recommendations() to analyze and recommend
4. Present results in persona-appropriate language

**Current Configuration:**
- Project: {project_id}
- Dataset: {dataset}
"""

    # Use different model names for API vs Vertex AI
    model_name = "gemini-2.0-flash" if os.getenv("GOOGLE_API_KEY") else "gemini-1.5-pro"

    # Create agent with function tools and Vertex AI configuration
    agent = LlmAgent(
        model=model_name,
        name="RootAgent",
        instruction=instruction,
        tools=[
            query_education_data,
            get_insights_and_recommendations
        ],
        vertexai=True,
        project=project_id,
        location=location
    )

    return agent
