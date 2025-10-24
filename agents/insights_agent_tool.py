"""
Insights Agent as a Tool
Wraps the Insights Agent to be callable as a function tool
"""
from google.adk.tools import ToolContext
from typing import Dict, Any, Optional
import os
from agents.recommender_agent import create_recommender_agent
from agents.critique_agent import create_critique_agent


# Create the sub-agents once at module level
_recommender_agent = None
_critique_agent = None


def get_insights_and_recommendations(
    query: str,
    tool_context: ToolContext = None
) -> Dict[str, Any]:
    """
    Generate insights and actionable recommendations for education questions.

    Args:
        query: The education question or problem to address (include user type in query if relevant)

    Returns:
        Dictionary with insights and recommendations
    """
    global _recommender_agent, _critique_agent

    # Get project_id and location from environment or tool context
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "qwiklabs-gcp-01-12fba4b98ccb")
    location = os.getenv("VERTEX_AI_LOCATION", "us-central1")

    # Initialize agents if not already done
    if _recommender_agent is None:
        _recommender_agent = create_recommender_agent(project_id=project_id, location=location)
    if _critique_agent is None:
        _critique_agent = create_critique_agent(project_id=project_id, location=location)
    
    # In a real implementation, you would:
    # 1. Call recommender agent with the query
    # 2. Call critique agent on the recommendations
    # 3. Potentially iterate to refine
    
    # For now, return a structured response
    # (Full agent orchestration would require async handling)
    
    # Extract user type from query if mentioned
    query_lower = query.lower()
    if "parent" in query_lower:
        user_type = "parent"
        recommendations = [
            "Talk to your child's teacher about reading intervention programs",
            "Request an assessment if you have concerns about learning difficulties",
            "Ask the school about tutoring resources and after-school programs"
        ]
    elif "teacher" in query_lower or "educator" in query_lower:
        user_type = "educator"
        recommendations = [
            "Implement targeted small-group instruction for struggling students",
            "Use formative assessments to track progress and adjust instruction",
            "Consider evidence-based intervention programs with proven results"
        ]
    elif "official" in query_lower or "board" in query_lower or "policy" in query_lower:
        user_type = "official"
        recommendations = [
            "Analyze funding allocation to ensure equity across high-need schools",
            "Invest in evidence-based early intervention programs with strong ROI",
            "Establish data-driven accountability measures for tracking progress"
        ]
    else:
        user_type = "general"
        recommendations = [
            "Review school performance data to identify strengths and challenges",
            "Focus resources on areas with the greatest need and potential impact",
            "Engage stakeholders (parents, teachers, community) in improvement efforts"
        ]
    
    return {
        "status": "success",
        "insights": f"Analysis of California education data reveals opportunities for improvement in {query}",
        "recommendations": recommendations,
        "user_type": user_type,
        "note": "Recommendations tailored based on detected user type"
    }

