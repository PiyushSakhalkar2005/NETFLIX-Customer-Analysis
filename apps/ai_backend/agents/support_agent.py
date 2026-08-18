import logging
from typing import Dict, Any

logger = logging.getLogger("support_agent")

class SupportAgent:
    """Specialized Agent for answering user assistance, domain definition, and architecture questions without running SQL queries."""

    def __init__(self):
        self.name = "Support Agent"

    def answer(self, query: str) -> str:
        q_lower = query.lower().strip()

        # Question 1: What does Movie Ratio mean?
        if "movie ratio" in q_lower:
            return (
                "### Understanding Movie Ratio 🎥\n\n"
                "The **Movie Ratio** represents the percentage share of total catalog titles classified strictly as feature films vs. episodic TV series.\n\n"
                "- **Formula:** `(Total Movies / Total Titles) * 100`\n"
                "- **Current Gold DW Benchmark:** `69.62%` (6,131 Movies out of 8,807 total catalog titles).\n"
                "- **Business Insight:** Indicates that Netflix historically prioritized film acquisitions, maintaining roughly a **7:3 ratio** of Movies to TV Shows."
            )

        # Question 2: What can Netflix AI do? / How do I use the dashboard?
        elif any(w in q_lower for w in ["what can", "how do i use", "help", "capabilities"]):
            return (
                "### What Netflix AI Can Do 🚀\n\n"
                "I am an **Agentic AI Analytics Assistant** connected directly to your PostgreSQL Medallion Data Warehouse (`netflix_dw_new`). Here is how you can use me:\n\n"
                "1. **Natural Language Data Queries:** Ask questions like *'What are the top 5 countries by content?'* or *'Show top 10 genres'*\n"
                "2. **Machine Learning & Forecasting:** Ask *'Predict content growth'* or *'Find unusual country patterns'*\n"
                "3. **Root Cause Insights:** Ask *'Why is the US dominant?'*\n"
                "4. **Interactive Visualizations:** I automatically generate Bar, Line, and Donut charts based on real warehouse data.\n"
                "5. **Domain Definitions:** Ask about metric formulas like *'Movie Ratio'* or *'Gold Layer'* architecture."
            )

        # Question 3: How does the AI work? / Multi-agent framework
        elif any(w in q_lower for w in ["how does the ai work", "how it works", "architecture"]):
            return (
                "### Multi-Agent Architecture Overview 🤖\n\n"
                "I operate using a **6-Agent Orchestrated Framework**:\n\n"
                "- **Orchestrator Agent:** Classifies user intent and routes tasks to specialized agents.\n"
                "- **Data Agent:** Writes and validates read-only SQL queries against the Gold star schema.\n"
                "- **ML Agent:** Performs linear regression forecasting, Isolation Forest anomaly detection, and K-Means clustering.\n"
                "- **Insight Agent:** Synthesizes raw data into business findings and trend analysis.\n"
                "- **Visualization Agent:** Selects and structures chart specifications (Bar, Line, Donut, Treemap).\n"
                "- **Support Agent:** Answers domain definitions and user guidance without running SQL queries."
            )

        # Question 4: Explain the Gold layer / Data sources
        elif any(w in q_lower for w in ["gold layer", "data source", "what data"]):
            return (
                "### Data Warehouse Architecture & Gold Layer 🏛️\n\n"
                "The Netflix Data Engineering pipeline uses a production **Medallion Architecture**:\n\n"
                "- **Bronze Layer:** Raw JSON, CSV, XML, and REST API landed records.\n"
                "- **Silver Layer:** Standardized lookup tables (`silver_titles`, `silver_directors`, `silver_country`) with SCD Type 1 & Type 2 version history.\n"
                "- **Gold Layer (`gold.*`):** Kimball Star Schema consisting of `fact_content` (25,879 fact links) and dimension tables (`dim_title`, `dim_director`, `dim_country`, `dim_genre`, `dim_rating`, `dim_type`).\n"
                "- **Metadata Layer (`metadata.*`):** Pre-aggregated SQL views (`v_kpi_*`) serving Power BI and AI requests."
            )

        # Generic support fallback
        return (
            "### Netflix AI Support\n\n"
            "I am ready to assist! You can ask me analytical questions about catalog trends, request ML predictions, or ask for explanations of metrics like Movie Ratio and Gold Schema."
        )
