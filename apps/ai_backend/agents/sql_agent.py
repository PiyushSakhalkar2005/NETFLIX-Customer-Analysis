import re
import logging
from typing import Dict, Any, Tuple, List
from apps.ai_backend.config import settings

logger = logging.getLogger("sql_agent")

class SQLAgent:
    """Specialized Agent for generating accurate, read-only SQL queries targeting Gold star schema & Metadata views."""
    
    def generate_sql(self, query: str, context: Dict[str, Any] = None) -> str:
        """Translates natural language user query into PostgreSQL SQL query."""
        q_lower = query.lower().strip()
        
        # 1. Check for LLM API integration if configured
        if settings.OPENAI_API_KEY or settings.GEMINI_API_KEY:
            try:
                sql = self._generate_sql_via_llm(query, context)
                if sql:
                    return sql
            except Exception as e:
                logger.warning(f"LLM SQL generation failed or key unavailable, using rule-based parser: {str(e)}")
                
        # 2. Rule-Based Natural Language Intent Engine for standard data queries
        return self._generate_sql_via_intent(q_lower, context)

    def _generate_sql_via_intent(self, q: str, context: Dict[str, Any] = None) -> str:
        # Context filters
        country_filter = ""
        type_filter = ""
        if context:
            if context.get("country"):
                country_filter = f" AND c.country = '{context['country']}'"
            if context.get("type"):
                type_filter = f" AND t.type = '{context['type']}'"

        # Pattern 1: General Summary / Total Counts / Movies vs TV Shows
        if any(w in q for w in ["total content", "how many", "summary", "overview", "ratio", "movies vs tv"]):
            return "SELECT * FROM metadata.v_kpi_general_summary;"
            
        # Pattern 2: Country Distribution / Top Countries
        elif any(w in q for w in ["country", "countries", "location", "geography"]):
            limit = 10
            match = re.search(r'top\s+(\d+)', q)
            if match:
                limit = int(match.group(1))
            return f"""
                SELECT country_name, titles_count, percentage_share 
                FROM metadata.v_kpi_country_distribution 
                ORDER BY titles_count DESC 
                LIMIT {limit};
            """.strip()

        # Pattern 3: Genre / Categories
        elif any(w in q for w in ["genre", "genres", "category", "categories", "listed_in"]):
            limit = 10
            match = re.search(r'top\s+(\d+)', q)
            if match:
                limit = int(match.group(1))
            return f"""
                SELECT genre_name, titles_count 
                FROM metadata.v_kpi_genre_distribution 
                ORDER BY titles_count DESC 
                LIMIT {limit};
            """.strip()

        # Pattern 4: Top Directors
        elif any(w in q for w in ["director", "directors", "filmmaker"]):
            limit = 10
            match = re.search(r'top\s+(\d+)', q)
            if match:
                limit = int(match.group(1))
            return f"""
                SELECT director_name, titles_count 
                FROM metadata.v_kpi_top_directors 
                ORDER BY titles_count DESC 
                LIMIT {limit};
            """.strip()

        # Pattern 5: Ratings Breakdown / Age Maturity
        elif any(w in q for w in ["rating", "ratings", "maturity", "tv-ma", "pg-13"]):
            return """
                SELECT rating_name, titles_count 
                FROM metadata.v_kpi_rating_distribution 
                ORDER BY titles_count DESC;
            """.strip()

        # Pattern 6: Release Trends by Year
        elif any(w in q for w in ["trend", "year", "annual", "release", "history", "timeline"]):
            return """
                SELECT release_year, type_name, releases_count 
                FROM metadata.v_kpi_release_trends 
                WHERE release_year >= 2000
                ORDER BY release_year ASC;
            """.strip()

        # Pattern 7: Specific Title Search or Detail Query
        elif "title" in q or "movie" in q or "show" in q:
            return """
                SELECT dt.title, dt.show_id, dt.description, dt.content_category, fc.release_year, fc.duration_minutes
                FROM gold.fact_content fc
                JOIN gold.dim_title dt ON fc.title_key = dt.title_key
                ORDER BY fc.release_year DESC
                LIMIT 15;
            """.strip()

        # Default Fallback Query
        return "SELECT * FROM metadata.v_kpi_general_summary;"

    def _generate_sql_via_llm(self, query: str, context: Dict[str, Any] = None) -> str:
        # Placeholder for LLM provider API call if keys are supplied
        return None
