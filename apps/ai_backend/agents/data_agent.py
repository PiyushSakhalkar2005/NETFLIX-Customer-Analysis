import re
import logging
from typing import Dict, Any, Tuple, List
from apps.ai_backend.config import settings
from apps.ai_backend.tools.sql_tool import run_sql_query

logger = logging.getLogger("data_agent")

class DataAgent:
    """Specialized Agent for generating and executing read-only SQL queries against Gold star schema and Metadata views."""

    def __init__(self):
        self.name = "Data Agent"

    def execute(self, query: str, context: Dict[str, Any] = None) -> Tuple[List[Dict[str, Any]], str]:
        """Generates SQL query and executes it safely via SQL tool."""
        upper_raw = query.upper()
        forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "CREATE", "GRANT", "REVOKE"]
        for word in forbidden:
            if f"{word} " in upper_raw or f" {word}" in upper_raw or upper_raw.endswith(f" {word};"):
                from apps.ai_backend.tools.sql_tool import SQLSecurityException
                raise SQLSecurityException(f"Security Enforcement: Forbidden SQL keyword '{word}' detected.")

        sql = self.generate_sql(query, context)
        rows, validated_sql = run_sql_query(sql)
        return rows, validated_sql

    def generate_sql(self, query: str, context: Dict[str, Any] = None) -> str:
        q_lower = query.lower().strip()

        # Extract context filters if passed from Power BI deep link
        country_filter = ""
        year_filter = ""
        type_filter = ""
        if context:
            if context.get("country"):
                country_filter = f" AND c.country = '{context['country']}'"
            if context.get("year"):
                year_filter = f" AND f.release_year = {int(context['year'])}"
            if context.get("type"):
                t_val = "Movie" if "movie" in str(context['type']).lower() else "TV Show"
                type_filter = f" AND t.type = '{t_val}'"

        # 1. Metric-Specific: Longest / Shortest / Average Movie Duration & TV Seasons
        if any(w in q_lower for w in ["longest movie", "which movie is the longest", "longest duration", "longest film", "longest feature"]):
            return """
                SELECT title, duration, release_year
                FROM silver.silver_titles
                WHERE type = 'Movie' AND duration LIKE '%min%'
                ORDER BY CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) DESC
                LIMIT 1;
            """.strip()

        elif any(w in q_lower for w in ["shortest movie", "which movie is the shortest", "shortest film", "shortest feature"]):
            return """
                SELECT title, duration, release_year
                FROM silver.silver_titles
                WHERE type = 'Movie' AND duration LIKE '%min%' AND CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) > 0
                ORDER BY CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) ASC
                LIMIT 1;
            """.strip()

        elif any(w in q_lower for w in ["minimum seasons", "lowest number of seasons"]):
            return """
                SELECT 
                    COUNT(*) AS total_tv_shows,
                    MIN(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)) AS min_seasons
                FROM silver.silver_titles
                WHERE type = 'TV Show' AND (duration LIKE '%Season%' OR duration LIKE '%Seasons%');
            """.strip()

        elif any(w in q_lower for w in ["maximum seasons", "most seasons", "highest season count", "longest running tv show"]):
            return """
                SELECT title, duration, release_year
                FROM silver.silver_titles
                WHERE type = 'TV Show' AND (duration LIKE '%Season%' OR duration LIKE '%Seasons%')
                ORDER BY CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) DESC
                LIMIT 1;
            """.strip()

        elif any(w in q_lower for w in [
            "average movie duration", "movie duration", "duration of movies", "movie length", "movie runtime", 
            "typical movie", "how long is a typical movie", "how long are movies", "how many minutes", "movie length average",
            "length of movies", "movie runtime average", "film", "films", "feature film", "duration of a netflix movie", "average length",
            "time does a movie take", "how much time"
        ]):
            return """
                SELECT 
                    COUNT(*) AS total_movies,
                    ROUND(AVG(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)), 1) AS average_movie_duration_minutes
                FROM silver.silver_titles
                WHERE type = 'Movie' AND duration LIKE '%min%';
            """.strip()

        elif any(w in q_lower for w in [
            "average tv show seasons", "average seasons", "number of seasons", "tv seasons", "season count",
            "tv show season count", "tv series average duration", "how long are tv series", "seasons does a show",
            "season average", "average length of tv shows", "how many seasons", "tv duration in seasons",
            "series length in seasons", "season distribution average"
        ]):
            return """
                SELECT 
                    COUNT(*) AS total_tv_shows,
                    ROUND(AVG(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)), 2) AS average_tv_seasons
                FROM silver.silver_titles
                WHERE type = 'TV Show' AND (duration LIKE '%Season%' OR duration LIKE '%Seasons%');
            """.strip()

        # 2. Country Distribution / Ranking
        elif any(w in q_lower for w in ["country", "countries", "geography", "us dominant", "united states", "nation", "produces", "dominates", "titles come from", "production nation", "from the us", "from us", "in the us"]):
            limit = 10
            match = re.search(r'top\s+(\d+)', q_lower)
            if match:
                limit = int(match.group(1))
            return f"""
                SELECT country_name, titles_count, percentage_share 
                FROM metadata.v_kpi_country_distribution 
                ORDER BY titles_count DESC 
                LIMIT {limit};
            """.strip()

        # 3. Total Content / Movie vs TV Summary & Content Age
        elif any(w in q_lower for w in ["how many", "total content", "total movies", "total tv", "movie ratio", "summary", "overview", "portfolio", "release age"]):
            if context and (context.get("country") or context.get("year")):
                return f"""
                    SELECT 
                        COUNT(DISTINCT f.title_key) AS total_titles,
                        COUNT(DISTINCT CASE WHEN t.type = 'Movie' THEN f.title_key END) AS total_movies,
                        COUNT(DISTINCT CASE WHEN t.type = 'TV Show' THEN f.title_key END) AS total_tv_shows,
                        ROUND(AVG(f.content_age), 1) AS average_content_age
                    FROM gold.fact_content f
                    JOIN gold.dim_type t ON f.type_key = t.type_key
                    JOIN gold.dim_country c ON f.country_key = c.country_key
                    WHERE 1=1 {country_filter} {year_filter};
                """.strip()
            return "SELECT * FROM metadata.v_kpi_general_summary;"


        # 3. Anomaly / Multi-dimensional Analysis for Countries (Fact Content Aggregation)
        elif any(w in q_lower for w in ["unusual", "anomaly", "anomalies", "outlier", "outliers", "segmentation", "cluster", "pattern"]):
            return f"""
                SELECT 
                    c.country AS country_name,
                    COUNT(DISTINCT f.title_key) AS titles_count,
                    COUNT(DISTINCT CASE WHEN t.type = 'Movie' THEN f.title_key END) AS movie_count,
                    COUNT(DISTINCT CASE WHEN t.type = 'TV Show' THEN f.title_key END) AS tv_count,
                    ROUND(AVG(f.duration_minutes), 1) AS avg_movie_duration,
                    ROUND(AVG(f.content_age), 1) AS avg_content_age
                FROM gold.fact_content f
                JOIN gold.dim_country c ON f.country_key = c.country_key
                JOIN gold.dim_type t ON f.type_key = t.type_key
                WHERE c.country <> 'Unknown Country' {type_filter} {year_filter}
                GROUP BY c.country
                HAVING COUNT(DISTINCT f.title_key) >= 10
                ORDER BY titles_count DESC;
            """.strip()

        # 4. Genre Analysis
        elif any(w in q_lower for w in ["genre", "genres", "category", "categories"]):
            limit = 10
            match = re.search(r'top\s+(\d+)', q_lower)
            if match:
                limit = int(match.group(1))

            # Extract country from prompt text if not in context
            prompt_country = None
            if "india" in q_lower:
                prompt_country = "India"
            elif "united states" in q_lower or "us" in q_lower:
                prompt_country = "United States"
            elif "united kingdom" in q_lower or "uk" in q_lower:
                prompt_country = "United Kingdom"

            target_country = (context and context.get("country")) or prompt_country

            if target_country or (context and (context.get("year") or context.get("type"))):
                c_clause = f" AND c.country = '{target_country}'" if target_country else ""
                return f"""
                    SELECT 
                        g.genre AS genre_name,
                        COUNT(DISTINCT f.title_key) AS titles_count
                    FROM gold.fact_content f
                    JOIN gold.dim_genre g ON f.genre_key = g.genre_key
                    JOIN gold.dim_country c ON f.country_key = c.country_key
                    JOIN gold.dim_type t ON f.type_key = t.type_key
                    WHERE g.genre <> 'Unknown' {c_clause} {type_filter} {year_filter}
                    GROUP BY g.genre
                    ORDER BY titles_count DESC 
                    LIMIT {limit};
                """.strip()

            return f"""
                SELECT genre_name, titles_count 
                FROM metadata.v_kpi_genre_distribution 
                ORDER BY titles_count DESC 
                LIMIT {limit};
            """.strip()


        # 5. Director Analysis
        elif any(w in q_lower for w in ["director", "directors", "filmmaker"]):
            limit = 10
            match = re.search(r'top\s+(\d+)', q_lower)
            if match:
                limit = int(match.group(1))
            return f"""
                SELECT director_name, titles_count 
                FROM metadata.v_kpi_top_directors 
                ORDER BY titles_count DESC 
                LIMIT {limit};
            """.strip()

        # 6. Rating Breakdown
        elif any(w in q_lower for w in ["rating", "ratings", "maturity"]):
            return """
                SELECT rating_name, titles_count 
                FROM metadata.v_kpi_rating_distribution 
                ORDER BY titles_count DESC;
            """.strip()

        # 7. Release Growth / Trend Data Extraction
        elif any(w in q_lower for w in ["trend", "growth", "predict", "forecast", "year", "annual", "release", "timeline"]):
            return f"""
                SELECT 
                    f.release_year,
                    COUNT(DISTINCT f.title_key) AS total_releases,
                    COUNT(DISTINCT CASE WHEN t.type = 'Movie' THEN f.title_key END) AS movie_releases,
                    COUNT(DISTINCT CASE WHEN t.type = 'TV Show' THEN f.title_key END) AS tv_releases
                FROM gold.fact_content f
                JOIN gold.dim_type t ON f.type_key = t.type_key
                JOIN gold.dim_country c ON f.country_key = c.country_key
                WHERE f.release_year >= 1995 AND f.release_year <= 2021 {country_filter} {type_filter}
                GROUP BY f.release_year
                ORDER BY f.release_year ASC;
            """.strip()

        # Default fallback query
        return "SELECT * FROM metadata.v_kpi_general_summary;"
