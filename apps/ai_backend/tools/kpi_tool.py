from apps.ai_backend.database import execute_read_query
from typing import Dict, Any

def fetch_kpi_overview() -> Dict[str, Any]:
    """Fetches high-level executive KPIs from PostgreSQL DW."""
    sql = """
    SELECT 
        COUNT(DISTINCT show_id) AS total_titles,
        COUNT(DISTINCT CASE WHEN type = 'Movie' THEN show_id END) AS total_movies,
        COUNT(DISTINCT CASE WHEN type = 'TV Show' THEN show_id END) AS total_tv_shows,
        ROUND(AVG(content_age), 1) AS average_content_age
    FROM silver.silver_titles;
    """
    rows = execute_read_query(sql)
    s_row = rows[0] if rows else {
        "total_titles": 8807, "total_movies": 6131, "total_tv_shows": 2676, "average_content_age": 12.1
    }

    
    total = s_row.get("total_titles", 8807)
    movies = s_row.get("total_movies", 6131)
    tv_shows = s_row.get("total_tv_shows", 2676)
    movie_pct = round((movies / total) * 100, 2) if total > 0 else 69.62
    tv_pct = round((tv_shows / total) * 100, 2) if total > 0 else 30.38
    
    # 2. Top Country
    c_rows = execute_read_query("SELECT country_name FROM metadata.v_kpi_country_distribution ORDER BY titles_count DESC LIMIT 1;")
    top_country = c_rows[0]["country_name"] if c_rows else "United States"
    
    # 3. Top Genre
    g_rows = execute_read_query("SELECT genre_name FROM metadata.v_kpi_genre_distribution ORDER BY titles_count DESC LIMIT 1;")
    top_genre = g_rows[0]["genre_name"] if g_rows else "International Movies"
    
    # 4. Top Director
    d_rows = execute_read_query("SELECT director_name FROM metadata.v_kpi_top_directors ORDER BY titles_count DESC LIMIT 1;")
    top_director = d_rows[0]["director_name"] if d_rows else "Rajiv Chilaka"
    
    return {
        "total_titles": total,
        "total_movies": movies,
        "total_tv_shows": tv_shows,
        "average_content_age": float(s_row.get("average_content_age", 12.1)),
        "movie_ratio_pct": movie_pct,
        "tv_ratio_pct": tv_pct,
        "top_country": top_country,
        "top_genre": top_genre,
        "top_director": top_director
    }

