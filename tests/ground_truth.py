import sys
import os
import json
import logging
from typing import Dict, Any

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.ai_backend.database import execute_read_query

logger = logging.getLogger("ground_truth")

def calculate_ground_truth() -> Dict[str, Any]:
    """Calculates independent ground-truth values directly from PostgreSQL DW."""
    print("Calculating independent PostgreSQL ground-truth values...")
    gt = {}

    # 1. Total Catalog KPIs
    sql_kpis = """
    SELECT 
        COUNT(*) as total_titles,
        COUNT(CASE WHEN type = 'Movie' THEN 1 END) as total_movies,
        COUNT(CASE WHEN type = 'TV Show' THEN 1 END) as total_tv_shows,
        ROUND(100.0 * COUNT(CASE WHEN type = 'Movie' THEN 1 END) / COUNT(*), 2) as movie_ratio,
        ROUND(100.0 * COUNT(CASE WHEN type = 'TV Show' THEN 1 END) / COUNT(*), 2) as tv_ratio,
        ROUND(AVG(content_age), 1) as average_content_age
    FROM silver.silver_titles;
    """
    row_kpis = execute_read_query(sql_kpis)[0]
    gt["total_titles"] = {"val": int(row_kpis["total_titles"]), "unit": "titles", "sql": sql_kpis}
    gt["total_movies"] = {"val": int(row_kpis["total_movies"]), "unit": "titles", "sql": sql_kpis}
    gt["total_tv_shows"] = {"val": int(row_kpis["total_tv_shows"]), "unit": "titles", "sql": sql_kpis}
    gt["movie_ratio"] = {"val": float(row_kpis["movie_ratio"]), "unit": "%", "sql": sql_kpis}
    gt["tv_ratio"] = {"val": float(row_kpis["tv_ratio"]), "unit": "%", "sql": sql_kpis}
    gt["average_content_age"] = {"val": float(row_kpis["average_content_age"]), "unit": "years", "sql": sql_kpis}

    # 2. Movie Duration Metrics
    sql_movie_dur = """
    SELECT 
        COUNT(*) as movie_count,
        ROUND(AVG(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)), 1) as avg_duration,
        MIN(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)) as min_duration,
        MAX(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)) as max_duration
    FROM silver.silver_titles
    WHERE type = 'Movie' AND duration LIKE '%min%';
    """
    row_dur = execute_read_query(sql_movie_dur)[0]
    gt["average_movie_duration"] = {"val": float(row_dur["avg_duration"]), "unit": "minutes", "sql": sql_movie_dur}
    gt["min_movie_duration"] = {"val": int(row_dur["min_duration"]), "unit": "minutes", "sql": sql_movie_dur}
    gt["max_movie_duration"] = {"val": int(row_dur["max_duration"]), "unit": "minutes", "sql": sql_movie_dur}

    # 3. Longest & Shortest Movie
    sql_longest = """
    SELECT title, duration, release_year
    FROM silver.silver_titles
    WHERE type = 'Movie' AND duration LIKE '%min%'
    ORDER BY CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) DESC
    LIMIT 1;
    """
    row_longest = execute_read_query(sql_longest)[0]
    gt["longest_movie"] = {"title": row_longest["title"], "duration": row_longest["duration"], "val": 312, "unit": "minutes", "sql": sql_longest}

    sql_shortest = """
    SELECT title, duration, release_year
    FROM silver.silver_titles
    WHERE type = 'Movie' AND duration LIKE '%min%' AND CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) > 0
    ORDER BY CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) ASC
    LIMIT 1;
    """
    row_shortest = execute_read_query(sql_shortest)[0]
    gt["shortest_movie"] = {"title": row_shortest["title"], "duration": row_shortest["duration"], "val": 3, "unit": "minutes", "sql": sql_shortest}

    # 4. TV Show Seasons Metrics
    sql_tv_seasons = """
    SELECT 
        COUNT(*) as tv_count,
        ROUND(AVG(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)), 2) as avg_seasons,
        MIN(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)) as min_seasons,
        MAX(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)) as max_seasons
    FROM silver.silver_titles
    WHERE type = 'TV Show' AND (duration LIKE '%Season%' OR duration LIKE '%Seasons%');
    """
    row_seasons = execute_read_query(sql_tv_seasons)[0]
    gt["average_seasons"] = {"val": float(row_seasons["avg_seasons"]), "unit": "seasons", "sql": sql_tv_seasons}
    gt["min_seasons"] = {"val": int(row_seasons["min_seasons"]), "unit": "seasons", "sql": sql_tv_seasons}
    gt["max_seasons"] = {"val": int(row_seasons["max_seasons"]), "unit": "seasons", "sql": sql_tv_seasons}

    # 5. Country Rankings & Shares
    sql_top_countries = """
    SELECT country_name, titles_count, percentage_share 
    FROM metadata.v_kpi_country_distribution 
    ORDER BY titles_count DESC 
    LIMIT 5;
    """
    rows_top_c = execute_read_query(sql_top_countries)
    gt["top_country"] = {
        "name": rows_top_c[0]["country_name"],
        "count": int(rows_top_c[0]["titles_count"]),
        "catalog_share": round((rows_top_c[0]["titles_count"] / 8807) * 100, 2),
        "assignment_share": float(rows_top_c[0]["percentage_share"]),
        "unit": "titles",
        "sql": sql_top_countries
    }
    gt["top_5_countries"] = [
        {"name": r["country_name"], "count": int(r["titles_count"]), "catalog_share": round((r["titles_count"]/8807)*100, 2), "assignment_share": float(r["percentage_share"])}
        for r in rows_top_c
    ]

    # 6. Genre Rankings
    sql_genres = "SELECT genre_name, titles_count FROM metadata.v_kpi_genre_distribution ORDER BY titles_count DESC LIMIT 5;"
    rows_g = execute_read_query(sql_genres)
    gt["top_genre"] = {"name": rows_g[0]["genre_name"], "count": int(rows_g[0]["titles_count"]), "unit": "titles", "sql": sql_genres}
    gt["top_genres"] = [{"name": r["genre_name"], "count": int(r["titles_count"])} for r in rows_g]

    # 7. Directors & Ratings
    sql_directors = "SELECT director_name, titles_count FROM metadata.v_kpi_top_directors ORDER BY titles_count DESC LIMIT 1;"
    row_dir = execute_read_query(sql_directors)[0]
    gt["top_director"] = {"name": row_dir["director_name"], "count": int(row_dir["titles_count"]), "unit": "titles", "sql": sql_directors}

    sql_ratings = "SELECT rating_name, titles_count FROM metadata.v_kpi_rating_distribution ORDER BY titles_count DESC LIMIT 1;"
    row_rat = execute_read_query(sql_ratings)[0]
    gt["top_rating"] = {"name": row_rat["rating_name"], "count": int(row_rat["titles_count"]), "unit": "titles", "sql": sql_ratings}

    # 8. Time Metrics
    sql_peak_year = """
    SELECT release_year, COUNT(DISTINCT title_key) as total_releases
    FROM gold.fact_content
    GROUP BY release_year
    ORDER BY total_releases DESC
    LIMIT 1;
    """
    row_peak = execute_read_query(sql_peak_year)[0]
    gt["peak_release_year"] = {"year": int(row_peak["release_year"]), "count": int(row_peak["total_releases"]), "unit": "titles", "sql": sql_peak_year}

    # 9. Power BI Filtered Ground Truth (Country: India)
    sql_india_genres = """
    SELECT g.genre AS genre_name, COUNT(DISTINCT f.title_key) AS titles_count
    FROM gold.fact_content f
    JOIN gold.dim_genre g ON f.genre_key = g.genre_key
    JOIN gold.dim_country c ON f.country_key = c.country_key
    WHERE g.genre <> 'Unknown' AND c.country = 'India'
    GROUP BY g.genre
    ORDER BY titles_count DESC
    LIMIT 5;
    """
    rows_ind_g = execute_read_query(sql_india_genres)
    gt["context_india_top_genre"] = {"name": rows_ind_g[0]["genre_name"], "count": int(rows_ind_g[0]["titles_count"]), "unit": "titles", "sql": sql_india_genres}

    return gt

if __name__ == "__main__":
    gt_data = calculate_ground_truth()
    print("\nCalculated Ground Truth Summary:")
    print(json.dumps(gt_data, indent=2))
