from typing import List, Dict, Any

class SummaryAgent:
    """Specialized Agent for translating raw SQL dataset rows into clean executive summary text."""
    
    def summarize(self, query: str, data: List[Dict[str, Any]], sql_query: str) -> str:
        if not data:
            return "No matching records were found in the Netflix Gold Data Warehouse for your query."
            
        q_lower = query.lower()
        
        # Scenario 1: General Summary KPI
        if "v_kpi_general_summary" in sql_query:
            row = data[0]
            total = row.get("total_titles", 8807)
            movies = row.get("total_movies", 6131)
            tv_shows = row.get("total_tv_shows", 2676)
            avg_age = row.get("average_content_age", 12.5)
            movie_ratio = round((movies / total) * 100, 2)
            tv_ratio = round((tv_shows / total) * 100, 2)
            
            return (
                f"### Netflix Portfolio Overview\n\n"
                f"- **Total Catalog Titles:** `{total:,}`\n"
                f"- **Movies:** `{movies:,}` ({movie_ratio}%)\n"
                f"- **TV Shows:** `{tv_shows:,}` ({tv_ratio}%)\n"
                f"- **Average Content Age:** `{avg_age}` years\n\n"
                f"Movies dominate the catalog accounting for nearly 70% of total titles."
            )

        # Scenario 2: Country Distribution
        elif "v_kpi_country_distribution" in sql_query:
            top_3 = data[:3]
            bullets = "\n".join([f"- **{r['country_name']}:** {r['titles_count']:,} titles ({r.get('percentage_share', 0)}%)" for r in top_3])
            return (
                f"### Content Distribution by Country\n\n"
                f"Analyzing spatial geographic distribution across the Netflix library:\n\n"
                f"{bullets}\n\n"
                f"The **{top_3[0]['country_name']}** leads as the largest single content producer in the warehouse."
            )

        # Scenario 3: Genre Distribution
        elif "v_kpi_genre_distribution" in sql_query:
            top_3 = data[:3]
            bullets = "\n".join([f"- **{r['genre_name']}:** {r['titles_count']:,} titles" for r in top_3])
            return (
                f"### Top Performing Genres\n\n"
                f"Distribution of catalog titles across top genres:\n\n"
                f"{bullets}\n\n"
                f"**{top_3[0]['genre_name']}** is the highest volume genre category."
            )

        # Scenario 4: Director Rankings
        elif "v_kpi_top_directors" in sql_query:
            top_3 = data[:3]
            bullets = "\n".join([f"- **{r['director_name']}:** {r['titles_count']} titles" for r in top_3])
            return (
                f"### Director Catalog Rankings\n\n"
                f"Top contributing directors in the dataset:\n\n"
                f"{bullets}"
            )

        # Scenario 5: Rating Distribution
        elif "v_kpi_rating_distribution" in sql_query:
            bullets = "\n".join([f"- **{r['rating_name']}:** {r['titles_count']:,} titles" for r in data[:5]])
            return (
                f"### Rating & Audience Classification Breakdown\n\n"
                f"{bullets}"
            )

        # Generic summary fallback
        count = len(data)
        first_row_keys = list(data[0].keys()) if data else []
        return f"Successfully retrieved **{count}** records matching your query. Key dimensions include: `{', '.join(first_row_keys[:4])}`."
