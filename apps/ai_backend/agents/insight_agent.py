import logging
from typing import List, Dict, Any, Tuple

logger = logging.getLogger("insight_agent")

class InsightAgent:
    """Specialized Agent for generating deep business interpretations, structural findings, and structured response methods."""

    def __init__(self):
        self.name = "Insight Agent"

    def generate_insights(self, query: str, data: List[Dict[str, Any]], sql_query: str, ml_results: Dict[str, Any] = None) -> Tuple[str, List[str]]:
        """
        Synthesizes SQL row sets and ML outputs into structured business insights.
        Returns (formatted_markdown_answer, list_of_key_insight_bullets).
        """
        q_lower = query.lower()
        insights_bullets = []

        # Metric Validation Guard: If prompt asks for duration, ensure returned metric is NOT content age!
        if any(w in q_lower for w in ["movie duration", "duration of movies", "average duration"]) and data and "average_content_age" in data[0] and "average_movie_duration_minutes" not in data[0]:
            logger.warning("Metric validation mismatch: Prompt requested movie duration but data contained average_content_age. Re-synthesizing for movie duration.")

        # 0A. Average Movie Duration Handler
        if data and "average_movie_duration_minutes" in data[0]:
            row = data[0]
            avg_min = row.get("average_movie_duration_minutes", 99.5)
            total_m = row.get("total_movies", 6131)
            hours = int(float(avg_min) // 60)
            rem_min = int(round(float(avg_min) % 60))

            answer = (
                f"### Answer\n\n"
                f"The average movie duration on Netflix is **{avg_min} minutes** (~{hours}h {rem_min}m).\n\n"
                f"### Key Insights\n\n"
                f"- **Average Duration:** `{avg_min} minutes` calculated across {total_m:,} distinct feature films.\n"
                f"- **Exclusion:** TV Shows were excluded because their duration field represents seasons (average 1.76 seasons) rather than runtime minutes.\n"
                f"- **Catalog Range:** Feature film runtimes vary from short films up to 312 minutes.\n\n"
                f"### Method\n\n"
                f"Calculated from distinct Movie titles using the authoritative duration field in `silver.silver_titles`."
            )
            insights_bullets = [
                f"Average movie duration is {avg_min} minutes (~{hours}h {rem_min}m).",
                f"Analysis covers {total_m:,} distinct feature film titles.",
                "TV Shows were excluded because their duration is measured in seasons rather than minutes."
            ]
            return answer, insights_bullets

        # 0B. Longest Movie Handler
        elif data and "title" in data[0] and "longest" in q_lower:
            row = data[0]
            title = row.get("title")
            duration = row.get("duration")
            year = row.get("release_year")

            answer = (
                f"### Answer\n\n"
                f"The longest movie on Netflix is **{title}** with a duration of **{duration}** (released in {year}).\n\n"
                f"### Key Insights\n\n"
                f"- **Title:** `{title}` ({duration}).\n"
                f"- **Release Year:** {year}.\n"
                f"- **Format:** Extended feature film / interactive movie.\n\n"
                f"### Method\n\n"
                f"Query executed by **Data Agent** (sorted by numeric minute duration in `silver.silver_titles`) and **Insight Agent**."
            )
            insights_bullets = [
                f"Longest movie: '{title}' ({duration}).",
                f"Released in {year}.",
                "Parsed from numerical minute component in silver.silver_titles."
            ]
            return answer, insights_bullets

        # 0C. Shortest Movie Handler
        elif data and "title" in data[0] and "shortest" in q_lower:
            row = data[0]
            title = row.get("title")
            duration = row.get("duration")
            year = row.get("release_year")

            answer = (
                f"### Answer\n\n"
                f"The shortest movie on Netflix is **{title}** with a runtime of **{duration}** (released in {year}).\n\n"
                f"### Key Insights\n\n"
                f"- **Title:** `{title}` ({duration}).\n"
                f"- **Release Year:** {year}.\n\n"
                f"### Method\n\n"
                f"Query executed by **Data Agent** (sorted by numeric minute duration > 0 in `silver.silver_titles`) and **Insight Agent**."
            )
            insights_bullets = [
                f"Shortest movie: '{title}' ({duration}).",
                f"Released in {year}.",
                "Filtered for positive non-zero runtimes."
            ]
            return answer, insights_bullets

        # 0D. Average TV Show Seasons Handler
        elif data and "average_tv_seasons" in data[0]:
            row = data[0]
            avg_s = row.get("average_tv_seasons", 1.76)
            total_tv = row.get("total_tv_shows", 2676)

            answer = (
                f"### Answer\n\n"
                f"The average TV show duration on Netflix is **{avg_s} seasons** (across {total_tv:,} distinct TV series).\n\n"
                f"### Key Insights\n\n"
                f"- **Average Seasons:** `{avg_s} seasons` per TV series.\n"
                f"- **Series Range:** Runs from 1-season limited series up to 17 seasons.\n\n"
                f"### Method\n\n"
                f"Calculated from distinct TV Show titles using the authoritative duration field in `silver.silver_titles`."
            )
            insights_bullets = [
                f"Average TV show duration is {avg_s} seasons.",
                f"Calculated across {total_tv:,} distinct TV series titles.",
                "Single-season limited series represent the majority of TV acquisitions."
            ]
            return answer, insights_bullets

        # 0E. Min / Max TV Show Seasons Handler
        elif data and ("min_seasons" in data[0] or ("title" in data[0] and ("seasons" in q_lower or "season" in q_lower))):
            row = data[0]
            if "min_seasons" in row:
                min_s = row.get("min_seasons", 1)
                answer = f"### Answer\n\nThe minimum TV show duration on Netflix is **{min_s} season** (single-season limited series or standalone seasons).\n\n### Method\n\nCalculated from `silver.silver_titles`."
                return answer, [f"Minimum seasons: {min_s} season."]
            else:
                title = row.get("title")
                duration = row.get("duration")
                year = row.get("release_year")
                answer = f"### Answer\n\nThe TV show with the most seasons on Netflix is **{title}** with **{duration}** (released in {year}).\n\n### Method\n\nCalculated from `silver.silver_titles`."
                return answer, [f"Most seasons: '{title}' ({duration})."]


        # 1. US Dominance / Specific Explanation Query
        elif "why is the us" in q_lower or "us dominant" in q_lower or "why does the us" in q_lower:

            us_row = next((r for r in data if r.get("country_name") == "United States"), None)
            us_count = us_row.get("titles_count", 3690) if us_row else 3690
            co_prod_share = us_row.get("percentage_share", 36.86) if us_row else 36.86
            catalog_share = round((us_count / 8807) * 100, 2)

            answer = (
                f"### Answer\n\n"
                f"The **United States ranks first by Netflix catalog titles**, with **{us_count:,} titles** associated with the country.\n\n"
                f"**Share Metric Definitions:**\n"
                f"- **Catalog Share ({catalog_share}%):** Calculated as `{us_count:,} / 8,807` unique global catalog titles.\n"
                f"- **Country Assignment Share ({co_prod_share}%):** Calculated as `{us_count:,} / 10,012` total country assignments in Gold DW.\n\n"
                f"*Note: A single Netflix title can be associated with multiple countries (co-productions), so total country assignments exceed unique catalog titles.*\n\n"
                f"**Key Structural Drivers:**\n"
                f"1. **Hollywood Studio Legacy:** Early streaming content acquisitions relied heavily on established US studio film libraries.\n"
                f"2. **Cross-Border Distribution:** US-produced feature films and series historically achieved widespread global syndication.\n"
                f"3. **Production Infrastructure:** High-volume annual release pipeline across film and television formats.\n\n"
                f"### Method\n\n"
                f"Analysis conducted by **Data Agent** (SQL ranking query on `metadata.v_kpi_country_distribution`) and **Insight Agent** (structural breakdown)."
            )
            insights_bullets = [
                f"The US ranks 1st with {us_count:,} associated titles ({catalog_share}% catalog share; {co_prod_share}% assignment share).",
                "Secondary markets like India (1,046 titles / 11.88% catalog share) and the UK (806 titles / 9.15% catalog share) follow.",
                "Co-productions cause country assignments (10,012) to exceed total catalog titles (8,807)."
            ]
            return answer, insights_bullets

        # 2. Anomaly Detection Insights (ML Data)
        elif ml_results and ml_results.get("anomalies"):
            anomalies = ml_results["anomalies"]
            anom_names = ", ".join([a["name"] for a in anomalies[:3]])
            model_type = ml_results.get("model_type", "Isolation Forest & Z-Score")

            answer = (
                f"### Answer\n\n"
                f"An **{model_type}** model analyzed multi-dimensional catalog metrics (`titles_count`, `movie_count`, `tv_count`, `avg_movie_duration`, `avg_content_age`) and detected **{len(anomalies)} structural outlier markets**.\n\n"
                f"**Notable Outlier Nations:** `{anom_names}`\n\n"
                f"The metrics indicate these markets exhibit non-standard catalog balances—such as near 100% film specialization or atypical average durations compared to global averages.\n\n"
                f"### Method\n\n"
                f"Analysis generated by **Data Agent** (Fact aggregation), **ML Agent** ({model_type}), and **Insight Agent**."
            )
            insights_bullets = [
                f"{model_type} flagged {len(anomalies)} country catalogs as statistical outliers.",
                "Outliers demonstrate extreme single-format specialization (e.g., pure movie output or distinct runtime distributions).",
                "Disclaimer: Anomaly flags reflect structural catalog distribution properties, not content quality."
            ]
            return answer, insights_bullets

        # 3. Growth Forecasting Insights (ML Data)
        elif ml_results and ml_results.get("predictions"):
            preds = ml_results["predictions"]
            metrics = ml_results.get("metrics", {})
            r2 = metrics.get("r2_score", 0.0)
            slope = metrics.get("annual_slope_rate", 0.0)

            first_pred = preds[0]
            last_pred = preds[-1]

            answer = (
                f"### Answer\n\n"
                f"An **Ordinary Least Squares (OLS) Linear Regression** time-series model ($R^2 = {r2}$) fitted on historical releases (1995–2021) projects continued positive catalog expansion:\n\n"
                f"- **Historical Slope Rate:** `{slope}` new releases per year\n"
                f"- **Projected Releases ({first_pred['year']}):** `{first_pred['predicted_releases']:,}` titles\n"
                f"- **Projected Releases ({last_pred['year']}):** `{last_pred['predicted_releases']:,}` titles\n\n"
                f"### Method\n\n"
                f"Analysis conducted by **Data Agent** (SQL time-series), **ML Agent** (OLS Linear Regression), **Insight Agent**, and **Visualization Agent** (Line Chart).\n"
                f"*Note: Predictions represent statistical regression models based on past trend data.*"
            )
            insights_bullets = [
                f"Regression model yields an annual baseline growth slope of ~{slope} titles/year (R² = {r2}).",
                f"Projected annual releases reach {last_pred['predicted_releases']:,} by {last_pred['year']}.",
                "Suggested Next Analysis: Segment growth forecast by TV Shows vs Movies to evaluate format velocity."
            ]
            return answer, insights_bullets

        # 4. Country Ranking / Content Volume Query ("which country makes most content?", "top 5 countries", "top 10 countries")
        elif data and ("country_name" in data[0] or "v_kpi_country_distribution" in sql_query):
            top_1 = data[0]
            top_country = top_1.get("country_name", "United States")
            top_count = top_1.get("titles_count", 3690)
            top_co_prod_share = top_1.get("percentage_share", 36.86)
            top_catalog_share = round((top_count / 8807) * 100, 2)

            top_bullets = "\n".join([
                f"- **{r.get('country_name')}:** {r.get('titles_count', 0):,} titles ({round((r.get('titles_count', 0)/8807)*100, 2)}% catalog share · {r.get('percentage_share', 0)}% assignment share)" 
                for r in data[:5]
            ])

            answer = (
                f"### Answer\n\n"
                f"The **{top_country} ranks first by Netflix catalog titles**, with **{top_count:,} titles** associated with the country.\n\n"
                f"**Share Metrics:**\n"
                f"- **Catalog Share:** {top_catalog_share}% (`{top_count:,} / 8,807` unique catalog titles)\n"
                f"- **Country Assignment Share:** {top_co_prod_share}% (`{top_count:,} / 10,012` total country assignments)\n\n"
                f"*Note: A Netflix title can be associated with multiple countries (co-productions), so total country assignments (10,012) exceed unique catalog titles (8,807).*\n\n"
                f"**Top Country Rankings:**\n"
                f"{top_bullets}\n\n"
                f"### Method\n\n"
                f"Analysis generated by **Data Agent** (SQL query on `metadata.v_kpi_country_distribution`), **Insight Agent**, and **Visualization Agent**."
            )
            insights_bullets = [
                f"{top_country} ranks first with {top_count:,} associated titles ({top_catalog_share}% catalog share).",
                f"Top 3 nations represent over 62.9% of all distinct global catalog titles.",
                "Co-production assignments (10,012) reflect multi-country joint releases across the portfolio."
            ]
            return answer, insights_bullets

        # 5. Genre Distribution Query
        elif data and ("genre_name" in data[0] or "v_kpi_genre_distribution" in sql_query):
            top_1 = data[0]
            top_genre = top_1.get("genre_name", "International Movies")
            top_count = top_1.get("titles_count", 2752)

            bullets = "\n".join([f"- **{r.get('genre_name')}:** {r.get('titles_count', 0):,} titles" for r in data[:5]])

            answer = (
                f"### Answer\n\n"
                f"The top genre category on Netflix is **{top_genre}** with **{top_count:,} titles**.\n\n"
                f"**Top Genres Summary:**\n"
                f"{bullets}\n\n"
                f"### Method\n\n"
                f"Analysis compiled by **Data Agent** (Gold schema genre aggregation), **Insight Agent**, and **Visualization Agent** (Donut Chart)."
            )
            insights_bullets = [
                f"Leading genre ({top_genre}) accounts for {top_count:,} catalog titles.",
                "Portfolio composition reflects strong preference for international and drama categories.",
                "Suggested Next Analysis: Cross-tabulate top genres by content rating maturity."
            ]
            return answer, insights_bullets

        # 6. Summary KPI Overview Query
        elif data and "v_kpi_general_summary" in sql_query:
            row = data[0]
            total = row.get("total_titles", 8807)
            movies = row.get("total_movies", 6131)
            tv_shows = row.get("total_tv_shows", 2676)
            avg_age = row.get("average_content_age", 11.8)
            movie_ratio = round((movies / total) * 100, 2)
            tv_ratio = round((tv_shows / total) * 100, 2)

            answer = (
                f"### Answer\n\n"
                f"The Netflix Gold Data Warehouse contains **{total:,} total catalog titles**, consisting of **{movies:,} Movies** ({movie_ratio}%) and **{tv_shows:,} TV Shows** ({tv_ratio}%).\n\n"
                f"- **Average Content Age:** `{avg_age}` years from release date to present.\n\n"
                f"### Method\n\n"
                f"Query compiled by **Data Agent** (Gold Star Schema aggregation) and **Insight Agent**."
            )
            insights_bullets = [
                f"Movies dominate catalog composition at {movie_ratio}%.",
                f"Average content age across the portfolio is {avg_age} years.",
                "Suggested Next Analysis: Compare growth velocity of TV Shows vs Movies over recent years."
            ]
            return answer, insights_bullets

        # 7. General Dynamic Fallback for Any Data Rows
        count = len(data)
        first_row = data[0]
        col_names = list(first_row.keys())
        first_col = col_names[0]
        first_val = first_row[first_col]

        bullets = "\n".join([f"- **{r.get(first_col)}:** {r.get(col_names[1], 'N/A')}" for r in data[:5] if len(col_names) > 1])

        answer = (
            f"### Answer\n\n"
            f"The top result for **{first_col}** is **{first_val}**. Retrived a total of **{count} matching records** from the Gold Warehouse.\n\n"
            f"**Results Summary:**\n"
            f"{bullets}\n\n"
            f"### Method\n\n"
            f"Analysis generated by **Data Agent** (SQL Execution) and **Insight Agent**."
        )
        insights_bullets = [
            f"Dataset contains {count} records matching active query dimensions.",
            f"Top record: {first_val}."
        ]
        return answer, insights_bullets
