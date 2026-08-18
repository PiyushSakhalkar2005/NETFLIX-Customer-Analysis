import logging
from typing import List, Dict, Any, Optional
from apps.ai_backend.models.schemas import ChartSpec

logger = logging.getLogger("viz_agent")

class VizAgent:
    """Specialized Visualization Agent for selecting optimal chart types and formatting datasets into ChartSpec."""

    def __init__(self):
        self.name = "Visualization Agent"

    def generate_chart_spec(self, query: str, data: List[Dict[str, Any]], sql_query: str = None, ml_results: Dict[str, Any] = None) -> Optional[ChartSpec]:
        q_lower = query.lower()

        # Case 1: ML Predictions Time Series Chart
        if ml_results and ml_results.get("predictions"):
            preds = ml_results["predictions"]
            chart_data = [{"year": str(p["year"]), "predicted_releases": p["predicted_releases"]} for p in preds]
            return ChartSpec(
                chart_type="line",
                title="ML Projected Annual Content Growth (Linear Regression)",
                x_key="year",
                y_keys=["predicted_releases"],
                data=chart_data
            )

        if not data:
            return None

        sql_lower = (sql_query or "").lower()

        # Case 2: Time Series Trend (Release Years) -> Line Chart
        if "release_year" in data[0] or "v_kpi_release_trends" in sql_lower or any(w in q_lower for w in ["trend", "growth", "history", "timeline"]):
            aggregated = {}
            for r in data:
                yr = str(r.get("release_year"))
                cnt = r.get("total_releases", r.get("releases_count", 0))
                if yr not in aggregated:
                    aggregated[yr] = {"year": yr, "releases": 0}
                aggregated[yr]["releases"] += cnt

            trend_data = sorted(list(aggregated.values()), key=lambda x: x["year"])
            return ChartSpec(
                chart_type="line",
                title="Annual Content Release Trend",
                x_key="year",
                y_keys=["releases"],
                data=trend_data
            )

        # Case 3: Country Ranking -> Bar / Horizontal Bar Chart
        elif "v_kpi_country_distribution" in sql_lower or "country_name" in data[0] or any(w in q_lower for w in ["country", "countries", "which country", "top 5", "top 10"]):
            limit = 10
            if "top 5" in q_lower:
                limit = 5
            return ChartSpec(
                chart_type="horizontal_bar" if limit > 5 else "bar",
                title=f"Top Content Producing Countries",
                x_key="country_name",
                y_keys=["titles_count"],
                data=data[:limit]
            )


        # Case 4: Genre Distribution -> Donut Composition Chart
        elif "v_kpi_genre_distribution" in sql_lower or "genre_name" in data[0]:
            return ChartSpec(
                chart_type="donut",
                title="Catalog Share by Genre Composition",
                x_key="genre_name",
                y_keys=["titles_count"],
                data=data[:8]
            )

        # Case 5: Rating Breakdown -> Bar Chart
        elif "v_kpi_rating_distribution" in sql_lower or "rating_name" in data[0]:
            return ChartSpec(
                chart_type="bar",
                title="Content Distribution by Audience Rating",
                x_key="rating_name",
                y_keys=["titles_count"],
                data=data[:8]
            )

        # Case 6: Directors -> Bar Chart
        elif "v_kpi_top_directors" in sql_lower or "director_name" in data[0]:
            return ChartSpec(
                chart_type="bar",
                title="Top Catalog Directors",
                x_key="director_name",
                y_keys=["titles_count"],
                data=data[:10]
            )

        # Fallback for 2-column tabular data
        keys = list(data[0].keys())
        if len(keys) >= 2:
            x_candidate = keys[0]
            y_candidate = keys[1]
            if isinstance(data[0].get(y_candidate), (int, float)):
                return ChartSpec(
                    chart_type="bar",
                    title="Analytical Chart Visualization",
                    x_key=x_candidate,
                    y_keys=[y_candidate],
                    data=data[:10]
                )

        return None
