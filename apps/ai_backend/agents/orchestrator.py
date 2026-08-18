import time
import logging
from typing import Dict, Any, List, Tuple
from apps.ai_backend.agents.data_agent import DataAgent
from apps.ai_backend.agents.ml_agent import MLAgent
from apps.ai_backend.agents.support_agent import SupportAgent
from apps.ai_backend.agents.insight_agent import InsightAgent
from apps.ai_backend.agents.viz_agent import VizAgent
from apps.ai_backend.models.schemas import ChatRequest, ChatResponse, TrajectoryStep

logger = logging.getLogger("orchestrator")

class AgentOrchestrator:
    """Master Router & Supervisor Agent orchestrating Multi-Agent workflows with clean single-entry trajectory tracking."""

    def __init__(self):
        self.data_agent = DataAgent()
        self.ml_agent = MLAgent()
        self.support_agent = SupportAgent()
        self.insight_agent = InsightAgent()
        self.viz_agent = VizAgent()

    def classify_intent_and_agents(self, query: str) -> Tuple[str, List[str]]:
        """Determines the query intent and selects required agents dynamically."""
        q_lower = query.lower().strip()

        # Category 1: Support / Domain / General guidance questions (Conceptual explanations)
        if any(w in q_lower for w in ["what does", "explain", "meaning of", "define", "how do i use", "how does the ai work", "what data", "gold schema", "schema", "scd"]):
            if not any(w in q_lower for w in ["average content age", "average movie", "how many", "top", "ranking", "predict"]):
                return "Support Query", ["Support Agent"]

        if any(w in q_lower for w in ["movie ratio", "what can"]):
            return "Support Query", ["Support Agent"]

        # Category 1.5: Movie & TV Content Duration & Season Analysis
        elif any(w in q_lower for w in [
            "movie duration", "duration of movies", "longest movie", "shortest movie", "movie length", "movie runtime", "runtime",
            "typical movie", "how long is a", "how long are movies", "how many minutes", "film", "feature film", "duration", "average length",
            "time does a movie take", "how much time", "running tv show",
            "seasons", "season count", "tv show season", "tv series average duration", "how many seasons", "number of seasons", "most seasons", "longest tv"
        ]):
            if any(w in q_lower for w in ["show me", "chart", "country", "genre", "breakdown"]):
                return "Movie & TV Content Duration Analysis", ["Data Agent", "Insight Agent", "Visualization Agent"]
            return "Movie & TV Content Duration Analysis", ["Data Agent", "Insight Agent"]




        # Category 2: Machine Learning Forecasting / Trend Prediction
        elif any(w in q_lower for w in ["predict", "forecast", "future growth"]):
            return "ML Forecasting & Growth Trend", ["Data Agent", "ML Agent", "Insight Agent", "Visualization Agent"]


        # Category 3: Anomaly Detection / Clustering / Outliers
        elif any(w in q_lower for w in ["unusual", "anomaly", "anomalies", "outlier", "outliers", "cluster", "segment"]):
            return "ML Anomaly & Segmentation Analysis", ["Data Agent", "ML Agent", "Insight Agent"]

        # Category 4: Strategic Business Insight ("Why is the US dominant?", "Why...")
        elif any(w in q_lower for w in ["why is", "why does", "why are", "reason for", "cause of"]):
            return "Strategic Business Insight", ["Data Agent", "Insight Agent"]

        # Category 5: Data Ranking & Visualization ("which country makes most content?", "top 5 countries", "top 10 countries", "most content", "genres")
        elif any(w in q_lower for w in [
            "most content", "which country", "top 5", "top 10", "top 8", "top", "genre", "genres", 
            "category", "categories", "chart", "trend", "distribution", "breakdown", "ranking", "highest"
        ]):
            return "Data Extraction & Visualization", ["Data Agent", "Insight Agent", "Visualization Agent"]

        # Category 6: Simple Data Query ("How many movies are there?")
        elif any(w in q_lower for w in ["how many", "total movies", "total tv", "total titles", "count"]):
            return "Simple Data Aggregation", ["Data Agent"]

        # Default Fallback Routing
        return "General Data Analysis", ["Data Agent", "Insight Agent"]

    def process_request(self, request: ChatRequest) -> ChatResponse:
        start_time = time.time()
        trajectory: List[TrajectoryStep] = []

        query = request.query
        context_dict = request.context.dict() if request.context else None

        # 1. Orchestrator Intent Routing
        intent, selected_agents = self.classify_intent_and_agents(query)
        trajectory.append(TrajectoryStep(
            agent="Orchestrator Router",
            status="Completed",
            details=f"Intent: '{intent}' | Routed to: {', '.join(selected_agents)}"
        ))

        agents_used = ["Orchestrator Router"] + selected_agents
        sql_query = None
        data = []
        ml_results = None
        answer = ""
        insights = []
        chart_spec = None

        # Path A: Support Agent Workflow
        if "Support Agent" in selected_agents:
            try:
                answer = self.support_agent.answer(query)
                trajectory.append(TrajectoryStep(
                    agent="Support Agent",
                    status="Completed",
                    details="Answered domain guidance query without SQL execution."
                ))
            except Exception as e:
                trajectory.append(TrajectoryStep(agent="Support Agent", status="Failed", details=str(e)))
                answer = f"Support Agent Error: {str(e)}"

        # Path B: Data / ML / Insight / Visualization Agent Workflow
        else:
            # 2. Data Agent Execution (Single final trajectory entry)
            if "Data Agent" in selected_agents:
                try:
                    data, sql_query = self.data_agent.execute(query, context_dict)
                    trajectory.append(TrajectoryStep(
                        agent="Data Agent",
                        status="Completed",
                        details=f"Executed read-only SQL. Retrieved {len(data)} rows."
                    ))
                except Exception as e:
                    trajectory.append(TrajectoryStep(agent="Data Agent", status="Failed", details=f"SQL Error: {str(e)}"))
                    execution_time = (time.time() - start_time) * 1000
                    return ChatResponse(
                        intent=intent,
                        agents_used=agents_used,
                        trajectory=trajectory,
                        answer=f"Error executing database query: {str(e)}",
                        insights=[],
                        sql_query=sql_query,
                        data=[],
                        chart=None,
                        ml_results=None,
                        execution_time_ms=round(execution_time, 2)
                    )

            # 3. ML Agent Execution (Single final trajectory entry)
            if "ML Agent" in selected_agents:
                try:
                    ml_results = self.ml_agent.analyze(query, data)
                    model_type = ml_results.get("model_type", "Statistical ML Model")
                    trajectory.append(TrajectoryStep(
                        agent="ML Agent",
                        status="Completed",
                        details=f"Executed {model_type} algorithm."
                    ))
                except Exception as e:
                    trajectory.append(TrajectoryStep(agent="ML Agent", status="Failed", details=f"ML Error: {str(e)}"))

            # 4. Insight Agent Execution (Single final trajectory entry)
            if "Insight Agent" in selected_agents:
                try:
                    answer, insights = self.insight_agent.generate_insights(query, data, sql_query or "", ml_results)
                    trajectory.append(TrajectoryStep(
                        agent="Insight Agent",
                        status="Completed",
                        details=f"Synthesized answer and {len(insights)} key insight findings."
                    ))
                except Exception as e:
                    trajectory.append(TrajectoryStep(agent="Insight Agent", status="Failed", details=f"Insight Error: {str(e)}"))
                    answer = f"Insight synthesis error: {str(e)}"
            elif "Data Agent" in selected_agents and not answer:
                # Basic data answer for simple queries like "How many movies are there?"
                if data and "total_movies" in data[0]:
                    row = data[0]
                    answer = f"### Answer\n\nThere are **{row.get('total_movies'):,} Movies** and **{row.get('total_tv_shows'):,} TV Shows** out of **{row.get('total_titles'):,} total titles** in the Gold Data Warehouse."
                    insights = [f"Movies account for {round((row.get('total_movies')/row.get('total_titles'))*100, 2)}% of the catalog."]
                elif data:
                    first_col = list(data[0].keys())[0]
                    first_val = data[0][first_col]
                    answer = f"### Answer\n\nQuery execution completed. The top result for `{first_col}` is **{first_val}** with **{len(data)} total records** returned."

            # 5. Visualization Agent Execution (Single final trajectory entry)
            if "Visualization Agent" in selected_agents:
                try:
                    chart_spec = self.viz_agent.generate_chart_spec(query, data, sql_query, ml_results)
                    if chart_spec:
                        trajectory.append(TrajectoryStep(
                            agent="Visualization Agent",
                            status="Completed",
                            details=f"Generated {chart_spec.chart_type.upper()} chart specification."
                        ))
                    else:
                        trajectory.append(TrajectoryStep(
                            agent="Visualization Agent",
                            status="Completed",
                            details="No visual chart required for dataset."
                        ))
                except Exception as e:
                    trajectory.append(TrajectoryStep(agent="Visualization Agent", status="Failed", details=f"Visualization Error: {str(e)}"))

        execution_time = (time.time() - start_time) * 1000
        return ChatResponse(
            intent=intent,
            agents_used=agents_used,
            trajectory=trajectory,
            answer=answer,
            insights=insights,
            sql_query=sql_query,
            data=data,
            chart=chart_spec,
            ml_results=ml_results,
            execution_time_ms=round(execution_time, 2)
        )

orchestrator = AgentOrchestrator()
