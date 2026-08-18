from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional

class ContextFilter(BaseModel):
    country: Optional[str] = None
    genre: Optional[str] = None
    type: Optional[str] = None # 'Movie' or 'TV Show'
    start_year: Optional[int] = None
    end_year: Optional[int] = None

class ChatRequest(BaseModel):
    query: str = Field(..., example="What are the top 5 countries by total Netflix content?")
    context: Optional[ContextFilter] = None

class ChartSpec(BaseModel):
    chart_type: str = Field(..., example="bar") # 'bar', 'horizontal_bar', 'line', 'pie', 'donut', 'treemap'
    title: str
    x_key: str
    y_keys: List[str]
    data: List[Dict[str, Any]]

class TrajectoryStep(BaseModel):
    agent: str
    status: str = Field(..., example="Completed") # 'Queued', 'Running', 'Completed', 'Failed'
    details: str

class ChatResponse(BaseModel):
    intent: str
    agents_used: List[str]
    trajectory: List[TrajectoryStep]
    answer: str
    insights: List[str] = []
    sql_query: Optional[str] = None
    data: List[Dict[str, Any]] = []
    chart: Optional[ChartSpec] = None
    ml_results: Optional[Dict[str, Any]] = None
    execution_time_ms: float = 0.0

class KPIOverviewResponse(BaseModel):
    total_titles: int
    total_movies: int
    total_tv_shows: int
    average_content_age: float
    movie_ratio_pct: float
    tv_ratio_pct: float
    top_country: str
    top_genre: str
    top_director: str
