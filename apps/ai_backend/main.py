import os
import sys
import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Ensure apps directory is in path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.ai_backend.config import settings
from apps.ai_backend.models.schemas import ChatRequest, ChatResponse, KPIOverviewResponse
from apps.ai_backend.agents.orchestrator import orchestrator
from apps.ai_backend.tools.kpi_tool import fetch_kpi_overview
from apps.ai_backend.database import get_database_schema_info, get_db_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ai_backend_main")

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="Agentic AI Analytics Engine for Netflix Data Warehouse (PostgreSQL Gold Layer)"
)

# Enable CORS for frontend UI (React / Vite / Power BI Embedded)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Production: restrict to frontend origin
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", tags=["Health"])
def root():
    return {
        "status": "online",
        "service": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "database": settings.DB_NAME,
        "read_only_mode": settings.READ_ONLY_MODE
    }

@app.get(f"{settings.API_V1_STR}/health", tags=["Health"])
def health_check():
    try:
        conn = get_db_connection()
        conn.close()
        db_status = "healthy"
    except Exception as e:
        logger.error(f"Health check DB error: {str(e)}")
        db_status = "unhealthy"
        
    return {
        "status": "healthy" if db_status == "healthy" else "degraded",
        "database_status": db_status,
        "target_dw": settings.DB_NAME
    }

@app.post(f"{settings.API_V1_STR}/chat", response_model=ChatResponse, tags=["Agentic AI"])
def process_chat_prompt(request: ChatRequest):
    """
    Main Agentic AI Chat Endpoint.
    Translates Natural Language queries into SQL, executes read-only queries against Gold schema,
    summarizes key insights, and provides data visualization specs.
    """
    try:
        response = orchestrator.process_request(request)
        return response
    except Exception as e:
        logger.error(f"Error processing agent request: {str(e)}", exc_info=True)
        # Never expose internal stack traces or file paths
        err_msg = str(e) if "Security Enforcement" in str(e) else "An error occurred while processing your request."
        raise HTTPException(status_code=500, detail=err_msg)


@app.get(f"{settings.API_V1_STR}/kpis", response_model=KPIOverviewResponse, tags=["Analytics"])
def get_kpi_overview():
    """Returns high-level summary KPIs (Total Content, Movies, TV Shows, Avg Duration, Top Country, Top Genre)."""
    try:
        return fetch_kpi_overview()
    except Exception as e:
        logger.error(f"Error fetching KPI overview: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get(f"{settings.API_V1_STR}/schema", tags=["Metadata"])
def get_warehouse_schema():
    """Serves database schema definitions for the Gold star schema & Metadata views."""
    try:
        return get_database_schema_info()
    except Exception as e:
        logger.error(f"Error fetching warehouse schema: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
