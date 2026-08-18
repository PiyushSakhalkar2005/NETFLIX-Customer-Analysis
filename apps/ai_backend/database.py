import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import List, Dict, Any, Tuple
from apps.ai_backend.config import settings

logger = logging.getLogger("ai_database")

def get_db_connection():
    """Returns a new psycopg2 connection to netflix_dw_new."""
    try:
        conn = psycopg2.connect(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            dbname=settings.DB_NAME
        )
        return conn
    except Exception as e:
        logger.error(f"Database connection error to {settings.DB_NAME}: {str(e)}")
        raise e

def execute_read_query(query: str, params: Tuple = None) -> List[Dict[str, Any]]:
    """
    Executes a read-only SQL query against PostgreSQL and returns results as dictionaries.
    Enforces basic SQL safety checks (SELECT / WITH only).
    """
    cleaned_query = query.strip().upper()
    if not (cleaned_query.startswith("SELECT") or cleaned_query.startswith("WITH")):
        raise ValueError("Security Enforcement: Only read-only SELECT or WITH statements are allowed.")
    
    dangerous_keywords = ["DROP ", "DELETE ", "UPDATE ", "INSERT ", "ALTER ", "TRUNCATE ", "GRANT ", "REVOKE "]
    for kw in dangerous_keywords:
        if kw in cleaned_query:
            raise ValueError(f"Security Enforcement: Forbidden SQL keyword '{kw.strip()}' detected.")
            
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            results = cursor.fetchall()
            return [dict(row) for row in results]
    finally:
        conn.close()

def get_database_schema_info() -> Dict[str, Any]:
    """Returns information about available gold tables and metadata KPI views."""
    query = """
    SELECT table_schema, table_name, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema IN ('gold', 'metadata')
    ORDER BY table_schema, table_name, ordinal_position;
    """
    rows = execute_read_query(query)
    
    schemas = {}
    for r in rows:
        t_key = f"{r['table_schema']}.{r['table_name']}"
        if t_key not in schemas:
            schemas[t_key] = []
        schemas[t_key].append({"column": r['column_name'], "type": r['data_type']})
    return schemas
