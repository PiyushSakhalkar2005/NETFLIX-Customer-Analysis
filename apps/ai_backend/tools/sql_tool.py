import sqlparse
import logging
from typing import List, Dict, Any, Tuple
from apps.ai_backend.database import get_db_connection

logger = logging.getLogger("sql_tool")

class SQLSecurityException(Exception):
    pass

def validate_and_format_sql(raw_sql: str) -> str:
    """Validates that a SQL string contains only read-only statements."""
    parsed = sqlparse.parse(raw_sql)
    if not parsed:
        raise SQLSecurityException("Empty or unparseable SQL statement.")
        
    for statement in parsed:
        stmt_type = statement.get_type()
        if stmt_type not in ["SELECT", "WITH", "UNKNOWN"]:
            # UNKNOWN covers complex WITH ... SELECT queries
            first_token = statement.token_first(skip_ws=True, skip_cm=True)
            if not first_token or first_token.value.upper() not in ["SELECT", "WITH"]:
                raise SQLSecurityException(f"Forbidden SQL Statement Type: {stmt_type}")
                
    upper_sql = raw_sql.upper()
    forbidden = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "CREATE", "GRANT", "REVOKE"]
    for word in forbidden:
        # Check whole word matches
        if f" {word} " in f" {upper_sql} " or upper_sql.startswith(f"{word} "):
            raise SQLSecurityException(f"Forbidden SQL Keyword detected: {word}")
            
    return raw_sql.strip()

def run_sql_query(sql: str) -> Tuple[List[Dict[str, Any]], str]:
    """
    Validates and executes SQL query.
    Returns (results, validated_sql).
    """
    clean_sql = validate_and_format_sql(sql)
    conn = get_db_connection()
    try:
        from psycopg2.extras import RealDictCursor
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(clean_sql)
            rows = cur.fetchall()
            return [dict(r) for r in rows], clean_sql
    except Exception as e:
        logger.error(f"SQL execution error for query [{clean_sql}]: {str(e)}")
        raise e
    finally:
        conn.close()
