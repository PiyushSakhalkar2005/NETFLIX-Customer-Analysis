import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional

class Settings(BaseSettings):
    PROJECT_NAME: str = "Netflix Agentic AI Analytics API"
    VERSION: str = "1.0.0"
    API_V1_STR: str = "/api/v1"
    
    # PostgreSQL Configuration
    DB_HOST: str = os.getenv("DB_HOST", "localhost")
    DB_PORT: int = int(os.getenv("DB_PORT", "5432"))
    DB_USER: str = os.getenv("DB_USER", "postgres")
    DB_PASSWORD: str = os.getenv("DB_PASSWORD", "root")
    DB_NAME: str = os.getenv("DB_NAME", "netflix_dw_new")
    
    # LLM Settings (Optional - fallback to intelligent SQL engine if not provided)
    OPENAI_API_KEY: Optional[str] = os.getenv("OPENAI_API_KEY", None)
    GEMINI_API_KEY: Optional[str] = os.getenv("GEMINI_API_KEY", None)
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gpt-4o-mini")
    
    # Security: Read-Only Enforcement
    READ_ONLY_MODE: bool = True
    ALLOWED_SCHEMAS: list[str] = ["gold", "silver", "metadata", "public"]
    
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

settings = Settings()
