-- PostgreSQL DDL for Metadata and Audit Framework

CREATE SCHEMA IF NOT EXISTS metadata;

-- 1. Table to track each pipeline run execution (Run level)
CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    pipeline_run_id VARCHAR(100) PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    batch_id VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    execution_duration_ms INT,
    execution_status VARCHAR(20) NOT NULL, -- SUCCESS, FAILED, SKIPPED
    execution_host VARCHAR(100),
    spark_application_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- 2. Table to track individual stages within a run (Stage level)
CREATE TABLE IF NOT EXISTS metadata.pipeline_steps (
    step_id SERIAL PRIMARY KEY,
    pipeline_run_id VARCHAR(100) NOT NULL REFERENCES metadata.pipeline_runs(pipeline_run_id),
    pipeline_stage VARCHAR(50) NOT NULL, -- Ingestion, Bronze, Validation, Silver, Gold, etc.
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    execution_duration_ms INT NOT NULL,
    execution_status VARCHAR(20) NOT NULL,
    records_read INT DEFAULT 0,
    records_written INT DEFAULT 0,
    records_inserted INT DEFAULT 0,
    records_updated INT DEFAULT 0,
    records_deleted INT DEFAULT 0,
    records_rejected INT DEFAULT 0,
    duplicate_count INT DEFAULT 0,
    validation_failures INT DEFAULT 0,
    error_message TEXT
);

-- 3. Table to track dataset metadata properties
CREATE TABLE IF NOT EXISTS metadata.dataset_metadata (
    dataset_id SERIAL PRIMARY KEY,
    pipeline_run_id VARCHAR(100) NOT NULL,
    dataset_name VARCHAR(100) NOT NULL, -- e.g. silver_titles, gold_fact_content
    storage_format VARCHAR(20) NOT NULL, -- PARQUET, CSV, POSTGRESQL
    destination_path TEXT NOT NULL,
    record_count INT NOT NULL,
    schema_definition TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- 4. Audit Log table for recording raw system notifications/warnings
CREATE TABLE IF NOT EXISTS metadata.audit_log (
    log_id SERIAL PRIMARY KEY,
    pipeline_run_id VARCHAR(100),
    log_level VARCHAR(10) NOT NULL,
    log_message TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);
