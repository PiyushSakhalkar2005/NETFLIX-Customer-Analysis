-- PostgreSQL schema for Pipeline Watermark tracking

CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.pipeline_watermarks (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_processed_timestamp TIMESTAMP NOT NULL,
    last_successful_batch_id VARCHAR(100) NOT NULL,
    last_business_key VARCHAR(100) DEFAULT 'show_id',
    last_run_duration_ms BIGINT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

-- Insert initial values for the pipeline
INSERT INTO metadata.pipeline_watermarks (pipeline_name, last_processed_timestamp, last_successful_batch_id, last_business_key, last_run_duration_ms)
VALUES ('netflix_titles_pipeline', '1970-01-01 00:00:00', 'INIT', 'show_id', 0)
ON CONFLICT (pipeline_name) DO NOTHING;
