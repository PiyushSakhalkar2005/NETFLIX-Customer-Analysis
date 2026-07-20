-- Audit Framework Queries

-- 1. Get the last successful run execution
SELECT pipeline_run_id, pipeline_name, batch_id, start_time, end_time, execution_duration_ms, execution_status
FROM metadata.pipeline_runs
WHERE execution_status = 'SUCCESS'
ORDER BY start_time DESC
LIMIT 1;

-- 2. Find all failed run executions
SELECT pipeline_run_id, pipeline_name, batch_id, start_time, end_time, execution_status
FROM metadata.pipeline_runs
WHERE execution_status = 'FAILED'
ORDER BY start_time DESC;

-- 3. Total records processed (reads vs writes) by stage and pipeline run
SELECT pipeline_run_id, pipeline_stage, SUM(records_read) as total_read, SUM(records_written) as total_written, SUM(records_inserted) as total_inserted, SUM(records_updated) as total_updated
FROM metadata.pipeline_steps
GROUP BY pipeline_run_id, pipeline_stage
ORDER BY pipeline_run_id DESC;

-- 4. Pipeline duration trends over historical runs
SELECT pipeline_run_id, pipeline_name, start_time, execution_duration_ms, execution_status
FROM metadata.pipeline_runs
ORDER BY start_time ASC;

-- 5. Identify the most failed pipeline stages
SELECT pipeline_stage, COUNT(*) as failure_count
FROM metadata.pipeline_steps
WHERE execution_status = 'FAILED'
GROUP BY pipeline_stage
ORDER BY failure_count DESC;

-- 6. Daily batch processing history
SELECT CAST(start_time AS DATE) as process_date, COUNT(DISTINCT batch_id) as batches_run, COUNT(DISTINCT pipeline_run_id) as runs_completed, SUM(records_written) as daily_records_written
FROM metadata.pipeline_runs r
JOIN metadata.pipeline_steps s ON r.pipeline_run_id = s.pipeline_run_id
GROUP BY CAST(start_time AS DATE)
ORDER BY process_date DESC;
