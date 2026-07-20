-- PostgreSQL SQL scripts for Slowly Changing Dimension (SCD) Type 2 Merge

-- 1. Expire existing matching active records in Master Target where columns have changed
UPDATE silver.silver_titles_scd2 AS tgt
SET 
    is_current = FALSE,
    effective_end_date = CURRENT_DATE - INTERVAL '1 day',
    record_updated_timestamp = CURRENT_TIMESTAMP
FROM staging_netflix_updates AS src
WHERE tgt.show_id = src.show_id
  AND tgt.is_current = TRUE
  AND (
      tgt.type IS DISTINCT FROM src.type OR
      tgt.title IS DISTINCT FROM src.title OR
      tgt.rating IS DISTINCT FROM src.rating OR
      tgt.duration IS DISTINCT FROM src.duration OR
      tgt.description IS DISTINCT FROM src.description OR
      tgt.release_year IS DISTINCT FROM src.release_year
  );

-- 2. Insert new versions (both brand-new shows and new versions of expired shows)
INSERT INTO silver.silver_titles_scd2 (
    show_id, type, title, date_added, release_year, rating, duration, description,
    content_age, duration_minutes, season_count, is_recent_release,
    effective_start_date, effective_end_date, is_current, version_number,
    record_created_timestamp, record_updated_timestamp, batch_id, pipeline_run_id, ingestion_timestamp
)
SELECT 
    src.show_id, src.type, src.title, src.date_added, src.release_year, src.rating, src.duration, src.description,
    src.content_age, src.duration_minutes, src.season_count, src.is_recent_release,
    CURRENT_DATE AS effective_start_date,
    '9999-12-31'::DATE AS effective_end_date,
    TRUE AS is_current,
    COALESCE(prev.max_version, 0) + 1 AS version_number,
    CURRENT_TIMESTAMP AS record_created_timestamp,
    CURRENT_TIMESTAMP AS record_updated_timestamp,
    src.batch_id, src.pipeline_run_id, src.ingestion_timestamp
FROM staging_netflix_updates AS src
-- Fetch previous highest version number to increment
LEFT JOIN (
    SELECT show_id, MAX(version_number) AS max_version 
    FROM silver.silver_titles_scd2 
    GROUP BY show_id
) AS prev ON src.show_id = prev.show_id
-- Only insert if there is no currently active matching record in target
-- (Which means either it's a new show_id, or it was just expired by Step 1!)
WHERE NOT EXISTS (
    SELECT 1 FROM silver.silver_titles_scd2 AS active 
    WHERE active.show_id = src.show_id 
      AND active.is_current = TRUE
);
