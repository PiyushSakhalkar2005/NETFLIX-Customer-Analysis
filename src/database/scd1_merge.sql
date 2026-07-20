-- PostgreSQL SQL scripts for Slowly Changing Dimension (SCD) Type 1 Merge

-- 1. Merge Incoming changes into Fact/Dimensional Titles Table
MERGE INTO silver.silver_titles AS tgt
USING (
    -- Simulated staging source batch containing cleansed & standardized Netflix updates
    SELECT 
        show_id, type, title, date_added, release_year, rating, duration, description, 
        content_age, duration_minutes, season_count, is_recent_release, 
        batch_id, pipeline_run_id, ingestion_timestamp
    FROM staging_netflix_updates
) AS src
ON tgt.show_id = src.show_id
WHEN MATCHED AND (
    tgt.type IS DISTINCT FROM src.type OR
    tgt.title IS DISTINCT FROM src.title OR
    tgt.date_added IS DISTINCT FROM src.date_added OR
    tgt.release_year IS DISTINCT FROM src.release_year OR
    tgt.rating IS DISTINCT FROM src.rating OR
    tgt.duration IS DISTINCT FROM src.duration OR
    tgt.description IS DISTINCT FROM src.description OR
    tgt.content_age IS DISTINCT FROM src.content_age OR
    tgt.duration_minutes IS DISTINCT FROM src.duration_minutes OR
    tgt.season_count IS DISTINCT FROM src.season_count OR
    tgt.is_recent_release IS DISTINCT FROM src.is_recent_release
) THEN
    UPDATE SET 
        type = src.type,
        title = src.title,
        date_added = src.date_added,
        release_year = src.release_year,
        rating = src.rating,
        duration = src.duration,
        description = src.description,
        content_age = src.content_age,
        duration_minutes = src.duration_minutes,
        season_count = src.season_count,
        is_recent_release = src.is_recent_release,
        batch_id = src.batch_id,
        pipeline_run_id = src.pipeline_run_id,
        ingestion_timestamp = src.ingestion_timestamp
WHEN NOT MATCHED THEN
    INSERT (
        show_id, type, title, date_added, release_year, rating, duration, description,
        content_age, duration_minutes, season_count, is_recent_release,
        batch_id, pipeline_run_id, ingestion_timestamp
    ) VALUES (
        src.show_id, src.type, src.title, src.date_added, src.release_year, src.rating, src.duration, src.description,
        src.content_age, src.duration_minutes, src.season_count, src.is_recent_release,
        src.batch_id, src.pipeline_run_id, src.ingestion_timestamp
    );

-- 2. Lookup table updates (Overwriting old mappings for updated show_ids)
-- Example for Countries:
DELETE FROM silver.silver_country 
WHERE show_id IN (SELECT DISTINCT show_id FROM staging_netflix_updates);

INSERT INTO silver.silver_country (show_id, country)
SELECT DISTINCT show_id, country 
FROM staging_netflix_updates_country;
