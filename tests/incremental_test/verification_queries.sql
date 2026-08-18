-- Verification Queries for Incremental Pipeline Execution Evidence

-- 1. Verify 5 TEST records in Bronze layer
SELECT show_id, title, ingestion_timestamp, batch_id, load_type 
FROM bronze.bronze_netflix_csv 
WHERE show_id IN ('TEST001','TEST002','TEST003','TEST004','TEST005')
ORDER BY show_id, ingestion_timestamp;

-- 2. Verify 5 TEST records in Silver Titles
SELECT show_id, title, type, date_added, release_year, rating, duration 
FROM silver.silver_titles 
WHERE show_id IN ('TEST001','TEST002','TEST003','TEST004','TEST005')
ORDER BY show_id;

-- 3. Verify Relational Normalization in Silver Lookup Tables
SELECT 'silver_country' AS lookup_table, show_id, country AS value FROM silver.silver_country WHERE show_id IN ('TEST001','TEST002','TEST003','TEST004','TEST005')
UNION ALL
SELECT 'silver_genres', show_id, genre FROM silver.silver_genres WHERE show_id IN ('TEST001','TEST002','TEST003','TEST004','TEST005')
UNION ALL
SELECT 'silver_directors', show_id, director FROM silver.silver_directors WHERE show_id IN ('TEST001','TEST002','TEST003','TEST004','TEST005')
UNION ALL
SELECT 'silver_cast', show_id, cast_member FROM silver.silver_cast WHERE show_id IN ('TEST001','TEST002','TEST003','TEST004','TEST005')
ORDER BY lookup_table, show_id;

-- 4. Verify SCD Type 2 state & history tracking (including update for TEST001)
SELECT show_id, version_number, rating, is_current, effective_start_date, effective_end_date 
FROM silver.silver_titles_scd2 
WHERE show_id IN ('TEST001','TEST002','TEST003','TEST004','TEST005')
ORDER BY show_id, version_number;

-- 5. Verify Gold Fact Content Star Schema joins
SELECT f.title_key, t.show_id, t.title, f.duration_minutes, f.season_count, f.ingestion_timestamp
FROM gold.fact_content f 
JOIN gold.dim_title t ON f.title_key = t.title_key 
WHERE t.show_id IN ('TEST001','TEST002','TEST003','TEST004','TEST005')
ORDER BY t.show_id;

-- 6. Verify Pipeline Watermark state
SELECT pipeline_name, last_processed_timestamp, last_successful_batch_id, updated_at 
FROM metadata.pipeline_watermarks 
WHERE pipeline_name = 'netflix_titles_pipeline';
