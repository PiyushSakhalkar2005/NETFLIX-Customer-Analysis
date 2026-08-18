-- Incremental Pipeline Test - Baseline (Before Test) Counts
-- Recorded on: 2026-08-12

SELECT 'bronze.bronze_netflix_csv' AS table_name, COUNT(*) AS row_count FROM bronze.bronze_netflix_csv
UNION ALL
SELECT 'silver.silver_titles', COUNT(*) FROM silver.silver_titles
UNION ALL
SELECT 'silver.silver_titles_scd2', COUNT(*) FROM silver.silver_titles_scd2
UNION ALL
SELECT 'silver.silver_country', COUNT(*) FROM silver.silver_country
UNION ALL
SELECT 'silver.silver_genres', COUNT(*) FROM silver.silver_genres
UNION ALL
SELECT 'silver.silver_directors', COUNT(*) FROM silver.silver_directors
UNION ALL
SELECT 'silver.silver_cast', COUNT(*) FROM silver.silver_cast
UNION ALL
SELECT 'gold.fact_content', COUNT(*) FROM gold.fact_content;

-- Watermark Check
SELECT pipeline_name, last_processed_timestamp, last_successful_batch_id, updated_at 
FROM metadata.pipeline_watermarks 
WHERE pipeline_name = 'netflix_titles_pipeline';
