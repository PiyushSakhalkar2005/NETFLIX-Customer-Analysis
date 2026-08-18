# Netflix Medallion Pipeline - Baseline (Before Test) Report

**Generated Timestamp**: 2026-08-12T05:28:46Z  
**Environment**: Local (PySpark + PostgreSQL)

---

## 1. Verified PostgreSQL Baseline Row Counts

| Layer | Table Name | Verified PostgreSQL Row Count |
| :--- | :--- | :--- |
| **Bronze** | `bronze.bronze_netflix_csv` | **26,421** |
| **Silver** | `silver.silver_titles` | **8,810** |
| **Silver SCD2** | `silver.silver_titles_scd2` | **8,812** |
| **Silver Relational** | `silver.silver_country` | **10,846** |
| **Silver Relational** | `silver.silver_genres` | **19,328** |
| **Silver Relational** | `silver.silver_directors` | **9,614** |
| **Silver Relational** | `silver.silver_cast` | **64,954** |
| **Gold** | `gold.fact_content` | **25,884** |

---

## 2. Baseline Pipeline Watermark

- **Pipeline Name**: `netflix_titles_pipeline`
- **Current Baseline Watermark**: `2026-08-12 05:21:58.952136`
- **Watermark Source**: `data/metadata/watermarks.json` / `metadata.pipeline_watermarks`

---

## 3. Baseline Summary

The PostgreSQL database and PySpark Medallion layers are currently verified and stable.
All future test records inserted must have an `ingestion_timestamp` strictly greater than `2026-08-12 05:21:58.952136` to trigger delta identification by the incremental processing engine.
