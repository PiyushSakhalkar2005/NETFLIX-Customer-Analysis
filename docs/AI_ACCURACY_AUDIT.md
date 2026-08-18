# Phase 6 — Comprehensive AI Answer Accuracy Audit Report

---

## 1. Executive Summary

A comprehensive, automated accuracy audit was conducted across the entire Netflix AI backend, database layer, and metric lineage.

- **Total Natural Language Test Cases Audited:** `105`
- **Passed Test Cases:** `88` (`83.81%`)
- **Failed Test Cases:** `17` (`16.19%`)
- **Execution Time:** `9.38 seconds`
- **Machine-Readable Results Location:** [`tests/results/accuracy_results.json`](file:///d:/Piyu/My%20Projects/Netflix%20project/tests/results/accuracy_results.json)

> [!IMPORTANT]
> **Audit Status:** Audit completed. **NO CODE MODIFICATIONS HAVE BEEN APPLIED YET** per Phase 6 directives. Recommended fixes are detailed in Section 5 below.

---

## 2. Failure Category Breakdown

| Category | Description | Count | Severity |
|---|---|---|---|
| **NUMERIC_MISMATCH** | AI response fallback string or SQL returned value did not match PostgreSQL ground truth | **14** | **HIGH** |
| **METRIC_MISMATCH** | User prompt requested a specific metric (e.g. TV seasons, movie length) but AI returned generic content age | **7** | **HIGH** |
| **ROUTING_MISMATCH** | Conceptual domain questions (`"What does Content Age mean?"`, `"explain gold schema"`) executed SQL instead of Support Agent | **2** | **MEDIUM** |
| **UNIT_MISMATCH** | Incorrect or missing unit representation | **0** | **LOW** |
| **FILTER_MISMATCH** | Power BI context filter injection failure | **0** | **LOW** |
| **SQL_MISMATCH** | Invalid SQL syntax or execution errors | **0** | **LOW** |

---

## 3. Discovered Grain & Data Vulnerabilities

1. **Exploded Fact Row Aggregation Risk:**
   `gold.fact_content` contains **25,879 exploded rows** across 8,807 unique catalog titles. Aggregating numeric metrics directly over `gold.fact_content` without `COUNT(DISTINCT title_key)` or `silver.silver_titles` distorts averages (e.g. content age shifts from `11.8` to `12.1`; movie duration shifts from `99.5 min` to `103.2 min`).

2. **Intent Keyword Synonym Gaps:**
   Phrasings like `"how long is a typical movie?"`, `"movie length"`, `"how long are tv series on average?"`, and `"season count"` lack keyword triggers in `orchestrator.py` and `data_agent.py`, causing them to fall back to `SELECT * FROM metadata.v_kpi_general_summary;`.

3. **Support Query Triggers:**
   Questions containing `"Content Age"` or `"gold schema"` were not captured by Support Agent intent rules, leading to unnecessary SQL execution.

---

## 4. Complete Audit Results Matrix (Sample & Highlights)

| ID | Natural Language Prompt | Expected Metric | AI Detected Intent | Ground Truth | AI Output Value | Status |
|---|---|---|---|---|---|---|
| 001 | `total how many movies are there` | `total_movies` | Simple Data Aggregation | **6,131** | **6,131** | **PASS** |
| 006 | `how many TV shows are there` | `total_tv_shows` | Simple Data Aggregation | **2,676** | **2,676** | **PASS** |
| 009 | `how many total titles are there` | `total_titles` | Simple Data Aggregation | **8,807** | **8,807** | **PASS** |
| 012 | `what is the average content age` | `average_content_age` | General Data Analysis | **11.8 yrs** | **11.8 yrs** | **PASS** |
| 016 | `tell me the average movie duration` | `average_movie_duration` | Movie & TV Content Duration Analysis | **99.5 min** | **99.5 min** | **PASS** |
| 018 | `how long is a typical movie?` | `average_movie_duration` | General Data Analysis | **99.5 min** | 11.8 yrs *(Fallback)* | **FAIL** |
| 024 | `what is the longest movie` | `longest_movie` | Movie & TV Content Duration Analysis | **312 min** | **312 min** | **PASS** |
| 027 | `what is the shortest movie` | `shortest_movie` | Movie & TV Content Duration Analysis | **3 min** | **3 min** | **PASS** |
| 036 | `average tv show seasons` | `average_seasons` | Movie & TV Content Duration Analysis | **1.76 seasons** | **1.76 seasons** | **PASS** |
| 038 | `how many seasons does a show usually have?` | `average_seasons` | Simple Data Aggregation | **1.76 seasons** | 11.8 yrs *(Fallback)* | **FAIL** |
| 051 | `which country makes most content?` | `top_country` | Data Extraction & Visualization | **United States (3,690)** | **United States (3,690)** | **PASS** |
| 066 | `what is the top genre on netflix?` | `top_genre` | Data Extraction & Visualization | **International Movies (2,752)** | **International Movies (2,752)** | **PASS** |
| 081 | `Predict content growth.` | `ml_forecast` | ML Forecasting & Growth Trend | **R² = 0.6981** | **R² = 0.6981** | **PASS** |
| 091 | `Which countries have unusual content patterns?` | `ml_anomaly` | ML Anomaly Analysis | **Isolation Forest Outliers** | **Outliers Flagged** | **PASS** |
| 096 | `What does Movie Ratio mean?` | `support_query` | Support Query | **69.62%** | **Support Guidance** | **PASS** |
| 097 | `What does Content Age mean?` | `support_query` | General Data Analysis | **Conceptual Answer** | Executed SQL | **FAIL** |
| 101 | `what are the top genres? (Context: India)` | `context_india_genres` | Data Extraction & Visualization | **International Movies (864)** | **International Movies (864)** | **PASS** |

---

## 5. Recommended Fixes (Ranked by Severity)

1. **High Severity — Expand Intent Keyword Triggers in `orchestrator.py` & `data_agent.py`:**
   - Add `"length"`, `"how long"`, `"runtime"`, `"typical movie"`, `"season count"`, `"how many seasons"` to the duration and TV season intent triggers.
2. **Medium Severity — Expand Support Agent Keyword Rules in `orchestrator.py`:**
   - Add `"content age"`, `"gold schema"`, `"schema"`, `"explain"` to Support Query triggers so conceptual questions do not execute unnecessary SQL.
3. **Low Severity — Add Fallback Guard in `insight_agent.py`:**
   - Ensure generic fallback answers check whether numeric fields match the prompt topic before generating structured responses.
