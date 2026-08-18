# Netflix Medallion Architecture Pipeline v2.0 - Runbook

## Overview
This runbook provides step-by-step instructions for operating, orchestrating, and verifying the Netflix Version 2 Data Pipeline with Apache Airflow, PySpark Medallion Architecture (Bronze, Silver, Gold), Slowly Changing Dimensions (SCD Type 1 & 2), and incremental watermarking.

---

## 1. Prerequisites & Environment Setup

### Software Requirements
- **Docker Desktop** (running with Linux containers)
- **Python 3.10+** (with PySpark 3.5.1, Psycopg2, PyYAML, Great Expectations)
- **PostgreSQL 13+** (running on local host port `5432` with database `netflix_dw_new`)

### PostgreSQL Configuration Requirement
Ensure `pg_hba.conf` in your local PostgreSQL installation includes IPv4/IPv6 entries allowing container subnet connections:
```text
host    all             all             0.0.0.0/0               scram-sha-256
host    all             all             ::/0                    scram-sha-256
```
Reload PostgreSQL configuration:
```sql
SELECT pg_reload_conf();
```

---

## 2. Docker & Airflow Startup

To spin up the Apache Airflow cluster with pre-configured Java 17 and PySpark dependencies:

```powershell
# Navigate to the project docker directory
cd "d:\Piyu\My Projects\Netflix project\docker"

# Start Docker containers in detached mode (building image if updated)
docker compose up -d --build
```

### Verify Container Health
```powershell
docker ps
```
Required Running Containers:
1. `docker-airflow-webserver-1` (Port 8080)
2. `docker-airflow-scheduler-1`
3. `docker-postgres-1` (Airflow internal DB, Port 5435)

### Airflow UI Access
- **URL**: [http://localhost:8080](http://localhost:8080)
- **Username**: `admin`
- **Password**: `admin`

---

## 3. How to Add New Data for Incremental Ingestion

To add new incremental updates or additions without modifying the original 8,807-record CSV source dataset:

1. Open `data/raw/netflix_updates.json`.
2. Append new or updated JSON records.
   - For **Updates** to existing titles: Use the existing `show_id` (e.g., `s1`). The pipeline automatically performs SCD Type 2 versioning in `silver_titles_scd2` and SCD Type 1 overwrite in `silver_titles`.
   - For **New Titles**: Assign a new `show_id` (e.g., `s99994`).

Example JSON payload:
```json
[
  {
    "show_id": "s99994",
    "type": "Movie",
    "title": "New Incremental Release",
    "director": "Alex Rivera",
    "cast": "Actor X, Actor Y",
    "country": "United States",
    "date_added": "August 10, 2026",
    "release_year": "2026",
    "rating": "TV-MA",
    "duration": "105 min",
    "listed_in": "Sci-Fi & Fantasy",
    "description": "An exciting sci-fi release ingested via incremental load."
  }
]
```

---

## 4. Pipeline Execution Commands

### A. INCREMENTAL Mode (Default)
Runs ingestion for new raw updates, validates Bronze data quality, filters records beyond the last watermark timestamp, performs SCD 1/2 merges, rebuilds the Gold star schema, and updates watermarks.

#### Option 1: Via PySpark Runner (Local CLI)
```powershell
# Step 1: Ingest landing JSON to Bronze
python src/ingestion/ingest_raw.py --source netflix_incremental_json

# Step 2: Run Bronze Data Quality Validation
python src/quality/run_validation.py --source netflix_incremental_json

# Step 3: Run Incremental Engine (Silver -> Gold -> Watermark)
python src/silver/run_incremental_pipeline.py

# Step 4: Sync Parquet data to PostgreSQL
python -c "from src.database.pipeline_reset_and_reload import load_parquet_to_postgres, load_audit_metadata; load_parquet_to_postgres('INCREMENTAL'); load_audit_metadata()"
```

#### Option 2: Via Airflow DAG Orchestration
```powershell
# Unpause the DAG (first time only)
docker exec docker-airflow-scheduler-1 airflow dags unpause netflix_data_pipeline

# Trigger DAG run manually
docker exec docker-airflow-scheduler-1 airflow dags trigger netflix_data_pipeline
```

---

### B. FULL Mode (Full Re-Processing)
Reprocesses all records across Bronze, Silver, and Gold layers regardless of watermarks.

#### Via PySpark Runner (Local CLI)
```powershell
# Step 1: Ingest FULL CSV to Bronze
python src/ingestion/ingest_raw.py --source netflix_csv

# Step 2: Validate Full Bronze dataset
python src/quality/run_validation.py --source netflix_csv

# Step 3: Force Full Load in Incremental Engine
python src/silver/run_incremental_pipeline.py --force-full

# Step 4: Sync Parquet data to PostgreSQL
python -c "from src.database.pipeline_reset_and_reload import load_parquet_to_postgres, load_audit_metadata; load_parquet_to_postgres('FULL'); load_audit_metadata()"
```

---

## 5. Database Verification

To verify row counts, SCD tracking, and watermark state in PostgreSQL without modifying data:

```powershell
python -c "
import psycopg2
conn = psycopg2.connect('postgresql://postgres:root@localhost:5432/netflix_dw_new')
cur = conn.cursor()
tables = [('silver', 'silver_titles'), ('silver', 'silver_titles_scd2'), ('gold', 'dim_title'), ('gold', 'fact_content'), ('metadata', 'pipeline_watermarks')]
for s, t in tables:
    cur.execute(f'SELECT COUNT(*) FROM {s}.{t}')
    print(f'{s}.{t}:', cur.fetchone()[0])
cur.execute('SELECT * FROM metadata.pipeline_watermarks;')
print('Watermarks:', cur.fetchall())
conn.close()
"
```

---

## 6. Power BI Refresh Verification

Power BI connects directly to PostgreSQL database `netflix_dw_new` and consumes pre-built business analytical views under the `metadata` schema:
- `metadata.v_kpi_general_summary`
- `metadata.v_kpi_country_distribution`
- `metadata.v_kpi_genre_distribution`
- `metadata.v_kpi_top_directors`
- `metadata.v_kpi_rating_distribution`
- `metadata.v_kpi_release_trends`

To refresh Power BI:
1. Open `FINAL Netflix dashboard.pbix` in Power BI Desktop.
2. Click **Home -> Refresh**.
3. Verify that total titles reflect updated row counts (e.g., 8,810 titles) without any broken visual errors.

---

## 7. Troubleshooting Guide

| Issue | Cause | Resolution |
| :--- | :--- | :--- |
| `psycopg2.OperationalError: connection to server at host.docker.internal failed` | PostgreSQL `pg_hba.conf` rejecting Docker host IP | Add `host all all 0.0.0.0/0 scram-sha-256` to `pg_hba.conf` and execute `SELECT pg_reload_conf();`. |
| Airflow DAG Task `validate_bronze_dq` fails | Missing argument `--batch-id` | Ensure `src/quality/run_validation.py` uses fallback `batch_id = args.batch_id or str(uuid.uuid4())`. |
| Spark `java.lang.NoClassDefFoundError` or `java` not found in Docker | OpenJDK not installed in Airflow image | Use `docker/airflow/Dockerfile` with Java 17 and rebuild containers (`docker compose up -d --build`). |
| Watermark not updating after incremental run | Zero new records detected | Check `ingestion_timestamp` column in BronzeParquet; watermarks only advance when new timestamps strictly exceed previous checkpoint. |

