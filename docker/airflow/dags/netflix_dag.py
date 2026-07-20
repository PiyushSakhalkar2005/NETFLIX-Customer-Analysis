import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

# Default arguments for Airflow Tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 7, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "netflix_data_pipeline",
    default_args=default_args,
    description="End-to-end Medallion architecture Netflix pipeline DAG run daily",
    schedule_interval="@daily",
    catchup=False,
) as dag:

    # 1. Initialize PostgreSQL target metadata schemas and watermarks tables
    init_db_schema = PostgresOperator(
        task_id="initialize_db_schema",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE SCHEMA IF NOT EXISTS metadata;
            CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
                pipeline_run_id VARCHAR(100) PRIMARY KEY,
                pipeline_name VARCHAR(100) NOT NULL,
                batch_id VARCHAR(100) NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                execution_duration_ms INT,
                execution_status VARCHAR(20) NOT NULL
            );
        """
    )

    # 2. Ingest landing zone CSV, JSON, and REST API records to raw storage
    ingest_landing = SparkSubmitOperator(
        task_id="ingest_landing_raw",
        application="/opt/airflow/src/ingestion/ingest_raw.py",
        conn_id="spark_default",
        verbose=True,
    )

    # 3. Load partition Parquet files into Bronze layer
    load_bronze = SparkSubmitOperator(
        task_id="load_bronze_layer",
        application="/opt/airflow/src/bronze/run_bronze_load.py",
        application_args=["--source", "netflix_csv"],
        conn_id="spark_default",
        verbose=True,
    )

    # 4. Run great expectations assertion rules checks on Bronze files
    validate_bronze = SparkSubmitOperator(
        task_id="validate_bronze_dq",
        application="/opt/airflow/src/quality/run_validation.py",
        application_args=["--source", "netflix_csv"],
        conn_id="spark_default",
        verbose=True,
    )

    # 5. Incremental Processing Engine: Runs Silver relational loads, SCD 1/2 merges, and Gold compiles
    orchestrate_incremental = SparkSubmitOperator(
        task_id="orchestrate_incremental_pipeline",
        application="/opt/airflow/src/silver/run_incremental_pipeline.py",
        conn_id="spark_default",
        verbose=True,
    )

    # Define DAG tasks dependencies tree
    init_db_schema >> ingest_landing >> load_bronze >> validate_bronze >> orchestrate_incremental
