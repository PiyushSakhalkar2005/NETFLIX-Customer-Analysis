import os
import sys
import uuid

project_root = r"d:\Piyu\My Projects\Netflix project"
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set HADOOP_HOME environment variable for local Windows Spark executions
hadoop_dir = os.path.join(project_root, "hadoop")
if os.path.exists(hadoop_dir):
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, to_date

def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("IngestIncrementalTest") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
        
    test_csv = os.path.join(project_root, "tests", "incremental_test", "test_incremental_5_records.csv")
    df_raw = spark.read \
        .option("header", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(test_csv)
        
    batch_id = f"test-batch-{str(uuid.uuid4())[:8]}"
    run_id = f"test-run-{str(uuid.uuid4())[:8]}"
    
    df_bronze = df_raw \
        .withColumn("batch_id", lit(batch_id)) \
        .withColumn("pipeline_run_id", lit(run_id)) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("netflix_medallion_pipeline")) \
        .withColumn("source_file", lit("test_incremental_5_records.csv")) \
        .withColumn("load_type", lit("INCREMENTAL")) \
        .withColumn("ingestion_date", to_date(current_timestamp()))
        
    dest_path = os.path.join(project_root, "data", "bronze", "bronze_netflix_csv")
    print(f"Appending {df_bronze.count()} test records to Bronze Parquet at: {dest_path}")
    
    df_bronze.write.mode("append").format("parquet").partitionBy("ingestion_date").save(dest_path)
    
    df_all = spark.read.parquet(dest_path)
    print(f"New Total Rows in Bronze Parquet: {df_all.count()}")
    
    print("\nInserted TEST records:")
    df_all.filter(df_all.show_id.isin(["TEST001","TEST002","TEST003","TEST004","TEST005"])) \
          .select("show_id", "title", "ingestion_timestamp", "batch_id") \
          .show(truncate=False)
          
    spark.stop()

if __name__ == "__main__":
    main()
