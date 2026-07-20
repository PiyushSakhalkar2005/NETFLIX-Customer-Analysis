import os
import sys
import shutil
import pytest
from pyspark.sql import SparkSession

# Ensure project root is in system path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set HADOOP_HOME and PYTHONPATH for local Windows test run compatibility
hadoop_dir = os.path.join(project_root, "hadoop")
if os.path.exists(hadoop_dir):
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")
os.environ["PYTHONPATH"] = project_root + os.pathsep + os.environ.get("PYTHONPATH", "")

from src.bronze.bronze_loader import BronzeLoader
from src.silver.incremental_engine import IncrementalEngine
from src.utils.config_loader import ConfigLoader

@pytest.fixture(scope="module")
def spark_module():
    """Module-level SparkSession for end-to-end integration testing."""
    session = SparkSession.builder \
        .appName("NetflixE2EIntegrationTests") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .getOrCreate()
    yield session
    session.stop()

def test_pipeline_end_to_end_flow(spark_module, tmp_path):
    """End-to-end integration test verifying file outputs from Bronze to Gold."""
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    metadata_dir = tmp_path / "metadata"
    
    # Create required directories
    for d in [raw_dir, bronze_dir, silver_dir, gold_dir, metadata_dir]:
        d.mkdir(parents=True, exist_ok=True)
        
    # 1. Create a dummy CSV source landing file
    csv_landing = raw_dir / "netflix_titles.csv"
    with open(csv_landing, "w", encoding="utf-8") as f:
        f.write("show_id,type,title,director,cast,country,date_added,release_year,rating,duration,listed_in,description\n")
        f.write("s1,Movie,First Movie,Dir A,Cast A,India,\"July 14, 2026\",2026,PG-13,90 min,Dramas,Interesting plot\n")
        f.write("s2,TV Show,First Show,Dir B,Cast B,USA,\"July 14, 2026\",2025,TV-MA,1 Season,Comedies,Funny show\n")
        
    # Configure path mock variables
    config = {
        "storage": {
            "raw_dir": str(raw_dir),
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
            "gold_dir": str(gold_dir),
            "metadata_dir": str(metadata_dir)
        },
        "ingestion": {
            "sources": {
                "netflix_csv": {
                    "format": "csv",
                    "path": str(csv_landing),
                    "load_type": "FULL",
                    "options": {
                        "header": "true",
                        "inferSchema": "false",
                        "delimiter": ","
                    }
                }
            }
        },
        "silver": {
            "null_replacements": {
                "director": "Unknown Director",
                "cast": "Unknown Cast",
                "country": "Unknown Country",
                "rating": "UR",
                "duration": "0 min"
            }
        },
        "incremental_processing": {
            "load_type": "incremental",
            "business_key": "show_id",
            "watermark_file_path": str(metadata_dir / "watermarks.json")
        }
    }
    
    # 2. Run Bronze Loader directly
    loader = BronzeLoader(spark_module, config)
    loader.load_source_to_bronze(source_name="netflix_csv", pipeline_run_id="r-e2e")
    
    # Verify Bronze files exist in directory
    bronze_sub = bronze_dir / "bronze_netflix_csv"
    assert os.path.exists(bronze_sub)
    
    # 3. Run Incremental Processing Engine (Saves Silver + Gold Dimensions & Facts)
    engine = IncrementalEngine(spark_module, config)
    results = engine.run_pipeline(batch_id="b-e2e", run_id="r-e2e", force_full_load=True)
    
    assert results["status"] == "SUCCESS"
    assert results["records_processed"] == 2
    
    # Verify Silver Dimension master output folders
    assert os.path.exists(silver_dir / "silver_titles")
    assert os.path.exists(silver_dir / "silver_country")
    
    # Verify Gold Fact Table master partition subfolders
    assert os.path.exists(gold_dir / "fact_content")
    assert os.path.exists(gold_dir / "dim_title")
    
    # Verify watermarks checkpoint is created
    assert os.path.exists(metadata_dir / "watermarks.json")
