import os
import sys
import json
import time
import yaml
from datetime import datetime
from typing import Dict, Any, Tuple

# Ensure project root is in system path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Patch typing.io for Python 3.13+ compatibility with older PySpark versions
try:
    import typing.io
except ImportError:
    import types
    import typing
    typing_io = types.ModuleType("typing.io")
    typing_io.BinaryIO = typing.BinaryIO
    typing_io.TextIO = typing.TextIO
    sys.modules["typing.io"] = typing_io

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, concat_ws, array, lit, current_timestamp, length, trim, expr, to_date
from great_expectations.dataset.sparkdf_dataset import SparkDFDataset

from src.utils.logger import get_logger

logger = get_logger("DataQualityFramework")

class DataQualityFramework:
    """Enterprise Data Quality Framework using Great Expectations for PySpark DataFrames."""
    
    def __init__(self, spark: SparkSession, rules_path: str = None):
        self.spark = spark
        if rules_path is None:
            rules_path = os.path.join(project_root, "src", "config", "data_quality_rules.yaml")
        
        if not os.path.exists(rules_path):
            raise FileNotFoundError(f"DQ rules file not found at: {rules_path}")
            
        with open(rules_path, "r", encoding="utf-8") as f:
            self.rules = yaml.safe_load(f).get("validation_rules", {})
            
    def validate_dataset(self, df: DataFrame, source_name: str, batch_id: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Executes Great Expectations validation checks on the PySpark DataFrame.
        Returns:
            Tuple[bool, Dict[str, Any]]: (success_flag, validation_summary_metrics)
        """
        logger.info(f"Starting Data Quality validation for batch {batch_id} using Great Expectations...")
        start_time = time.time()
        
        # 1. Wrap the DataFrame in Great Expectations Spark Dataset
        ge_dataset = SparkDFDataset(df)
        
        # 2. Schema Validation
        req_cols = self.rules.get("required_columns", [])
        ge_dataset.expect_table_columns_to_match_set(column_set=req_cols)
        
        # 3. Null Validations
        for col_name in self.rules.get("null_checks", []):
            ge_dataset.expect_column_values_to_not_be_null(column=col_name)
            
        # 4. Uniqueness Validations
        for col_name in self.rules.get("unique_checks", []):
            ge_dataset.expect_column_values_to_be_unique(column=col_name)
            
        # 5. Datatype Validations
        # Validate release_year is castable to integer (regex or type matching)
        ge_dataset.expect_column_values_to_be_of_type(column="release_year", type_="StringType") # Raw is string
        
        # 6. Domain Validations
        val_sets = self.rules.get("value_sets", {})
        if "type" in val_sets:
            ge_dataset.expect_column_values_to_be_in_set(column="type", value_set=val_sets["type"])
        if "rating" in val_sets:
            # allow nulls for domain check in GE since null checks are separate
            ge_dataset.expect_column_values_to_be_in_set(column="rating", value_set=val_sets["rating"])
            
        # 7. Business Rules
        biz_rules = self.rules.get("business_rules", {})
        ge_dataset.expect_column_values_to_be_between(
            column="release_year", 
            min_value=1800, 
            max_value=biz_rules.get("max_release_year", 2026),
            parse_strings_as_datetimes=False
        )
        ge_dataset.expect_column_value_lengths_to_be_between(
            column="title", 
            min_value=biz_rules.get("min_title_length", 1)
        )
        ge_dataset.expect_column_values_to_not_be_null(column="duration")
        
        # 8. Data Completeness Checks
        for col_name in self.rules.get("completeness_checks", []):
            # Not null and not empty
            ge_dataset.expect_column_values_to_not_be_null(column=col_name)
            ge_dataset.expect_column_value_lengths_to_be_between(column=col_name, min_value=1)
            
        # 9. Extract GE Results
        validation_results = ge_dataset.validate()
        elapsed_time_ms = (time.time() - start_time) * 1000
        
        success = validation_results["success"]
        stats = validation_results["statistics"]
        
        logger.info(
            f"GE validation complete in {elapsed_time_ms:.2f}ms. "
            f"Success: {success}. Tests Run: {stats['evaluated_expectations']}. "
            f"Passed: {stats['successful_expectations']}. Failed: {stats['unsuccessful_expectations']}."
        )
        
        # Log individual failed expectations
        if not success:
            for res in validation_results["results"]:
                if not res["success"]:
                    logger.warning(
                        f"Expectation FAILED: {res['expectation_config']['expectation_type']} on "
                        f"kwargs {res['expectation_config']['kwargs']}. Details: {res.get('result', {})}"
                    )
                    
        return success, {
            "success": success,
            "evaluated_expectations": stats["evaluated_expectations"],
            "successful_expectations": stats["successful_expectations"],
            "unsuccessful_expectations": stats["unsuccessful_expectations"],
            "validation_duration_ms": elapsed_time_ms
        }

    def generate_html_report(self, results: Dict[str, Any], dest_path: str):
        """Generates a premium HTML Data Quality Report from Great Expectations results."""
        logger.info(f"Generating HTML report at: {dest_path}")
        
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        
        status_color = "#10B981" if results["success"] else "#EF4444"
        status_text = "PASSED" if results["success"] else "FAILED"
        
        html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>Data Quality Report - Netflix Pipeline</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background-color: #F3F4F6;
            margin: 0;
            padding: 40px;
            color: #1F2937;
        }}
        .container {{
            max-width: 1000px;
            margin: 0 auto;
            background: #FFFFFF;
            padding: 30px;
            border-radius: 12px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.05);
        }}
        .header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 2px solid #E5E7EB;
            padding-bottom: 20px;
            margin-bottom: 30px;
        }}
        .header h1 {{
            margin: 0;
            font-size: 28px;
            color: #111827;
        }}
        .status-badge {{
            background-color: {status_color};
            color: white;
            padding: 8px 16px;
            border-radius: 30px;
            font-weight: bold;
            font-size: 14px;
            letter-spacing: 1px;
        }}
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 20px;
            margin-bottom: 30px;
        }}
        .metric-card {{
            background: #F9FAFB;
            border: 1px solid #E5E7EB;
            padding: 20px;
            border-radius: 8px;
            text-align: center;
        }}
        .metric-card h3 {{
            margin: 0 0 10px 0;
            font-size: 14px;
            color: #6B7280;
            text-transform: uppercase;
        }}
        .metric-card p {{
            margin: 0;
            font-size: 24px;
            font-weight: bold;
            color: #111827;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Netflix Data Quality Validation Report</h1>
            <span class="status-badge">{status_text}</span>
        </div>
        <div class="metrics-grid">
            <div class="metric-card">
                <h3>Total Expectations</h3>
                <p>{results['evaluated_expectations']}</p>
            </div>
            <div class="metric-card">
                <h3>Passed</h3>
                <p style="color: #10B981;">{results['successful_expectations']}</p>
            </div>
            <div class="metric-card">
                <h3>Failed</h3>
                <p style="color: #EF4444;">{results['unsuccessful_expectations']}</p>
            </div>
            <div class="metric-card">
                <h3>Duration (ms)</h3>
                <p>{results['validation_duration_ms']:.2f}</p>
            </div>
        </div>
        <p style="color: #6B7280; font-size: 12px; margin-top: 50px;">Report Generated: {datetime.utcnow().isoformat()} UTC</p>
    </div>
</body>
</html>
"""
        with open(dest_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        logger.info("HTML Report written successfully.")

    def extract_and_save_rejected(self, df: DataFrame, dest_dir: str, batch_id: str) -> Tuple[int, int]:
        """
        Filters and extracts failed records based on config business logic,
        stamps them with reasons, and writes to target rejected directory.
        Returns:
            Tuple[int, int]: (passed_count, failed_count)
        """
        logger.info("Extracting failed records using parallel Spark filters...")
        
        errors = []
        
        # Null check errors
        for c in self.rules.get("null_checks", []):
            errors.append(when(col(c).isNull(), lit(f"Null value in required column '{c}'")))
            
        # Duplicate show_id errors
        df_counts = df.groupBy("show_id").count()
        df = df.join(df_counts, on="show_id", how="left")
        errors.append(when(col("count") > 1, lit("Duplicate show_id key value")))
        
        # Datatype check errors
        errors.append(when(col("release_year").cast("int").isNull() & col("release_year").isNotNull(), lit("release_year datatype is not castable to integer")))
        errors.append(when(to_date(trim(col("date_added")), "MMMM d, yyyy").isNull() & col("date_added").isNotNull(), lit("date_added has invalid date format")))
        
        # Domain checks
        val_sets = self.rules.get("value_sets", {})
        if "type" in val_sets:
            errors.append(when(~col("type").isin(val_sets["type"]), lit("Invalid type (not Movie or TV Show)")))
        if "rating" in val_sets:
            errors.append(when(~col("rating").isin(val_sets["rating"]) & col("rating").isNotNull(), lit("Invalid rating categorization")))
            
        # Business rules
        biz_rules = self.rules.get("business_rules", {})
        errors.append(when(col("release_year").cast("int") > biz_rules.get("max_release_year", 2026), lit("release_year cannot be in the future")))
        errors.append(when(col("duration").isNull() | (trim(col("duration")) == ""), lit("duration cannot be empty")))
        errors.append(when(length(trim(col("title"))) < biz_rules.get("min_title_length", 1), lit("title length is 0")))
        
        # Completeness
        for c in self.rules.get("completeness_checks", []):
            errors.append(when(col(c).isNull() | (trim(col(c)) == ""), lit(f"Required field '{c}' is blank or empty")))
            
        # Combine error reasons using Spark SQL array functions
        df_errors = df.withColumn("reasons", array(*errors))
        df_errors = df_errors.withColumn("reasons", expr("filter(reasons, x -> x IS NOT NULL)"))
        
        # Split into Valid and Rejected sets
        df_failed = df_errors.filter("size(reasons) > 0") \
            .withColumn("reason_for_failure", concat_ws(", ", col("reasons"))) \
            .withColumn("validation_timestamp", current_timestamp()) \
            .withColumn("batch_id", lit(batch_id)) \
            .drop("count", "reasons")
            
        df_passed = df_errors.filter("size(reasons) == 0").drop("count", "reasons")
        
        failed_count = df_failed.count()
        passed_count = df_passed.count()
        
        # Save rejected records if any failures occurred
        if failed_count > 0:
            os.makedirs(dest_dir, exist_ok=True)
            rejected_path = os.path.join(dest_dir, f"rejected_batch_{batch_id}")
            logger.warning(f"Detected {failed_count} invalid records. Writing to: {rejected_path}")
            
            df_failed.write \
                .mode("overwrite") \
                .format("parquet") \
                .save(rejected_path)
        else:
            logger.info("No invalid records detected in this batch.")
            
        return passed_count, failed_count
