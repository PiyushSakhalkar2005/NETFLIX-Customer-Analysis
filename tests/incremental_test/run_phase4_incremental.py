import os
import sys
import subprocess
import json

project_root = r"d:\Piyu\My Projects\Netflix project"
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set environment
env = os.environ.copy()
env["HADOOP_HOME"] = os.path.join(project_root, "hadoop")
env["PATH"] = os.path.join(project_root, "hadoop", "bin") + os.pathsep + env.get("PATH", "")

from src.database.pipeline_reset_and_reload import load_parquet_to_postgres, load_audit_metadata

def main():
    print("=" * 60)
    print("PHASE 4: EXECUTING INCREMENTAL PIPELINE RUN")
    print("=" * 60)
    
    # 1. Read previous watermark
    wm_file = os.path.join(project_root, "data", "metadata", "watermarks.json")
    with open(wm_file, "r") as f:
        wm_before = json.load(f)["netflix_titles_pipeline"]["last_processed_timestamp"]
    print(f"Previous Watermark: {wm_before}")
    
    # 2. Execute Incremental Engine script
    cmd = [sys.executable, "src/silver/run_incremental_pipeline.py"]
    print(f"Executing command: {' '.join(cmd)}")
    
    res = subprocess.run(cmd, env=env, capture_output=True, text=True, cwd=project_root)
    print("--- PIPELINE STDOUT ---")
    print(res.stdout)
    if res.stderr:
        print("--- PIPELINE STDERR ---")
        print(res.stderr)
        
    if res.returncode != 0:
        raise RuntimeError("Incremental pipeline script failed!")
        
    # 3. Sync Parquet changes into PostgreSQL
    print("\nSyncing incremental Parquet updates to PostgreSQL via UPSERT...")
    load_parquet_to_postgres(mode="INCREMENTAL")
    load_audit_metadata()
    
    # 4. Read new watermark
    with open(wm_file, "r") as f:
        wm_after = json.load(f)["netflix_titles_pipeline"]["last_processed_timestamp"]
    print(f"\nNew Watermark: {wm_after}")
    print("PHASE 4 INCREMENTAL RUN COMPLETED SUCCESSFULLY.")

if __name__ == "__main__":
    main()
