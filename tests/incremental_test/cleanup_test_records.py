import os
import sys
import psycopg2

project_root = r"d:\Piyu\My Projects\Netflix project"

def main():
    print("=" * 60)
    print("INCREMENTAL TEST CLEANUP SCRIPT")
    print("Deletes ONLY test records: TEST001, TEST002, TEST003, TEST004, TEST005")
    print("=" * 60)
    
    conn = psycopg2.connect(host='localhost', port=5432, user='postgres', password='root', dbname='netflix_dw_new')
    cur = conn.cursor()
    
    test_ids = ('TEST001','TEST002','TEST003','TEST004','TEST005')
    
    try:
        # Delete from Gold Fact via joining dim_title
        cur.execute("""
            DELETE FROM gold.fact_content 
            WHERE title_key IN (
                SELECT title_key FROM gold.dim_title WHERE show_id IN %s
            );
        """, (test_ids,))
        print(f"  Deleted {cur.rowcount} rows from gold.fact_content")
        
        # Delete from Gold dim_title
        cur.execute("DELETE FROM gold.dim_title WHERE show_id IN %s;", (test_ids,))
        print(f"  Deleted {cur.rowcount} rows from gold.dim_title")
        
        # Delete from Silver tables
        tables = [
            "silver.silver_cast",
            "silver.silver_country",
            "silver.silver_directors",
            "silver.silver_genres",
            "silver.silver_titles",
            "silver.silver_titles_scd2",
            "bronze.bronze_netflix_csv"
        ]
        
        for t in tables:
            cur.execute(f"DELETE FROM {t} WHERE show_id IN %s;", (test_ids,))
            print(f"  Deleted {cur.rowcount} rows from {t}")
            
        conn.commit()
        print("Cleanup completed successfully. Production dataset remains intact.")
    except Exception as e:
        conn.rollback()
        print("Error during cleanup:", e)
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
