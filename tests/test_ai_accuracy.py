import sys
import os
import json
import time
import urllib.request
from typing import Dict, Any, List

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Ensure UTF-8 output encoding for Windows console
sys.stdout.reconfigure(encoding='utf-8')

from tests.ground_truth import calculate_ground_truth

API_URL = "http://127.0.0.1:8000/api/v1/chat"

def run_accuracy_audit():
    print("==========================================================================")
    print("STARTING COMPREHENSIVE NETFLIX AI ACCURACY AUDIT (105 TEST CASES)")
    print("==========================================================================")

    gt = calculate_ground_truth()

    with open("tests/ai_accuracy_cases.json", "r", encoding="utf-8") as f:
        cases = json.load(f)

    results = []
    passed_count = 0
    failed_count = 0

    failure_categories = {
        "METRIC_MISMATCH": 0,
        "NUMERIC_MISMATCH": 0,
        "UNIT_MISMATCH": 0,
        "ROUTING_MISMATCH": 0,
        "FILTER_MISMATCH": 0,
        "SQL_MISMATCH": 0
    }

    start_time = time.time()

    for idx, case in enumerate(cases, 1):
        prompt = case["prompt"]
        exp_metric = case["expected_metric"]
        context = case.get("context")

        payload = {"query": prompt}
        if context:
            payload["context"] = context

        req = urllib.request.Request(
            API_URL, 
            data=json.dumps(payload).encode("utf-8"), 
            headers={"Content-Type": "application/json"}
        )

        status = "PASS"
        fail_reasons = []
        actual_metric = "UNKNOWN"
        ai_val = None
        sql_query = "N/A"
        intent = "N/A"
        agents_used = []

        try:
            with urllib.request.urlopen(req) as res:
                resp_data = json.loads(res.read().decode("utf-8"))
                answer = resp_data.get("answer", "")
                sql_query = resp_data.get("sql_query", "N/A")
                intent = resp_data.get("intent", "N/A")
                agents_used = resp_data.get("agents_used", [])
                rows = resp_data.get("data", [])

                # 1. Semantic Intent / Metric Identification
                if "duration" in prompt.lower() and "average_content_age" in str(rows) and "average_movie_duration" not in str(rows):
                    status = "FAIL"
                    fail_reasons.append("METRIC_MISMATCH: Requested movie duration but returned content age")
                    failure_categories["METRIC_MISMATCH"] += 1
                elif "season" in prompt.lower() and "average_content_age" in str(rows) and "average_tv_seasons" not in str(rows):
                    status = "FAIL"
                    fail_reasons.append("METRIC_MISMATCH: Requested TV seasons but returned content age")
                    failure_categories["METRIC_MISMATCH"] += 1

                # 2. Ground Truth Value Comparison
                if exp_metric in gt:
                    expected_info = gt[exp_metric]
                    if isinstance(expected_info, dict) and "val" in expected_info:
                        exp_val = expected_info["val"]
                        exp_unit = expected_info.get("unit", "")
                        
                        # Validate value in answer or data rows
                        raw_exp = str(exp_val)
                        formatted_exp = f"{exp_val:,}" if isinstance(exp_val, int) else str(exp_val)
                        
                        if formatted_exp not in answer and raw_exp not in answer and (not rows or raw_exp not in str(rows)):
                            status = "FAIL"
                            fail_reasons.append(f"NUMERIC_MISMATCH: Expected ground-truth value {exp_val} ({exp_unit}) not found in response")
                            failure_categories["NUMERIC_MISMATCH"] += 1
                        
                        if exp_unit and exp_unit.lower() not in answer.lower() and (not rows or exp_unit.lower() not in str(rows)):
                            # Minor unit warning check
                            pass

                # 3. Power BI Filter Verification
                if context and context.get("country") == "India" and "India" not in sql_query:
                    status = "FAIL"
                    fail_reasons.append("FILTER_MISMATCH: Power BI country filter 'India' ignored in SQL")
                    failure_categories["FILTER_MISMATCH"] += 1

                # 4. Support Agent Non-SQL Verification
                if exp_metric == "support_query" and "Support Agent" not in agents_used:
                    status = "FAIL"
                    fail_reasons.append("ROUTING_MISMATCH: Conceptual question did not route to Support Agent")
                    failure_categories["ROUTING_MISMATCH"] += 1

        except Exception as e:
            status = "FAIL"
            fail_reasons.append(f"API_ERROR: {str(e)}")

        if status == "PASS":
            passed_count += 1
        else:
            failed_count += 1

        res_entry = {
            "id": case["id"],
            "prompt": prompt,
            "expected_metric": exp_metric,
            "intent": intent,
            "agents_used": agents_used,
            "sql_query": sql_query,
            "status": status,
            "failure_reasons": fail_reasons
        }
        results.append(res_entry)

        # Console Progress
        sym = "✅ PASS" if status == "PASS" else "❌ FAIL"
        print(f"[{idx:03d}/105] {sym} | '{prompt[:35]}...' -> {intent} ({', '.join(agents_used)})")
        if fail_reasons:
            print(f"       Reasons: {'; '.join(fail_reasons)}")

    elapsed = round(time.time() - start_time, 2)

    os.makedirs("tests/results", exist_ok=True)
    with open("tests/results/accuracy_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print("\n==========================================================================")
    print("COMPREHENSIVE AI ACCURACY AUDIT SUMMARY REPORT")
    print("==========================================================================")
    print(f"Total Test Cases Run     : {len(cases)}")
    print(f"Passed Test Cases        : {passed_count}")
    print(f"Failed Test Cases        : {failed_count}")
    print(f"Overall Accuracy Rate    : {round((passed_count / len(cases)) * 100, 2)}%")
    print(f"Audit Execution Time     : {elapsed} seconds")
    print("\n--- Failure Category Breakdown ---")
    for cat, count in failure_categories.items():
        print(f"  - {cat:20s}: {count}")

    print("==========================================================================")
    print("Machine-readable audit results saved to tests/results/accuracy_results.json")

    return results, passed_count, failed_count

if __name__ == "__main__":
    run_accuracy_audit()
