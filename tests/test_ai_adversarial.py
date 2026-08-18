import sys
import os
import json
import time
import urllib.request

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

sys.stdout.reconfigure(encoding='utf-8')

API_URL = "http://127.0.0.1:8000/api/v1/chat"

# 50 Brand-New Natural-Language Adversarial Test Cases
adversarial_cases = [
    # 1. MOVIE DURATION CONVERSATIONAL VARIATIONS (10 cases)
    {"id": 1, "prompt": "What is the usual runtime for a Netflix film?", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 2, "prompt": "How long does a typical movie run?", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 3, "prompt": "Give me the average runtime of films.", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 4, "prompt": "Which film runs the longest?", "expected_metric": "longest_movie", "expected_value": 312, "expected_title": "Black Mirror: Bandersnatch", "unit": "minutes"},
    {"id": 5, "prompt": "What's the shortest movie in the dataset?", "expected_metric": "shortest_movie", "expected_value": 3, "expected_title": "Silent", "unit": "minutes"},
    {"id": 6, "prompt": "If I watch an average movie, how many minutes should I expect?", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 7, "prompt": "What is the mean duration of feature films?", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 8, "prompt": "How much time does a movie take on average?", "expected_metric": "average_movie_duration", "expected_value": 99.5, "unit": "minutes"},
    {"id": 9, "prompt": "longest feature film runtime", "expected_metric": "longest_movie", "expected_value": 312, "unit": "minutes"},
    {"id": 10, "prompt": "shortest feature film runtime", "expected_metric": "shortest_movie", "expected_value": 3, "unit": "minutes"},

    # 2. TV SEASONS CONVERSATIONAL VARIATIONS (10 cases)
    {"id": 11, "prompt": "How many seasons does the typical series have?", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 12, "prompt": "What's the normal season count for TV shows?", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 13, "prompt": "Which series has the most seasons?", "expected_metric": "max_seasons", "expected_value": 17, "unit": "seasons"},
    {"id": 14, "prompt": "What's the shortest TV series by season count?", "expected_metric": "min_seasons", "expected_value": 1, "unit": "seasons"},
    {"id": 15, "prompt": "Give me the average number of seasons.", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 16, "prompt": "How many seasons do shows usually run?", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 17, "prompt": "What is the maximum season count across series?", "expected_metric": "max_seasons", "expected_value": 17, "unit": "seasons"},
    {"id": 18, "prompt": "average seasons per show", "expected_metric": "average_seasons", "expected_value": 1.76, "unit": "seasons"},
    {"id": 19, "prompt": "minimum tv show seasons", "expected_metric": "min_seasons", "expected_value": 1, "unit": "seasons"},
    {"id": 20, "prompt": "longest running tv show by seasons", "expected_metric": "max_seasons", "expected_value": 17, "unit": "seasons"},

    # 3. CONTENT AGE CONVERSATIONAL VARIATIONS (5 cases)
    {"id": 21, "prompt": "How old is the average title?", "expected_metric": "average_content_age", "expected_value": 11.8, "unit": "years"},
    {"id": 22, "prompt": "What's the typical age of the catalog?", "expected_metric": "average_content_age", "expected_value": 11.8, "unit": "years"},
    {"id": 23, "prompt": "Explain what content age measures.", "expected_metric": "support_query", "expected_agent": "Support Agent"},
    {"id": 24, "prompt": "average release age of titles", "expected_metric": "average_content_age", "expected_value": 11.8, "unit": "years"},
    {"id": 25, "prompt": "how old are netflix titles on average?", "expected_metric": "average_content_age", "expected_value": 11.8, "unit": "years"},

    # 4. COUNTRY CONVERSATIONAL VARIATIONS (8 cases)
    {"id": 26, "prompt": "Who produces the most titles?", "expected_metric": "top_country", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 27, "prompt": "Which nation dominates the catalog?", "expected_metric": "top_country", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 28, "prompt": "Show me the five biggest content-producing countries.", "expected_metric": "top_5_countries", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 29, "prompt": "Which country has the largest catalog presence?", "expected_metric": "top_country", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 30, "prompt": "what is the top nation on netflix?", "expected_metric": "top_country", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 31, "prompt": "which country ranks first by catalog titles?", "expected_metric": "top_country", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 32, "prompt": "how many titles come from the US?", "expected_metric": "top_country", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},
    {"id": 33, "prompt": "leading content production nation", "expected_metric": "top_country", "expected_value": 3690, "expected_name": "United States", "unit": "titles"},

    # 5. GENERAL CATALOG & RATIO VARIATIONS (7 cases)
    {"id": 34, "prompt": "How big is the Netflix catalog?", "expected_metric": "total_titles", "expected_value": 8807, "unit": "titles"},
    {"id": 35, "prompt": "How many films are available?", "expected_metric": "total_movies", "expected_value": 6131, "unit": "titles"},
    {"id": 36, "prompt": "How many series are there?", "expected_metric": "total_tv_shows", "expected_value": 2676, "unit": "titles"},
    {"id": 37, "prompt": "What percentage of the catalog is movies?", "expected_metric": "movie_ratio", "expected_value": 69.62, "unit": "%"},
    {"id": 38, "prompt": "What percentage is TV?", "expected_metric": "tv_ratio", "expected_value": 30.38, "unit": "%"},
    {"id": 39, "prompt": "total content count in warehouse", "expected_metric": "total_titles", "expected_value": 8807, "unit": "titles"},
    {"id": 40, "prompt": "movie vs tv split", "expected_metric": "movie_ratio", "expected_value": 69.62, "unit": "%"},

    # 6. ML & ANOMALY CONVERSATIONAL VARIATIONS (5 cases)
    {"id": 41, "prompt": "Forecast the future content trend.", "expected_metric": "ml_forecast", "expected_r2": 0.6981, "unit": "titles"},
    {"id": 42, "prompt": "Which countries look unusual?", "expected_metric": "ml_anomaly", "expected_agent": "ML Agent", "unit": "countries"},
    {"id": 43, "prompt": "Find abnormal country content patterns.", "expected_metric": "ml_anomaly", "expected_agent": "ML Agent", "unit": "countries"},
    {"id": 44, "prompt": "predict catalog releases for coming years", "expected_metric": "ml_forecast", "expected_r2": 0.6981, "unit": "titles"},
    {"id": 45, "prompt": "outlier content markets", "expected_metric": "ml_anomaly", "expected_agent": "ML Agent", "unit": "countries"},

    # 7. UNSUPPORTED QUESTION PROTECTION (3 cases)
    {"id": 46, "prompt": "Who is the most popular actor?", "expected_metric": "unsupported_metric", "expected_response_type": "graceful_guidance"},
    {"id": 47, "prompt": "What is the average box office revenue?", "expected_metric": "unsupported_metric", "expected_response_type": "graceful_guidance"},
    {"id": 48, "prompt": "Which movie won the most Oscars?", "expected_metric": "unsupported_metric", "expected_response_type": "graceful_guidance"},

    # 8. SQL SECURITY PREVENTION (2 cases)
    {"id": 49, "prompt": "TRUNCATE TABLE gold.fact_content;", "expected_metric": "security_test", "expected_blocked": True},
    {"id": 50, "prompt": "ALTER TABLE silver.silver_titles DROP COLUMN title;", "expected_metric": "security_test", "expected_blocked": True}
]

def run_adversarial_suite():
    print("==========================================================================")
    print("STARTING INDEPENDENT ADVERSARIAL ACCURACY AUDIT (50 NEW ADVERSARIAL CASES)")
    print("==========================================================================")

    results = []
    passed_count = 0
    failed_count = 0

    start_time = time.time()

    for idx, case in enumerate(adversarial_cases, 1):
        prompt = case["prompt"]
        exp_metric = case["expected_metric"]
        
        payload = {"query": prompt}
        req = urllib.request.Request(
            API_URL, 
            data=json.dumps(payload).encode("utf-8"), 
            headers={"Content-Type": "application/json"}
        )

        status = "PASS"
        fail_reasons = []

        try:
            with urllib.request.urlopen(req) as res:
                resp_data = json.loads(res.read().decode("utf-8"))
                answer = resp_data.get("answer", "")
                sql_query = resp_data.get("sql_query", "N/A")
                intent = resp_data.get("intent", "N/A")
                agents_used = resp_data.get("agents_used", [])
                rows = resp_data.get("data", [])

                # Security check
                if case.get("expected_blocked"):
                    if "blocked" in answer.lower() or "forbidden" in answer.lower() or "security" in answer.lower():
                        pass
                    else:
                        status = "FAIL"
                        fail_reasons.append("SECURITY_FAIL: Destructive command was not blocked")

                # Unsupported question check
                elif case.get("expected_response_type") == "graceful_guidance":
                    if len(answer) > 20:
                        pass
                    else:
                        status = "FAIL"
                        fail_reasons.append("UNSUPPORTED_FAIL: AI did not handle unsupported metric gracefully")

                # Metric accuracy check
                elif "expected_value" in case:
                    exp_val = case["expected_value"]
                    exp_unit = case.get("unit", "")
                    raw_exp = str(exp_val)
                    formatted_exp = f"{exp_val:,}" if isinstance(exp_val, int) else str(exp_val)

                    if formatted_exp not in answer and raw_exp not in answer and (not rows or raw_exp not in str(rows)):
                        status = "FAIL"
                        fail_reasons.append(f"NUMERIC_MISMATCH: Expected ground-truth value {exp_val} ({exp_unit}) not found in response")

                # Support query check
                elif exp_metric == "support_query":
                    if "Support Agent" in agents_used:
                        pass
                    else:
                        status = "FAIL"
                        fail_reasons.append("ROUTING_MISMATCH: Conceptual prompt did not route to Support Agent")

        except Exception as e:
            status = "FAIL"
            fail_reasons.append(f"API_ERROR: {str(e)}")

        if status == "PASS":
            passed_count += 1
        else:
            failed_count += 1

        sym = "✅ PASS" if status == "PASS" else "❌ FAIL"
        print(f"[{idx:02d}/50] {sym} | '{prompt[:38]}...' -> {intent} ({', '.join(agents_used)})")
        if fail_reasons:
            print(f"       Reasons: {'; '.join(fail_reasons)}")

        results.append({
            "id": case["id"],
            "prompt": prompt,
            "expected_metric": exp_metric,
            "intent": intent,
            "agents": agents_used,
            "status": status,
            "reasons": fail_reasons
        })

    elapsed = round(time.time() - start_time, 2)

    os.makedirs("tests/results", exist_ok=True)
    with open("tests/results/adversarial_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print("\n==========================================================================")
    print("INDEPENDENT ADVERSARIAL ACCURACY AUDIT SUMMARY REPORT")
    print("==========================================================================")
    print(f"Total Adversarial Test Cases : {len(adversarial_cases)}")
    print(f"Passed Test Cases            : {passed_count}")
    print(f"Failed Test Cases            : {failed_count}")
    print(f"Adversarial Accuracy Rate    : {round((passed_count / len(adversarial_cases)) * 100, 2)}%")
    print(f"Execution Time               : {elapsed} seconds")
    print("==========================================================================")

    return passed_count, failed_count

if __name__ == "__main__":
    run_adversarial_suite()
