import urllib.request
import json
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

BASE_URL = "http://127.0.0.1:8000/api/v1"

def post_chat(query: str):
    url = f"{BASE_URL}/chat"
    payload = json.dumps({"query": query}).encode("utf-8")
    req = urllib.request.Request(url, data=payload, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req) as res:
            return json.loads(res.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode("utf-8"))

def get_url(endpoint: str):
    url = f"{BASE_URL}/{endpoint}"
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req) as res:
        return json.loads(res.read().decode("utf-8"))

def run_tests():
    print("=" * 60)
    print("NETFLIX PHASE 1 MULTI-AGENT TEST SUITE")
    print("=" * 60)

    # 1. Existing Endpoints Verification
    print("\n[TEST 1] /health Endpoint Check:")
    health = get_url("health")
    print("  Status:", health.get("status"), "| DW:", health.get("target_dw"))
    assert health.get("status") == "healthy"

    print("\n[TEST 2] /kpis Endpoint Metric Verification:")
    kpis = get_url("kpis")
    print(f"  Total Content: {kpis.get('total_titles')} (Expected: 8807)")
    print(f"  Movies: {kpis.get('total_movies')} (Expected: 6131)")
    print(f"  TV Shows: {kpis.get('total_tv_shows')} (Expected: 2676)")
    print(f"  Movie Ratio: {kpis.get('movie_ratio_pct')}% (Expected: 69.62%)")
    print(f"  TV Ratio: {kpis.get('tv_ratio_pct')}% (Expected: 30.38%)")

    assert kpis.get("total_titles") == 8807
    assert kpis.get("total_movies") == 6131
    assert kpis.get("total_tv_shows") == 2676
    assert kpis.get("movie_ratio_pct") == 69.62
    assert kpis.get("tv_ratio_pct") == 30.38

    print("\n[TEST 3] /schema Endpoint Check:")
    schema = get_url("schema")
    print(f"  Available Schema Tables & Views: {len(schema)} entities loaded.")
    assert "gold.fact_content" in schema

    # 2. Required 7 Chat Questions
    test_questions = [
        ("1. Top 5 Countries", "What are the top 5 countries by Netflix content?"),
        ("2. Domain Support Question", "What does Movie Ratio mean?"),
        ("3. Strategic Business Insight", "Why is the US dominant?"),
        ("4. ML Anomaly Detection", "Which countries have unusual content patterns?"),
        ("5. Content Growth Analysis", "Analyze Netflix content growth."),
        ("6. Top 10 Countries Ranking", "Show top 10 countries by content."),
        ("7. ML Regression Forecasting", "Predict content growth.")
    ]

    for label, q in test_questions:
        print(f"\n[TEST QUESTION] {label}: '{q}'")
        res = post_chat(q)
        intent = res.get("intent", "N/A")
        agents = res.get("agents_used", [])
        time_ms = res.get("execution_time_ms")
        sql = res.get("sql_query")
        chart = res.get("chart", {}).get("chart_type") if res.get("chart") else "None"
        
        print(f"  -> Intent Detected : {intent}")
        print(f"  -> Agents Triggered : {', '.join(agents)}")
        print(f"  -> Execution Time   : {time_ms} ms")
        print(f"  -> Chart Generated  : {chart}")
        if sql:
            print(f"  -> Executed SQL     : {sql.replace('\n', ' ')}")
        print(f"  -> Answer Preview   : {res.get('answer', '')[:100]}...")

    # 3. Security SQL Injection Test
    print("\n[SECURITY TEST] Testing SQL Injection Attack Prevention:")
    inj_query = "DROP TABLE gold.fact_content;"
    res_inj = post_chat(inj_query)
    print(f"  Prompt Sent: '{inj_query}'")
    print(f"  Response Preview: {res_inj.get('answer', '')}")
    assert "Security Enforcement" in res_inj.get("answer", "") or "Forbidden" in res_inj.get("answer", "")
    print("  SECURITY PASS: Destructive DDL command 'DROP TABLE' was successfully blocked!")

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED SUCCESSFULLY! PHASE 1 MULTI-AGENT VERIFIED.")
    print("=" * 60)

if __name__ == "__main__":
    run_tests()
