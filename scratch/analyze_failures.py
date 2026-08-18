import json

with open("tests/results/accuracy_results.json", "r", encoding="utf-8") as f:
    results = json.load(f)

failures = [r for r in results if r["status"] == "FAIL"]

print(f"=== ANALYZING {len(failures)} FAILING TEST CASES ===")
for idx, f in enumerate(failures, 1):
    print(f"\nFailure {idx}: ID {f['id']} | Prompt: '{f['prompt']}'")
    print(f"  Expected Metric: {f['expected_metric']}")
    print(f"  Intent: {f['intent']} | Agents: {', '.join(f['agents_used'])}")
    print(f"  SQL: {f['sql_query']}")
    print(f"  Reasons: {'; '.join(f['failure_reasons'])}")
