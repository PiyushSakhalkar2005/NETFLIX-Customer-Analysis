import json

with open("tests/results/adversarial_results.json", "r", encoding="utf-8") as f:
    results = json.load(f)

failures = [r for r in results if r["status"] == "FAIL"]

print(f"=== ANALYZING {len(failures)} FAILING ADVERSARIAL CASES ===")
for idx, f in enumerate(failures, 1):
    print(f"\nFailure {idx}: ID {f['id']} | Prompt: '{f['prompt']}'")
    print(f"  Expected Metric: {f['expected_metric']}")
    print(f"  Intent: {f['intent']} | Agents: {', '.join(f['agents'])}")
    print(f"  Reasons: {'; '.join(f['reasons'])}")
