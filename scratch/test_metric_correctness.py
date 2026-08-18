import urllib.request
import json
import sys

# Ensure UTF-8 output
sys.stdout.reconfigure(encoding='utf-8')

API_URL = "http://127.0.0.1:8000/api/v1/chat"

test_prompts = [
    ("tell me the average movie duration", ["99.5", "minutes"], ["11.8", "Content Age"]),
    ("what is the average content age", ["11.8", "Content Age"], ["99.5", "minutes"]),
    ("how many movies are there", ["6,131", "Movies"], ["11.8"]),
    ("how many TV shows are there", ["2,676", "TV Shows"], ["11.8"]),
    ("what is the movie ratio", ["69.62%"], ["11.8"]),
    ("what is the longest movie", ["Black Mirror: Bandersnatch", "312 min"], []),
    ("what is the shortest movie", ["Silent", "3 min"], []),
    ("show me average movie duration", ["99.5", "minutes"], [])
]

print("==========================================================")
print("TESTING ALL 8 METRIC CORRECTNESS PROMPTS")
print("==========================================================")

for q, expected_keywords, forbidden_keywords in test_prompts:
    payload = json.dumps({"query": q}).encode("utf-8")
    req = urllib.request.Request(API_URL, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as res:
        data = json.loads(res.read().decode("utf-8"))
        answer = data["answer"]
        intent = data["intent"]
        agents = data["agents_used"]
        chart = data.get("chart")
        
        print(f"\nQuery: '{q}'")
        print(f"  Intent Detected : {intent}")
        print(f"  Agents Triggered: {', '.join(agents)}")
        print(f"  Chart Spec      : {chart['chart_type'] if chart else 'None'}")
        print(f"  Answer Output:\n{answer}")

        # Assert expected keywords are present
        for kw in expected_keywords:
            assert kw.lower() in answer.lower(), f"FAILED: Expected keyword '{kw}' not found in answer for '{q}'!"
        
        # Assert forbidden keywords are NOT present for movie duration query
        if forbidden_keywords:
            for fkw in forbidden_keywords:
                assert fkw.lower() not in answer.lower() or "exclusion" in answer.lower(), f"FAILED: Forbidden keyword '{fkw}' found in answer for '{q}'!"

        print("  --> TEST PASSED!")

print("\n==========================================================")
print("ALL 8 METRIC CORRECTNESS TESTS PASSED PERFECTLY!")
print("==========================================================")
