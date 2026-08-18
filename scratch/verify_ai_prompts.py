import urllib.request
import json

API_URL = "http://127.0.0.1:8000/api/v1/chat"

prompts = [
    ("total how many movies are there", 6131),
    ("how many TV shows are there", 2676),
    ("how many total titles are there", 8807)
]

print("=== VERIFYING NETFLIX AI PROMPTS WITH UPDATED KPI VIEW ===")

for q, expected in prompts:
    payload = json.dumps({"query": q}).encode("utf-8")
    req = urllib.request.Request(API_URL, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as res:
        data = json.loads(res.read().decode("utf-8"))
        answer = data["answer"]
        rows = data["data"]
        print(f"\nQuery: '{q}'")
        print(f"  SQL Query: {data['sql_query']}")
        print(f"  Returned Data Rows: {rows}")
        print(f"  Answer Preview:\n{answer}")
        
        assert str(expected) in answer or (rows and str(expected) in str(rows[0].values())), f"Expected {expected} not found!"
        print(f"  VERIFIED: Found {expected} in AI response.")

print("\nALL AI PROMPTS VERIFIED SUCCESSFULLY!")
