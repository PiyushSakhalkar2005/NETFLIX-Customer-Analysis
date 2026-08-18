import urllib.request
import json
import sys

sys.stdout.reconfigure(encoding='utf-8')

API_URL = "http://127.0.0.1:8000/api/v1/chat"

step11_prompts = [
    "tell me the average movie duration",
    "which movie is the longest?",
    "how many seasons does a show usually have?",
    "what is the maximum number of seasons?",
    "What does Content Age mean?",
    "explain the gold schema",
    "what are the top 5 countries?",
    "how many movies are there?",
    "how many TV shows are there?",
    "predict content growth"
]

print("==========================================================================")
print("PHASE 7 STEP 11 MANUAL PROMPT INSPECTION")
print("==========================================================================")

for idx, q in enumerate(step11_prompts, 1):
    payload = json.dumps({"query": q}).encode("utf-8")
    req = urllib.request.Request(API_URL, data=payload, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as res:
        data = json.loads(res.read().decode("utf-8"))
        print(f"\n[{idx:02d}] Prompt: '{q}'")
        print(f"     Intent       : {data['intent']}")
        print(f"     Agents Used  : {', '.join(data['agents_used'])}")
        print(f"     Executed SQL : {data['sql_query']}")
        print(f"     Chart Spec   : {data['chart']['chart_type'] if data.get('chart') else 'None'}")
        print(f"     Returned Data: {data['data']}")
        print(f"     Answer Output:\n{data['answer']}")
        print("-" * 70)
