# Power BI → Netflix AI Integration Guide

This document explains the deep-linking and context-passing integration between the Power BI dashboard (`FINAL Netflix dashboard.pbix`) and the Netflix Agentic AI Analytics Platform.

---

## 1. Complete Integration Architecture

```text
Power BI Report Visuals
       │
       ▼
"Ask AI Analyst" Action Button / Card Link
       │
       ▼ (Passes URL Query String: ?query=...&country=...&year=...&type=...)
Netflix AI Frontend UI (Port 3000)
       │
       ▼ (Displays "⚡ POWER BI CONTEXT" Banner)
Backend API (/api/v1/chat)
       │
       ▼ (Injects WHERE Clauses into SQL Queries)
PostgreSQL Gold Data Warehouse
       │
       ▼
Multi-Agent Response (Answer, Key Insights, Filtered Visual Chart)
```

---

## 2. Supported Context Parameters

The Netflix AI frontend and backend support 4 standard URL query string parameters:

| Parameter | Type | Example | Description |
|---|---|---|---|
| `query` / `q` | String | `What are the top genres?` | Initial natural language question. |
| `country` | String | `India` | Filters Gold SQL queries to a specific country origin (`c.country = 'India'`). |
| `year` | Integer | `2020` | Filters release year (`f.release_year = 2020`). |
| `type` / `content_type` | String | `TV Show` | Filters format type (`t.type = 'TV Show'`). |

---

## 3. Example Deep-Link Launch URL

```text
http://localhost:3000/?query=What+are+the+top+genres%3F&country=India&year=2020&type=TV+Show
```

### Frontend Context Display
When launched with URL parameters, the AI frontend automatically renders a context banner in the sidebar:

```text
⚡ POWER BI CONTEXT
Country: India | Year: 2020 | Type: TV Show
[← Reset Filter]
```

---

## 4. How Context Modifies Agent Query Execution

When context parameters are passed to `DataAgent`, generated SQL queries dynamically append corresponding `WHERE` conditions:

```sql
-- Executed Gold DW Query with Power BI Context
SELECT 
    g.genre AS genre_name,
    COUNT(DISTINCT f.title_key) AS titles_count
FROM gold.fact_content f
JOIN gold.dim_genre g ON f.genre_key = g.genre_key
JOIN gold.dim_country c ON f.country_key = c.country_key
JOIN gold.dim_type t ON f.type_key = t.type_key
WHERE g.genre <> 'Unknown' 
  AND c.country = 'India' 
  AND f.release_year = 2020 
  AND t.type = 'TV Show'
GROUP BY g.genre
ORDER BY titles_count DESC 
LIMIT 10;
```

---

## 5. Security & Isolation Guarantee

* **Read-Only Protection:** Power BI deep links cannot trigger data mutation or DDL execution.
* **Separation of Layers:** The Power BI `.pbix` report reads from `metadata.v_kpi_*` SQL views; the AI system operates on top of the same views and Gold star schema tables without altering the Power BI file structure.
