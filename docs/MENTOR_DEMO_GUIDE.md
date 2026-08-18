# Netflix AI Analytics - Mentor Demonstration Guide

This guide outlines a 5-step demonstration sequence and provides concise answers for potential mentor questions regarding system architecture, data engineering, machine learning, Power BI integration, and agentic AI design.

---

## 1. Demonstration Sequence

### Demo 1 — Basic Data & Visualization Query
* **User Prompt:** `"What are the top 5 countries by Netflix content?"`
* **Click Prompt Chip:** `🌍 Top 5 Countries`
* **Agents Triggered:** `Data Agent` → `Insight Agent` → `Visualization Agent`
* **What to Highlight to Mentor:**
  - Show the **Intent Badge** (`Data Extraction & Visualization`).
  - Show the **Multi-Agent Trajectory Card** (`● Data Agent — Completed`, `● Insight Agent — Completed`, `● Visualization Agent — Completed`).
  - Point out the executed **Read-Only SQL Query** on `metadata.v_kpi_country_distribution`.
  - Highlight the dynamic **Bar Chart** rendering United States (3,690 titles), India (1,046), UK (806), Canada (445), France (393).

---

### Demo 2 — Support & Domain Definition Query
* **User Prompt:** `"What does Movie Ratio mean?"`
* **Click Prompt Chip:** `ℹ️ Movie Ratio`
* **Agents Triggered:** `Support Agent`
* **What to Highlight to Mentor:**
  - Show execution time: **`0.05 ms`**.
  - Emphasize that **no SQL database queries were run unnecessarily** for non-analytical domain questions.
  - Show formula explanation (`(Total Movies / Total Titles) * 100`) and benchmark figure (`69.62%`).

---

### Demo 3 — Machine Learning Anomaly Detection
* **User Prompt:** `"Which countries have unusual content patterns?"`
* **Click Prompt Chip:** `🔍 Unusual Patterns (ML)`
* **Agents Triggered:** `Data Agent` → `ML Agent` → `Insight Agent`
* **What to Highlight to Mentor:**
  - Point out **ML Agent** execution of **Isolation Forest & Z-Score Analysis**.
  - Show multi-dimensional feature space (`titles_count`, `movie_count`, `tv_count`, `avg_movie_duration`, `avg_content_age`).
  - Explain structural outlier findings (e.g. India's film-heavy profile >90%, Japan's TV animation focus).

---

### Demo 4 — Machine Learning Regression Forecasting
* **User Prompt:** `"Predict content growth."`
* **Click Prompt Chip:** `🔮 Predict Growth (ML)`
* **Agents Triggered:** `Data Agent` → `ML Agent` → `Insight Agent` → `Visualization Agent`
* **What to Highlight to Mentor:**
  - Highlight **Ordinary Least Squares (OLS) Linear Regression** fitted on historical releases (1995–2021).
  - Point out regression metrics: $R^2 = 0.6981$, slope $= +47.16$ titles/year.
  - Show the 5-year prediction line chart (2022–2026).
  - Emphasize the model disclaimer: *"Predictions represent statistical regression models based on past trend data."*

---

### Demo 5 — Power BI Deep-Link & Context Flow
* **Browser URL:** `http://localhost:3000/?query=What+are+the+top+genres%3F&country=India&year=2020&type=TV+Show`
* **What to Highlight to Mentor:**
  - Show the **Power BI Context Banner** in the left sidebar: `POWER BI CONTEXT: Country: India | Year: 2020 | Type: TV Show`.
  - Show that the SQL query dynamically appends context filters: `WHERE g.genre <> 'Unknown' AND c.country = 'India'`.
  - Show the rendered **Donut Chart** visualization.
  - Show the **← Reset** button to clear Power BI filters on demand.

---

## 2. Prepared Mentor Questions & Answers

### Architecture
* **Q: Why did you use multiple specialized agents instead of one monolithic agent?**
  * *Answer:* Single LLMs/agents suffer from prompt drift and unpredictable tool misuse when handling complex tasks. Separating responsibilities into specialized agents (Data, ML, Support, Insight, Viz) enforces strict scoping, deterministic security controls, and faster execution.
* **Q: Why do you need an Orchestrator Router?**
  * *Answer:* The Orchestrator acts as the supervisor, performing intent classification to decide the minimal required set of agents for a query (e.g. routing support questions in 0.05ms without SQL execution).
* **Q: What happens if an agent fails?**
  * *Answer:* The Orchestrator catches exceptions gracefully, records the state as `Failed` in the trajectory log, and returns a safe fallback message without exposing system tracebacks or crashing the server.

### Data & Security
* **Q: Why query the Gold layer instead of Bronze or Silver?**
  * *Answer:* The Gold layer implements a Kimball Star Schema (`fact_content` and dimension tables) and pre-aggregated metadata views (`v_kpi_*`). Querying Gold guarantees optimal analytical performance and consistent metrics aligned with Power BI reports.
* **Q: How is SQL security protected against injection or destructive queries?**
  * *Answer:* Security is enforced at two levels: (1) Prompt-level keyword scanning for DDL/DML keywords (`DROP`, `DELETE`, `UPDATE`, `TRUNCATE`), and (2) SQL parser validation (`sqlparse`) enforcing read-only `SELECT` / `WITH` execution against PostgreSQL.

### Machine Learning
* **Q: Why use Linear Regression for content growth?**
  * *Answer:* OLS Linear Regression provides a transparent, non-black-box baseline ($R^2 = 0.6981$) for annual historical release sequence trends.
* **Q: Why use Isolation Forest for anomaly detection?**
  * *Answer:* Isolation Forest is an unsupervised non-parametric tree ensemble that isolates multi-attribute catalog outliers (across volume, format ratio, and duration) without requiring labeled anomaly datasets.
* **Q: How did you select features for ML models?**
  * *Answer:* We selected feature columns directly existing in the Gold schema (`titles_count`, `movie_count`, `tv_count`, `avg_movie_duration`, `avg_content_age`, `release_year`).

### Power BI & Agentic AI
* **Q: How does Power BI communicate with the AI platform?**
  * *Answer:* Power BI passes active filter state via URL query parameters (`?query=...&country=...&year=...&type=...`) when the user clicks the report link, pre-populating context in the AI system.
* **Q: What makes this solution "Agentic AI"?**
  * *Answer:* The system exhibits agentic behavior through intent classification, dynamic tool invocation (SQL tool, ML models, chart generator), state tracking across execution trajectories, and contextual reasoning over warehouse metrics.
