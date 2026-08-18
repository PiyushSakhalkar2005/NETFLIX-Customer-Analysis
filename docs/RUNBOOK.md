# Netflix AI Platform - Operations & Startup Runbook

This document provides step-by-step instructions for running, operating, and verifying the Netflix Agentic AI Analytics Platform and backend service layer.

---

## 1. Prerequisites & System Requirements

- **Python:** 3.10+ (Tested on Python 3.13)
- **PostgreSQL:** Version 13+ running on `localhost:5432` with database `netflix_dw_new`
- **Required Libraries:** FastAPI, Uvicorn, Pydantic, Psycopg2, PyYAML, SQLParse, Scikit-Learn, NumPy

---

## 2. Environment Configuration

The backend reads settings from `apps/ai_backend/config.py` (or optional `.env` file):

```ini
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=root
DB_NAME=netflix_dw_new
READ_ONLY_MODE=true
```

---

## 3. System Startup Instructions

### Step 1: Start Backend API Service (Port 8000)
Open a terminal at the project root directory (`d:\Piyu\My Projects\Netflix project`):

```powershell
python -m uvicorn apps.ai_backend.main:app --host 127.0.0.1 --port 8000
```

*Expected Output:*
```text
INFO: Started server process
INFO: Waiting for application startup.
INFO: Application startup complete.
INFO: Uvicorn running on http://127.0.0.1:8000
```

### Step 2: Start Frontend Web Server (Port 3000)
Open a second terminal at the project root directory:

```powershell
python -m http.server 3000 --directory "apps/ai_frontend"
```

*Expected Output:*
```text
Serving HTTP on 0.0.0.0 port 3000 (http://0.0.0.0:3000/) ...
```

---

## 4. Web Application Access & Health Verification

1. **Open Frontend UI:** Open your browser and navigate to:
   ```text
   http://localhost:3000/
   ```
2. **Verify Backend Health:**
   ```text
   http://localhost:8000/api/v1/health
   ```
   *Expected JSON:* `{"status": "healthy", "database_status": "healthy", "target_dw": "netflix_dw_new"}`
3. **Verify API Interactive Documentation:**
   ```text
   http://localhost:8000/docs
   ```

---

## 5. Automated System Verification Test

Run the automated test suite to verify all 4 API endpoints, 6 agents, and SQL security enforcement:

```powershell
python tests/test_multi_agent_phase1.py
```

*Expected Result:* `ALL TESTS PASSED SUCCESSFULLY!`

---

## 6. System Shutdown Instructions

To gracefully stop the running servers:
1. In the backend terminal, press `Ctrl + C`.
2. In the frontend terminal, press `Ctrl + C`.

---

## 7. Troubleshooting Guide

| Issue | Cause | Resolution |
|---|---|---|
| `ConnectionRefusedError: [WinError 10061]` | PostgreSQL or FastAPI server is not running on port 8000. | Ensure Uvicorn server is started on port 8000. |
| `psycopg2.OperationalError: database "netflix_dw_new" does not exist` | Database name mismatch or local PG service down. | Ensure PostgreSQL service is running on port 5432 and `netflix_dw_new` exists. |
| `CORS Error in browser console` | Browser blocking cross-origin requests. | FastAPI is pre-configured with CORS middleware allowing `*`. Verify backend port is 8000. |
