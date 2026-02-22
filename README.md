
# Basement Bets

A personal sports betting tracker and analysis platform.

## Setup

### Environment
1.  Copy `.env.template` to `.env`.
2.  Populate `POSTGRES_URL` (or `DATABASE_URL`).
3.  Populate `BASEMENT_PASSWORD` for admin access.

### Database
This project uses **Postgres** (Neon / Vercel Postgres recommended). SQLite is no longer supported for runtime.

**Initialize Schema:**
```bash
./run.sh
# Then call:
curl -X POST http://localhost:8000/api/admin/init-db
```
*Note: This is non-destructive. Returns success if tables exist.*

**Migration (Optional)**
If you have data in a legacy `data/bets.db` SQLite file:
```bash
python3 scripts/migrate_sqlite_to_postgres.py
```

### Running Locally
```bash
./run.sh
```
Starts Backend (FastAPI: 8000) and Frontend (Vite: 5173).

## Developer Notes

**Reset Database:**
To wipe all data and start fresh:
1.  Set `BASEMENT_DB_RESET=1` in `.env`.
2.  Restart server or run init script.
3.  Remove the variable immediately.

**Smoke Test:**
```bash
python3 scripts/db_smoke_test.py
```

## Multi-Agent Decision System (AgentOS)

The backend features an additive AgentOS allowing granular rules, human-in-the-loop review queues, and Kelly-optimized EV sorting inside horizontal pipelines.

**Core Agents:**
- `EventOpsAgent` / `MarketDataAgent`: Generates isolated event & offer states obeying boundaries.
- `PricingAgent(NCAAM)`: Wraps statistical models matching output to unified `FairPrice` arrays. 
- `EdgeEVAgent`: Responsible exclusively for strictly-typed float Math (Removes previously unstable sorting by string issues).
- `RiskManagerAgent`: Correlation haircuts and Kelly fraction scaling.
- `BetBuilderAgent`: Trims output by daily limits (e.g. `AGENTS_MAX_PLAYS_PER_DAY=5`).
- `JournalAgent`: **Serverless Safe**. Uses one connection pool max to insert large `decision_runs` and `pending_decisions` blocks simultaneously.
- `PerformanceAuditorAgent`: Nightly analyzer matching OK decisions to Game Results via `brier` scoring.

**Enable via `.env`:**
```dotenv
AGENTS_ENABLED=true
AGENTS_IDEMPOTENCY_WINDOW_SECONDS=90
AGENTS_SIZING_MODE=fractional_kelly
AGENTS_REVIEW_CONFIDENCE_THRESHOLD=0.55
```
