
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

## DraftKings Automated Ingest

Scrapes "My Bets → Settled" nightly (2am EST / 07:00 UTC) and bulk-upserts into `bets` via Postgres.

### Architecture
- **Vercel Cron** → `POST /api/jobs/queue_sync/draftkings` → creates a `sync_jobs` row  
- **Local worker** (`scripts/sync_worker.py`) claims the job and runs the scraper  
- Scraper results stored in `bets` (idempotent via `external_id`) and logged in `book_ingest_runs`

### Setup

**1. One-time login (required)**

Launch Chrome with your persistent DK profile and log in manually:
```bash
DK_PROFILE_PATH=/path/to/your/chrome/profile python3 -c "
from src.scrapers.user_draftkings import DraftKingsScraper
DraftKingsScraper(profile_path='$DK_PROFILE_PATH').scrape_settled_bets_automated()
"
```
Log in via the browser window, then Ctrl+C. The session is saved to the profile.

**2. Configure `.env`**
```dotenv
DK_PROFILE_PATH=/absolute/path/to/persistent/chrome/profile
NEON_HOST=ep-your-host.us-east-2.aws.neon.tech
EGRESS_ALLOWLIST=draftkings.com,sportsbook.draftkings.com,${NEON_HOST}
EGRESS_MODE=restricted
DK_SCROLL_PAGES=5
NEWER_THAN_DAYS=7
MAX_BETS_PER_RUN=50
```

**3. Initialize DB schema** (first time only)
```bash
python3 -c "from src.database import init_dk_ingest_db; init_dk_ingest_db()"
```

**4. Run the worker**
```bash
python scripts/sync_worker.py --loop
```

### Manual queue + status check
```bash
# Queue a job now
curl -X POST http://localhost:8000/api/jobs/queue_sync/draftkings

# Check last ingest status
curl http://localhost:8000/api/ingest/status
```

### NEEDS_AUTH recovery
If the worker reports `NEEDS_AUTH`, re-open DraftKings with the same profile and log in again:
```bash
# Then restart the worker and re-queue
curl -X POST http://localhost:8000/api/jobs/queue_sync/draftkings
python scripts/sync_worker.py --once
```

### Egress note
`egress_guard.py` restricts Python HTTP client calls to the `EGRESS_ALLOWLIST`.
It does **not** control Selenium/Chrome browser asset fetching — use OS-level firewall rules for full network isolation.
