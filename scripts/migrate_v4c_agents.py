import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.database import get_admin_db_connection

def migrate():
    print("Running Multi-Agent Decision System Migration...")
    
    schema = """
    CREATE TABLE IF NOT EXISTS decision_runs (
        run_id TEXT PRIMARY KEY,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        league TEXT NOT NULL,
        status TEXT NOT NULL,
        inputs_hash TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        model_version TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_decision_runs_inputs ON decision_runs(inputs_hash);

    CREATE TABLE IF NOT EXISTS decision_recommendations (
        id SERIAL PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES decision_runs(run_id) ON DELETE CASCADE,
        rec_id TEXT NOT NULL UNIQUE,
        event_id TEXT NOT NULL,
        market_type TEXT NOT NULL,
        side TEXT NOT NULL,
        line REAL,
        odds INTEGER,
        stake REAL,
        ev_pct REAL,
        confidence REAL,
        payload_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS pending_decisions (
        id SERIAL PRIMARY KEY,
        run_id TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        status TEXT NOT NULL DEFAULT 'PENDING',
        inputs_hash TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        reason TEXT,
        expires_at TIMESTAMPTZ
    );

    CREATE TABLE IF NOT EXISTS performance_reports (
        id SERIAL PRIMARY KEY,
        run_date TEXT NOT NULL,
        league TEXT NOT NULL,
        summary_json TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    
    with get_admin_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(schema)
        conn.commit()
    print("Migration successful: Added Multi-Agent schema objects.")

if __name__ == '__main__':
    migrate()
