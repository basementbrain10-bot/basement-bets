import json
import os
from typing import Optional

from src.database import get_admin_db_connection


def _force_reset() -> bool:
    return os.environ.get("BASEMENT_DB_RESET") == "1"


def init_council_signals_db():
    """Create a dedicated table to store council signals for offline analysis/training."""
    drops = ["DROP TABLE IF EXISTS council_signals CASCADE;"] if _force_reset() else []

    schema = """
    CREATE TABLE IF NOT EXISTS council_signals (
        id BIGSERIAL PRIMARY KEY,
        run_id TEXT,
        event_id TEXT,
        league TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        signals_json JSONB NOT NULL,
        sources JSONB,
        UNIQUE(run_id, event_id)
    );
    CREATE INDEX IF NOT EXISTS idx_council_signals_event_time ON council_signals(event_id, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_council_signals_run ON council_signals(run_id);
    """

    with get_admin_db_connection() as conn:
        with conn.cursor() as cur:
            for d in drops:
                try:
                    cur.execute(d)
                except Exception:
                    pass
            cur.execute(schema)
        conn.commit()

    print("Council signals table initialized.")
