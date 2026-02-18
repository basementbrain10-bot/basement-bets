import sys
import os
import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.database import get_admin_db_connection


def ensure_table():
    sql = """
    CREATE TABLE IF NOT EXISTS data_health (
      source TEXT PRIMARY KEY,
      last_success_at TIMESTAMPTZ,
      last_row_count BIGINT,
      status TEXT NOT NULL DEFAULT 'unknown',
      notes TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """
    with get_admin_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def upsert(source: str, status: str, row_count: int | None = None, notes: str | None = None):
    sql = """
    INSERT INTO data_health(source, last_success_at, last_row_count, status, notes, updated_at)
    VALUES (%s, NOW(), %s, %s, %s, NOW())
    ON CONFLICT (source) DO UPDATE SET
      last_success_at=EXCLUDED.last_success_at,
      last_row_count=COALESCE(EXCLUDED.last_row_count, data_health.last_row_count),
      status=EXCLUDED.status,
      notes=EXCLUDED.notes,
      updated_at=NOW();
    """
    with get_admin_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (source, row_count, status, notes))
        conn.commit()


def main():
    print(f"[{datetime.datetime.now()}] update_data_health")
    ensure_table()

    # Odds health
    with get_admin_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM odds_snapshots WHERE captured_at >= NOW() - INTERVAL '6 hours'")
            n = cur.fetchone()[0]
    status = 'ok' if n and n > 0 else 'stale'
    upsert('odds', status=status, row_count=n, notes='rows in last 6h')

    # Torvik health
    with get_admin_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT COUNT(*) FROM bt_team_metrics_daily WHERE date >= (NOW() AT TIME ZONE 'America/New_York')::date - INTERVAL '2 days'")
                n = cur.fetchone()[0]
                status = 'ok' if n and n > 0 else 'stale'
            except Exception as e:
                n = None
                status = 'error'
                notes = str(e)
            else:
                notes = 'rows in last 2 days'
    upsert('torvik', status=status, row_count=n, notes=notes)

    # Results health
    with get_admin_db_connection() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT COUNT(*) FROM game_results WHERE updated_at >= NOW() - INTERVAL '2 days'")
                n = cur.fetchone()[0]
                status = 'ok' if n and n > 0 else 'stale'
                notes = 'rows updated in last 2 days'
            except Exception as e:
                n = None
                status = 'error'
                notes = str(e)
    upsert('results', status=status, row_count=n, notes=notes)

    print('[update_data_health] done')


if __name__ == '__main__':
    main()
