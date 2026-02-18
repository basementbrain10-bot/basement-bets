import sys
import os
import json
import datetime
from datetime import datetime as dt, timedelta, timezone

# Allow running from repo root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.database import get_admin_db_connection, get_db_connection, _exec
from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2


def ensure_table():
    sql = """
    CREATE TABLE IF NOT EXISTS daily_top_picks (
      date_et DATE NOT NULL,
      event_id TEXT NOT NULL,
      league TEXT NOT NULL DEFAULT 'NCAAM',
      computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      model_version TEXT,

      is_actionable BOOLEAN NOT NULL DEFAULT FALSE,
      reason TEXT,

      rec_json JSONB,
      context_json JSONB,

      PRIMARY KEY(date_et, event_id)
    );

    CREATE INDEX IF NOT EXISTS ix_daily_top_picks_date_league ON daily_top_picks(date_et, league);
    """
    with get_admin_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def fetch_event_ids_for_date(date_et: str, limit_games: int = 250):
    # Use same dedupe logic as /api/board.
    q = """
    WITH base_events AS (
      SELECT e.*,
        DATE(e.start_time AT TIME ZONE 'America/New_York') AS day_et,
        LOWER(regexp_replace(
          replace(replace(replace(replace(COALESCE(e.home_team,''), 'North Carolina State', 'NC State'), 'N.C. State', 'NC State'), 'N.C. St.', 'NC State'), 'NC St.', 'NC State'),
          '[^a-z0-9]+', '', 'g'
        )) AS home_key,
        LOWER(regexp_replace(
          replace(replace(replace(replace(COALESCE(e.away_team,''), 'North Carolina State', 'NC State'), 'N.C. State', 'NC State'), 'N.C. St.', 'NC State'), 'NC St.', 'NC State'),
          '[^a-z0-9]+', '', 'g'
        )) AS away_key,
        CASE
          WHEN e.id LIKE 'action:ncaam:%%' THEN 0
          WHEN e.id LIKE 'espn:ncaam:%%' THEN 1
          ELSE 2
        END AS src_rank
      FROM events e
      WHERE e.league='NCAAM'
        AND DATE(e.start_time AT TIME ZONE 'America/New_York') = %(d)s
    ),
    dedup_events AS (
      SELECT *
      FROM (
        SELECT *,
          ROW_NUMBER() OVER (PARTITION BY league, day_et, home_key, away_key ORDER BY src_rank ASC, start_time ASC) AS rn
        FROM base_events
      ) t
      WHERE rn = 1
    )
    SELECT id
    FROM dedup_events
    ORDER BY start_time ASC
    LIMIT %(lim)s
    """
    with get_db_connection() as conn:
        rows = _exec(conn, q, {"d": date_et, "lim": int(limit_games)}).fetchall()
        return [r['id'] if isinstance(r, dict) else r[0] for r in rows]


def upsert_pick(date_et: str, event_id: str, res: dict):
    rec = None
    try:
        rec = (res.get('recommendations') or [None])[0]
    except Exception:
        rec = None

    is_actionable = bool(rec)
    reason = None
    if not is_actionable:
        reason = (res.get('block_reason') or res.get('headline') or res.get('recommendation') or res.get('error') or 'No bet') if isinstance(res, dict) else 'No bet'

    # context_json is a JSON string currently in the model; store as JSONB if possible
    ctx = None
    try:
        cj = res.get('context_json')
        if cj:
            ctx = json.loads(cj) if isinstance(cj, str) else cj
    except Exception:
        ctx = None

    sql = """
    INSERT INTO daily_top_picks(date_et, event_id, league, computed_at, model_version, is_actionable, reason, rec_json, context_json)
    VALUES (%(d)s, %(eid)s, 'NCAAM', NOW(), %(mv)s, %(act)s, %(reason)s, %(rec)s::jsonb, %(ctx)s::jsonb)
    ON CONFLICT (date_et, event_id) DO UPDATE SET
      computed_at = EXCLUDED.computed_at,
      model_version = EXCLUDED.model_version,
      is_actionable = EXCLUDED.is_actionable,
      reason = EXCLUDED.reason,
      rec_json = EXCLUDED.rec_json,
      context_json = COALESCE(EXCLUDED.context_json, daily_top_picks.context_json)
    """
    payload = {
        "d": date_et,
        "eid": event_id,
        "mv": res.get('model_version') if isinstance(res, dict) else None,
        "act": bool(is_actionable),
        "reason": reason,
        "rec": json.dumps(rec) if rec is not None else None,
        "ctx": json.dumps(ctx) if ctx is not None else None,
    }

    with get_db_connection() as conn:
        _exec(conn, sql, payload)
        conn.commit()


def main():
    # date comes from env or argv
    date_et = None
    if len(sys.argv) > 1:
        date_et = sys.argv[1]
    if not date_et:
        date_et = dt.now(timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=-5))).strftime('%Y-%m-%d')
        # better: ask DB for ET date
        try:
            with get_db_connection() as conn:
                date_et = _exec(conn, "SELECT (NOW() AT TIME ZONE 'America/New_York')::date::text").fetchone()[0]
        except Exception:
            pass

    limit_games = int(os.getenv('TOP_PICKS_LIMIT_GAMES', '250'))

    print(f"[{dt.now().isoformat()}] build_daily_top_picks date_et={date_et} limit_games={limit_games}")
    ensure_table()

    event_ids = fetch_event_ids_for_date(date_et, limit_games=limit_games)
    print(f"events: {len(event_ids)}")

    model = NCAAMMarketFirstModelV2()

    ok = 0
    err = 0
    for eid in event_ids:
        try:
            res = model.analyze(eid, relax_gates=True, persist=False)
            upsert_pick(date_et, eid, res if isinstance(res, dict) else {})
            ok += 1
        except Exception as e:
            err += 1
            # still upsert a no-bet row with error reason
            try:
                upsert_pick(date_et, eid, {"recommendations": [], "error": str(e), "model_version": getattr(model, 'VERSION', None), "block_reason": str(e)})
            except Exception:
                pass

    print(f"done ok={ok} err={err}")


if __name__ == '__main__':
    main()
