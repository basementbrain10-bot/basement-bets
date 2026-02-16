"""Losslessly normalize bets.status and bets.date.

- Preserve existing raw values into status_raw/date_raw (if not already set)
- Create a canonical ET date column date_et for stable YTD/all-time reporting
- Normalize status into WON/LOST/PUSH/CASHED_OUT/PENDING/UNKNOWN (configurable)

This does not delete any rows.

Run:
  source .venv_backtest/bin/activate
  python scripts/normalize_bets_status_and_date_lossless.py --apply

Dry run:
  python scripts/normalize_bets_status_and_date_lossless.py
"""

from __future__ import annotations

import os
import sys
import argparse
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Optional

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.database import get_admin_db_connection, get_db_connection, _exec

ET = ZoneInfo("America/New_York")


def norm_status(s: Any) -> str:
    if s is None:
        return "UNKNOWN"
    up = str(s).strip().upper()

    if up in ("WON", "WIN"):
        return "WON"
    if up in ("LOST", "LOSS", "LOSE"):
        return "LOST"
    if up in ("PUSH", "PUSHED"):
        return "PUSH"
    if up in ("CASHED OUT", "CASHED_OUT", "CASHOUT", "CASH OUT"):
        return "CASHED OUT"
    if up in ("PENDING", "OPEN", "PEND", "IN PLAY"):
        return "PENDING"

    return up if up else "UNKNOWN"


def parse_date_et(date_val: Any) -> Optional[str]:
    """Return YYYY-MM-DD in ET if parseable."""
    if date_val is None:
        return None
    s = str(date_val).strip()
    if not s:
        return None

    # Fast path: ISO YYYY-MM-DD
    try:
        if len(s) == 10 and s[4] == '-' and s[7] == '-':
            dt = datetime.fromisoformat(s)
            return dt.date().isoformat()
    except Exception:
        pass

    # Try common formats, including "Jan 11, 2026, 7:58pm ET"
    fmts = [
        "%b %d, %Y, %I:%M%p ET",
        "%b %d, %Y, %I:%M %p ET",
        "%b %d, %Y, %I:%M%p",
        "%Y-%m-%d %H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S",
    ]

    for f in fmts:
        try:
            dt = datetime.strptime(s, f)
            if dt.tzinfo is None:
                # If the string includes 'ET' assume ET
                if "ET" in s.upper():
                    dt = dt.replace(tzinfo=ET)
                else:
                    # assume ET for legacy bet exports
                    dt = dt.replace(tzinfo=ET)
            dt_et = dt.astimezone(ET)
            return dt_et.date().isoformat()
        except Exception:
            continue

    return None


def main(apply: bool = False, limit: int = 0):
    # Ensure columns exist (migrations)
    from src.database import init_bets_db
    init_bets_db()

    with get_db_connection() as conn:
        rows = _exec(conn, """
        SELECT id, date, status, status_raw, date_raw, date_et
        FROM bets
        ORDER BY id ASC
        """).fetchall()

    if limit and limit > 0:
        rows = rows[:limit]

    updates = []
    for r in rows:
        rr = dict(r) if not isinstance(r, dict) else r
        status0 = rr.get('status')
        date0 = rr.get('date')

        status_raw = rr.get('status_raw')
        date_raw = rr.get('date_raw')

        # preserve raw if empty
        status_raw_new = status_raw if (status_raw is not None and str(status_raw).strip() != '') else (str(status0) if status0 is not None else None)
        date_raw_new = date_raw if (date_raw is not None and str(date_raw).strip() != '') else (str(date0) if date0 is not None else None)

        status_norm = norm_status(status0)
        date_et_str = parse_date_et(date0)

        updates.append({
            'id': rr['id'],
            'status': status_norm,
            'status_raw': status_raw_new,
            'date_raw': date_raw_new,
            'date_et': date_et_str,
        })

    # summarize diffs
    changed = 0
    no_date = 0
    for u in updates:
        if u['date_et'] is None:
            no_date += 1
        # can't compare cheaply without another fetch; assume many change
        if u['status'] is not None:
            changed += 1

    print(f"Rows scanned: {len(updates)}")
    print(f"Rows with unparseable date -> date_et NULL: {no_date}")

    if not apply:
        print("Dry run. Add --apply to write changes.")
        print("Sample updates:")
        for u in updates[:10]:
            print(u)
        return

    upd_sql = """
    UPDATE bets
    SET
      status = %(status)s,
      status_raw = %(status_raw)s,
      date_raw = %(date_raw)s,
      date_et = %(date_et)s,
      updated_at = NOW(),
      updated_by = 'clawdbot',
      update_note = 'lossless normalize status/date'
    WHERE id = %(id)s
    """

    with get_db_connection() as conn:
        for u in updates:
            _exec(conn, upd_sql, u)
        conn.commit()

    print(f"Updated {len(updates)} rows")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true')
    ap.add_argument('--limit', type=int, default=0)
    args = ap.parse_args()
    main(apply=bool(args.apply), limit=int(args.limit))
