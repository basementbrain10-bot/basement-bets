#!/usr/bin/env python3
"""Print Top N NCAAM picks from cached daily_top_picks table.

Designed for cron: fast, no model compute.

Outputs a ready-to-send text block.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Optional

# Allow running from repo root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.database import get_db_connection, _exec


def _ev_from_rec(rec: dict) -> Optional[float]:
    # rec['edge'] is like "+12.3%" (string). Convert to 0.123.
    try:
        e = rec.get('edge')
        if e is None:
            return None
        if isinstance(e, (int, float)):
            return float(e)
        s = str(e).strip().replace('%', '')
        return float(s) / 100.0
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--date', default=None, help='ET date YYYY-MM-DD (default today ET)')
    ap.add_argument('--limit', type=int, default=6)
    args = ap.parse_args()

    with get_db_connection() as conn:
        date_et = args.date
        if not date_et:
            date_et = _exec(conn, "SELECT (NOW() AT TIME ZONE 'America/New_York')::date::text").fetchone()[0]

        rows = _exec(conn, """
          SELECT d.event_id, d.rec_json,
                 (e.start_time AT TIME ZONE 'America/New_York') as start_et,
                 e.away_team, e.home_team
          FROM daily_top_picks d
          JOIN events e ON e.id=d.event_id
          WHERE d.date_et=%s AND d.is_actionable=TRUE AND d.rec_json IS NOT NULL
          ORDER BY (regexp_replace(COALESCE(d.rec_json->>'edge','0%'),'[^0-9\\.\\-]+','','g'))::float DESC
          LIMIT %s
        """, (date_et, int(args.limit))).fetchall()

    if not rows:
        print(f"No bets for {date_et} (no edges passed gates)")
        return

    print(f"Top {min(len(rows), int(args.limit))} Plays {date_et} • Sorted by EV%")
    for i, r in enumerate(rows, start=1):
        rec = r['rec_json'] or {}
        start_et = r.get('start_et')
        t = start_et.strftime('%-I:%M %p ET') if hasattr(start_et, 'strftime') else ''
        matchup = f"{r.get('away_team')} @ {r.get('home_team')}"
        sel = rec.get('selection')
        odds = rec.get('price')
        ev_pct = rec.get('edge')
        conf = rec.get('confidence')
        # Normalize odds
        odds_s = f"{odds:+d}" if isinstance(odds, int) else (str(odds) if odds is not None else 'odds n/a')
        print(f"{i}) {t} • {matchup} • {sel} • {odds_s} • EV {ev_pct} • {conf}")


if __name__ == '__main__':
    main()
