"""Backfill bets.source for existing rows.

Goal: enable safe purging of early bulk historical imports without touching slip-scraped or manual adds.

Heuristics:
- If source already set: keep.
- If external_id is not null: source='sportsbook_id'
- If description == 'Imported Bet' OR external_id startswith manual_import_: source='manual_import'
- If created_at between 2026-01-18 and 2026-01-27 (bulk import window) and external_id is null:
    - If wager <= 0 OR status='VOID': source='artifact'
    - Else: source='import_initial_files'
- Else if external_id is null: source='manual_historical'

Run:
  source .venv_backtest/bin/activate
  python scripts/backfill_bet_source.py --apply

Dry run:
  python scripts/backfill_bet_source.py
"""

from __future__ import annotations

import os
import sys
import argparse
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.database import get_db_connection, _exec

BULK_START = datetime.fromisoformat('2026-01-18T00:00:00')
BULK_END = datetime.fromisoformat('2026-01-28T00:00:00')


def classify(row: dict) -> str:
    if row.get('source'):
        return row['source']

    ext = row.get('external_id')
    desc = (row.get('description') or '').strip()
    st = (row.get('status') or '').strip().upper()
    wager = float(row.get('wager') or 0.0)
    created = row.get('created_at')

    if ext:
        # Historical file ingests used synthetic ids like manual_import_####.
        # Treat those as initial import provenance (not sportsbook-native ids).
        if str(ext).startswith('manual_import_'):
            return 'import_initial_files'
        return 'sportsbook_id'

    if desc.lower().startswith('imported bet'):
        return 'manual_import'

    # bulk import window
    try:
        if created and BULK_START <= created < BULK_END:
            if st == 'VOID' or wager <= 0:
                return 'artifact'
            return 'import_initial_files'
    except Exception:
        pass

    return 'manual_historical'


def main(apply: bool = False):
    with get_db_connection() as conn:
        try:
            _exec(conn, "ALTER TABLE bets ADD COLUMN IF NOT EXISTS source TEXT;")
            conn.commit()
        except Exception:
            pass

    with get_db_connection() as conn:
        rows = _exec(conn, """
          SELECT id, external_id, description, status, wager, created_at, source
          FROM bets
        """).fetchall()

    rows = [dict(r) for r in rows]
    updates = []
    for r in rows:
        src = classify(r)
        if (r.get('source') or None) != src:
            updates.append((src, int(r['id'])))

    from collections import Counter
    ctr = Counter([u[0] for u in updates])
    print('rows_needing_update', len(updates))
    print('by_source', dict(ctr))

    if not apply:
        print('Dry run. Use --apply.')
        return

    with get_db_connection() as conn:
        for src, bid in updates:
            _exec(conn, "UPDATE bets SET source=%s WHERE id=%s;", (src, bid))
        conn.commit()

    print('updated', len(updates))


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()
    main(apply=bool(args.apply))
