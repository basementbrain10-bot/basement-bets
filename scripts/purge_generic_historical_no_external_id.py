"""Archive+purge generic historical bet-import rows without external_id.

User instruction (2026-02-16): purge generic ones first; keep true manual historical entries and rows with external_id.

This targets settled (non PENDING/OPEN/VOID) rows with external_id IS NULL and description in a known generic set
that came from initial historical files.

It archives rows to bets_archive then deletes them from bets.

Run:
  source .venv_backtest/bin/activate
  python scripts/purge_generic_historical_no_external_id.py --apply

Dry run:
  python scripts/purge_generic_historical_no_external_id.py
"""

from __future__ import annotations

import os
import sys
import argparse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.database import get_db_connection, _exec

GENERIC_DESCS = ['NCAAM', 'NCAAF', 'NFL', 'NRFI', 'See Below', 'See Below:']
NOTE = 'archived+deleted: generic historical import (no external_id)'


def main(apply: bool = False):
    with get_db_connection() as conn:
        ids = _exec(conn, """
            SELECT id
            FROM bets
            WHERE UPPER(status) NOT IN ('PENDING','OPEN','VOID')
              AND external_id IS NULL
              AND description = ANY(%s)
            ORDER BY id ASC;
        """, (GENERIC_DESCS,)).fetchall()
        ids = [int(r['id']) for r in ids]

    print('candidates', len(ids))
    if ids:
        print('sample_ids', ids[:20])

    if not apply:
        print('Dry run. Use --apply to archive+delete.')
        return

    with get_db_connection() as conn:
        _exec(conn, "CREATE TABLE IF NOT EXISTS bets_archive (LIKE bets INCLUDING DEFAULTS INCLUDING CONSTRAINTS);")
        if ids:
            _exec(conn, "INSERT INTO bets_archive SELECT * FROM bets WHERE id = ANY(%s);", (ids,))
            _exec(conn, """
                UPDATE bets_archive
                SET updated_at=NOW(), updated_by='clawdbot', update_note=COALESCE(update_note,'') || ' | ' || %s
                WHERE id = ANY(%s);
            """, (NOTE, ids))
            cur = _exec(conn, "DELETE FROM bets WHERE id = ANY(%s);", (ids,))
            print('deleted', int(cur.rowcount or 0))
        conn.commit()


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()
    main(apply=bool(args.apply))
