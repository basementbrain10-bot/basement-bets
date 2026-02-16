"""Archive + remove stale pending bets before 2026 (per user request).

Behavior (when run with --apply):
- Creates archive table `bets_archive` (same columns as `bets`) if missing.
- Copies rows where status='PENDING' and date_et < '2026-01-01' into archive,
  forcing status='VOID' and appending an update_note.
- Deletes those rows from `bets`.

Dry run default prints counts and example ids.

Run:
  source .venv_backtest/bin/activate
  python scripts/archive_and_delete_pending_bets_pre2026.py
  python scripts/archive_and_delete_pending_bets_pre2026.py --apply
"""

from __future__ import annotations

import os
import sys
import argparse

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.database import get_db_connection, _exec


CUTOFF = '2026-01-01'
NOTE = 'archived+deleted: stale pending pre-2026 (receipt/incomplete import)'


def main(apply: bool = False):
    with get_db_connection() as conn:
        # Identify candidate ids
        ids = _exec(conn, """
            SELECT id
            FROM bets
            WHERE status='PENDING'
              AND (date_et IS NOT NULL AND date_et < %(cutoff)s::date)
            ORDER BY id ASC
        """, {'cutoff': CUTOFF}).fetchall()
        ids = [int(r['id']) for r in ids]

        print(f"Pending rows pre-2026: {len(ids)}")
        if ids:
            print('Sample ids:', ids[:20])

        if not apply:
            print('Dry run. Use --apply to archive+delete.')
            return

        # Create archive table with same structure (including indexes/constraints are not copied; ok)
        _exec(conn, """
            CREATE TABLE IF NOT EXISTS bets_archive (LIKE bets INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
        """)

        # Copy rows into archive, forcing status='VOID' and adding note.
        # We keep status_raw/date_raw etc as-is.
        _exec(conn, """
            INSERT INTO bets_archive
            SELECT
              b.*
            FROM bets b
            WHERE b.id = ANY(%(ids)s);
        """, {'ids': ids})

        # Update archived rows to VOID + note (in archive)
        _exec(conn, """
            UPDATE bets_archive
            SET status='VOID',
                updated_at = NOW(),
                updated_by = 'clawdbot',
                update_note = COALESCE(update_note,'') || ' | ' || %(note)s
            WHERE id = ANY(%(ids)s);
        """, {'ids': ids, 'note': NOTE})

        # Delete from live table
        del_cur = _exec(conn, """
            DELETE FROM bets
            WHERE id = ANY(%(ids)s);
        """, {'ids': ids})

        conn.commit()

        print(f"Archived {len(ids)} and deleted {del_cur.rowcount} rows.")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()
    main(apply=bool(args.apply))
