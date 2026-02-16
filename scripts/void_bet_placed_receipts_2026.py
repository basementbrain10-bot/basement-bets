"""Void known non-settled receipt rows imported as bets (lossless).

This script targets FanDuel "Sports: Bet Placed" receipt rows that do not represent settled bets,
plus known placeholder manual imports.

Run:
  source .venv_backtest/bin/activate
  python scripts/void_bet_placed_receipts_2026.py
"""

from __future__ import annotations

import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.database import get_db_connection, _exec


def main():
    upd_fd = """
    UPDATE bets
    SET status=%(st)s,
        updated_at=NOW(),
        updated_by='clawdbot',
        update_note=COALESCE(update_note,'') || %(note)s
    WHERE date_et >= %(day)s
      AND status=%(pending)s
      AND provider=%(prov)s
      AND profit=0
      AND (description ILIKE 'Sports: Bet Placed%%' OR description ILIKE '%%Sports: Bet Placed%%');
    """

    upd_dk = """
    UPDATE bets
    SET status = CASE WHEN profit > 0 THEN 'WON' WHEN profit < 0 THEN 'LOST' ELSE 'PUSH' END,
        updated_at=NOW(),
        updated_by='clawdbot',
        update_note=COALESCE(update_note,'') || %(note)s
    WHERE date_et >= %(day)s
      AND status=%(pending)s
      AND provider=%(prov)s
      AND profit <> 0;
    """

    upd_manual = """
    UPDATE bets
    SET status='VOID',
        updated_at=NOW(),
        updated_by='clawdbot',
        update_note=COALESCE(update_note,'') || ' | auto-void: manual import placeholder'
    WHERE external_id='manual_import_971';
    """

    with get_db_connection() as conn:
        c1 = _exec(conn, upd_fd, {
            'st': 'VOID',
            'note': ' | auto-void: fanduel bet-placed receipt (no settlement)',
            'day': '2026-01-01',
            'pending': 'PENDING',
            'prov': 'FanDuel',
        })
        c2 = _exec(conn, upd_dk, {
            'note': ' | auto-fix: pending status corrected from profit',
            'day': '2026-01-01',
            'pending': 'PENDING',
            'prov': 'DraftKings',
        })
        c3 = _exec(conn, upd_manual)
        conn.commit()

    print('voided_fanduel_betplaced:', int(c1.rowcount or 0))
    print('fixed_dk_pending_profit:', int(c2.rowcount or 0))
    print('voided_manual_placeholder:', int(c3.rowcount or 0))


if __name__ == '__main__':
    main()
