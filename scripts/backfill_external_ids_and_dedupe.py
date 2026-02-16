"""Backfill external_id from raw_text and dedupe bets.

Motivation:
- Many settled bets were imported from DK/FanDuel text dumps but external_id was not extracted.
- Without external_id, idempotency is weak and repeated imports inflate bet counts.

What this does (when --apply):
1) Extract candidate external ids from raw_text:
   - DraftKings: DK\d{8,}
   - FanDuel: BET ID: O/... (O/\S+)
2) For each (user_id, provider, external_id) group with duplicates:
   - Archive duplicates into bets_archive (create if needed)
   - Delete all but the lowest id
3) Update remaining rows to set external_id.

Dry-run default prints counts and top duplicate groups.

Run:
  source .venv_backtest/bin/activate
  python scripts/backfill_external_ids_and_dedupe.py
  python scripts/backfill_external_ids_and_dedupe.py --apply
"""

from __future__ import annotations

import os
import sys
import re
import argparse
from collections import defaultdict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.database import get_db_connection, _exec

RE_DK = re.compile(r"(DK\d{8,})")  # matches DK bet ids even when concatenated
RE_FD = re.compile(r"BET ID:\s*(O/\S+)", re.IGNORECASE)


def extract_external_id(provider: str, raw_text: str) -> str | None:
    p = (provider or '').strip().lower()
    rt = raw_text or ''
    if not rt:
        return None
    if p == 'draftkings':
        m = RE_DK.search(rt)
        return m.group(1) if m else None
    if p == 'fanduel':
        m = RE_FD.search(rt)
        return m.group(1) if m else None
    return None


def main(apply: bool = False):
    # Pull candidate rows
    with get_db_connection() as conn:
        rows = _exec(conn, """
            SELECT id, user_id, provider, external_id, raw_text
            FROM bets
            WHERE UPPER(status) NOT IN ('PENDING','OPEN','VOID')
              AND external_id IS NULL
              AND raw_text IS NOT NULL AND raw_text <> ''
        """).fetchall()

    rows = [dict(r) for r in rows]

    # Build mapping
    updates = []
    by_key = defaultdict(list)  # (user_id, provider, ext) -> [id]

    for r in rows:
        ext = extract_external_id(r.get('provider') or '', r.get('raw_text') or '')
        if not ext:
            continue
        updates.append({'id': int(r['id']), 'user_id': r.get('user_id'), 'provider': r.get('provider'), 'external_id': ext})
        by_key[(r.get('user_id'), r.get('provider'), ext)].append(int(r['id']))

    dup_groups = {k: v for k, v in by_key.items() if len(v) > 1}

    print(f"candidate rows w/ external_id extracted: {len(updates)}")
    print(f"duplicate external_id groups: {len(dup_groups)}")

    if dup_groups:
        print("Top dup groups (first 10):")
        shown = 0
        for (uid, prov, ext), ids in sorted(dup_groups.items(), key=lambda x: -len(x[1]))[:10]:
            print({'provider': prov, 'external_id': ext, 'n': len(ids), 'ids': ids[:10]})
            shown += 1

    if not apply:
        print("Dry run. Use --apply to archive/delete duplicates + update external_id.")
        return

    # Ensure archive table exists
    with get_db_connection() as conn:
        _exec(conn, "CREATE TABLE IF NOT EXISTS bets_archive (LIKE bets INCLUDING DEFAULTS INCLUDING CONSTRAINTS);")
        conn.commit()

    # 1) Archive+delete duplicates first (so updates won't violate unique index)
    total_deleted = 0
    with get_db_connection() as conn:
        for (uid, prov, ext), ids in dup_groups.items():
            keep = min(ids)
            drop = [i for i in ids if i != keep]
            if not drop:
                continue
            # archive
            _exec(conn, "INSERT INTO bets_archive SELECT * FROM bets WHERE id = ANY(%s);", (drop,))
            # mark archived rows as VOID for clarity
            _exec(conn, """
                UPDATE bets_archive
                SET status='VOID',
                    updated_at=NOW(),
                    updated_by='clawdbot',
                    update_note=COALESCE(update_note,'') || ' | archived duplicate by external_id backfill'
                WHERE id = ANY(%s);
            """, (drop,))
            # delete
            cur = _exec(conn, "DELETE FROM bets WHERE id = ANY(%s);", (drop,))
            total_deleted += int(cur.rowcount or 0)
        conn.commit()

    print('deleted duplicates:', total_deleted)

    # 2) Update remaining rows with extracted external_id
    # Recompute candidate list now that dups were deleted.
    with get_db_connection() as conn:
        rows2 = _exec(conn, """
            SELECT id, provider, raw_text
            FROM bets
            WHERE UPPER(status) NOT IN ('PENDING','OPEN','VOID')
              AND external_id IS NULL
              AND raw_text IS NOT NULL AND raw_text <> ''
        """).fetchall()

        updated = 0
        for r in rows2:
            d = dict(r)
            ext = extract_external_id(d.get('provider') or '', d.get('raw_text') or '')
            if not ext:
                continue
            # update; if unique index conflicts, skip (should be rare after dedupe)
            try:
                _exec(conn, "UPDATE bets SET external_id=%s WHERE id=%s;", (ext, int(d['id'])))
                updated += 1
            except Exception:
                pass
        conn.commit()

    print('updated external_id:', updated)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()
    main(apply=bool(args.apply))
