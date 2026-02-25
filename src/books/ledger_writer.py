"""Batch ledger writer for DK ingest pipeline.

Provides true bulk upserts using psycopg2.extras.execute_batch.
ONE connection per ingest run — no per-row commits.
"""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Tuple

try:
    from psycopg2.extras import execute_batch
except ImportError:  # pragma: no cover
    execute_batch = None  # type: ignore


class LedgerWriter:
    """Bulk-upsert bets into the `bets` table idempotently."""

    BATCH_SIZE = 200

    # Columns we are safe to overwrite on conflict
    SAFE_UPDATE_COLS = (
        "status",
        "profit",
        "odds",
        "raw_text",
        "updated_at",
        "is_live",
        "is_bonus",
        "account_id",
        "source",
    )

    @staticmethod
    def _stable_external_id(raw_text: str) -> str:
        """Generate a deterministic external_id from raw_text using sha256."""
        h = hashlib.sha256(raw_text.encode("utf-8", errors="replace")).hexdigest()
        return f"dk_scrape_{h[:16]}"

    def upsert_bets(self, conn, bets: list[dict]) -> Tuple[int, int]:
        """
        Bulk-upsert *bets* into the `bets` table.

        Args:
            conn: An open psycopg2 connection (caller manages lifecycle).
            bets: List of enriched bet dicts.

        Returns:
            (inserted, updated) counts.
        """
        if not bets:
            return 0, 0

        insert_sql = """
        INSERT INTO bets (
            user_id, provider, account_id, date, sport, bet_type,
            wager, profit, status, description, selection, odds,
            is_live, is_bonus, raw_text, external_id, source,
            created_at, updated_at
        )
        VALUES (
            %(user_id)s, %(provider)s, %(account_id)s, %(date)s, %(sport)s, %(bet_type)s,
            %(wager)s, %(profit)s, %(status)s, %(description)s, %(selection)s, %(odds)s,
            %(is_live)s, %(is_bonus)s, %(raw_text)s, %(external_id)s, %(source)s,
            NOW(), NOW()
        )
        ON CONFLICT (user_id, provider, external_id) WHERE external_id IS NOT NULL
        DO UPDATE SET
            status      = EXCLUDED.status,
            profit      = EXCLUDED.profit,
            odds        = EXCLUDED.odds,
            raw_text    = EXCLUDED.raw_text,
            is_live     = EXCLUDED.is_live,
            is_bonus    = EXCLUDED.is_bonus,
            account_id  = EXCLUDED.account_id,
            source      = EXCLUDED.source,
            updated_at  = NOW()
        RETURNING xmax
        """

        # Normalize & ensure external_id exists
        rows = []
        for bet in bets:
            raw_text = bet.get("raw_text") or ""
            ext_id = bet.get("external_id") or self._stable_external_id(raw_text)
            rows.append({
                "user_id":      str(bet.get("user_id", "")),
                "provider":     bet.get("provider", "DraftKings"),
                "account_id":   bet.get("account_id", "Main"),
                "date":         bet.get("date") or datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "sport":        bet.get("sport") or "Unknown",
                "bet_type":     bet.get("bet_type") or "Straight",
                "wager":        float(bet.get("wager") or 0),
                "profit":       round(float(bet.get("profit") or 0), 2),
                "status":       (bet.get("status") or "PENDING").upper(),
                "description":  bet.get("description") or bet.get("selection") or "",
                "selection":    bet.get("selection") or "",
                "odds":         int(bet["odds"]) if bet.get("odds") is not None else None,
                "is_live":      bool(bet.get("is_live", False)),
                "is_bonus":     bool(bet.get("is_bonus", False)),
                "raw_text":     raw_text,
                "external_id":  ext_id,
                "source":       bet.get("source", "dk_scrape"),
            })

        inserted = 0
        updated = 0

        with conn.cursor() as cur:
            if execute_batch is not None:
                # Use execute_batch for efficient bulk INSERT ... ON CONFLICT
                # We can't get xmax from execute_batch directly, so we track
                # by querying existing external_ids first.
                ext_ids = [r["external_id"] for r in rows]
                cur.execute(
                    "SELECT external_id FROM bets WHERE provider=%s AND user_id=%s AND external_id = ANY(%s)",
                    (rows[0]["provider"], rows[0]["user_id"], ext_ids),
                )
                existing_ids = {r[0] for r in cur.fetchall()}

                execute_batch(cur, insert_sql, rows, page_size=self.BATCH_SIZE)

                for r in rows:
                    if r["external_id"] in existing_ids:
                        updated += 1
                    else:
                        inserted += 1
            else:
                # Fallback: one-by-one (psycopg2.extras not available)
                for row in rows:
                    cur.execute(insert_sql, row)
                    result = cur.fetchone()
                    # xmax == 0 means INSERT; xmax > 0 means UPDATE
                    if result and result[0] == 0:
                        inserted += 1
                    else:
                        updated += 1

        conn.commit()
        return inserted, updated
