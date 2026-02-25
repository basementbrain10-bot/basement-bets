from __future__ import annotations

from typing import Dict

from src.database import get_db_connection, _exec
from src.utils.sport_normalization import normalize_sport


class SportCleanupService:
    """Utilities to normalize bet.sport values in the DB.

    Why:
    - Prevent duplicates like 'World Cup' vs 'WORLD CUP'
    - Keep filters/analytics consistent
    """

    def normalize_user_bets_sport(self, user_id: str, limit: int = 50000) -> Dict:
        if not user_id:
            return {"scanned": 0, "updated": 0}

        q = """
        SELECT id, sport
        FROM bets
        WHERE user_id = %s
        ORDER BY id DESC
        LIMIT %s
        """

        upd = """
        UPDATE bets
        SET sport = %s
        WHERE id = %s AND user_id = %s
        """

        scanned = 0
        updated = 0
        with get_db_connection() as conn:
            rows = _exec(conn, q, (user_id, int(limit))).fetchall()
            for r in rows:
                scanned += 1
                bid = int(r["id"])
                cur = r.get("sport")
                norm = normalize_sport(cur)
                # Only write when it actually changes (case/whitespace/alias)
                if (cur is None and norm == "UNKNOWN"):
                    continue
                if cur is not None and str(cur).strip() == norm:
                    continue
                if cur is not None and normalize_sport(cur) == norm and str(cur).strip().upper() == norm:
                    # already canonical
                    continue

                _exec(conn, upd, (norm, bid, user_id))
                updated += 1

            conn.commit()

        return {"scanned": scanned, "updated": updated}
