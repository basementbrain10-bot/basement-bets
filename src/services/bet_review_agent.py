import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

try:
    from src.database import get_db_connection, _exec, update_bet_fields
except ImportError:
    from database import get_db_connection, _exec, update_bet_fields


class BetReviewAgent:
    """Backfill missing/invalid fields on stored bets.

    Scope (safe + deterministic):
      - sport: if missing/Unknown, infer via shared detect_sport()
      - bet_type: if missing/Unknown, infer from text keywords
      - selection: if missing/Unknown, infer from description/raw_text first meaningful line

    Notes:
      - We intentionally do NOT touch wager/profit/status/date amounts in bulk.
      - Writes are audited via update_bet_fields(update_note=...).
    """

    def _parse_date(self, d: str) -> Optional[datetime]:
        if not d:
            return None
        d = str(d).strip().split('T')[0].split(' ')[0]
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(d, fmt)
            except Exception:
                pass
        return None

    def _infer_bet_type(self, text: str) -> Optional[str]:
        t = (text or '').lower()
        if not t:
            return None
        if 'sgp' in t or 'same game parlay' in t:
            return 'Same Game Parlay'
        if 'parlay' in t:
            return 'Parlay'
        if 'round robin' in t:
            return 'Round Robin'
        if 'moneyline' in t or re.search(r"\bml\b", t):
            return 'Moneyline'
        if 'spread' in t:
            return 'Spread'
        if 'total' in t or 'over' in t or 'under' in t:
            return 'Total'
        if 'prop' in t:
            return 'Prop'
        return None

    def _infer_selection(self, raw_text: str, description: str) -> Optional[str]:
        # Prefer explicit selection-like lines in raw_text.
        for src in (raw_text or '', description or ''):
            if not src:
                continue
            lines = [ln.strip() for ln in str(src).split('\n') if ln.strip()]
            for ln in lines:
                # Skip obvious noise
                if re.search(r"^(wager:|paid:|placed:|bet id:|total wager|returned|won|lost)$", ln.strip(), re.I):
                    continue
                if re.search(r"^\$", ln.strip()):
                    continue
                # If it looks like a matchup or team line, accept.
                if '@' in ln or re.search(r"\b(vs\.?|versus)\b", ln, re.I):
                    return ln
                # Or a non-trivial line.
                if len(ln) >= 4:
                    return ln
        return None

    def backfill_missing_fields(
        self,
        user_id: str,
        days_back: int = 3650,
        limit: int = 20000,
        dry_run: bool = True,
    ) -> Dict:
        if not user_id:
            return {"scanned": 0, "updated": 0, "dry_run": dry_run, "changes": []}

        from src.parsers.sport_detection import detect_sport

        now = datetime.now()
        cutoff = (now - timedelta(days=int(days_back))).date().isoformat()

        q = """
        SELECT id, date, sport, bet_type, selection, raw_text, description, provider
        FROM bets
        WHERE user_id = %s AND date >= %s
        ORDER BY date DESC
        LIMIT %s
        """

        with get_db_connection() as conn:
            rows = _exec(conn, q, (user_id, cutoff, int(limit))).fetchall()

        scanned = 0
        updated = 0
        changes: List[Dict] = []

        for r in rows:
            b = dict(r)
            scanned += 1

            text = " ".join([
                str(b.get('raw_text') or ''),
                str(b.get('selection') or ''),
                str(b.get('description') or ''),
                str(b.get('provider') or ''),
            ])

            patch: Dict = {}

            # sport
            cur_sport = str(b.get('sport') or '').strip()
            if not cur_sport or cur_sport.lower() in ('unknown', 'n/a', 'na'):
                s = detect_sport(text)
                if s and s != 'Unknown':
                    patch['sport'] = s

            # bet_type
            cur_bt = str(b.get('bet_type') or '').strip()
            if not cur_bt or cur_bt.lower() in ('unknown', 'n/a', 'na'):
                bt = self._infer_bet_type(text)
                if bt:
                    patch['bet_type'] = bt

            # selection
            cur_sel = str(b.get('selection') or '').strip()
            if not cur_sel or cur_sel.lower() in ('unknown', 'n/a', 'na'):
                sel = self._infer_selection(str(b.get('raw_text') or ''), str(b.get('description') or ''))
                if sel:
                    patch['selection'] = sel

            if patch:
                changes.append({"bet_id": b.get('id'), "patch": patch})
                if not dry_run:
                    ok = update_bet_fields(int(b['id']), patch, user_id=user_id, update_note='audit: backfill missing fields')
                    if ok:
                        updated += 1

        return {
            "scanned": scanned,
            "updated": updated,
            "dry_run": dry_run,
            "proposed": len(changes),
            "changes": changes[:200],  # cap in response
        }
