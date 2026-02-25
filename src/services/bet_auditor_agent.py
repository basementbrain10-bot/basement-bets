import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

try:
    from src.database import get_db_connection, _exec
except ImportError:
    from database import get_db_connection, _exec


class BetAuditorAgent:
    """Audits stored bets for obvious metadata inconsistencies.

    Current focus:
      - sport mismatches vs canonical `events` table (league + teams)

    Why:
      Sportsbook export rows (especially SGP / bonus bets) can land with the wrong sport.
      This agent tries to infer the correct league by matching team names against
      the events table within a tight date window.
    """

    def _norm(self, s: str) -> str:
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", " ", (s or "").lower())).strip()

    def _extract_matchup(self, text: str) -> Optional[Tuple[str, str]]:
        """Extract (team_a, team_b) from selection/description.

        Supports common separators: '@', 'vs', 'v', 'versus'.
        Returns normalized-ish raw strings (not DB ids).
        """
        if not text:
            return None

        s = str(text)
        # Make a single delimiter we can split on.
        s2 = re.sub(r"\b(vs\.?|versus| v )\b", "@", s, flags=re.IGNORECASE)
        if '@' not in s2:
            return None

        parts = [p.strip() for p in s2.split('@') if p.strip()]
        if len(parts) < 2:
            return None

        # Heuristic: take the first two chunks.
        a, b = parts[0], parts[1]

        # Remove odds/lines and common betting tokens.
        def clean_team(x: str) -> str:
            x = re.sub(r"[\+\-]?\d+(?:\.\d+)?", "", x)
            x = re.sub(r"\b(ml|moneyline|spread|total|over|under|alt)\b", "", x, flags=re.IGNORECASE)
            x = re.sub(r"\s+", " ", x).strip()
            return x

        a = clean_team(a)
        b = clean_team(b)
        if len(a) < 3 or len(b) < 3:
            return None
        return (a, b)

    def _parse_date(self, d: str) -> Optional[datetime]:
        if not d:
            return None
        d = str(d).strip()
        d = d.split('T')[0].split(' ')[0]
        for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
            try:
                return datetime.strptime(d, fmt)
            except Exception:
                pass
        return None

    def _find_event_league(self, team_a: str, team_b: str, bet_dt: datetime) -> Optional[Dict]:
        """Try to match an events row by team substring within a +/-1d window."""
        if not team_a or not team_b or not bet_dt:
            return None

        a = self._norm(team_a)
        b = self._norm(team_b)
        if not a or not b:
            return None

        start = (bet_dt - timedelta(days=1)).date().isoformat()
        end = (bet_dt + timedelta(days=1)).date().isoformat()

        q = """
        SELECT league, home_team, away_team, start_time
        FROM events
        WHERE start_time::date >= %s AND start_time::date <= %s
          AND (
            (LOWER(home_team) LIKE %s AND LOWER(away_team) LIKE %s)
            OR
            (LOWER(home_team) LIKE %s AND LOWER(away_team) LIKE %s)
          )
        ORDER BY start_time ASC
        LIMIT 1
        """
        # substring match; we keep them a bit loose but still require both teams.
        params = (
            start,
            end,
            f"%{a}%",
            f"%{b}%",
            f"%{b}%",
            f"%{a}%",
        )

        with get_db_connection() as conn:
            row = _exec(conn, q, params).fetchone()
            if not row:
                return None
            d = dict(row)
            return {
                "league": d.get("league"),
                "home_team": d.get("home_team"),
                "away_team": d.get("away_team"),
                "start_time": d.get("start_time"),
            }

    def audit_sport_mismatches(self, user_id: str, days_back: int = 60, limit: int = 800) -> List[Dict]:
        """Return a list of bets whose sport disagrees with matched event league."""
        if not user_id:
            return []

        q = """
        SELECT id, date, sport, bet_type, provider, description, selection, raw_text, is_bonus
        FROM bets
        WHERE user_id = %s
        ORDER BY date DESC
        LIMIT %s
        """

        out: List[Dict] = []
        now = datetime.now()

        with get_db_connection() as conn:
            rows = _exec(conn, q, (user_id, int(limit))).fetchall()

        for r in rows:
            b = dict(r)
            bet_dt = self._parse_date(b.get('date'))
            if not bet_dt:
                continue
            if (now - bet_dt).days > int(days_back):
                continue

            text = (b.get('selection') or '')
            if not text:
                text = (b.get('description') or '')

            current = str(b.get('sport') or '').upper().strip()

            # Primary path: matchup -> events table -> league
            matchup = self._extract_matchup(text)
            if matchup:
                team_a, team_b = matchup
                ev = self._find_event_league(team_a, team_b, bet_dt)
                if ev and ev.get('league'):
                    suggested = str(ev.get('league') or '').upper().strip()
                    if current and suggested and current != suggested:
                        out.append({
                            "bet_id": b.get('id'),
                            "date": str(bet_dt.date().isoformat()),
                            "provider": b.get('provider'),
                            "bet_type": b.get('bet_type'),
                            "is_bonus": bool(b.get('is_bonus')),
                            "sport": current,
                            "suggested_sport": suggested,
                            "matchup": f"{ev.get('away_team')} @ {ev.get('home_team')}" if ev.get('home_team') and ev.get('away_team') else f"{team_a} @ {team_b}",
                            "reason": "Matched teams in events table within +/- 1 day"
                        })
                        continue

            # Fallback path: if sport is UNKNOWN/blank (common for parlays/SGP text),
            # try detect_sport on the raw text blob.
            if current in ("", "UNKNOWN", "UNK"):
                try:
                    from src.parsers.sport_detection import detect_sport
                except Exception:
                    from parsers.sport_detection import detect_sport

                blob = " ".join([
                    str(b.get('raw_text') or ''),
                    str(b.get('selection') or ''),
                    str(b.get('description') or ''),
                    str(b.get('provider') or ''),
                ])
                suggested = str(detect_sport(blob) or '').upper().strip()
                if suggested and suggested not in ("UNKNOWN", "UNK"):
                    out.append({
                        "bet_id": b.get('id'),
                        "date": str(bet_dt.date().isoformat()),
                        "provider": b.get('provider'),
                        "bet_type": b.get('bet_type'),
                        "is_bonus": bool(b.get('is_bonus')),
                        "sport": current or "UNKNOWN",
                        "suggested_sport": suggested,
                        "matchup": None,
                        "reason": "Sport was UNKNOWN; suggested via detect_sport"
                    })

        return out
