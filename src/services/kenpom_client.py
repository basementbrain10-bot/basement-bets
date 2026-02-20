"""
KenPom Client for Ensemble Model

Fetches KenPom efficiency ratings from database and calculates adjustments
"""

from typing import Dict, Optional, List, Any
from src.database import get_db_connection, _exec
from src.utils.team_matcher import TeamMatcher

class KenPomClient:
    """Client for KenPom efficiency data + related daily tables."""
    
    def __init__(self):
        self.matcher = TeamMatcher()

    def _latest_asof_date(self, table: str) -> Optional[str]:
        with get_db_connection() as conn:
            row = _exec(conn, f"SELECT MAX(asof_date)::text AS d FROM {table}").fetchone()
            return row['d'] if row and row.get('d') else None

    def _pair_headers(self, headers: list[str] | None, cols: list[Any] | None) -> Dict[str, Any]:
        if not headers or not cols:
            return {}
        # Normalize lengths
        m = min(len(headers), len(cols))
        out = {}
        for i in range(m):
            k = str(headers[i]).strip() if headers[i] is not None else ''
            if not k:
                continue
            out[k] = cols[i]
        return out

    def _find_numeric(self, mapping: Dict[str, Any], candidates: List[str]) -> Optional[float]:
        """Fuzzy find numeric value by header substring candidates."""
        def to_f(x):
            try:
                if x is None:
                    return None
                s = str(x).replace('%','').replace('+','').strip()
                return float(s)
            except Exception:
                return None
        for k, v in (mapping or {}).items():
            lk = str(k).lower()
            if any(c.lower() in lk for c in candidates):
                f = to_f(v)
                if f is not None:
                    return f
        return None

    def get_team_rating(self, team_name: str) -> Optional[Dict]:
        """
        Get KenPom rating for a team
        
        Args:
            team_name: Team name to lookup
            
        Returns:
            Dict with adj_em, adj_o, adj_d, adj_t or None
        """
        # Ratings are stored as daily snapshots.
        matched_name = self.matcher.find_source_name(team_name, "kenpom_team_ratings_daily", "team_name")
        if not matched_name:
            return None

        with get_db_connection() as conn:
            query = """
            SELECT team_name, rank, adj_em, adj_o, adj_d, adj_t
            FROM kenpom_team_ratings_daily
            WHERE team_name = %s
            ORDER BY asof_date DESC
            LIMIT 1
            """
            cursor = _exec(conn, query, (matched_name,))
            row = cursor.fetchone()
            
            if row:
                return {
                    'team': row['team_name'],
                    'rank': row['rank'],
                    'adj_em': row['adj_em'],
                    'adj_o': row['adj_o'],
                    'adj_d': row['adj_d'],
                    'adj_t': row['adj_t']
                }
            
            return None
    
    def get_home_court(self, team_name: str) -> Optional[Dict]:
        matched_name = self.matcher.find_source_name(team_name, "kenpom_home_court_daily", "team_name")
        if not matched_name:
            return None

        with get_db_connection() as conn:
            row = _exec(conn, """
                SELECT team_name, hca, asof_date
                FROM kenpom_home_court_daily
                WHERE team_name=%s
                ORDER BY asof_date DESC
                LIMIT 1
            """, (matched_name,)).fetchone()
            if not row:
                return None
            return {"team": row["team_name"], "hca": row["hca"], "asof_date": row["asof_date"]}

    def get_ref_metrics(self, ref_names: List[str]) -> List[Dict]:
        if not ref_names:
            return []
        asof = self._latest_asof_date('kenpom_ref_ratings_daily')
        if not asof:
            return []

        outs = []
        with get_db_connection() as conn:
            for nm in ref_names:
                if not nm:
                    continue
                # Ref name matching is tricky; do case-insensitive exact first.
                row = _exec(conn, """
                    SELECT ref_name, metrics, asof_date
                    FROM kenpom_ref_ratings_daily
                    WHERE asof_date=%s AND LOWER(ref_name)=LOWER(%s)
                    LIMIT 1
                """, (asof, nm)).fetchone()
                if row:
                    outs.append({"ref_name": row["ref_name"], "metrics": row["metrics"], "asof_date": row["asof_date"]})
        return outs

    def estimate_crew_avg_fouls(self, ref_names: List[str]) -> Optional[float]:
        """Best-effort: derive a crew avg fouls number from kenpom ref metrics, if headers exist."""
        refs = self.get_ref_metrics(ref_names)
        if not refs:
            return None

        vals = []
        for r in refs:
            m = r.get('metrics') or {}
            headers = m.get('headers') or []
            cols = m.get('cols') or []
            mapping = self._pair_headers(headers, cols)
            v = self._find_numeric(mapping, ["foul", "pf", "fouls/game", "fouls per game"])
            if v is not None:
                vals.append(v)
        if not vals:
            return None
        return sum(vals) / len(vals)

    def get_team_player_agg(self, team_name: str) -> Optional[Dict]:
        matched = self.matcher.find_source_name(team_name, "kenpom_team_player_agg_daily", "team_name")
        if not matched:
            return None
        asof = self._latest_asof_date('kenpom_team_player_agg_daily')
        if not asof:
            return None

        with get_db_connection() as conn:
            row = _exec(conn, """
                SELECT team_name, asof_date, n_players, minutes_weight_sum,
                       ortg_w, usage_w, efg_w, ts_w, ast_rate_w, reb_rate_w,
                       tov_rate_w, ft_rate_w, three_par_w, top7_minutes_pct
                FROM kenpom_team_player_agg_daily
                WHERE asof_date=%s AND team_name=%s
                LIMIT 1
            """, (asof, matched)).fetchone()
            if not row:
                return None
            return dict(row)

    def get_player_stats_for_team(self, team_name: str, limit: int = 40) -> List[Dict]:
        matched = self.matcher.find_source_name(team_name, "kenpom_player_stats_daily", "team_name")
        if not matched:
            return []
        asof = self._latest_asof_date('kenpom_player_stats_daily')
        if not asof:
            return []

        with get_db_connection() as conn:
            rows = _exec(conn, """
                SELECT player_name, team_name, metrics
                FROM kenpom_player_stats_daily
                WHERE asof_date=%s AND team_name=%s
                LIMIT %s
            """, (asof, matched, int(limit))).fetchall()
            out = []
            for row in rows or []:
                out.append({"player_name": row["player_name"], "team_name": row["team_name"], "metrics": row["metrics"]})
            return out

    def calculate_kenpom_adjustment(self, home_team: str, away_team: str) -> Dict:
        """
        Calculate spread/total adjustment based on KenPom ratings
        
        Logic:
        - AdjEM difference → spread adjustment
        - AdjT average → total adjustment
        
        Args:
            home_team: Home team name
            away_team: Away team name
            
        Returns:
            Dict with spread_adj, total_adj, summary
        """
        home_rating = self.get_team_rating(home_team)
        away_rating = self.get_team_rating(away_team)
        
        if not home_rating or not away_rating:
            return {
                'spread_adj': 0.0,
                'total_adj': 0.0,
                'summary': 'KenPom data not available'
            }
        
        # Spread adjustment based on AdjEM difference
        # AdjEM is already adjusted for home court (~3.5 pts)
        em_diff = home_rating['adj_em'] - away_rating['adj_em']
        spread_adj = em_diff * 0.1  # 10 pt AdjEM diff = 1 pt spread
        
        # Total adjustment based on tempo
        avg_tempo = (home_rating['adj_t'] + away_rating['adj_t']) / 2
        baseline_tempo = 68.0  # National average
        total_adj = (avg_tempo - baseline_tempo) * 0.3  # Faster tempo = higher total
        
        summary = f"KenPom: {home_rating['team']} (#{home_rating['rank']}) vs {away_rating['team']} (#{away_rating['rank']})"
        
        return {
            'spread_adj': round(spread_adj, 1),
            'total_adj': round(total_adj, 1),
            'summary': summary,
            'home_adj_em': home_rating['adj_em'],
            'away_adj_em': away_rating['adj_em'],
            'home_rank': home_rating.get('rank'),
            'away_rank': away_rating.get('rank'),
        }


# Example usage
if __name__ == "__main__":
    client = KenPomClient()
    
    # Test lookup
    duke = client.get_team_rating("Duke")
    if duke:
        print(f"Duke: #{duke['rank']} - AdjEM {duke['adj_em']:.2f}")
    
    # Test adjustment
    adj = client.calculate_kenpom_adjustment("Duke", "North Carolina")
    print(f"\nKenPom Adjustment:")
    print(f"  Spread: {adj['spread_adj']:+.1f} pts")
    print(f"  Total: {adj['total_adj']:+.1f} pts")
    print(f"  {adj['summary']}")
