"""
Edge Scanner Service
Separates scanning/batch processing logic from core model.
"""
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from src.database import get_db_connection, _exec
from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2

class EdgeScanner:
    """
    Scans upcoming games for betting edges using the canonical model (Market-First V2).
    """
    
    def __init__(self, model=None):
        self.model = model or NCAAMMarketFirstModelV2()
        
    def find_edges(self, days_ahead: int = 3, min_edge: float = 2.5, force_refresh: bool = False) -> List[Dict]:
        """
        Scan upcoming games and return list of actionable opportunities.
        """
        print(f"[EdgeScanner] Scanning next {days_ahead} days...")
        
        # 1. Fetch upcoming events
        query = """
        SELECT e.id, e.home_team, e.away_team, e.start_time, e.league
        FROM events e
        WHERE e.league = 'NCAAM'
          AND e.start_time BETWEEN NOW() AND NOW() + INTERVAL '%s days'
        ORDER BY e.start_time ASC
        """
        
        edges = []
        
        with get_db_connection() as conn:
            events = _exec(conn, query, (days_ahead,)).fetchall()
            print(f"[EdgeScanner] Found {len(events)} upcoming events.")
            
            for ev in events:
                try:
                    # Run Analysis
                    # analyze() handles fetching market snapshots, signals, logic, and persistence.
                    res = self.model.analyze(ev['id'])
                    
                    if not res or 'error' in res:
                        continue
                        
                    # Check for recommendations
                    # The model already filters recommendations based on internal 'Sniper' logic (2.5/3.0).
                    if res.get('recommendations'):
                        # Flatten for API response (one item per edge)
                        for rec in res['recommendations']:
                            # Map to API 'edge' format (legacy UI compat)
                            edge_obj = {
                                "game_id": res['event_id'],
                                "game": f"{res['away_team']} @ {res['home_team']}",
                                "matchup": f"{res['away_team']} @ {res['home_team']}",
                                "home_team": res['home_team'],
                                "away_team": res['away_team'],
                                "market_type": rec['bet_type'], # SPREAD or TOTAL
                                "bet_on": rec['selection'], # e.g. 'Duke' or 'OVER'
                                "line": rec.get('line'), # e.g. -5.5
                                "price": -110, # Simplified or from rec
                                "market_line": rec.get('market_line'),
                                "fair_line": rec.get('fair_line'),
                                "edge": rec.get('edge_points'), # Numeric Points Edge
                                "ev": rec.get('edge'), # "6.50%" string from analyze or float?
                                                       # analyze() outputs "6.50%" in recommendations list for UI.
                                                       # But also has 'ev' key as float in raw.
                                                       # Let's fix this api contract.
                                "confidence": rec.get('confidence'),
                                "start_time": ev['start_time'].isoformat()
                            }
                            edges.append(edge_obj)
                            
                except Exception as e:
                    print(f"[EdgeScanner] Error analyzing {ev['id']}: {e}")
                    continue
                    
        print(f"[EdgeScanner] Scan complete. Found {len(edges)} actionable edges.")
        return edges
