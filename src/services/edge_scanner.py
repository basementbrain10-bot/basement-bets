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
        
    def find_edges(self, days_ahead: int = 3, max_plays: int = 3, force_refresh: bool = False) -> List[Dict]:
        """Scan upcoming games and return list of publishable opportunities.

        Rules:
        - Straight bets only (SPREAD/TOTAL)
        - Publish at most `max_plays` per run/day (0 allowed)
        - If none pass all gates, publish an empty list (UI should show "No Plays")
        """
        print(f"[EdgeScanner] Scanning next {days_ahead} days...")
        
        # 0. Ingest latest data for situational signals
        try:
            from src.scrapers.torvik_game_logs import TorvikGameLogsScraper
            from src.scrapers.officiating import OfficiatingScraper
            
            print("[EdgeScanner] Ingesting game logs for shooting regression...")
            game_log_scraper = TorvikGameLogsScraper()
            # Only fetch for teams playing soon (performance optimization)
            with get_db_connection() as conn:
                teams_query = """
                SELECT DISTINCT home_team FROM events WHERE league = 'NCAAM' AND start_time BETWEEN NOW() AND NOW() + INTERVAL '%s days'
                UNION
                SELECT DISTINCT away_team FROM events WHERE league = 'NCAAM' AND start_time BETWEEN NOW() AND NOW() + INTERVAL '%s days'
                """
                teams = [r[0] for r in _exec(conn, teams_query, (days_ahead, days_ahead)).fetchall()]
            
            if teams:
                game_log_scraper.run(teams)
            
            print("[EdgeScanner] Assigning referees for upcoming games...")
            ref_scraper = OfficiatingScraper()
            ref_scraper.bulk_assign_by_pattern()
            
        except Exception as e:
            print(f"[EdgeScanner] Warning: Failed to ingest signal data: {e}")
        
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
                            from src.agents.edge_ev_agent import EdgeEVAgent
                            from src.agents.contracts import MarketOffer, FairPrice
                            
                            # Synthesize offer + fair state to pass strictly to EdgeEVAgent
                            p_str = -110
                            offer = MarketOffer(
                                event_id=res['event_id'], league="NCAAM", market_type=rec['bet_type'], 
                                side=rec['selection_side'], odds_american=p_str, book="action_network", line=rec.get('line')
                            )
                            
                            # Identify strict model win_prob depending on market and side
                            debug = res.get('debug', {})
                            p_fair = 0.5
                            if rec['bet_type'] == "SPREAD":
                                p_fair = debug.get('win_prob_spread_home', 0.5) if rec['selection_side'] == "HOME" else debug.get('win_prob_spread_away', 0.5)
                            else:
                                p_fair = debug.get('win_prob_total_over', 0.5) if rec['selection_side'] == "OVER" else debug.get('win_prob_total_under', 0.5)
                                
                            fair = FairPrice(
                                event_id=res['event_id'], market_type=rec['bet_type'], side=rec['selection_side'],
                                p_fair=float(p_fair), confidence=float(rec.get('confidence', 0.5)), 
                                model_sources=[], rationale=[], fair_line=rec.get('fair_line')
                            )
                            
                            agent = EdgeEVAgent()
                            evals = agent.execute({"fairs": [fair], "offers": [offer]})
                            ev_res = evals[0] if evals else None
                            
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
                                "price": p_str,
                                "market_line": rec.get('market_line'),
                                "fair_line": rec.get('fair_line'),
                                "edge": ev_res.edge_points if ev_res else rec.get('edge_points'),
                                "ev": ev_res.ev_display if ev_res else rec.get('edge'), 
                                "ev_pct": ev_res.ev_pct if ev_res else 0.0,
                                "ev_per_unit": ev_res.ev_per_unit if ev_res else 0.0,
                                "implied_p": ev_res.implied_p if ev_res else 0.0,
                                "confidence": rec.get('confidence'),
                                "start_time": ev['start_time'].isoformat()
                            }
                            edges.append(edge_obj)
                            
                except Exception as e:
                    print(f"[EdgeScanner] Error analyzing {ev['id']}: {e}")
                    continue
                    
        # Keep top-N by EV (descending). Use the stable strict Pydantic numeric float generated via EdgeEVAgent
        try:
            edges.sort(key=lambda x: float(x.get('ev_pct') or 0.0), reverse=True)
        except Exception:
            pass

        if max_plays is not None:
            edges = edges[: max(0, int(max_plays))]

        print(f"[EdgeScanner] Scan complete. Publishing {len(edges)} plays.")
        return edges
