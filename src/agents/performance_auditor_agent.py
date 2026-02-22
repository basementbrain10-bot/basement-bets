import datetime
import json
from typing import Any, Dict, List
from src.agents.base import BaseAgent
from src.database import get_db_connection, _exec

class PerformanceAuditorAgent(BaseAgent):
    """
    Nightly/batch auditor.
    Connects to 'decision_runs' and grades outcomes iteratively via 'game_results'.
    Stores aggregates in 'performance_reports'.
    """
    def execute(self, context: Dict[str, Any], *args, **kwargs) -> Dict[str, Any]:
        target_date_str = context.get('date') # Format YYYY-MM-DD
        if not target_date_str:
            target_date_str = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            
        # Target period boundary
        start_ts = f"{target_date_str}T00:00:00Z"
        end_ts = f"{target_date_str}T23:59:59Z"

        # Find decisions built on this UTC day
        with get_db_connection() as conn:
            runs_query = """
            SELECT run_id, payload_json, league FROM decision_runs 
            WHERE created_at >= %s AND created_at <= %s AND status = 'OK'
            """
            runs_rows = _exec(conn, runs_query, (start_ts, end_ts)).fetchall()

            if not runs_rows:
                return {"message": f"No OK decisions observed for {target_date_str}."}

            brier_components = []
            graded_recs = 0
            
            # Aggregate payload info
            for row in runs_rows:
                payload = json.loads(row['payload_json'])
                recs = payload.get('recommendations', [])
                
                for r in recs:
                    offer = r.get('offer', {})
                    event_id = offer.get('event_id')
                    market_type = offer.get('market_type')
                    side = offer.get('side')
                    
                    if not event_id:
                        continue
                        
                    # Lookup corresponding finalized game results
                    res_query = "SELECT home_score, away_score, final FROM game_results WHERE event_id = %s"
                    res_row = _exec(conn, res_query, (event_id,)).fetchone()
                    
                    if res_row and res_row['final']:
                        # We have a strict outcome! Basic Brier tracking:
                        # (predicted_prob - actual_outcome)^2 
                        
                        actual_outcome = None
                        if market_type == "SPREAD":
                            # Extremely simple pseudo grading: did side win/cover
                            home_margin = res_row['home_score'] - res_row['away_score']
                            # E.g offer.line is -5.5 for HOME side
                            # if home_margin > 5.5 => 1
                            if offer.get("line") is not None:
                                # For a home bet, home MUST win by MORE than absolute spread (if favorite)
                                adj = (home_margin + offer['line']) if side == "HOME" else (-home_margin + offer['line'])
                                if abs(adj) < 1e-9:
                                    pass # push
                                elif adj > 0:
                                    actual_outcome = 1.0 # Win
                                else:
                                    actual_outcome = 0.0 # Loss
                        elif market_type == "TOTAL":
                            total = res_row['home_score'] + res_row['away_score']
                            if offer.get("line") is not None:
                                diff = total - offer['line']
                                if abs(diff) < 1e-9:
                                    pass # push
                                elif (side == "OVER" and diff > 0) or (side == "UNDER" and diff < 0):
                                    actual_outcome = 1.0
                                else:
                                    actual_outcome = 0.0

                        if actual_outcome is not None:
                            pred_p = float(r.get("implied_p", 0.5))
                            brier = (pred_p - actual_outcome) ** 2
                            brier_components.append(brier)
                            graded_recs += 1

            
            # Construct JSON summary string
            mean_brier = (sum(brier_components) / len(brier_components)) if brier_components else 0.0
            
            summary = {
                "target_date": target_date_str,
                "runs_processed": len(runs_rows),
                "total_recommendations_graded": graded_recs,
                "mean_brier_score": mean_brier
            }
            
            # Insert additive performance log
            insert_perf_sql = """
                INSERT INTO performance_reports (run_date, league, summary_json, created_at)
                VALUES (%s, %s, %s, NOW())
            """
            _exec(conn, insert_perf_sql, (target_date_str, "NCAAM", json.dumps(summary)))
            conn.commit()

        return summary
