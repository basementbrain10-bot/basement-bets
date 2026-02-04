
import sys
import os
import argparse
from datetime import datetime, timedelta

# Add repo root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database import get_db_connection, _exec
from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2

def run_predictions(sport='ncaam'):
    model = NCAAMMarketFirstModelV2()
    
    # Get games for today (ET)
    # Using simplistic TZ logic for now (Server is ET-ish or UTC, we want local "today")
    # Events table stores UTC.
    now = datetime.utcnow()
    # Approx ET is UTC-5
    now_et = now - timedelta(hours=5)
    today_str = now_et.strftime('%Y-%m-%d')
    
    print(f"--- Basement Bets V2 Predictions for {today_str} ---")
    
    query = """
        SELECT id, home_team, away_team, start_time 
        FROM events 
        WHERE league = 'NCAAM'
        AND start_time >= NOW() - INTERVAL '4 hours'
        AND start_time <= NOW() + INTERVAL '24 hours'
        ORDER BY start_time ASC
    """
    
    with get_db_connection() as conn:
        games = _exec(conn, query).fetchall()
        
    if not games:
        print("No active/upcoming games found in window.")
        return

    count = 0
    recs = 0
    
    print(f"{'MATCHUP':<40} | {'MKT':<6} | {'FAIR':<6} | {'EDGE':<6} | {'REC'}")
    print("-" * 100)

    for g in games:
        try:
            res = model.analyze(g['id'])
            
            # Check for recommendation
            recommendations = res.get('recommendations', [])
            
            if recommendations:
                top_rec = recommendations[0] # Best one
                
                # Format
                matchup = f"{g['away_team']} @ {g['home_team']}"
                mkt_line = top_rec.get('market_line', 0.0)
                fair_line = top_rec.get('fair_line', 0.0)
                edge_pct = top_rec.get('edge', '0%')
                
                # Only show if meaningful edge or user asked
                print(f"{matchup:<40} | {mkt_line:<6} | {fair_line:<6} | {edge_pct:<6} | {top_rec['selection']} ({top_rec['confidence']})")
                recs += 1
            count += 1
        except Exception as e:
            # print(f"Err {g['id']}: {e}")
            pass
            
    if recs == 0:
        print(f"Scanned {count} games. No actionable edges found (strict model).")
    else:
        print(f"\nFound {recs} plays from {count} games.")

if __name__ == "__main__":
    run_predictions()
