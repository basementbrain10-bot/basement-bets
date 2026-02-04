
import sys
import os
import json
from datetime import datetime

# Add repo root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database import get_db_connection, _exec
from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2

def run_recent_backtest(limit=15):
    print(f"--- Running Backtest on Last {limit} Finished Games ---")
    model = NCAAMMarketFirstModelV2()
    
    # Fetch finished games with scores
    # We need games where we have ODDS history to simulate the "decision point"
    # But for a quick V2 check, we can just use the Model's opinion vs Result
    # ignoring CLV for a moment to see if the "Fair Line" predicted the winner.
    
    query = """
        SELECT e.id, e.home_team, e.away_team, e.start_time, gr.home_score, gr.away_score
        FROM events e
        JOIN game_results gr ON e.id = gr.event_id
        WHERE e.league = 'NCAAM'
        AND gr.home_score IS NOT NULL
        ORDER BY e.start_time DESC
        LIMIT :limit
    """
    
    with get_db_connection() as conn:
        games = _exec(conn, query, {"limit": limit}).fetchall()
        
    if not games:
        print("No recent finished games found.")
        return

    results = []
    
    print(f"{'DATE':<12} | {'MATCHUP':<35} | {'RES':<8} | {'MODEL':<6} | {'ERR':<5} | {'REC'}")
    print("-" * 100)

    correct_spread_picks = 0
    total_spread_picks = 0

    for g in games:
        try:
            # Run Model
            # Note: analyze() usually pulls 'current' odds. 
            # For backtesting past games, it might pull 'closed' odds if logic allows,
            # or it might fail to find live odds.
            # V2 model pulls "recent odds" from DB. If the game is over, it should find closing lines.
            
            res = model.analyze(g['id'])
            
            # Prediction
            mu_spread = res.get('mu_final') # Home Spread (negative = home fav)
            if mu_spread is None: continue
            
            # Outcome
            # Spread Result: (Away Score - Home Score) ? 
            # Spread is "Home -5". If Home score 80, Away 70. Margin = +10.
            # Did Home Cover? Margin (+10) > -Spread (5). Yes.
            
            # Let's align signs.
            # mu_spread = -5.0 (Home favored by 5)
            # Actual Margin (Home - Away)
            margin = g['home_score'] - g['away_score']
            
            # Error = |Actual Margin - Expected Margin|
            # Expected Margin = -mu_spread
            expected_margin = -mu_spread
            error = abs(margin - expected_margin)
            
            # Pick Logic
            # The model output `recommendations` list has the specific bets.
            recs = res.get('recommendations', [])
            rec_str = ""
            
            won_bet = False
            has_bet = False
            
            for r in recs:
                if r['bet_type'] == 'SPREAD':
                    has_bet = True
                    # Check if win
                    # r['selection'] e.g. "Duke -5.5"
                    # We need structured side/line from the recommendation object if available
                    # The V2 model returns UI strings in 'recommendations'.
                    # Let's look at the raw 'outputs_json' or check `res['pick']`
                    
                    pick_side = res.get('pick') # e.g. "Duke"
                    bet_line = res.get('bet_line') # e.g. -5.5
                    
                    if pick_side and bet_line is not None:
                        # Determine if Cover
                        # If pick is Home
                        if pick_side == g['home_team']:
                            # Home needs to win by more than -bet_line
                            # Margin +10. Line -5.5.
                            # Cover if Margin > -Line? 
                            # If Line is -5.5, Cover if Margin > 5.5.
                            # If Line is +3.5, Cover if Margin > -3.5.
                            
                            # General: Cover if (Margin + Line) > 0 ?
                            # -5.5 Line. +10 Margin. 10 + (-5.5) = 4.5 > 0. Win.
                            if (margin + bet_line) > 0:
                                won_bet = True
                                rec_str = f"✅ {pick_side} {bet_line}"
                            else:
                                rec_str = f"❌ {pick_side} {bet_line}"
                        else:
                            # Pick is Away
                            # Margin is (Home - Away). Away Margin is (Away - Home) = -Margin.
                            # Cover if (-Margin + Line) > 0
                            if (-margin + bet_line) > 0:
                                won_bet = True
                                rec_str = f"✅ {pick_side} {bet_line}"
                            else:
                                rec_str = f"❌ {pick_side} {bet_line}"
            
            if has_bet:
                total_spread_picks += 1
                if won_bet: correct_spread_picks += 1
            
            date_str = g['start_time'].strftime('%m-%d')
            matchup = f"{g['away_team']} @ {g['home_team']}"
            score = f"{g['away_score']}-{g['home_score']}"
            
            print(f"{date_str:<12} | {matchup:<35} | {score:<8} | {mu_spread:<6.1f} | {error:<5.1f} | {rec_str}")
            
        except Exception as e:
            # print(f"Skip {g['id']}: {e}")
            pass

    print("-" * 100)
    if total_spread_picks > 0:
        print(f"Record on Games with Edges: {correct_spread_picks}/{total_spread_picks} ({(correct_spread_picks/total_spread_picks)*100:.1f}%)")
    else:
        print("No actionable edges found in this sample.")

if __name__ == "__main__":
    run_recent_backtest()
