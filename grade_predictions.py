"""
Grade model predictions by matching to actual game results.
Uses event_id to directly join predictions with final scores.
"""
from src.database import get_db_connection

def grade_model_predictions():
    print("--- Grading Model Predictions ---")
    
    with get_db_connection() as conn:
        cur = conn.cursor()
        
        # Get pending predictions with corresponding game results
        cur.execute("""
        SELECT 
            mp.id,
            mp.event_id,
            mp.market_type,
            mp.pick,
            mp.bet_line,
            mp.selection,
            e.home_team,
            e.away_team,
            gr.home_score,
            gr.away_score,
            gr.final
        FROM model_predictions mp
        JOIN events e ON mp.event_id = e.id
        LEFT JOIN game_results gr ON mp.event_id = gr.event_id
        WHERE (mp.outcome IS NULL OR mp.outcome = 'PENDING')
          AND gr.final IS TRUE
          AND gr.home_score IS NOT NULL
          AND gr.away_score IS NOT NULL
        """)
        
        pending_with_results = cur.fetchall()
        print(f"Found {len(pending_with_results)} pending predictions with final scores")
        
        graded = 0
        for row in pending_with_results:
            mp_id, event_id, market_type, pick, bet_line, selection, home_team, away_team, home_score, away_score, final = row
            
            if home_score is None or away_score is None:
                continue
                
            home_score = int(home_score)
            away_score = int(away_score)
            home_margin = home_score - away_score
            total_score = home_score + away_score
            
            outcome = None
            
            # Determine which side was picked
            selection_str = str(selection or pick or '')
            is_home = home_team and (home_team in selection_str)
            is_away = away_team and (away_team in selection_str)
            
            # If still unclear, check if 'HOME' or 'AWAY' in pick
            if not is_home and not is_away:
                pick_str = str(pick or '').upper()
                is_home = 'HOME' in pick_str
                is_away = 'AWAY' in pick_str
            
            if market_type == 'SPREAD':
                if bet_line is not None:
                    if is_home:
                        # Home team bet: home_score + bet_line vs away_score
                        result = home_score + bet_line - away_score
                    else:
                        # Away team bet: away_score + (-bet_line) vs home_score
                        result = away_score + (-bet_line) - home_score
                    
                    if result > 0:
                        outcome = 'WON'
                    elif result < 0:
                        outcome = 'LOST'
                    else:
                        outcome = 'PUSH'
                        
            elif market_type == 'MONEYLINE':
                if is_home:
                    outcome = 'WON' if home_margin > 0 else 'LOST' if home_margin < 0 else 'PUSH'
                elif is_away:
                    outcome = 'WON' if home_margin < 0 else 'LOST' if home_margin > 0 else 'PUSH'
                    
            elif market_type == 'TOTAL':
                if bet_line is not None:
                    is_over = 'OVER' in (selection_str.upper() + str(pick or '').upper())
                    is_under = 'UNDER' in (selection_str.upper() + str(pick or '').upper())
                    
                    if is_over:
                        if total_score > bet_line:
                            outcome = 'WON'
                        elif total_score < bet_line:
                            outcome = 'LOST'
                        else:
                            outcome = 'PUSH'
                    elif is_under:
                        if total_score < bet_line:
                            outcome = 'WON'
                        elif total_score > bet_line:
                            outcome = 'LOST'
                        else:
                            outcome = 'PUSH'
            
            if outcome:
                cur.execute("UPDATE model_predictions SET outcome = %s WHERE id = %s", (outcome, mp_id))
                graded += 1
                print(f"  {away_team} {away_score} @ {home_team} {home_score}: {market_type} {pick} {bet_line} -> {outcome}")
        
        conn.commit()
        
        # Summary stats
        cur.execute("""
        SELECT 
            outcome,
            COUNT(*) as cnt
        FROM model_predictions
        WHERE outcome IN ('WON', 'LOST', 'PUSH')
        GROUP BY outcome
        ORDER BY outcome
        """)
        print(f"\n--- Summary ---")
        print(f"Graded {graded} predictions this run")
        for row in cur.fetchall():
            print(f"  {row[0]}: {row[1]}")
        
        # Win rate
        cur.execute("""
        SELECT 
            SUM(CASE WHEN outcome = 'WON' THEN 1 ELSE 0 END) as won,
            SUM(CASE WHEN outcome = 'LOST' THEN 1 ELSE 0 END) as lost,
            SUM(CASE WHEN outcome = 'PUSH' THEN 1 ELSE 0 END) as push
        FROM model_predictions
        WHERE outcome IN ('WON', 'LOST', 'PUSH')
        """)
        r = cur.fetchone()
        won, lost, push = r[0] or 0, r[1] or 0, r[2] or 0
        total = won + lost
        if total > 0:
            win_rate = (won / total) * 100
            print(f"\n  Win Rate: {win_rate:.1f}% ({won}-{lost}-{push})")

if __name__ == "__main__":
    grade_model_predictions()
