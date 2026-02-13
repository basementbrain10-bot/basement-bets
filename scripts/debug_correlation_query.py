
import os
import sys
sys.path.append(os.getcwd())
import pandas as pd
from src.database import get_db_connection

def debug_query():
    print("Debugging Query Steps...")
    
    with get_db_connection() as conn:
        # 1. Check Events in Range
        q1 = """
        SELECT COUNT(*) FROM events e
        WHERE e.start_time >= '2025-11-01' AND e.start_time < '2026-05-01'
          AND (e.league = 'NCAAM' OR e.league = 'ncaab')
        """
        c1 = pd.read_sql_query(q1, conn).iloc[0, 0]
        print(f"1. Events in Range: {c1}")
        
        # 2. Check Results Joined
        q2 = q1.replace("FROM events e", "FROM events e JOIN game_results gr ON e.id = gr.event_id")
        q2 += " AND gr.final = TRUE"
        c2 = pd.read_sql_query(q2, conn).iloc[0, 0]
        print(f"2. + Final Results: {c2}")
        
        # 3. Check Odds Subquery (Sample)
        # Find one event and check its odds timestamps
        q3 = """
        SELECT e.id, e.start_time, e.home_team 
        FROM events e 
        JOIN game_results gr ON e.id = gr.event_id
        WHERE e.start_time >= '2025-11-01' AND (e.league = 'NCAAM') AND gr.final = TRUE
        LIMIT 1
        """
        sample = pd.read_sql_query(q3, conn)
        if not sample.empty:
            eid = sample.iloc[0]['id']
            start = sample.iloc[0]['start_time']
            print(f"\nSample Event: {eid} ({sample.iloc[0]['home_team']}) Start: {start}")
            
            q_odds = f"SELECT captured_at, market_type, line_value FROM odds_snapshots WHERE event_id = '{eid}' ORDER BY captured_at DESC"
            odds = pd.read_sql_query(q_odds, conn)
            print("Odds Snapshots:")
            print(odds)
        else:
            print("No sample event found.")

if __name__ == "__main__":
    debug_query()
