
import os
import sys
sys.path.append(os.getcwd())
from src.database import get_db_connection, _exec

def check_counts():
    print("Checking DB Counts for Action Network Backfill...")
    with get_db_connection() as conn:
        # Events
        c_ev = _exec(conn, "SELECT COUNT(*) FROM events WHERE id LIKE 'action%%'").fetchone()[0]
        print(f"Action Events: {c_ev}")
        
        # Results
        c_res = _exec(conn, "SELECT COUNT(*) FROM game_results WHERE event_id LIKE 'action%%'").fetchone()[0]
        print(f"Action Results: {c_res}")
        
        # Odds
        c_odds = _exec(conn, "SELECT COUNT(*) FROM odds_snapshots WHERE event_id LIKE 'action%%'").fetchone()[0]
        print(f"Action Odds: {c_odds}")
        
        # Intersection (Event + Result)
        q_join = "SELECT COUNT(*) FROM events e JOIN game_results gr ON e.id = gr.event_id WHERE e.id LIKE 'action%%'"
        c_join = _exec(conn, q_join).fetchone()[0]
        print(f"Action Events with Results: {c_join}")
        
        # Intersection (Event + Result + Odds)
        q_full = """
        SELECT COUNT(DISTINCT e.id) 
        FROM events e 
        JOIN game_results gr ON e.id = gr.event_id 
        JOIN odds_snapshots os ON os.event_id = e.id
        WHERE e.id LIKE 'action%%'
        """
        c_full = _exec(conn, q_full).fetchone()[0]
        print(f"Action Events with Results AND Odds: {c_full}")

if __name__ == "__main__":
    check_counts()
