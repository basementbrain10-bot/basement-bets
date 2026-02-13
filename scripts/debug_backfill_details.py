
import os
import sys
sys.path.append(os.getcwd())
from src.database import get_db_connection, _exec

def debug_details():
    with get_db_connection() as conn:
        print("Checking Market Types for Action Events...")
        types = _exec(conn, "SELECT DISTINCT market_type FROM odds_snapshots WHERE event_id LIKE 'action%%'").fetchall()
        print(f"Market Types: {types}")
        
        print("\nChecking Intersection Sample...")
        # Find one event with results and odds
        q = """
        SELECT e.id, e.start_time, e.home_team
        FROM events e 
        JOIN game_results gr ON e.id = gr.event_id 
        JOIN odds_snapshots os ON os.event_id = e.id
        WHERE e.id LIKE 'action%%'
        LIMIT 1
        """
        row = _exec(conn, q).fetchone()
        if not row:
            print("No intersection found (weird, previous count said 420).")
            return
            
        eid, start, home = row
        print(f"Sample Event: {eid}")
        print(f"Start Time: {start} (Type: {type(start)})")
        print(f"Home Team: {home}")
        
        # Check Snapshots for this event
        print("\nSnapshots for this event:")
        snaps = _exec(conn, "SELECT market_type, captured_at, line_value FROM odds_snapshots WHERE event_id = %s", (eid,)).fetchall()
        for s in snaps:
            mt, cap, val = s
            print(f"  Type: {mt}, Cap: {cap}, Val: {val}")
            print(f"  Cap <= Start? {cap} <= {start} is {cap <= start}")

if __name__ == "__main__":
    debug_details()
