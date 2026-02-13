
import os
import sys
sys.path.append(os.getcwd())
from src.database import get_db_connection, _exec
import datetime

def check_game():
    gid = "action:ncaam:266442" # Nov 20 Purdue vs Memphis
    print(f"Checking {gid}...")
    
    with get_db_connection() as conn:
        # 1. Event
        row = _exec(conn, "SELECT id, start_time, home_team, away_team, league FROM events WHERE id = %s", (gid,)).fetchone()
        if not row:
            print("Event NOT FOUND.")
            return
        print(f"Event Found: {row}")
        start_time = row[1]
        
        # 2. Results
        res = _exec(conn, "SELECT home_score, away_score, final FROM game_results WHERE event_id = %s", (gid,)).fetchone()
        print(f"Results: {res}")
        
        # 3. Odds
        odds = _exec(conn, "SELECT count(*) FROM odds_snapshots WHERE event_id = %s AND market_type='SPREAD'", (gid,)).fetchone()[0]
        print(f"Spread Snapshots: {odds}")
        
        last_snap = _exec(conn, "SELECT captured_at FROM odds_snapshots WHERE event_id = %s AND market_type='SPREAD' ORDER BY captured_at DESC LIMIT 1", (gid,)).fetchone()
        if last_snap:
            cap = last_snap[0]
            print(f"Last Spread Snap Captured: {cap}")
            print(f"Start Time: {start_time}")
            # Check comparison logic from SQL (start_time cast to UTC)
            # Python check:
            if start_time.tzinfo is None:
                start_utc = start_time.replace(tzinfo=datetime.timezone.utc)
            else:
                start_utc = start_time
            print(f"Captured <= Start (UTC)? {cap} <= {start_utc} is {cap <= start_utc}")
            
        # 4. Metrics Join (Fuzzy)
        print("Checking Metrics Join...")
        # Check Home: Purdue Boilermakers
        home_team = row[2]
        q_met = "SELECT team_text FROM bt_team_metrics_daily WHERE %s LIKE '%%' || team_text || '%%' LIMIT 1"
        met = _exec(conn, q_met, (home_team.lower(),)).fetchone()
        print(f"Home Metrics Match? {met}")

if __name__ == "__main__":
    check_game()
