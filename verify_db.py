
from src.database import get_db_connection, _exec
from datetime import datetime, timezone

def check_data():
    today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    today_yyyymmdd = datetime.now(timezone.utc).strftime('%Y%m%d')
    
    print(f"Checking Data for Today: {today_str} ({today_yyyymmdd})")
    print("-" * 50)
    
    with get_db_connection() as conn:
        # 1. Events
        cur = _exec(conn, """
            SELECT count(*) FROM events 
            WHERE start_time >= %s::timestamp 
            AND start_time < %s::timestamp + interval '24 hours'
            AND league = 'NCAAM'
        """, (today_str, today_str))
        event_count = cur.fetchone()[0]
        print(f"Events (NCAAM): {event_count}")
        
        # 2. Odds Snapshots (Recent)
        cur = _exec(conn, """
            SELECT count(*) FROM odds_snapshots 
            WHERE captured_at > %s::timestamp
        """, (today_str,))
        odds_count = cur.fetchone()[0]
        print(f"Odds Snapshots (Since midnight): {odds_count}")
        
        # 3. Torvik Schedule
        cur = _exec(conn, """
            SELECT count(*) FROM bt_daily_schedule_raw 
            WHERE date = %s
        """, (today_yyyymmdd,))
        torvik_count = cur.fetchone()[0]
        print(f"Torvik Schedule (Raw): {torvik_count}")
        
        # 4. Torvik Metrics
        cur = _exec(conn, """
            SELECT count(*) FROM bt_team_metrics_daily 
            WHERE date = %s
        """, (today_yyyymmdd,))
        metrics_count = cur.fetchone()[0]
        print(f"Torvik Metrics (Daily): {metrics_count}")

if __name__ == "__main__":
    check_data()
