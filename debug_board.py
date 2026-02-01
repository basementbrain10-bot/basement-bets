
from src.database import get_db_connection, _exec
from datetime import datetime, timedelta

def debug_board():
    league = "NCAAM"
    date_str = "2026-01-31"
    days = 1
    
    start_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    end_date = (start_date + timedelta(days=days - 1))
    
    print(f"Querying Board for {league} from {start_date} to {end_date}")

    query = """
    SELECT e.id, e.home_team, e.away_team, e.start_time,
           -- SPREAD (HOME/AWAY)
           s_home.line_value as home_spread,
           s_home.price as spread_home_odds,
           -- TOTAL
           t_over.line_value as total_line
    FROM events e
    LEFT JOIN (
        SELECT DISTINCT ON (event_id) event_id, line_value, price
        FROM odds_snapshots
        WHERE market_type = 'SPREAD' AND side = 'HOME'
        ORDER BY event_id, captured_at DESC
    ) s_home ON e.id = s_home.event_id
    LEFT JOIN (
        SELECT DISTINCT ON (event_id) event_id, line_value, price
        FROM odds_snapshots
        WHERE market_type = 'TOTAL' AND side = 'OVER'
        ORDER BY event_id, captured_at DESC
    ) t_over ON e.id = t_over.event_id
    WHERE e.league = :league
      AND DATE(e.start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') BETWEEN :start_date AND :end_date
    ORDER BY e.start_time ASC
    LIMIT 5
    """
    
    with get_db_connection() as conn:
        rows = _exec(conn, query, {"league": league, "start_date": str(start_date), "end_date": str(end_date)}).fetchall()
        print(f"Found {len(rows)} games.")
        for r in rows:
            print(f"{r['away_team']} @ {r['home_team']} | Spread: {r['home_spread']} | Total: {r['total_line']}")

if __name__ == "__main__":
    debug_board()
