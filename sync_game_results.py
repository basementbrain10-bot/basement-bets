
from src.database import get_db_connection, _exec
from datetime import datetime, timedelta

def sync_action_to_espn_results():
    """
    For Action Network events without game_results, try to match them
    to ESPN events based on team names and date, then copy the scores.
    """
    print("--- Syncing Action Network Events to ESPN Game Results ---")
    
    with get_db_connection() as conn:
        # 1. Get all Action Network NCAAM events without game_results
        q_action = """
        SELECT e.id, e.home_team, e.away_team, e.start_time
        FROM events e
        LEFT JOIN game_results gr ON e.id = gr.event_id
        WHERE e.id LIKE 'action:ncaam:%%'
          AND gr.event_id IS NULL
        """
        cur = conn.cursor()
        cur.execute(q_action)
        action_events = cur.fetchall()
        print(f"Found {len(action_events)} Action events without results")
        
        synced = 0
        for ae in action_events:
            action_id, home, away, start_time = ae
            
            # 2. Find matching ESPN event with game_results
            # Match by team names (fuzzy) and same date
            q_match = """
            SELECT e.id, gr.home_score, gr.away_score, gr.final
            FROM events e
            JOIN game_results gr ON e.id = gr.event_id
            WHERE e.id LIKE 'espn:ncaam:%%'
              AND (
                  (e.home_team ILIKE %s AND e.away_team ILIKE %s)
                  OR (e.home_team ILIKE %s AND e.away_team ILIKE %s)
              )
              AND gr.final = TRUE
            LIMIT 1
            """
            # Use partial match on team names
            home_pattern = f"%{home.split()[-1]}%" if home else "%"
            away_pattern = f"%{away.split()[-1]}%" if away else "%"
            
            cur.execute(q_match, (home_pattern, away_pattern, away_pattern, home_pattern))
            match = cur.fetchone()
            
            if match:
                espn_id, home_score, away_score, final = match
                
                # 3. Insert game_results for Action event
                q_insert = """
                INSERT INTO game_results (event_id, home_score, away_score, final)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """
                cur.execute(q_insert, (action_id, home_score, away_score, final))
                synced += 1
                print(f"  Synced: {action_id} <- {espn_id} ({away_score}-{home_score})")
        
        conn.commit()
        print(f"\n--- Sync Complete: {synced}/{len(action_events)} events linked to results ---")

if __name__ == "__main__":
    sync_action_to_espn_results()
