"""
Fetch final scores from ESPN for completed games with missing scores.
"""
import requests
from src.database import get_db_connection

def fetch_espn_scores():
    print("--- Fetching Missing ESPN Scores ---")
    
    with get_db_connection() as conn:
        cur = conn.cursor()
        
        # Get ESPN events with 0-0 scores that are marked as final
        cur.execute('''
        SELECT gr.event_id
        FROM game_results gr
        WHERE gr.event_id LIKE 'espn:ncaam:%%'
          AND gr.home_score = 0 AND gr.away_score = 0
          AND gr.final = TRUE
        ''')
        events = cur.fetchall()
        print(f"Found {len(events)} ESPN events with 0-0 scores")
        
        updated = 0
        for (event_id,) in events:
            # Extract ESPN game ID
            parts = event_id.split(':')
            if len(parts) != 3:
                continue
            espn_id = parts[2]
            
            # Fetch from ESPN scoreboard API
            url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary?event={espn_id}"
            try:
                resp = requests.get(url, timeout=5)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                
                # Extract scores from boxscore or header
                header = data.get('header', {})
                competitions = header.get('competitions', [{}])[0]
                competitors = competitions.get('competitors', [])
                
                home_score = None
                away_score = None
                
                for c in competitors:
                    score = int(c.get('score', 0))
                    if c.get('homeAway') == 'home':
                        home_score = score
                    else:
                        away_score = score
                
                if home_score is not None and away_score is not None and (home_score > 0 or away_score > 0):
                    cur.execute('''
                    UPDATE game_results 
                    SET home_score = %s, away_score = %s
                    WHERE event_id = %s
                    ''', (home_score, away_score, event_id))
                    updated += 1
                    print(f"  Updated: {event_id} -> {away_score}-{home_score}")
                    
            except Exception as e:
                print(f"  Error fetching {espn_id}: {e}")
                continue
        
        conn.commit()
        print(f"\n--- Done: Updated {updated}/{len(events)} events with real scores ---")

if __name__ == "__main__":
    fetch_espn_scores()
