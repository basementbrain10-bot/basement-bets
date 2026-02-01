"""
Fetch final scores from Action Network for all NCAAM games.
Action Network is the canonical source for grading results.
"""
import requests
from datetime import datetime, timedelta
from src.database import get_db_connection

HEADERS = {
    'Authority': 'api.actionnetwork.com',
    'Accept': 'application/json',
    'Origin': 'https://www.actionnetwork.com',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
}

def fetch_action_scores(date_str: str):
    """Fetch game scores from Action Network for a specific date."""
    url = "https://api.actionnetwork.com/web/v2/scoreboard/ncaab"
    params = {
        "bookIds": "15,30,79,2988,75,123,71,68,69",
        "periods": "event",
        "date": date_str,
        "division": "D1",
    }
    
    try:
        resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get('games', [])
    except Exception as e:
        print(f"Error fetching {date_str}: {e}")
        return []

def extract_scores(game):
    """Extract home/away scores from Action Network game object."""
    home_score = None
    away_score = None
    
    # Check boxscore first
    boxscore = game.get('boxscore', {})
    if boxscore:
        home_score = boxscore.get('total_home_points')
        away_score = boxscore.get('total_away_points')
    
    # Also check teams array for score
    if home_score is None or away_score is None:
        teams = game.get('teams', [])
        home_id = game.get('home_team_id')
        away_id = game.get('away_team_id')
        
        for t in teams:
            if t.get('id') == home_id:
                home_score = home_score or t.get('score')
            elif t.get('id') == away_id:
                away_score = away_score or t.get('score')
    
    return home_score, away_score

def populate_action_scores():
    """Main: Fetch scores from Action Network and populate game_results."""
    print("--- Populating Game Results from Action Network ---")
    
    # Fetch games for the last 7 days
    dates = []
    today = datetime.now()
    for i in range(7):
        d = today - timedelta(days=i)
        dates.append(d.strftime("%Y%m%d"))
    
    all_games = []
    for date_str in dates:
        games = fetch_action_scores(date_str)
        print(f"  {date_str}: {len(games)} games")
        all_games.extend(games)
    
    print(f"\nTotal games fetched: {len(all_games)}")
    
    # Filter to completed games
    completed = [g for g in all_games if g.get('status') in ('complete', 'final', 'closed')]
    print(f"Completed games: {len(completed)}")
    
    with get_db_connection() as conn:
        cur = conn.cursor()
        updated = 0
        inserted = 0
        
        for game in completed:
            action_id = f"action:ncaam:{game.get('id')}"
            home_score, away_score = extract_scores(game)
            
            if home_score is None or away_score is None:
                continue
            
            # Get team names for debugging
            teams = game.get('teams', [])
            home_id = game.get('home_team_id')
            away_id = game.get('away_team_id')
            home_name = next((t.get('full_name') for t in teams if t.get('id') == home_id), 'Unknown')
            away_name = next((t.get('full_name') for t in teams if t.get('id') == away_id), 'Unknown')
            
            # Check if game_result exists
            cur.execute("SELECT event_id FROM game_results WHERE event_id = %s", (action_id,))
            exists = cur.fetchone()
            
            if exists:
                # Update existing
                cur.execute("""
                UPDATE game_results 
                SET home_score = %s, away_score = %s, final = TRUE
                WHERE event_id = %s
                """, (int(home_score), int(away_score), action_id))
                if cur.rowcount > 0:
                    updated += 1
            else:
                # Insert new - only if event exists in events table
                cur.execute("SELECT id FROM events WHERE id = %s", (action_id,))
                event_exists = cur.fetchone()
                
                if event_exists:
                    try:
                        cur.execute("""
                        INSERT INTO game_results (event_id, home_score, away_score, final)
                        VALUES (%s, %s, %s, TRUE)
                        ON CONFLICT (event_id) DO UPDATE SET
                            home_score = EXCLUDED.home_score,
                            away_score = EXCLUDED.away_score,
                            final = TRUE
                        """, (action_id, int(home_score), int(away_score)))
                        inserted += 1
                        print(f"  + {away_name} {away_score} @ {home_name} {home_score}")
                    except Exception as e:
                        print(f"  Error inserting {action_id}: {e}")
        
        conn.commit()
        print(f"\n--- Done: {updated} updated, {inserted} inserted ---")

if __name__ == "__main__":
    populate_action_scores()
