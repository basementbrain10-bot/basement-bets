"""
BartTorvik Team Game Logs Scraper

Fetches team game-by-game results including 3PT%, fouls, margin.
Used to populate team_game_logs table for shooting regression analysis.
"""
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from src.database import get_db_connection, _exec
from src.utils.naming import standardize_team_name

class TorvikGameLogsScraper:
    """
    Scrapes team game logs from BartTorvik.
    Each team has a page like: barttorvik.com/team.php?team=Duke&year=2026
    """
    
    BASE_URL = "https://barttorvik.com"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def fetch_team_games(self, team_name: str, year: int = None) -> List[Dict]:
        """
        Fetch game-by-game stats for a team.
        Returns list of game dicts with 3PT%, fouls, margin, etc.
        """
        if year is None:
            year = datetime.now().year
            # CBB season spans years - if before June, use current year
            if datetime.now().month < 6:
                year = datetime.now().year
            else:
                year = datetime.now().year + 1
        
        # BartTorvik team page has game log data
        # URL format: /team.php?team=Duke&year=2026
        team_slug = team_name.replace(' ', '+')
        url = f"{self.BASE_URL}/team.php?team={team_slug}&year={year}"
        
        games = []
        
        try:
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200:
                print(f"[TORVIK] Failed to fetch {team_name}: HTTP {resp.status_code}")
                return games
            
            # Parse HTML for game log table
            # BartTorvik uses JavaScript to render, so we need to find the JSON data
            # or use a different approach
            
            # Alternative: Use the schedule.php JSON endpoint we already have
            # and cross-reference with team results
            
            # For MVP, try to extract from HTML if possible
            html = resp.text
            
            # Look for game data in the page
            # BartTorvik embeds data in JavaScript variables
            if 'var games =' in html:
                # Extract JSON from page
                import re
                match = re.search(r'var games = (\[.*?\]);', html, re.DOTALL)
                if match:
                    import json
                    games_data = json.loads(match.group(1))
                    for g in games_data:
                        games.append({
                            'team_text': team_name,
                            'game_date': g.get('date'),
                            'opponent': g.get('opp'),
                            'points': g.get('pts'),
                            'three_p_made': g.get('fg3'),
                            'three_p_attempted': g.get('fga3'),
                            'three_p_pct': g.get('fg3_pct', 0.0),
                            'fouls': g.get('pf'),
                            'opponent_rank': g.get('opp_rank'),
                            'is_home': g.get('loc') == 'H',
                            'margin': g.get('pts', 0) - g.get('opp_pts', 0)
                        })
            else:
                print(f"[TORVIK] Could not find game data for {team_name}")
                
        except Exception as e:
            print(f"[TORVIK] Error fetching {team_name}: {e}")
        
        return games
    
    def fetch_from_schedule_json(self, team_name: str) -> List[Dict]:
        """
        Alternative: Use the schedule.php JSON we know works.
        Filter for games involving this team and extract stats.
        """
        today = datetime.now()
        games = []
        
        # Look back 30 days for recent games
        for days_back in range(0, 30, 7):
            date = today - timedelta(days=days_back)
            date_str = date.strftime('%Y%m%d')
            url = f"{self.BASE_URL}/schedule.php?date={date_str}&json=1"
            
            try:
                resp = self.session.get(url, timeout=10)
                if resp.status_code == 200 and resp.text.strip().startswith('['):
                    import json
                    data = json.loads(resp.text)
                    
                    for game in data:
                        # Check if team is involved
                        home = game.get('home', '')
                        away = game.get('away', '')
                        
                        if team_name.lower() in home.lower():
                            games.append(self._parse_schedule_game(game, team_name, is_home=True))
                        elif team_name.lower() in away.lower():
                            games.append(self._parse_schedule_game(game, team_name, is_home=False))
                            
            except Exception as e:
                continue
        
        return games
    
    def _parse_schedule_game(self, game: dict, team_name: str, is_home: bool) -> dict:
        """Parse a schedule.php game entry into our format."""
        return {
            'team_text': team_name,
            'game_date': game.get('date'),
            'opponent': game.get('away') if is_home else game.get('home'),
            'points': None,  # Not in schedule data
            'three_p_made': None,
            'three_p_attempted': None,
            'three_p_pct': None,
            'fouls': None,
            'opponent_rank': None,
            'is_home': is_home,
            'margin': game.get('margin') if is_home else -game.get('margin', 0)
        }
    
    def save_games(self, games: List[Dict]):
        """Insert games into team_game_logs table."""
        if not games:
            return 0
            
        query = """
        INSERT INTO team_game_logs 
            (team_text, game_date, opponent, points, three_p_made, three_p_attempted, 
             three_p_pct, fouls, opponent_rank, is_home, margin)
        VALUES 
            (%(team_text)s, %(game_date)s, %(opponent)s, %(points)s, %(three_p_made)s, 
             %(three_p_attempted)s, %(three_p_pct)s, %(fouls)s, %(opponent_rank)s, 
             %(is_home)s, %(margin)s)
        ON CONFLICT (team_text, game_date, opponent) DO UPDATE SET
            three_p_pct = EXCLUDED.three_p_pct,
            margin = EXCLUDED.margin,
            fouls = EXCLUDED.fouls
        """
        
        count = 0
        with get_db_connection() as conn:
            cur = conn.cursor()
            for g in games:
                try:
                    cur.execute(query, g)
                    count += 1
                except Exception as e:
                    print(f"[TORVIK] Failed to save game: {e}")
            conn.commit()
        
        return count
    
    def run(self, teams: List[str] = None):
        """
        Fetch and save game logs for specified teams.
        If no teams specified, fetch for all teams in events table.
        """
        if teams is None:
            # Get unique teams from recent events
            with get_db_connection() as conn:
                result = _exec(conn, """
                    SELECT DISTINCT home_team FROM events WHERE league = 'NCAAM'
                    UNION
                    SELECT DISTINCT away_team FROM events WHERE league = 'NCAAM'
                """).fetchall()
                teams = [r[0] for r in result if r[0]]
        
        print(f"[TORVIK] Fetching game logs for {len(teams)} teams...")
        
        total = 0
        for team in teams:
            games = self.fetch_team_games(team)
            if not games:
                games = self.fetch_from_schedule_json(team)
            saved = self.save_games(games)
            total += saved
            if saved:
                print(f"  {team}: {saved} games")
        
        print(f"[TORVIK] Total: {total} game logs saved.")
        return total


if __name__ == "__main__":
    scraper = TorvikGameLogsScraper()
    scraper.run()
