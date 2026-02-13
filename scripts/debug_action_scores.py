
import sys
import os
import json
sys.path.append(os.getcwd())
from src.action_network import get_todays_games

def debug_scores():
    print("Fetching raw games for 2025-11-20...")
    # Using get_todays_games directly to see raw data? 
    # Actually get_todays_games returns parsed data if I modified it.
    # I want to see RAW response to find where score is.
    
    # I can use requests directly.
    import requests
    url = "https://api.actionnetwork.com/web/v1/scoreboard/ncaab?date=20251120"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
    }
    resp = requests.get(url, headers=headers)
    data = resp.json()
    
    games = data.get('games', [])
    print(f"Found {len(games)} games.")
    for g in games[:1]:
        print(f"ID: {g.get('id')}Start: {g.get('start_time')}")
        odds = g.get('odds', [])
        print(f"Odds Array Len: {len(odds)}")
        if odds:
            print(f"First Odd: {json.dumps(odds[0], indent=2)}")
        
        print("Checking boxscore latest_odds...")
        box = g.get('boxscore', {})
        print(json.dumps(box.get('latest_odds'), indent=2))


if __name__ == "__main__":
    debug_scores()
