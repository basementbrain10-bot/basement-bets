import requests
import json
from datetime import datetime

date_str = "20260220"
url = "https://api.actionnetwork.com/web/v2/scoreboard/ncaab"
headers = {
    'Authority': 'api.actionnetwork.com',
    'Accept': 'application/json',
    'Origin': 'https://www.actionnetwork.com',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36'
}
params = {
    "bookIds": "15,30,79,2988,75,123,71,68,69",
    "periods": "event",
    "date": date_str,
    "division": "D1",
}

print(f"Fetching games for {date_str}...")
resp = requests.get(url, params=params, headers=headers, timeout=20)
data = resp.json()
games = data.get('games', [])

found = False
for g in games:
    away = g.get('away_team', {}).get('full_name')
    home = g.get('home_team', {}).get('full_name')
    if 'Purdue' in str(away) or 'Purdue' in str(home) or 'Indiana' in str(away) or 'Indiana' in str(home):
        print(f"Found Game: {away} @ {home} | ID: {g.get('id')} | Status: {g.get('status')}")
        found = True

if not found:
    print("Purdue vs Indiana not found in Action Network scoreboard for this date/division.")
