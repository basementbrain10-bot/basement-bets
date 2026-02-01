
from src.parsers.espn_client import EspnClient
from src.services.odds_fetcher_service import OddsFetcherService
import json

def debug_game():
    date_str = "20260131"
    
    print(f"--- Checking ESPN for {date_str} ---")
    espn = EspnClient()
    events = espn.fetch_scoreboard("NCAAM", date_str) # This ingests too, which might restore it!
    
    found_espn = False
    for e in events:
        if "DePaul" in e['home_team'] or "DePaul" in e['away_team'] or "Xavier" in e['home_team'] or "Xavier" in e['away_team']:
            print(f"Found in ESPN: {e['away_team']} @ {e['home_team']} (ID: {e['id']})")
            found_espn = True
            
    if not found_espn:
        print("Not found in ESPN schedule.")

    print(f"\n--- Checking Action Network for {date_str} ---")
    odds_service = OddsFetcherService()
    try:
        odds = odds_service.fetch_odds("NCAAM", date_str)
        found_action = False
        for o in odds:
             if "DePaul" in o['home_team'] or "DePaul" in o['away_team'] or "Xavier" in o['home_team'] or "Xavier" in o['away_team']:
                print(f"Found in Action: {o['away_team']} @ {o['home_team']} (Spread: {o.get('home_spread')})")
                found_action = True
        
        if not found_action:
            print("Not found in Action Network odds.")
            
    except Exception as e:
        print(f"Action fetch failed: {e}")

if __name__ == "__main__":
    debug_game()
