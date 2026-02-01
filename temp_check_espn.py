
from src.parsers.espn_client import EspnClient
import json

def check_dates():
    espn = EspnClient()
    dates = ['20260130', '20260131', '20260201', '20260202']
    
    found_any = False
    for d in dates:
        print(f"Checking {d}...")
        try:
            # fetch_scoreboard triggers ingestion, so this actively fixes if found!
            events = espn.fetch_scoreboard('NCAAM', d)
            for e in events:
                ht = e.get('home_team', '')
                at = e.get('away_team', '')
                if 'DePaul' in ht or 'DePaul' in at or 'Xavier' in ht or 'Xavier' in at:
                    print(f"MATCH FOUND on {d}: {at} @ {ht} (ID: {e['id']})")
                    found_any = True
        except Exception as e:
            print(f"Error checking {d}: {e}")

    if not found_any:
        print("DePaul/Xavier NOT found in ESPN for Jan 30 - Feb 2.")

if __name__ == "__main__":
    check_dates()
