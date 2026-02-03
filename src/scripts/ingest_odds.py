import sys
import os
import datetime

# Allow running from root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.services.odds_adapter import OddsAdapter
from src.action_network import ActionNetworkClient

def main():
    print(f"[{datetime.datetime.now()}] Starting Odds Ingestion...")
    
    adapter = OddsAdapter()
    client = ActionNetworkClient()
    
    # Leagues to ingest
    leagues = ['ncaab', 'nba'] # Add others if needed: 'nfl', 'nhl'
    
    for league in leagues:
        print(f"Fetching odds for {league}...")
        try:
            # fetch_odds defaults to today's date if not provided
            raw_data = client.fetch_odds(league)
            
            if not raw_data:
                print(f"No data found for {league}.")
                continue
                
            # Store (upserts events and odds_snapshots)
            # map 'ncaab' -> 'NCAAM' for adapter if needed, usually adapter handles 'ncaab' fine
            # or pass standard league code
            league_code = "NCAAM" if league == 'ncaab' else league.upper()
            
            count = adapter.normalize_and_store(raw_data, league=league_code, provider="action_network")
            print(f"SUCCESS: Stored {count} snapshots for {league} ({len(raw_data)} games processed)")
            
        except Exception as e:
            print(f"ERROR processing {league}: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
