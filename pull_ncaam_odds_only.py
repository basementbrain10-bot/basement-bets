
import pandas as pd
import datetime
import os
import sys
from time import sleep

# Ensure we can import utils (from scripts) and src
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"))
from utils import get_todays_games, filter_data_on_change
from src.services.odds_adapter import OddsAdapter

DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../data/bets_db'))
HEADERS = {
    'Authority': 'api.actionnetwork',
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0'
}

def main():
    sport = 'ncaab'
    print(f"Processing {sport} (NCAAM only)...")
    
    # Dates: Today + next 3 days
    today = datetime.date.today()
    date_format = '%Y%m%d'
    dates = [(today + datetime.timedelta(days=i)).strftime(date_format) for i in range(0, 4)]
    
    print(f"Fetching dates: {dates}")
    
    try:
        df_new = get_todays_games(sport, dates, HEADERS)
    except Exception as e:
        print(f"Error fetching data: {e}")
        return

    if df_new is None or df_new.empty:
        print(f"No new data found for {sport}.")
        return

    print(f"Found {len(df_new)} rows.")
    
    # Ingest into DB
    try:
        adapter = OddsAdapter()
        df_clean = df_new.where(pd.notnull(df_new), None)
        raw_data = df_clean.to_dict('records')
        
        canonical_league = 'NCAAM'
        count = adapter.normalize_and_store(raw_data, league=canonical_league, provider="action_network")
        print(f"SUCCESS: Ingested {count} snapshots for {canonical_league}.")
    except Exception as e:
        print(f"DB Ingestion Error: {e}")

if __name__ == "__main__":
    main()
