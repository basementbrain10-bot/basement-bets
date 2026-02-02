#!/usr/bin/env python3
"""
Populate Torvik Data using BartTorvikClient.

Uses the get_efficiency_ratings() method which fetches from 
2026_team_results.json and persists to bt_team_metrics_daily.

Run locally when you need a database refresh:
  python scripts/populate_torvik.py
"""
import sys
sys.path.insert(0, '.')

from datetime import datetime
from src.services.barttorvik import BartTorvikClient

def main():
    print("=" * 50)
    print("Torvik Database Populator")
    print(f"Time: {datetime.now().isoformat()}")
    print("=" * 50)
    
    client = BartTorvikClient()
    
    print("\n[STEP 1] Fetching efficiency ratings...")
    print("(This uses 2026_team_results.json and auto-persists to DB)\n")
    
    ratings = client.get_efficiency_ratings(year=2026)
    
    if ratings:
        print("=" * 50)
        print(f"✅ Loaded {len(ratings)} teams into database")
        
        # Show sample
        print("\nSample teams:")
        for name, stats in list(ratings.items())[:5]:
            print(f"  {name}: AdjOE={stats['off_rating']:.1f}, AdjDE={stats['def_rating']:.1f}, Tempo={stats['tempo']:.1f}")
    else:
        print("=" * 50)
        print("⚠️ Could not fetch ratings (Torvik may be blocking)")


if __name__ == "__main__":
    main()
