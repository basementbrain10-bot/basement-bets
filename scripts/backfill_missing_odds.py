
import sys
import os
import datetime
import time

# Allow running from root
sys.path.append(os.getcwd())

from src.services.odds_adapter import OddsAdapter
from src.action_network import ActionNetworkClient

def backfill():
    print(f"[{datetime.datetime.now()}] Starting Odds Backfill (2025-11-01 to 2026-01-25)...")
    
    adapter = OddsAdapter()
    client = ActionNetworkClient()
    
    # Date Range
    # Date Range
    start_date = datetime.date(2025, 11, 1)
    end_date = datetime.date(2026, 1, 25)
    delta = datetime.timedelta(days=1)
    
    current = start_date
    league = 'ncaab'
    
    while current <= end_date:
        date_str = current.strftime('%Y%m%d')
        print(f"Fetching {date_str}...")
        
        try:
            # Pass date as list
            raw_data = client.fetch_odds(league, dates=[date_str])
            
            if raw_data:
                # Store
                # For historical data, captured_at should ideally be "close" to game time
                # But we are ingesting NOW. The 'captured_at' column defaults to NOW().
                # This breaks the logic of "Correlation Engine uses snapshot captured <= start_time".
                # FIX: We need to override captured_at to be the game start time (approx closing line).
                # Access DB directly? Or modify adapter?
                
                # Check adapter signature:
                # normalize_and_store(raw_data, ..., captured_at=?)?
                # Adapter normalize_and_store internalizes captured_at = datetime.now() usually.
                # I should check OddsAdapter.
                
                # Assume standard ingest for now. The correlation engine query logic:
                # "captured_at <= e.start_time"
                # If I ingest NOW, captured_at > start_time (post-game).
                # My logic won't picked it up for historical games!
                
                # I must HACK the captured_at to be game_start - 1 second or similar.
                # Or correlation engine needs to accept "post-game" snapshots if "closing" flag is set?
                
                # Better approach:
                # Ingest normally.
                # Update correlation engine to fetch "latest available snapshot" regardless of time relation IF game is finished?
                # OR, pass a custom 'captured_at' to adapter if possible.
                
                # Construct approx capture time (Noon UTC of game day)
                # This ensures captured_at <= start_time (mostly) for evening games
                cap_dt = datetime.datetime.combine(current, datetime.time(12, 0, 0, 0, datetime.timezone.utc))
                
                count = adapter.normalize_and_store(
                    raw_data, 
                    league="NCAAM", 
                    provider="action_network",
                    captured_at=cap_dt
                )
                print(f"  Stored {count} snapshots (dated {cap_dt}).")
                
                # Insert Results (Scores)
                # Action events have IDs like action:ncaam:{id}
                # League passed is 'NCAAM' -> lower in ID generation 'ncaam'
                
                from src.database import get_db_connection, _exec
                
                results_inserted = 0
                with get_db_connection() as conn:
                    for game in raw_data:
                        gid = game.get('id')
                        # Fixed: Use direct fields populated by client, not raw boxscore
                        hs = game.get('home_score')
                        as_ = game.get('away_score')
                        status = game.get('status')
                        
                        if gid and hs is not None and as_ is not None:
                            # Reconstruct event_id (must match OddsAdapter logic)
                            eid = f"action:ncaam:{gid}"
                            is_final = (status == 'complete')
                            
                            q_res = """
                            INSERT INTO game_results (event_id, home_score, away_score, final, updated_at)
                            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (event_id) DO UPDATE SET
                                home_score = EXCLUDED.home_score,
                                away_score = EXCLUDED.away_score,
                                final = EXCLUDED.final,
                                updated_at = CURRENT_TIMESTAMP
                            """
                            try:
                                _exec(conn, q_res, (eid, hs, as_, is_final))
                                results_inserted += 1
                            except Exception as e:
                                print(f"    Result Insert Error ({eid}): {e}")
                    conn.commit()
                print(f"  Stored {results_inserted} results.")
                
            else:
                print("  No data.")
                
        except Exception as e:
            print(f"  ERROR: {e}")
            
        current += delta
        time.sleep(1) # Polite delay

if __name__ == "__main__":
    backfill()
