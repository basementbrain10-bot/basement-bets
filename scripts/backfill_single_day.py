
import sys
import os
import datetime
import time
sys.path.append(os.getcwd())

from src.services.odds_adapter import OddsAdapter
from src.action_network import ActionNetworkClient
from src.database import get_db_connection, _exec

def run_target():
    print("Purging bad data for 266442...")
    print("Purging bad data for 266442...")
    with get_db_connection() as conn:
        id_target = 'action:ncaam:266442'
        _exec(conn, "DELETE FROM odds_snapshots WHERE event_id = %s", (id_target,))
        _exec(conn, "DELETE FROM game_results WHERE event_id = %s", (id_target,))
        _exec(conn, "DELETE FROM events WHERE id = %s", (id_target,))
        # Clean future events (Cascading)
        # future_cond = "id LIKE 'action:%%' AND start_time > '2026-01-30'"
        # _exec(conn, f"DELETE FROM model_predictions WHERE event_id IN (SELECT id FROM events WHERE {future_cond})")
        # _exec(conn, f"DELETE FROM odds_snapshots WHERE event_id IN (SELECT id FROM events WHERE {future_cond})")
        # _exec(conn, f"DELETE FROM game_results WHERE event_id IN (SELECT id FROM events WHERE {future_cond})")
        # _exec(conn, f"DELETE FROM events WHERE {future_cond}")
        conn.commit()
        
    print("Backfilling Nov 20...")
    adapter = OddsAdapter()
    client = ActionNetworkClient()
    
    # Capture Time = Nov 20 Noon
    cap_dt = datetime.datetime(2025, 11, 20, 12, 0, 0, tzinfo=datetime.timezone.utc)
    date_str = "20251120"
    
    raw_games = client.fetch_odds('ncaab', dates=[date_str])
    print(f"Fetched {len(raw_games)} games.")
    
    # Store
    count = adapter.normalize_and_store(
        raw_games, 
        league="NCAAM", 
        provider="action_network", 
        captured_at=cap_dt
    )
    print(f"Stored {count} snapshots.")
    
    # Insert Scores (copied logic)
    results = 0
    with get_db_connection() as conn:
        for game in raw_games:
            gid = game.get('id')
            # Extract scores (already normalized in client)
            hs = game.get('home_score')
            as_ = game.get('away_score')
            status = game.get('status')
            
            if gid and hs is not None:
                # Debug Check
                # print(f"Inserting Result {gid}: {hs}-{as_}")
                eid = f"action:ncaam:{gid}"
                final = (status == 'complete')
                q = """
                INSERT INTO game_results (event_id, home_score, away_score, final, updated_at)
                VALUES (:eid, :hs, :as_, :final, CURRENT_TIMESTAMP)
                ON CONFLICT (event_id) DO UPDATE SET
                    home_score=EXCLUDED.home_score,
                    away_score=EXCLUDED.away_score,
                    final=EXCLUDED.final
                """
                _exec(conn, q, {"eid": eid, "hs": hs, "as_": as_, "final": final})
                results += 1
        conn.commit()
    print(f"Stored {results} results.")

if __name__ == "__main__":
    run_target()
