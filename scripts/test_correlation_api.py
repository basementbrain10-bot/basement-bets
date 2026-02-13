
import sys
import os
import json
sys.path.append(os.getcwd())

from src.api import get_correlation_summary, get_game_correlation
# Mock request object? 
# These are FastAPI endpoints, they return data directly if called as functions usually, 
# but might depend on Request object if using dependencies.
# Checking src/api.py signatures.

# src/api.py:
# @app.get(...)
# def get_correlation_summary(): ... returns dict
#
# @app.get(...)
# def get_game_correlation(event_id: str): ... returns dict

# So I can just call them.

def test_api():
    print("Testing Correlation API functions...")
    
    # 1. Summary
    try:
        summary = get_correlation_summary()
        print(f"Summary Keys: {list(summary.keys())}")
        if 'error' in summary:
             print(f"Summary Error: {summary['error']}")
        else:
             print("Summary returned successfully.")
    except Exception as e:
        print(f"Summary Failed: {e}")

    # 2. Game Correlation (Need a valid event_id from DB)
    # I'll just try a dummy or fetch one.
    from src.database import get_db_connection, _exec
    with get_db_connection() as conn:
        res = _exec(conn, "SELECT id FROM events WHERE league='NCAAM' ORDER BY start_time DESC LIMIT 1").fetchone()
        eid = res[0] if res else "dummy"
        
    print(f"Testing Game Correlation for {eid}...")
    try:
        game_corr = get_game_correlation(eid)
        print("Game Correlation Result:")
        print(json.dumps(game_corr, indent=2))
    except Exception as e:
        print(f"Game Correlation Failed: {e}")

if __name__ == "__main__":
    test_api()
