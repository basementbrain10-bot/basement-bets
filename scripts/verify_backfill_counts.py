import sys
import os
sys.path.append(os.getcwd())
from src.database import get_db_connection, _exec

def verify():
    with get_db_connection() as conn:
        obs = _exec(conn, "SELECT COUNT(*) FROM odds_snapshots WHERE captured_at >= '2025-11-01'").fetchone()[0]
        res = _exec(conn, "SELECT COUNT(*) FROM game_results WHERE updated_at >= '2025-11-01'").fetchone()[0]
        
        print(f"Odds Snapshots (Post-Nov 1): {obs}")
        print(f"Game Results (Post-Nov 1): {res}")

if __name__ == "__main__":
    verify()
