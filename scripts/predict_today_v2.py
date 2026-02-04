
import sys
import os
import argparse
from datetime import datetime, timedelta

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None

# Add repo root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.database import get_db_connection, _exec
from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2


def run_predictions(window_hours: int = 24, lookback_hours: int = 4, show_errors: bool = True):
    model = NCAAMMarketFirstModelV2()

    # Use proper ET date display when available
    now_utc = datetime.utcnow()
    if ZoneInfo:
        now_et = datetime.now(tz=ZoneInfo("America/New_York"))
        today_str = now_et.strftime("%Y-%m-%d")
    else:
        # fallback (approx ET)
        today_str = (now_utc - timedelta(hours=5)).strftime("%Y-%m-%d")

    print(f"--- Basement Bets V2 Predictions for {today_str} ---")

    query = """
        SELECT id, home_team, away_team, start_time
        FROM events
        WHERE league = 'NCAAM'
          AND start_time >= NOW() - INTERVAL :lookback
          AND start_time <= NOW() + INTERVAL :window
        ORDER BY start_time ASC
    """

    with get_db_connection() as conn:
        games = _exec(
            conn,
            query,
            {"lookback": f"{int(lookback_hours)} hours", "window": f"{int(window_hours)} hours"},
        ).fetchall()

    if not games:
        print("No active/upcoming games found in window.")
        return

    scanned = 0
    recs = 0
    errors = 0

    print(f"{'MATCHUP':<40} | {'MKT':<8} | {'FAIR':<8} | {'EDGE':<8} | {'REC'}")
    print("-" * 110)

    for g in games:
        scanned += 1
        game_id = g["id"] if isinstance(g, dict) else g[0]
        try:
            res = model.analyze(game_id)
            recommendations = res.get("recommendations", []) or []

            if recommendations:
                top_rec = recommendations[0]
                matchup = f"{g['away_team']} @ {g['home_team']}"
                mkt_line = top_rec.get("market_line", 0.0)
                fair_line = top_rec.get("fair_line", 0.0)
                edge_pct = top_rec.get("edge", "0%")

                print(
                    f"{matchup:<40} | {str(mkt_line):<8} | {str(fair_line):<8} | {str(edge_pct):<8} | {top_rec.get('selection')} ({top_rec.get('confidence')})"
                )
                recs += 1
        except Exception as e:
            errors += 1
            if show_errors:
                matchup = f"{g['away_team']} @ {g['home_team']}"
                print(f"[ERR] {matchup} (id={game_id}): {e}")

    if recs == 0:
        print(f"Scanned {scanned} games. No actionable edges found.")
    else:
        print(f"\nFound {recs} plays from {scanned} games.")

    if errors and show_errors:
        print(f"[WARN] {errors} games errored during analyze().")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--window-hours", type=int, default=24)
    ap.add_argument("--lookback-hours", type=int, default=4)
    ap.add_argument("--quiet-errors", action="store_true")
    args = ap.parse_args()

    run_predictions(
        window_hours=args.window_hours,
        lookback_hours=args.lookback_hours,
        show_errors=not args.quiet_errors,
    )
