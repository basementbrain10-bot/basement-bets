
import sys
import os
import argparse
from datetime import datetime

# Add repo root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.database import get_db_connection, _exec
from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2


def run_recent_backtest(limit: int = 15, show_errors: bool = True):
    print(f"--- Running Backtest on Last {limit} Finished Games ---")
    model = NCAAMMarketFirstModelV2()

    query = """
        SELECT e.id, e.home_team, e.away_team, e.start_time, gr.home_score, gr.away_score
        FROM events e
        JOIN game_results gr ON e.id = gr.event_id
        WHERE e.league = 'NCAAM'
          AND gr.home_score IS NOT NULL
        ORDER BY e.start_time DESC
        LIMIT :limit
    """

    with get_db_connection() as conn:
        games = _exec(conn, query, {"limit": int(limit)}).fetchall()

    if not games:
        print("No recent finished games found.")
        return

    print(f"{'DATE':<8} | {'MATCHUP':<35} | {'RES':<9} | {'MU':<7} | {'ERR':<6} | {'REC'}")
    print("-" * 110)

    correct_spread_picks = 0
    total_spread_picks = 0
    errors = 0

    for g in games:
        game_id = g["id"] if isinstance(g, dict) else g[0]
        try:
            res = model.analyze(game_id)

            mu_spread = res.get("mu_final")  # home spread (negative = home favored)
            if mu_spread is None:
                continue

            margin = (g["home_score"] - g["away_score"]) if isinstance(g, dict) else (g[4] - g[5])
            expected_margin = -float(mu_spread)
            error = abs(margin - expected_margin)

            recs = res.get("recommendations", []) or []
            rec_str = ""

            won_bet = False
            has_bet = False

            for r in recs:
                if r.get("bet_type") != "SPREAD":
                    continue
                has_bet = True

                pick_side = res.get("pick")
                bet_line = res.get("bet_line")

                if pick_side and bet_line is not None:
                    bet_line = float(bet_line)
                    if pick_side == g["home_team"]:
                        won_bet = (margin + bet_line) > 0
                    else:
                        won_bet = ((-margin) + bet_line) > 0

                    icon = "✅" if won_bet else "❌"
                    rec_str = f"{icon} {pick_side} {bet_line}"

            if has_bet:
                total_spread_picks += 1
                if won_bet:
                    correct_spread_picks += 1

            date_val = g["start_time"] if isinstance(g, dict) else g[3]
            date_str = date_val.strftime("%m-%d") if hasattr(date_val, "strftime") else str(date_val)[:5]
            matchup = f"{g['away_team']} @ {g['home_team']}"
            score = f"{g['away_score']}-{g['home_score']}"

            print(f"{date_str:<8} | {matchup:<35} | {score:<9} | {mu_spread:<7.1f} | {error:<6.1f} | {rec_str}")

        except Exception as e:
            errors += 1
            if show_errors:
                matchup = f"{g['away_team']} @ {g['home_team']}"
                print(f"[ERR] {matchup} (id={game_id}): {e}")

    print("-" * 110)
    if total_spread_picks > 0:
        print(
            f"Record on Games with SPREAD recs: {correct_spread_picks}/{total_spread_picks} ({(correct_spread_picks/total_spread_picks)*100:.1f}%)"
        )
    else:
        print("No actionable SPREAD edges found in this sample.")

    if errors and show_errors:
        print(f"[WARN] {errors} games errored during analyze().")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=15)
    ap.add_argument("--quiet-errors", action="store_true")
    args = ap.parse_args()

    run_recent_backtest(limit=args.limit, show_errors=not args.quiet_errors)
