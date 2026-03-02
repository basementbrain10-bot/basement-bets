
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

from src.database import get_db_connection, _exec, ensure_recommended_slates_tables
from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2
from src.services.grading_service import GradingService


def run_predictions(window_hours: int = 24, lookback_hours: int = 4, show_errors: bool = True, grade_completed: bool = True):
    # If our lookback window includes already-completed games, grade them first so
    # yesterday's results don't sit PENDING.
    if grade_completed:
        try:
            svc = GradingService()
            graded = svc._evaluate_db_predictions()
            if graded:
                print(f"[predict_today_v2] graded {graded} completed games before recommending")
        except Exception as e:
            if show_errors:
                print(f"[predict_today_v2] grading prepass failed: {e}")

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
          AND DATE(start_time AT TIME ZONE 'America/New_York') = :today_et
        ORDER BY start_time ASC
    """

    with get_db_connection() as conn:
        games = _exec(
            conn,
            query,
            {
                "lookback": f"{int(lookback_hours)} hours",
                "window": f"{int(window_hours)} hours",
                "today_et": today_str,
            },
        ).fetchall()

    if not games:
        print("No active/upcoming games found in window.")
        return

    scanned = 0
    recs = 0
    errors = 0

    # Track persisted picks so we can store the exact "recommended" slate.
    # Each entry: {prediction_id, ev_per_unit, selection, price, event_id, matchup}
    persisted = []

    print(f"{'MATCHUP':<40} | {'MKT':<8} | {'LINE':<8} | {'ODDS':<6} | {'EV%':<6} | {'REC'}")
    print("-" * 130)

    for g in games:
        scanned += 1
        game_id = g["id"] if isinstance(g, dict) else g[0]
        try:
            res = model.analyze(game_id)
            recommendations = res.get("recommendations", []) or []

            if recommendations:
                top_rec = recommendations[0]
                matchup = f"{g['away_team']} @ {g['home_team']}"
                # UI recommendation shape includes bet_type, selection, price, edge (EV%), book
                mkt = str(top_rec.get('bet_type') or '').upper() or '—'
                line = top_rec.get('market_line')
                price = top_rec.get('price')
                ev = top_rec.get('edge')

                def fmt_line(x):
                    try:
                        if x is None:
                            return '—'
                        return f"{float(x):g}"
                    except Exception:
                        return str(x)

                def fmt_odds(x):
                    try:
                        if x is None:
                            return '—'
                        x = int(x)
                        return f"+{x}" if x > 0 else str(x)
                    except Exception:
                        return str(x) if x is not None else '—'

                def fmt_ev(x):
                    try:
                        if x is None:
                            return '—'
                        # edge is a string like "12.1%"
                        if isinstance(x, str) and '%' in x:
                            return x.strip()
                        return f"{float(x) * 100:.1f}%"
                    except Exception:
                        return '—'

                # Always print a user-readable selection that includes the bet line.
                # Some downstream formatters summarize pick as 'home'/'away'. Here we force a concrete label.
                sel_raw = str(top_rec.get('selection') or '').strip()
                line_val = top_rec.get('market_line')

                home = g['home_team']
                away = g['away_team']

                def fmt_signed(x):
                    try:
                        if x is None:
                            return None
                        v = float(x)
                        # keep one decimal for spreads/totals
                        return f"{v:+.1f}".replace('+', '') if v < 0 else f"+{v:.1f}".replace('+', '+')
                    except Exception:
                        return str(x)

                selection = '—'
                if mkt == 'SPREAD':
                    # selection may be "Team -4.5" already OR "home"/"away".
                    if sel_raw.lower() in ('home', 'away'):
                        if line_val is not None:
                            # market_line is home perspective (neg means home favored)
                            hv = float(line_val)
                            if sel_raw.lower() == 'home':
                                selection = f"{home} {hv:+.1f}".replace('+', '+')
                            else:
                                selection = f"{away} {(-hv):+.1f}".replace('+', '+')
                        else:
                            selection = home if sel_raw.lower() == 'home' else away
                    else:
                        selection = sel_raw
                elif mkt == 'TOTAL':
                    # selection could be "OVER 147.5" or just "OVER"
                    if sel_raw.lower().startswith('over'):
                        selection = f"OVER {fmt_line(line_val)}" if line_val is not None else 'OVER'
                    elif sel_raw.lower().startswith('under'):
                        selection = f"UNDER {fmt_line(line_val)}" if line_val is not None else 'UNDER'
                    else:
                        selection = sel_raw or (f"TOTAL {fmt_line(line_val)}" if line_val is not None else 'TOTAL')
                else:
                    selection = sel_raw or '—'

                conf = top_rec.get('confidence')
                book = top_rec.get('book')
                conf_s = f" ({conf})" if conf else ''
                book_s = f" [{book}]" if book else ''

                print(
                    f"{matchup:<40} | {mkt:<8} | {fmt_line(line):<8} | {fmt_odds(price):<6} | {fmt_ev(ev):<6} | {selection}{conf_s}{book_s}"
                )
                recs += 1

                # IMPORTANT: res['id'] is an analysis-run id, not necessarily a persisted DB row.
                # To keep "recommended" aligned with grading, map to the actual model_predictions row id.
                try:
                    evu = res.get('ev_per_unit')
                    if evu is not None:
                        mp_id = None
                        with get_db_connection() as conn:
                            row = _exec(conn, """
                              SELECT id
                              FROM model_predictions
                              WHERE event_id=%s
                                AND selection=%s
                                AND COALESCE(bet_price, price) = %s
                              ORDER BY analyzed_at DESC
                              LIMIT 1
                            """, (str(game_id), str(selection), int(price) if price is not None else None)).fetchone()
                            if row:
                                mp_id = row['id'] if isinstance(row, dict) else row[0]

                        if mp_id:
                            persisted.append({
                                'prediction_id': str(mp_id),
                                'ev_per_unit': float(evu),
                                'selection': str(selection),
                                'price': price,
                                'event_id': str(game_id),
                                'matchup': matchup,
                                'market_type': str(mkt) if mkt is not None else None,
                                'bet_line': float(line_val) if line_val is not None else (float(line) if line is not None else None),
                            })
                except Exception:
                    pass
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

    # Persist a run marker so the web dashboard can explain mismatches between
    # cached daily_top_picks and the latest full run.
    try:
        status = 'NO_BETS' if recs == 0 else 'OK'
        with get_db_connection() as conn:
            _exec(conn, """
              CREATE TABLE IF NOT EXISTS model_run_log (
                id SERIAL PRIMARY KEY,
                league TEXT NOT NULL,
                date_et TEXT NOT NULL,
                run_type TEXT NOT NULL,
                status TEXT NOT NULL,
                scanned INTEGER,
                recs INTEGER,
                notes TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
              );
            """)
            _exec(conn, """
              INSERT INTO model_run_log (league, date_et, run_type, status, scanned, recs, notes)
              VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                'NCAAM',
                today_str,
                'full',
                status,
                int(scanned),
                int(recs),
                f"predict_today_v2 window={int(window_hours)}h lookback={int(lookback_hours)}h",
            ))
            try:
                conn.commit()
            except Exception:
                pass
    except Exception as e:
        if show_errors:
            print(f"[predict_today_v2] run-log insert failed: {e}")

    # Persist the exact recommended slate (Top 6 by EV/u among persisted picks).
    try:
        ensure_recommended_slates_tables()
        top = sorted(persisted, key=lambda x: float(x.get('ev_per_unit') or 0.0), reverse=True)[:6]
        if top:
            import uuid
            slate_id = str(uuid.uuid4())
            with get_db_connection() as conn:
                _exec(conn, """
                  INSERT INTO recommended_slates (id, league, date_et, source)
                  VALUES (%s, %s, %s, %s)
                """, (slate_id, 'NCAAM', today_str, 'full'))
                for i, it in enumerate(top, start=1):
                    _exec(conn, """
                      INSERT INTO recommended_slate_items (
                        slate_id, prediction_id, rank,
                        event_id, selection, bet_price, bet_line, market_type
                      )
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                      ON CONFLICT (slate_id, prediction_id) DO NOTHING
                    """, (
                        slate_id,
                        str(it.get('prediction_id')),
                        int(i),
                        str(it.get('event_id')) if it.get('event_id') is not None else None,
                        str(it.get('selection')) if it.get('selection') is not None else None,
                        int(it.get('price')) if it.get('price') is not None else None,
                        float(it.get('bet_line')) if it.get('bet_line') is not None else None,
                        str(it.get('market_type')) if it.get('market_type') is not None else None,
                    ))
                conn.commit()
            print(f"[recommended_slate] id={slate_id} items={len(top)}")
    except Exception as e:
        if show_errors:
            print(f"[predict_today_v2] recommended_slate persist failed: {e}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--window-hours", type=int, default=24)
    ap.add_argument("--lookback-hours", type=int, default=4)
    ap.add_argument("--quiet-errors", action="store_true")
    ap.add_argument("--no-grade", action="store_true", help="Skip grading completed games before recommending")
    args = ap.parse_args()

    run_predictions(
        window_hours=args.window_hours,
        lookback_hours=args.lookback_hours,
        show_errors=not args.quiet_errors,
        grade_completed=not args.no_grade,
    )
