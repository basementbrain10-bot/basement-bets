import os
import sys
import json
from datetime import datetime, timedelta

# Add src to path
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database import get_db_connection, _exec, update_model_prediction_result, upsert_game_result
from parsers.espn_client import EspnClient
from services.odds_selection_service import OddsSelectionService
from src.action_network import get_todays_games

# Load env variables if not already loaded
from dotenv import load_dotenv
load_dotenv()

class GradingService:
    def __init__(self):
        self.espn_client = EspnClient()
        self.odds_selector = OddsSelectionService()

    def grade_predictions(self, *, backfill_days: int = 3, max_clv_rows: int = 250, max_grade_rows: int = 500, skip_clv: bool = False):
        """Grade pending predictions.

        IMPORTANT: This method can be invoked from a Vercel function (timeout-sensitive).
        Defaults are intentionally bounded.
        """
        print("[GRADING] Starting grading process...")

        # 1) Update Game Results (Ingest latest finals)
        # NOTE: We currently grade NCAAM using Action Network as the source of truth.
        active_leagues = ['NCAAM']
        for league in active_leagues:
            self._ingest_latest_scores(league)

        # 2) Compute CLV for started games (bounded)
        clv_count = 0
        if not skip_clv:
            clv_count = self._compute_clv_for_started_games(max_rows=max_clv_rows, lookback_days=backfill_days)

        # 3) Grade outcomes for finals (bounded)
        graded_count = self._evaluate_db_predictions(max_rows=max_grade_rows)

        return {"status": "Success", "graded": graded_count, "clv_updates": clv_count, "skip_clv": bool(skip_clv), "backfill_days": backfill_days}

    def _ingest_latest_scores(self, league):
        """Fetch scores and upsert to game_results.

        Primary goal: keep grading unblocked.

        - For NCAAM: prefer Action Network (our events are action:ncaam:* so matching is clean).
        - For other leagues: keep ESPN for now.
        """
        print(f"[GRADING] Fetching scores for {league}...")

        # Backfill window: history tab can include older games; keep this reasonably small
        # to avoid heavy API usage.
        backfill_days = int(os.getenv('GRADING_FINALS_BACKFILL_DAYS', '10'))
        dates = [
            (datetime.now() - timedelta(days=d)).strftime("%Y%m%d")
            for d in range(0, backfill_days + 1)
        ]

        # 1) Action Network primary (NCAAM)
        if league == 'NCAAM':
            # Cache the set of known event ids so we don't violate FK constraints in game_results.
            known_event_ids = set()
            try:
                with get_db_connection() as conn:
                    rows = _exec(
                        conn,
                        """
                        SELECT id
                        FROM events
                        WHERE league = 'NCAAM'
                          AND id LIKE 'action:ncaam:%%'
                          AND start_time >= (NOW() - (%(days)s || ' days')::interval)
                        """,
                        {"days": backfill_days + 2},
                    ).fetchall()
                    known_event_ids = {r['id'] for r in rows}
            except Exception as e:
                print(f"[GRADING] Warning: could not prefetch known NCAAM Action event ids: {e}")

            count = 0
            seen_game_ids = set()
            for date_str in dates:
                try:
                    # Action Network date param appears to be UTC-based; to reliably capture
                    # late-night ET games, also query the following UTC day.
                    try:
                        dt0 = datetime.strptime(date_str, "%Y%m%d")
                        date_next = (dt0 + timedelta(days=1)).strftime("%Y%m%d")
                    except Exception:
                        date_next = None

                    query_dates = [date_str] + ([date_next] if date_next else [])
                    games = get_todays_games('ncaab', query_dates)
                    for g in games:
                        if not g.get('completed'):
                            continue
                        game_id = g.get('id') or g.get('game_id')
                        if not game_id:
                            continue
                        if game_id in seen_game_ids:
                            continue
                        seen_game_ids.add(game_id)

                        # Our internal event ids are namespaced.
                        event_id = f"action:ncaam:{game_id}"
                        if known_event_ids and event_id not in known_event_ids:
                            continue

                        home_score = None
                        away_score = None
                        # The helper tries to include scores when available
                        scores = g.get('scores') or []
                        if len(scores) >= 2:
                            # scores are [{name, score}, {name, score}]
                            # Determine which is home/away using team names
                            home_team = g.get('home_team')
                            away_team = g.get('away_team')
                            for s in scores:
                                if s.get('name') == home_team:
                                    home_score = s.get('score')
                                if s.get('name') == away_team:
                                    away_score = s.get('score')

                        if home_score is None or away_score is None:
                            continue

                        res_data = {
                            "event_id": event_id,
                            "home_score": int(home_score),
                            "away_score": int(away_score),
                            "final": True,
                            "period": "FINAL",
                        }
                        upsert_game_result(res_data)
                        count += 1
                except Exception as e:
                    print(f"[GRADING] Action Network error fetching NCAAM {date_str}: {e}")

            # Note: we also run ESPN fallback below to support legacy espn:ncaam:* event ids.

        # 2) ESPN fallback (DISABLED)
        # We intentionally do *not* call ESPN here to avoid hangs/loops in scheduled jobs.
        # If you need ESPN again, re-enable behind an env flag.
        return

    def _compute_clv_for_started_games(self, *, max_rows: int = 250, lookback_days: int = 3):
        """Compute CLV for games that have started.

        Bounded for serverless execution.
        """
        min_ev = float(os.getenv('GRADING_MIN_EV_PER_UNIT', '0.02'))
        query = """
        SELECT m.id, m.event_id, m.market_type, m.pick, m.open_line, m.open_price, e.start_time, e.league
        FROM model_predictions m
        JOIN events e ON m.event_id = e.id
        WHERE m.close_line IS NULL 
          AND e.start_time < CURRENT_TIMESTAMP
          AND e.start_time > (CURRENT_TIMESTAMP - (%(d)s || ' days')::interval)
          AND COALESCE(m.ev_per_unit, 0) >= %(min_ev)s
        ORDER BY e.start_time DESC
        LIMIT %(lim)s
        """
        
        with get_db_connection() as conn:
            rows = _exec(conn, query, {"d": int(lookback_days), "lim": int(max_rows), "min_ev": float(min_ev)}).fetchall()
            
        updates = 0
        now = datetime.now() # naive or tz aware? DB timestamps usually naive UTC or similar in this app
        
        # print(f"[CLV] Checking {len(rows)} candidates for closing lines...")
        
        for r in rows:
            # Parse start_time
            st_raw = r['start_time']
            if not st_raw: continue
            
            # Simple check: has game started?
            # Assume DB stores ISO strings or datetime objects
            if isinstance(st_raw, str):
                try:
                    start_dt = datetime.fromisoformat(st_raw.replace('Z', '+00:00'))
                    # Strip TZ for naive comparison if needed, or ensure now is TZ aware
                    if start_dt.tzinfo:
                        start_dt = start_dt.replace(tzinfo=None) # naive UTC assumption
                except:
                    continue
            else:
                start_dt = st_raw # datetime object
            
            if start_dt > now:
                continue # Not started yet
                
            # Game Started: Find Closing Line
            # 1. Fetch all snapshots for event
            raw_snaps = []
            snap_q = "SELECT * FROM odds_snapshots WHERE event_id = :eid AND captured_at <= :st ORDER BY captured_at DESC LIMIT 100"
            
            # We need to format start_dt back to string for query if needed
            st_str = start_dt.isoformat()
            
            with get_db_connection() as conn:
                raw_snaps = [dict(s) for s in _exec(conn, snap_q, {"eid": r['event_id'], "st": st_str}).fetchall()]
                
            if not raw_snaps:
                # print(f"[CLV] No snapshots found before start for {r['event_id']}")
                continue
                
            # 2. Select 'Best' Closing Snapshot
            # Determine side from pick/market
            # Market type: SPREAD, TOTAL
            # Pick: Team Name (Spread) or OVER/UNDER (Total)
            
            # Map pick to side
            target_side = None
            if r['market_type'] == 'TOTAL':
                target_side = r['pick'] # OVER/UNDER
            elif r['market_type'] == 'SPREAD':
                # Pick is a Team Name. We need HOME/AWAY.
                # Use event or logic? 
                # Ideally selection service handles 'HOME'/'AWAY' if we pass it? 
                # Or we try to match pick to name?
                # Let's try both HOME/AWAY and see which matches pick?
                # Actually, simpler: Select Best SPREAD (priority). 
                # If that snapshot's home team == pick, side is HOME.
                pass
            
            # Use Selector
            best_snap = self.odds_selector.select_best_snapshot(raw_snaps, r['market_type'])
            
            if best_snap:
                close_line = best_snap.get('line_value')
                close_price = best_snap.get('price')
                
                if close_line is not None:
                    # Calculate CLV Points
                    # Spread: (Close - Open) * Direction
                    # If I extracted pick direction correctly...
                    
                    # Problem: r['pick'] is 'Duke'. best_snap has 'spread_home' (Duke by -5).
                    # We need to normalize.
                    
                    # If best_snap is flat row from DB: market_type, side, line_value...
                    # Oh, select_best_snapshot returns a SNAPSHOT ROW.
                    # It has 'side' (HOME/AWAY/OVER/UNDER).
                    pass
                    
                    # Logic:
                    # If I bet HOME -3. (Open = -3). 
                    # Closing Snap: HOME -5. (Close = -5).
                    # CLV = (-5) - (-3) = -2? 
                    # Wait. -5 is "Better" or "Worse"?
                    # Favored by 5 is "Better" than Favored by 3? 
                    # Actually, if I bet -3 (needing to win by >3), and market closes -5 (expects win by 5),
                    # I got a "good" number. -3 is EASIER to cover than -5.
                    # So CLV = Open - Close? (-3) - (-5) = +2 points. 
                    
                    # Example 2: Bet Under dog +7. Open = +7. 
                    # Closes +4. (Market lost faith in dog).
                    # I have +7. Market has +4.
                    # +7 is Better than +4.
                    # CLV = Open - Close = 7 - 4 = +3 points.
                    
                    # Does this hold? Open - Close works for standard conventions?
                    # Home -3 vs Home -5: -3 - (-5) = +2. Yes.
                    # Home +7 vs Home +4: 7 - 4 = +3. Yes.
                    
                    # Exception: TOTALS.
                    # Bet OVER 140. Closes 145.
                    # 140 is easier than 145. (Good).
                    # Open - Close = 140 - 145 = -5. (Bad sign?).
                    # For OVER, Lower is better. So Open < Close is GOOD.
                    # CLV = Close - Open? 145 - 140 = +5.
                    # Bet UNDER 140. Closes 135.
                    # 140 is easier than 135. (Good).
                    # Open > Close is GOOD.
                    # CLV = Open - Close? 140 - 135 = +5.
                    
                    # Formula:
                    # SPREAD: Open - Close (since line is relative to MY side... wait, DB lines are usually Home relative or Side relative?)
                    # model_predictions.bet_line IS relative to the pick in V2!
                    # So if I picked Duke -5, bet_line is -5.
                    
                    # We need the close_line RELATIVE TO THE PICK.
                    # best_snap has 'line_value' and 'side'.
                    # If best_snap['side'] == 'HOME' and pick is Home Team -> line matches.
                    # If best_snap['side'] == 'AWAY' and pick is Home Team -> line is inverted?
                    # The DB `odds_snapshots` usually stores line for the specific side.
                    # EXCEPT: Totals usually store the total score.
                    
                    # Let's trust `odds_selector` picked a relevant line.
                    # But `odds_selector` blindly picks "best" purely by priority. It might pick an AWAY line when I bet HOME line?
                    # Actually `odds_selector.select_best_snapshot` takes a `side` arg!
                    # I should pass the side I bet on!
                    
                    # Resolve side from pick
                    mapped_side = None
                    if r['market_type'] == 'TOTAL':
                        mapped_side = r['pick'] # OVER/UNDER
                    elif r['market_type'] == 'SPREAD':
                        q_ev = "SELECT home_team, away_team FROM events WHERE id=:eid"
                        with get_db_connection() as conn:
                            evt = _exec(conn, q_ev, {"eid": r['event_id']}).fetchone()
                            if evt:
                                if r['pick'] == evt['home_team']: mapped_side = 'HOME'
                                elif r['pick'] == evt['away_team']: mapped_side = 'AWAY'
                    
                    if mapped_side:
                        # Re-select with strict side
                        specific_snap = self.odds_selector.select_best_snapshot(raw_snaps, r['market_type'], side=mapped_side)
                        if specific_snap:
                            close_line = specific_snap['line_value']
                            close_price = specific_snap['price']
                            
                            # Final CLV calc
                            if r['market_type'] == 'SPREAD':
                                clv = r['open_line'] - close_line # Open - Close (since lower magnitude negative is good/bad??)
                                # Let's re-verify:
                                # Bet Home -3. Close Home -5.
                                # -3 - (-5) = +2. Good.
                                # Bet Home +7. Close Home +4.
                                # +7 - 4 = +3. Good.
                                # Bet Home +3. Close Home +5.
                                # +3 - 5 = -2. Bad.
                                # Seems correct: Open - Close.
                                
                            elif r['market_type'] == 'TOTAL':
                                if r['pick'] == 'OVER':
                                    clv = close_line - r['open_line'] # Close - Open
                                else:
                                    clv = r['open_line'] - close_line # Open - Close
                            else:
                                clv = 0 # ML todo
                                
                            # Update DB
                            u_q = """
                            UPDATE model_predictions SET 
                                close_line=:cl, close_price=:cp, clv_points=:cv, close_captured_at=:ts
                            WHERE id=:id
                            """
                            with get_db_connection() as conn:
                                _exec(conn, u_q, {
                                    "cl": close_line,
                                    "cp": close_price,
                                    "cv": clv,
                                    "ts": specific_snap['captured_at'],
                                    "id": r['id']
                                })
                                conn.commit()
                            updates += 1
                            
        return updates

    def _evaluate_db_predictions(self, *, max_rows: int = 500):
        """Grade outcomes for pending predictions where the game is FINAL.

        Bounded for serverless execution.
        """
        min_ev = float(os.getenv('GRADING_MIN_EV_PER_UNIT', '0.02'))
        query = """
        SELECT m.id, m.market_type, m.pick, m.bet_line, m.book,
               e.home_team, e.away_team,
               gr.home_score, gr.away_score, gr.final
        FROM model_predictions m
        JOIN events e ON m.event_id = e.id
        JOIN game_results gr ON e.id = gr.event_id
        WHERE (m.outcome = 'PENDING' OR m.outcome IS NULL)
          AND gr.final = TRUE
          AND COALESCE(m.ev_per_unit, 0) >= %(min_ev)s
        ORDER BY m.analyzed_at DESC
        LIMIT %(lim)s
        """

        with get_db_connection() as conn:
            rows = _exec(conn, query, {"lim": int(max_rows), "min_ev": float(min_ev)}).fetchall()
            
        print(f"[GRADING] Found {len(rows)} pending bets with final scores.")
        
        graded = 0
        for row in rows:
            try:
                outcome = self._grade_row(dict(row))
                if outcome != 'PENDING':
                    update_model_prediction_result(row['id'], outcome)
                    graded += 1
            except Exception as e:
                print(f"[GRADING] Error grading row {row['id']}: {e}")
                
        return graded

    def _grade_row(self, row):
        from src.utils.normalize import normalize_market
        market = normalize_market(row['market_type'])
        pick = row['pick']
        line = float(row['bet_line']) if row['bet_line'] is not None else 0.0

        # Guardrails: ignore placeholder/auto predictions so they don't clog Pending.
        if not pick or str(pick).upper() == 'NONE':
            return 'VOID'
        if market not in ('SPREAD', 'TOTAL', 'MONEYLINE'):
            return 'VOID'
        
        h_score = row['home_score']
        a_score = row['away_score']
        
        outcome = 'PENDING'
        
        if market == 'SPREAD':
            if pick == row['home_team']:
                score = h_score
                opp_score = a_score
            elif pick == row['away_team']:
                score = a_score
                opp_score = h_score
            else:
                return 'VOID'
            
            if score + line > opp_score: outcome = 'WON'
            elif score + line < opp_score: outcome = 'LOST'
            else: outcome = 'PUSH'
            
        elif market == 'TOTAL':
            total_score = h_score + a_score
            if pick.upper() == 'OVER':
                outcome = 'WON' if total_score > line else 'LOST' if total_score < line else 'PUSH'
            elif pick.upper() == 'UNDER':
                outcome = 'WON' if total_score < line else 'LOST' if total_score > line else 'PUSH'
                
        elif market == 'MONEYLINE':
            winner = row['home_team'] if h_score > a_score else row['away_team']
            if pick == winner: outcome = 'WON'
            else: outcome = 'LOST'
            
        return outcome

if __name__ == "__main__":
    service = GradingService()
    res = service.grade_predictions()
    print(res)
