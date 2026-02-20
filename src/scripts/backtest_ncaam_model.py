import sys
import os
import json
from datetime import datetime, timedelta, timezone

# Allow running from repo root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.database import get_db_connection, _exec
from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2


def _et_date(d: datetime) -> str:
    try:
        return d.astimezone(timezone.utc).strftime('%Y-%m-%d')
    except Exception:
        return d.strftime('%Y-%m-%d')


def _latest_line_before(conn, event_id: str, market_type: str, side: str, cutoff: datetime):
    # Use captured_at if exists; fallback to latest.
    row = _exec(conn, """
        SELECT line_value, price, captured_at
        FROM odds_snapshots
        WHERE event_id=%s AND market_type=%s AND side=%s
          AND captured_at <= %s
        ORDER BY captured_at DESC
        LIMIT 1
    """, (event_id, market_type, side, cutoff)).fetchone()
    return dict(row) if row else None


def _american_profit_per_unit(price: float) -> float:
    """Profit (not including stake) for risking 1 unit at given American odds."""
    if price is None:
        price = -110
    p = float(price)
    if p == 0:
        return 0.0
    if p > 0:
        return p / 100.0
    return 100.0 / abs(p)


def _grade_spread(pick: str, home_team: str, away_team: str, line: float, home_score: float, away_score: float) -> str:
    # pick is team name
    if pick == home_team:
        adj = (home_score + line) - away_score
    elif pick == away_team:
        adj = (away_score + line) - home_score
    else:
        return 'VOID'
    if abs(adj) < 1e-9:
        return 'PUSH'
    return 'WIN' if adj > 0 else 'LOSS'


def _grade_total(side: str, line: float, home_score: float, away_score: float) -> str:
    tot = home_score + away_score
    s = str(side or '').upper().strip()
    if abs(tot - line) < 1e-9:
        return 'PUSH'
    if s == 'OVER':
        return 'WIN' if tot > line else 'LOSS'
    if s == 'UNDER':
        return 'WIN' if tot < line else 'LOSS'
    return 'VOID'


def main():
    days = int(os.getenv('BACKTEST_DAYS', '14'))
    limit = int(os.getenv('BACKTEST_LIMIT', '200'))
    mode = str(os.getenv('BACKTEST_MODE', 'mae')).strip().lower()  # mae|simulate

    # Use fixed weights if provided
    print('[backtest] env overrides:', {
        'NCAAM_W_BASE': os.getenv('NCAAM_W_BASE'),
        'NCAAM_W_KENPOM': os.getenv('NCAAM_W_KENPOM'),
        'NCAAM_CAP_SPREAD': os.getenv('NCAAM_CAP_SPREAD'),
        'NCAAM_CAP_TOTAL': os.getenv('NCAAM_CAP_TOTAL'),
    })

    model = NCAAMMarketFirstModelV2()

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)

    with get_db_connection() as conn:
        # Pull completed games
        rows = _exec(conn, """
            SELECT e.id, e.home_team, e.away_team, e.start_time,
                   r.home_score, r.away_score
            FROM events e
            JOIN game_results r ON r.event_id = e.id
            WHERE e.league='NCAAM'
              AND r.final = TRUE
              AND e.start_time >= %s
            ORDER BY e.start_time DESC
            LIMIT %s
        """, (start, limit)).fetchall()

        games = [dict(r) for r in rows]

    if not games:
        print('[backtest] no games found')
        return

    # Metrics (MAE mode)
    n_spread = 0
    n_total = 0
    mae_spread_vs_close = 0.0
    mae_total_vs_close = 0.0
    mae_spread_vs_final = 0.0
    mae_total_vs_final = 0.0

    # Metrics (simulate mode)
    bets = []

    samples = []

    with get_db_connection() as conn:
        for g in games:
            event_id = g['id']
            st = g['start_time']
            if isinstance(st, str):
                try:
                    st = datetime.fromisoformat(st.replace('Z', '+00:00'))
                except Exception:
                    st = now
            if st.tzinfo is None:
                st = st.replace(tzinfo=timezone.utc)

            cutoff = st - timedelta(minutes=10)  # approximate "bet-time/close"

            # Pull bet-time consensus lines (per-side)
            s_home = _latest_line_before(conn, event_id, 'SPREAD', 'HOME', cutoff)
            s_away = _latest_line_before(conn, event_id, 'SPREAD', 'AWAY', cutoff)
            t_over = _latest_line_before(conn, event_id, 'TOTAL', 'OVER', cutoff)
            t_under = _latest_line_before(conn, event_id, 'TOTAL', 'UNDER', cutoff)

            # Need both spread_home + total for the model to run
            if not s_home or not t_over:
                continue
            if s_home.get('line_value') is None or t_over.get('line_value') is None:
                continue

            market_snapshot = {
                'spread_home': float(s_home['line_value']),
                'spread_price_home': float(s_home.get('price') or -110),
                'total': float(t_over['line_value']),
                'total_over_price': float(t_over.get('price') or -110),
                # best-price placeholders (use same bet-time snapshot for fairness)
                '_best_spread_home': {'line_value': float(s_home['line_value']), 'price': float(s_home.get('price') or -110), 'book': 'bt'},
                '_best_spread_away': {'line_value': float(s_away['line_value']), 'price': float(s_away.get('price') or -110), 'book': 'bt'} if s_away else None,
                '_best_total_over': {'line_value': float(t_over['line_value']), 'price': float(t_over.get('price') or -110), 'book': 'bt'},
                '_best_total_under': {'line_value': float(t_under['line_value']), 'price': float(t_under.get('price') or -110), 'book': 'bt'} if t_under else None,
                '_raw_snaps': []
            }

            # Run model (no persistence)
            try:
                res = model.analyze(event_id, market_snapshot=market_snapshot, event_context={
                    'id': event_id,
                    'home_team': g['home_team'],
                    'away_team': g['away_team'],
                    'league': 'NCAAM',
                    'sport': 'NCAAM',
                    'start_time': st,
                }, relax_gates=False, persist=False)
                dbg = (res.get('debug') or {})
            except Exception:
                continue

            mu_spread = dbg.get('mu_spread_power')
            mu_total = dbg.get('mu_total_power')

            # Final outcomes
            home_score = float(g.get('home_score'))
            away_score = float(g.get('away_score'))
            home_margin = home_score - away_score
            final_total = home_score + away_score

            if mode == 'mae':
                # Spread MAE
                if s_home and mu_spread is not None:
                    n_spread += 1
                    close = float(s_home['line_value'])
                    mae_spread_vs_close += abs(float(mu_spread) - close)
                    mae_spread_vs_final += abs(float(mu_spread) - home_margin)

                # Total MAE
                if t_over and mu_total is not None:
                    n_total += 1
                    close = float(t_over['line_value'])
                    mae_total_vs_close += abs(float(mu_total) - close)
                    mae_total_vs_final += abs(float(mu_total) - final_total)

                if len(samples) < 8:
                    samples.append({
                        'event_id': event_id,
                        'start_time': st.isoformat(),
                        'home': g['home_team'],
                        'away': g['away_team'],
                        'mu_spread': mu_spread,
                        'close_spread': s_home['line_value'] if s_home else None,
                        'final_margin': home_margin,
                        'mu_total': mu_total,
                        'close_total': t_over['line_value'] if t_over else None,
                        'final_total': final_total,
                    })

            elif mode == 'simulate':
                # Use the model's own published-bet logic: it returns a single persisted rec when it has one.
                if not res or not res.get('market_type') or not res.get('pick'):
                    continue

                market_type = str(res.get('market_type') or '').upper().strip()
                ev = float(res.get('ev_per_unit') or 0.0)

                # Only count bets that would be published/recommended
                min_ev = float(os.getenv('NCAAM_PUBLISH_MIN_EV', '0.02'))
                if ev < min_ev:
                    continue

                bet_line = float(res.get('bet_line')) if res.get('bet_line') is not None else None
                bet_price = float(res.get('bet_price') or -110)

                outcome = 'VOID'
                pnl = 0.0

                if market_type == 'SPREAD' and bet_line is not None:
                    outcome = _grade_spread(str(res.get('pick')), g['home_team'], g['away_team'], bet_line, home_score, away_score)
                elif market_type == 'TOTAL' and bet_line is not None:
                    # res.pick will be OVER/UNDER
                    outcome = _grade_total(str(res.get('pick')), bet_line, home_score, away_score)

                if outcome == 'WIN':
                    pnl = _american_profit_per_unit(bet_price)
                elif outcome == 'LOSS':
                    pnl = -1.0
                elif outcome == 'PUSH':
                    pnl = 0.0
                else:
                    pnl = 0.0

                bets.append({
                    'event_id': event_id,
                    'start_time': st.isoformat(),
                    'market_type': market_type,
                    'pick': res.get('pick'),
                    'bet_line': bet_line,
                    'bet_price': bet_price,
                    'ev_per_unit': ev,
                    'outcome': outcome,
                    'pnl_units': pnl,
                })

                if len(samples) < 8:
                    samples.append(bets[-1])

    # Summaries
    sim = None
    if mode == 'simulate':
        n_bets = len(bets)
        wins = sum(1 for b in bets if b['outcome'] == 'WIN')
        losses = sum(1 for b in bets if b['outcome'] == 'LOSS')
        pushes = sum(1 for b in bets if b['outcome'] == 'PUSH')
        roi = (sum(b['pnl_units'] for b in bets) / n_bets) if n_bets else None
        sim = {
            'n_bets': n_bets,
            'wins': wins,
            'losses': losses,
            'pushes': pushes,
            'win_rate': (wins / (wins + losses)) if (wins + losses) else None,
            'roi_units_per_bet': roi,
            'total_pnl_units': sum(b['pnl_units'] for b in bets) if n_bets else 0.0,
            'bets_by_market': {
                'SPREAD': sum(1 for b in bets if b['market_type'] == 'SPREAD'),
                'TOTAL': sum(1 for b in bets if b['market_type'] == 'TOTAL'),
            }
        }

    out = {
        'mode': mode,
        'window_days': days,
        'limit': limit,
        'n_games': len(games),
        'n_spread': n_spread,
        'n_total': n_total,
        'mae_spread_vs_close': (mae_spread_vs_close / n_spread) if n_spread else None,
        'mae_total_vs_close': (mae_total_vs_close / n_total) if n_total else None,
        'mae_spread_vs_final': (mae_spread_vs_final / n_spread) if n_spread else None,
        'mae_total_vs_final': (mae_total_vs_final / n_total) if n_total else None,
        'simulation': sim,
        'env': {
            'NCAAM_W_BASE': os.getenv('NCAAM_W_BASE'),
            'NCAAM_W_KENPOM': os.getenv('NCAAM_W_KENPOM'),
            'NCAAM_CAP_SPREAD': os.getenv('NCAAM_CAP_SPREAD'),
            'NCAAM_CAP_TOTAL': os.getenv('NCAAM_CAP_TOTAL'),
            'NCAAM_PUBLISH_MIN_EV': os.getenv('NCAAM_PUBLISH_MIN_EV'),
            'BACKTEST_NO_NETWORK': os.getenv('BACKTEST_NO_NETWORK'),
        },
        'samples': samples,
    }

    txt = json.dumps(out, indent=2, default=str)
    print(txt)

    # also write an artifact file for Actions
    try:
        with open('backtest_output.json', 'w', encoding='utf-8') as f:
            f.write(txt)
    except Exception:
        pass


if __name__ == '__main__':
    main()
