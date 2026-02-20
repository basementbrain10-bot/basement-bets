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


def main():
    days = int(os.getenv('BACKTEST_DAYS', '14'))
    limit = int(os.getenv('BACKTEST_LIMIT', '200'))

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

    # Metrics
    n_spread = 0
    n_total = 0
    mae_spread_vs_close = 0.0
    mae_total_vs_close = 0.0
    mae_spread_vs_final = 0.0
    mae_total_vs_final = 0.0

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

            cutoff = st - timedelta(minutes=10)  # approximate "close"

            close_spread = _latest_line_before(conn, event_id, 'SPREAD', 'HOME', cutoff)
            close_total = _latest_line_before(conn, event_id, 'TOTAL', 'OVER', cutoff)

            # Skip if no close lines
            if not close_spread and not close_total:
                continue

            # Run model (no persistence)
            try:
                res = model.analyze(event_id, market_snapshot=None, event_context={
                    'id': event_id,
                    'home_team': g['home_team'],
                    'away_team': g['away_team'],
                    'league': 'NCAAM',
                    'sport': 'NCAAM',
                    'start_time': st,
                }, relax_gates=True, persist=False)
                dbg = (res.get('debug') or {})
            except Exception as e:
                continue

            mu_spread = dbg.get('mu_spread_final')
            mu_total = dbg.get('mu_total_final')

            # Final outcomes
            home_margin = float(g.get('home_score') - g.get('away_score'))
            final_total = float(g.get('home_score') + g.get('away_score'))

            # Spread MAE
            if close_spread and mu_spread is not None:
                n_spread += 1
                close = float(close_spread['line_value'])
                mae_spread_vs_close += abs(float(mu_spread) - close)
                mae_spread_vs_final += abs(float(mu_spread) - home_margin)

            # Total MAE
            if close_total and mu_total is not None:
                n_total += 1
                close = float(close_total['line_value'])
                mae_total_vs_close += abs(float(mu_total) - close)
                mae_total_vs_final += abs(float(mu_total) - final_total)

            if len(samples) < 8:
                samples.append({
                    'event_id': event_id,
                    'start_time': st.isoformat(),
                    'home': g['home_team'],
                    'away': g['away_team'],
                    'mu_spread': mu_spread,
                    'close_spread': close_spread['line_value'] if close_spread else None,
                    'final_margin': home_margin,
                    'mu_total': mu_total,
                    'close_total': close_total['line_value'] if close_total else None,
                    'final_total': final_total,
                })

    out = {
        'window_days': days,
        'limit': limit,
        'n_games': len(games),
        'n_spread': n_spread,
        'n_total': n_total,
        'mae_spread_vs_close': (mae_spread_vs_close / n_spread) if n_spread else None,
        'mae_total_vs_close': (mae_total_vs_close / n_total) if n_total else None,
        'mae_spread_vs_final': (mae_spread_vs_final / n_spread) if n_spread else None,
        'mae_total_vs_final': (mae_total_vs_final / n_total) if n_total else None,
        'env': {
            'NCAAM_W_BASE': os.getenv('NCAAM_W_BASE'),
            'NCAAM_W_KENPOM': os.getenv('NCAAM_W_KENPOM'),
            'NCAAM_CAP_SPREAD': os.getenv('NCAAM_CAP_SPREAD'),
            'NCAAM_CAP_TOTAL': os.getenv('NCAAM_CAP_TOTAL'),
        },
        'samples': samples,
    }

    print(json.dumps(out, indent=2, default=str))


if __name__ == '__main__':
    main()
