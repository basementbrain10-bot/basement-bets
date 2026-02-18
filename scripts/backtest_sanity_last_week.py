#!/usr/bin/env python3
"""Backtest last 7 days with/without sanity guardrails.

Runs NCAAMMarketFirstModelV2 on completed games and compares record for games where a SPREAD/TOTAL
recommendation is produced.

Usage:
  python scripts/backtest_sanity_last_week.py --days 7 --limit 400

Env toggles:
  SANITY_ENABLE=1 enables sanity scoring / blocks. (default 1)
"""

import os
import sys
import argparse
from datetime import datetime, timedelta, timezone

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database import get_db_connection, _exec
from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2


def grade_pick(market_type: str, pick: str, bet_line: float, home_team: str, away_team: str, home_score: int, away_score: int) -> str:
    margin = float(home_score) - float(away_score)
    mt = str(market_type or '').upper()
    pk = str(pick or '')
    if mt == 'SPREAD':
        # pick is team name, bet_line is signed for that team
        if pk == home_team:
            return 'W' if (margin + float(bet_line)) > 0 else 'L'
        if pk == away_team:
            return 'W' if ((-margin) + float(bet_line)) > 0 else 'L'
        return 'L'

    if mt == 'TOTAL':
        total = float(home_score) + float(away_score)
        side = pk.strip().upper()
        if side == 'OVER':
            return 'W' if total > float(bet_line) else 'L'
        if side == 'UNDER':
            return 'W' if total < float(bet_line) else 'L'
        return 'L'

    return 'L'


def run(days: int = 7, limit: int = 400, sanity_enable: bool = True):
    os.environ['SANITY_ENABLE'] = '1' if sanity_enable else '0'
    model = NCAAMMarketFirstModelV2()

    q = """
    SELECT e.id, e.home_team, e.away_team, e.start_time, gr.home_score, gr.away_score
    FROM events e
    JOIN game_results gr ON gr.event_id = e.id
    WHERE e.league='NCAAM'
      AND gr.final = TRUE
      AND e.start_time >= (NOW() - (%(d)s || ' days')::interval)
    ORDER BY e.start_time DESC
    LIMIT %(lim)s
    """

    with get_db_connection() as conn:
        games = _exec(conn, q, {'d': int(days), 'lim': int(limit)}).fetchall()

    picks = 0
    wins = 0
    losses = 0
    errors = 0

    for g in games:
        g = dict(g)
        eid = g['id']
        try:
            res = model.analyze(eid, relax_gates=True, persist=False)
            recs = res.get('recommendations') or []
            if not recs:
                continue

            # take top rec
            top = recs[0]
            mt = top.get('bet_type')
            sel = top.get('selection')

            # infer pick+line from model outputs
            pick = res.get('pick')
            bet_line = res.get('bet_line')
            if not mt or pick is None or bet_line is None:
                continue

            # normalize for total
            if str(mt).upper() == 'TOTAL':
                pick = str(pick).upper()

            outcome = grade_pick(mt, pick, float(bet_line), g['home_team'], g['away_team'], g['home_score'], g['away_score'])
            picks += 1
            if outcome == 'W':
                wins += 1
            else:
                losses += 1
        except Exception:
            errors += 1

    wr = (wins / picks * 100.0) if picks else 0.0
    return {'sanity': sanity_enable, 'games': len(games), 'picks': picks, 'wins': wins, 'losses': losses, 'win_rate': wr, 'errors': errors}


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=7)
    ap.add_argument('--limit', type=int, default=400)
    args = ap.parse_args()

    r0 = run(days=args.days, limit=args.limit, sanity_enable=False)
    r1 = run(days=args.days, limit=args.limit, sanity_enable=True)

    print('WITHOUT sanity:', r0)
    print('WITH sanity:', r1)
