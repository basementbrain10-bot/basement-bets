#!/usr/bin/env python3
"""Backtest: win rate on TOP 6 recommended bets per day for last N days.

We run the current NCAAMMarketFirstModelV2 on completed games for each day,
collect all gated recommendations, then take the top 6 by EV.

We run twice:
- sanity disabled (SANITY_ENABLE=0)
- sanity enabled  (SANITY_ENABLE=1)  [Mode A: blocks suspect data]

Outputs:
- daily top6 record
- aggregate win rate
- and a diff showing which bets changed between the two modes

Usage:
  python scripts/backtest_top6_last_week.py --days 7
"""

import os
import sys
import json
import argparse
from collections import defaultdict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database import get_db_connection, _exec
from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2


def et_date_key(dt):
    # DB returns tz-aware; if string, keep first 10 chars.
    try:
        return dt.astimezone().date().isoformat()
    except Exception:
        s = str(dt)
        return s[:10]


def grade_rec(rec: dict, home: str, away: str, hs: int, as_: int) -> str:
    mt = str(rec.get('market') or rec.get('bet_type') or '').upper()
    side = str(rec.get('side') or '').lower().strip()
    line = rec.get('line')
    if line is None:
        return 'L'
    line = float(line)

    margin = float(hs) - float(as_)

    if mt == 'SPREAD':
        if side == 'home':
            return 'W' if (margin + line) > 0 else 'L'
        if side == 'away':
            return 'W' if ((-margin) + line) > 0 else 'L'
        return 'L'

    if mt == 'TOTAL':
        total = float(hs) + float(as_)
        if side == 'over':
            return 'W' if total > line else 'L'
        if side == 'under':
            return 'W' if total < line else 'L'
        return 'L'

    return 'L'


def rec_id(event_id: str, rec: dict) -> str:
    mt = str(rec.get('market') or '').upper()
    side = str(rec.get('side') or '').lower().strip()
    line = rec.get('line')
    price = rec.get('price')
    return f"{event_id}:{mt}:{side}:{line}:{price}"


def run(days: int, sanity: bool) -> dict:
    os.environ['SANITY_ENABLE'] = '1' if sanity else '0'
    model = NCAAMMarketFirstModelV2()

    q = """
    SELECT e.id, e.home_team, e.away_team, e.start_time, gr.home_score, gr.away_score
    FROM events e
    JOIN game_results gr ON gr.event_id = e.id
    WHERE e.league='NCAAM'
      AND gr.final = TRUE
      AND e.start_time >= (NOW() - (%(d)s || ' days')::interval)
    ORDER BY e.start_time DESC
    """

    with get_db_connection() as conn:
        rows = _exec(conn, q, {'d': int(days)}).fetchall()

    by_day = defaultdict(list)
    for r in rows:
        r = dict(r)
        by_day[et_date_key(r['start_time'])].append(r)

    daily = {}
    all_top_ids = []

    for day in sorted(by_day.keys()):
        cands = []
        for g in by_day[day]:
            try:
                res = model.analyze(g['id'], relax_gates=False, persist=False)
                out = json.loads(res.get('outputs_json') or '{}')
                recs = out.get('recommendations') or []
                for rec in recs:
                    # require EV and win_prob present
                    ev = float(rec.get('ev') or 0.0)
                    cands.append({
                        **rec,
                        'event_id': g['id'],
                        'home_team': g['home_team'],
                        'away_team': g['away_team'],
                        'home_score': g['home_score'],
                        'away_score': g['away_score'],
                        'ev': ev,
                    })
            except Exception:
                continue

        cands.sort(key=lambda x: float(x.get('ev') or 0.0), reverse=True)
        top = cands[:6]

        wins = 0
        losses = 0
        top_list = []
        for rec in top:
            outcome = grade_rec(rec, rec['home_team'], rec['away_team'], rec['home_score'], rec['away_score'])
            if outcome == 'W':
                wins += 1
            else:
                losses += 1
            rid = rec_id(rec['event_id'], rec)
            all_top_ids.append(rid)
            top_list.append({
                'rid': rid,
                'event_id': rec['event_id'],
                'market': rec.get('market'),
                'side': rec.get('side'),
                'line': rec.get('line'),
                'price': rec.get('price'),
                'ev': float(rec.get('ev') or 0.0),
                'win_prob': rec.get('win_prob'),
                'outcome': outcome,
            })

        daily[day] = {
            'bets': len(top_list),
            'wins': wins,
            'losses': losses,
            'win_rate': (wins / len(top_list) * 100.0) if top_list else 0.0,
            'top6': top_list,
        }

    agg_bets = sum(v['bets'] for v in daily.values())
    agg_wins = sum(v['wins'] for v in daily.values())
    agg_wr = (agg_wins / agg_bets * 100.0) if agg_bets else 0.0

    return {
        'sanity': sanity,
        'days': days,
        'daily': daily,
        'aggregate': {'bets': agg_bets, 'wins': agg_wins, 'win_rate': agg_wr},
        'top_ids': all_top_ids,
    }


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=7)
    args = ap.parse_args()

    r_base = run(days=args.days, sanity=False)
    r_sane = run(days=args.days, sanity=True)

    # Diff which bets changed
    base_ids = set(r_base['top_ids'])
    sane_ids = set(r_sane['top_ids'])

    print("=== AGGREGATE ===")
    print("BASE:", r_base['aggregate'])
    print("SANE:", r_sane['aggregate'])
    print()

    print("=== BET DIFF (top6 across days) ===")
    print("Kept:", len(base_ids & sane_ids))
    print("Removed by sanity:", len(base_ids - sane_ids))
    print("Added by sanity:", len(sane_ids - base_ids))

    # Print daily summary
    print("\n=== DAILY ===")
    for day in sorted(r_base['daily'].keys() | r_sane['daily'].keys()):
        b = r_base['daily'].get(day, {})
        s = r_sane['daily'].get(day, {})
        print(f"{day}  base {b.get('wins',0)}/{b.get('bets',0)} ({b.get('win_rate',0):.1f}%) | sane {s.get('wins',0)}/{s.get('bets',0)} ({s.get('win_rate',0):.1f}%)")
