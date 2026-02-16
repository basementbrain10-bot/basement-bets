"""Backfill model_predictions semantics for the last N days (default 14).

Fixes:
- TOTAL rows: mu_market/mu_torvik/mu_final/sigma/edge_points/fair_line should be absolute totals.
- SPREAD rows: mu_* and fair_line should be in pick-side perspective (so comparable to close_line).

Data sources:
- inputs_json.market: {spread_home,total,...}
- inputs_json.torvik: {spread,total,...} (best-effort)
- outputs_json.mu_spread / outputs_json.mu_total
- outputs_json.debug.sigma_spread / sigma_total (fallback: existing sigma)

This is a one-time repair to make the last 14 days usable for calibration/uncertainty.

Run:
  source .venv_backtest/bin/activate
  python scripts/backfill_prediction_semantics_ncaam_last14d.py --days 14 --dry-run
  python scripts/backfill_prediction_semantics_ncaam_last14d.py --days 14
"""

from __future__ import annotations

import os
import sys
import json
import argparse
from typing import Any, Dict, Optional, Tuple

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.database import get_db_connection, _exec


def _safe_json(s: Any) -> Dict[str, Any]:
    if not s:
        return {}
    if isinstance(s, dict):
        return s
    try:
        return json.loads(s)
    except Exception:
        return {}


def _to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _pick_side(pick: str, home: str, away: str) -> Optional[str]:
    p = (pick or '').strip()
    if not p:
        return None
    if p.lower() == 'home':
        return 'home'
    if p.lower() == 'away':
        return 'away'
    if home and p == home:
        return 'home'
    if away and p == away:
        return 'away'
    return None


def compute_updates(row: Dict[str, Any]) -> Dict[str, Any]:
    market_type = str(row.get('market_type') or '').upper()
    pick = str(row.get('pick') or '')
    home = str(row.get('home_team') or '')
    away = str(row.get('away_team') or '')

    inputs = _safe_json(row.get('inputs_json'))
    outputs = _safe_json(row.get('outputs_json'))

    market = (inputs.get('market') or {}) if isinstance(inputs, dict) else {}
    torvik = (inputs.get('torvik') or {}) if isinstance(inputs, dict) else {}

    # home-perspective
    mkt_spread_home = _to_float(market.get('spread_home'))
    mkt_total = _to_float(market.get('total'))

    tv_spread_home = _to_float(torvik.get('spread'))
    tv_total = _to_float(torvik.get('total'))

    mu_spread_home = _to_float(outputs.get('mu_spread'))
    mu_total = _to_float(outputs.get('mu_total'))

    debug = (outputs.get('debug') or {}) if isinstance(outputs, dict) else {}
    sigma_spread = _to_float(debug.get('sigma_spread'))
    sigma_total = _to_float(debug.get('sigma_total'))

    # fallbacks
    if sigma_spread is None and market_type == 'SPREAD':
        sigma_spread = _to_float(row.get('sigma'))
    if sigma_total is None and market_type == 'TOTAL':
        sigma_total = _to_float(row.get('sigma'))

    upd: Dict[str, Any] = {"id": row.get('id')}

    if market_type == 'TOTAL':
        # absolute totals
        upd['mu_market'] = mkt_total
        upd['mu_torvik'] = tv_total
        upd['mu_final'] = mu_total
        upd['fair_line'] = mu_total
        upd['sigma'] = sigma_total
        if (mu_total is not None) and (mkt_total is not None):
            upd['edge_points'] = round(mu_total - mkt_total, 2)
        else:
            upd['edge_points'] = None
        return upd

    if market_type == 'SPREAD':
        side = _pick_side(pick, home, away)
        if side is None:
            # can't confidently re-orient; leave as-is
            return upd

        if side == 'home':
            mu_market = mkt_spread_home
            mu_torvik = tv_spread_home
            mu_final = mu_spread_home
        else:
            mu_market = (-mkt_spread_home) if mkt_spread_home is not None else None
            mu_torvik = (-tv_spread_home) if tv_spread_home is not None else None
            mu_final = (-mu_spread_home) if mu_spread_home is not None else None

        upd['mu_market'] = mu_market
        upd['mu_torvik'] = mu_torvik
        upd['mu_final'] = mu_final
        upd['fair_line'] = mu_final
        upd['sigma'] = sigma_spread
        if (mu_market is not None) and (mu_final is not None):
            upd['edge_points'] = round(mu_market - mu_final, 2)
        else:
            upd['edge_points'] = None
        return upd

    return upd


def main(days: int = 14, dry_run: bool = False, limit: Optional[int] = None):
    q = """
    SELECT mp.id, mp.event_id, mp.analyzed_at, mp.market_type, mp.pick,
           mp.sigma, mp.mu_market, mp.mu_torvik, mp.mu_final, mp.fair_line, mp.edge_points,
           mp.inputs_json, mp.outputs_json,
           e.home_team, e.away_team
    FROM model_predictions mp
    JOIN events e ON e.id = mp.event_id
    WHERE mp.analyzed_at >= NOW() - (%(days)s || ' days')::interval
      AND mp.market_type IN ('SPREAD','TOTAL')
    ORDER BY mp.analyzed_at ASC
    """

    with get_db_connection() as conn:
        rows = _exec(conn, q, {"days": int(days)}).fetchall()

    if limit:
        rows = rows[: int(limit)]

    total = len(rows)
    changed = 0
    skipped = 0

    print(f"Loaded {total} rows to evaluate (days={days})")

    updates = []
    for r in rows:
        rr = dict(r) if not isinstance(r, dict) else r
        upd = compute_updates(rr)
        if upd.keys() == {'id'}:
            skipped += 1
            continue
        updates.append(upd)

    print(f"Computed updates for {len(updates)} rows; skipped={skipped}")

    if dry_run:
        # show a few
        for u in updates[:10]:
            print(u)
        return

    upd_sql = """
    UPDATE model_predictions
    SET mu_market=%(mu_market)s,
        mu_torvik=%(mu_torvik)s,
        mu_final=%(mu_final)s,
        sigma=%(sigma)s,
        fair_line=%(fair_line)s,
        edge_points=%(edge_points)s
    WHERE id=%(id)s
    """

    with get_db_connection() as conn:
        for u in updates:
            _exec(conn, upd_sql, u)
            changed += 1
        conn.commit()

    print(f"Updated {changed} rows")


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=14)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--limit', type=int, default=None)
    args = ap.parse_args()

    main(days=args.days, dry_run=bool(args.dry_run), limit=args.limit)
