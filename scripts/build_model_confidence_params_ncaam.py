"""Build uncertainty parameters for NCAAM model confidence.

We estimate a simple model-error stddev (tau) for spreads and totals using last N days:
  tau_spread = std(close_line - mu_final) for SPREAD predictions where close_line exists
  tau_total  = std(close_line - mu_final) for TOTAL predictions where close_line exists

This tau is used to simulate uncertainty in mu when computing a confidence label.

Output:
  data/model_params/model_confidence_params_ncaam.json

Run:
  source .venv_backtest/bin/activate
  python scripts/build_model_confidence_params_ncaam.py --days 14
"""

from __future__ import annotations

import os
import sys
import json
import argparse
from typing import Any, List, Tuple

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.database import get_db_connection, _exec


def _to_float(x: Any):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _std(xs: List[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    m = sum(xs) / n
    v = sum((x - m) ** 2 for x in xs) / (n - 1)
    return v ** 0.5


def main(days: int = 14, out_path: str | None = None):
    q = """
    SELECT market_type, mu_final, close_line
    FROM model_predictions
    WHERE analyzed_at >= NOW() - INTERVAL :days
      AND close_line IS NOT NULL
      AND mu_final IS NOT NULL
      AND market_type IN ('SPREAD','TOTAL')
    """

    spread_err: List[float] = []
    total_err: List[float] = []

    with get_db_connection() as conn:
        rows = _exec(conn, q, {"days": f"{int(days)} days"}).fetchall()

    for r in rows:
        rr = dict(r) if not isinstance(r, dict) else r
        mkt = str(rr.get('market_type') or '').upper()
        mu = _to_float(rr.get('mu_final'))
        cl = _to_float(rr.get('close_line'))
        if mu is None or cl is None:
            continue
        err = cl - mu
        if mkt == 'SPREAD':
            spread_err.append(err)
        elif mkt == 'TOTAL':
            total_err.append(err)

    tau_spread = _std(spread_err)
    tau_total = _std(total_err)

    artifact = {
        "sport": "ncaam",
        "days": int(days),
        "n_spread": len(spread_err),
        "n_total": len(total_err),
        "tau_spread": float(tau_spread),
        "tau_total": float(tau_total),
        "method": "std(close_line - mu_final)",
        "notes": "tau represents uncertainty in mu; used for win_prob lower-bound confidence",
    }

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if out_path is None:
        out_path = os.path.join(repo_root, 'data', 'model_params', 'model_confidence_params_ncaam.json')

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(artifact, f, indent=2)
        f.write('\n')

    print(f"Wrote {out_path}")
    print(json.dumps(artifact, indent=2))


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--days', type=int, default=14)
    ap.add_argument('--out', type=str, default=None)
    args = ap.parse_args()
    main(days=args.days, out_path=args.out)
