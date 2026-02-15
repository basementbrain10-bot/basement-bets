"""Build a monotonic calibration mapping for model win_prob using last N days.

We fit an isotonic regression (PAVA) mapping from raw win_prob -> calibrated probability,
using graded outcomes (W/L only; pushes excluded).

Output artifact:
  data/model_params/winprob_calibration_ncaam.json

Run:
  source .venv_backtest/bin/activate
  python scripts/build_winprob_calibration_ncaam.py --days 14
"""

from __future__ import annotations

import os
import sys
import json
import argparse
from dataclasses import dataclass
from typing import Any, List, Tuple

# repo root on path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.database import get_db_connection, _exec


def norm_result(x: Any) -> str | None:
    if x is None:
        return None
    s = str(x).strip().upper()
    if s in ("WON", "WIN", "W"):
        return "W"
    if s in ("LOST", "LOSS", "L"):
        return "L"
    if s in ("PUSH", "P"):
        return "P"
    return None


@dataclass
class Obs:
    p: float
    y: int
    w: float = 1.0


def pava(observations: List[Obs]) -> List[Tuple[float, float]]:
    """Weighted isotonic regression via Pool Adjacent Violators Algorithm.

    Returns a list of blocks as (p_mean, y_mean) in increasing p order.
    """

    # sort by p
    xs = sorted(observations, key=lambda o: o.p)

    # each block: sum_w, sum_wy, sum_wp
    blocks: List[dict] = []
    for o in xs:
        blocks.append({"w": o.w, "wy": o.w * o.y, "wp": o.w * o.p})
        # merge backwards while violating monotonicity (y_hat decreasing)
        while len(blocks) >= 2:
            b2 = blocks[-1]
            b1 = blocks[-2]
            y1 = b1["wy"] / b1["w"]
            y2 = b2["wy"] / b2["w"]
            if y1 <= y2:
                break
            # merge
            merged = {"w": b1["w"] + b2["w"], "wy": b1["wy"] + b2["wy"], "wp": b1["wp"] + b2["wp"]}
            blocks[-2] = merged
            blocks.pop()

    out: List[Tuple[float, float]] = []
    for b in blocks:
        p_mean = b["wp"] / b["w"]
        y_mean = b["wy"] / b["w"]
        out.append((float(p_mean), float(y_mean)))

    return out


def to_piecewise_points(blocks: List[Tuple[float, float]], min_points: int = 8) -> List[Tuple[float, float]]:
    """Compress blocks into a reasonable number of points for interpolation."""
    if not blocks:
        return []

    # If too many blocks, sample by quantiles of p_mean.
    if len(blocks) <= min_points:
        return blocks

    # sample evenly across blocks
    k = min_points
    idxs = [round(i * (len(blocks) - 1) / (k - 1)) for i in range(k)]
    idxs = sorted(set(idxs))
    pts = [blocks[i] for i in idxs]

    # ensure endpoints
    if pts[0][0] != blocks[0][0]:
        pts[0] = blocks[0]
    if pts[-1][0] != blocks[-1][0]:
        pts[-1] = blocks[-1]

    # enforce monotonic y
    fixed: List[Tuple[float, float]] = []
    last_y = -1.0
    for p, y in pts:
        y = max(last_y, y)
        fixed.append((p, y))
        last_y = y
    return fixed


def main(days: int = 14, out_path: str | None = None):
    q = """
    SELECT win_prob, outcome
    FROM model_predictions
    WHERE analyzed_at >= NOW() - INTERVAL :days
      AND win_prob IS NOT NULL
      AND outcome IS NOT NULL
    """

    obs: List[Obs] = []
    with get_db_connection() as conn:
        rows = _exec(conn, q, {"days": f"{int(days)} days"}).fetchall()

    for r in rows:
        rr = dict(r) if not isinstance(r, dict) else r
        res = norm_result(rr.get("outcome"))
        if res not in ("W", "L"):
            continue
        try:
            p = float(rr.get("win_prob"))
        except Exception:
            continue
        if not (0.0 < p < 1.0):
            continue
        obs.append(Obs(p=p, y=(1 if res == "W" else 0), w=1.0))

    if len(obs) < 200:
        raise SystemExit(f"Not enough decided observations for calibration: {len(obs)}")

    blocks = pava(obs)
    pts = to_piecewise_points(blocks, min_points=12)

    # De-dupe identical p's and clip extreme calibrated probs (avoid 0/1 blowups)
    dedup = []
    last_p = None
    for p, y in pts:
        if last_p is not None and abs(p - last_p) < 1e-9:
            continue
        y = max(0.01, min(0.99, float(y)))
        dedup.append((float(p), float(y)))
        last_p = p

    artifact = {
        "sport": "ncaam",
        "days": int(days),
        "n_obs": len(obs),
        "method": "isotonic_pava_piecewise_linear",
        "points": [{"p": p, "p_cal": y} for p, y in dedup],
    }

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if out_path is None:
        out_path = os.path.join(repo_root, "data", "model_params", "winprob_calibration_ncaam.json")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(artifact, f, indent=2)
        f.write("\n")

    print(f"Wrote {out_path}")
    print(f"n_obs={artifact['n_obs']} points={len(artifact['points'])}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--out", type=str, default=None)
    args = ap.parse_args()

    main(days=args.days, out_path=args.out)
