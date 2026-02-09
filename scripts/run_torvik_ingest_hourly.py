"""Hourly Torvik schedule ingest for ET-today and ET-tomorrow.

This exists to avoid brittle shell quoting in cron.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import os
import sys

# Ensure repo root is on PYTHONPATH so `src.*` imports work when run as a script.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from src.services.bt_schedule_ingest import ingest_daily_schedule


def main() -> None:
    et = ZoneInfo("America/New_York")
    now = datetime.now(et)
    dates = [now.strftime("%Y%m%d"), (now + timedelta(days=1)).strftime("%Y%m%d")]

    results = []
    for d in dates:
        res = ingest_daily_schedule(d, allow_selenium=True)
        results.append((d, res))
        print(f"[torvik] {d} {res}")

    # Exit code semantics: non-zero if any blocked
    for _, res in results:
        if (res or {}).get("status") == "blocked":
            raise SystemExit(2)


if __name__ == "__main__":
    main()
