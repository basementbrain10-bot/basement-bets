"""Local sync worker for sportsbook bet history.

This is intended to run on the Mac (not in Vercel serverless) because it may
need an interactive browser session for DraftKings.

Usage:
  python scripts/sync_worker.py --once
  python scripts/sync_worker.py --loop

Environment:
  Uses DATABASE_URL / DATABASE_URL_UNPOOLED from src.config.settings.
"""

from __future__ import annotations

import argparse
import os
import sys
import socket
import time
from typing import Optional

# Allow running from repo root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.sync_jobs import (
    DEFAULT_USER_ID,
    claim_next_job,
    mark_job_done,
    mark_job_error,
    mark_job_needs_login,
)


def _worker_id() -> str:
    host = socket.gethostname()
    pid = os.getpid()
    return f"{host}:{pid}"


def _ingest_bets_v2(user_id: str, provider_label: str, bets: list[dict]) -> int:
    """Ingest parsed bet dicts into bets_v2 via insert_bet_v2.

    Accepts heterogeneous bet dicts from DK/FD parsers.
    """
    import hashlib
    from datetime import datetime

    from src.database import insert_bet_v2
    from src.services.event_linker import EventLinker

    linker = EventLinker()
    saved = 0

    for bet in bets:
        # Normalize fields from various parsers
        status = (bet.get("status") or "PENDING").upper()
        stake = float(bet.get("stake") or bet.get("wager") or 0.0)
        odds = bet.get("odds")
        profit = bet.get("profit")

        placed_at = bet.get("placed_at") or bet.get("date") or ""
        date_part = placed_at.split(" ")[0].split("T")[0] if placed_at else datetime.now().strftime("%Y-%m-%d")

        doc = {
            "user_id": user_id,
            "account_id": bet.get("account_id"),
            "provider": provider_label,
            "date": date_part,
            "sport": bet.get("sport") or "Unknown",
            "bet_type": bet.get("market_type") or bet.get("bet_type"),
            "wager": stake,
            "profit": round(float(profit), 2) if profit is not None else 0.0,
            "status": status,
            "description": bet.get("event_name") or bet.get("description"),
            "selection": bet.get("selection"),
            "odds": int(odds) if odds is not None else None,
            "is_live": bool(bet.get("is_live", False)),
            "is_bonus": bool(bet.get("is_bonus", False)),
            "raw_text": bet.get("raw_text"),
        }

        # Idempotency hash
        raw_string = f"{user_id}|{doc['provider']}|{doc['date']}|{doc['description']}|{doc['wager']}|{doc.get('selection')}|{doc.get('odds')}"
        doc["hash_id"] = hashlib.sha256(raw_string.encode()).hexdigest()
        doc["is_parlay"] = "parlay" in str(doc.get("bet_type") or "").lower()

        leg = {
            "leg_type": doc["bet_type"],
            "selection": doc["selection"],
            "market_key": doc["bet_type"],
            "odds_american": doc["odds"],
            "status": doc["status"],
            "subject_id": None,
            "side": None,
            "line_value": bet.get("line") or bet.get("points"),
        }

        try:
            link_result = linker.link_leg(leg, doc["sport"], doc["date"], doc["description"])
            leg["event_id"] = link_result.get("event_id")
            leg["selection_team_id"] = link_result.get("selection_team_id")
            leg["link_status"] = link_result.get("link_status")
        except Exception:
            leg["event_id"] = None
            leg["selection_team_id"] = None
            leg["link_status"] = "UNLINKED"

        insert_bet_v2(doc, legs=[leg])
        saved += 1

    return saved


def _run_fanduel_job(job: dict) -> dict:
    """FanDuel sync using stored token preference."""
    from src.database import get_user_preference
    from src.api_clients.fanduel_client import FanDuelAPIClient

    user_id = job.get("user_id")
    token = get_user_preference(str(user_id), "fanduel_token")
    if not token:
        raise RuntimeError("No stored FanDuel token. Use Add Bet → FanDuel to store token once.")

    client = FanDuelAPIClient(auth_token=token)
    bets = client.fetch_bets(to_record=50)
    saved = _ingest_bets_v2(user_id=str(user_id), provider_label="FanDuel", bets=bets)
    return {"bets_fetched": len(bets), "bets_saved": saved}


def _run_draftkings_job(job: dict) -> dict:
    """DraftKings sync via interactive browser automation on the Mac."""
    from src.scrapers.user_draftkings import DraftKingsScraper
    from src.parsers.draftkings_text import DraftKingsTextParser

    user_id = job.get("user_id")

    scraper = DraftKingsScraper()
    raw_text = scraper.scrape()

    parser = DraftKingsTextParser()
    parsed = parser.parse(raw_text)

    saved = _ingest_bets_v2(user_id=str(user_id), provider_label="DraftKings", bets=parsed)
    return {"bets_fetched": len(parsed), "bets_saved": saved}


def _run_dk_ingest_job(job: dict) -> dict:
    """
    Automated DraftKings settled-bets ingest using DraftKingsIngestService.
    Claimed by the local worker when job_type='DK_INGEST'.

    Notes:
      - Ensures DK ingest schema exists (book_ingest_runs + sync_jobs columns).
    """
    from src.services.dk_ingest_service import DraftKingsIngestService
    from src.database import init_dk_ingest_db
    import json as _json

    try:
        init_dk_ingest_db()
    except Exception as e:
        print(f"[sync-worker] init_dk_ingest_db warn: {e}")

    meta = job.get("meta") or {}
    # payload_json may be a dict already (psycopg2 JSONB) or a JSON string
    payload_raw = job.get("payload_json") or {}
    if isinstance(payload_raw, str):
        try:
            payload_raw = _json.loads(payload_raw)
        except Exception:
            payload_raw = {}

    user_id = str(payload_raw.get("user_id") or job.get("user_id") or DEFAULT_USER_ID)
    account_id = str(payload_raw.get("account_id") or meta.get("account_id") or "Main")

    svc = DraftKingsIngestService()
    result = svc.run_draftkings_ingest(user_id=user_id, account_id=account_id)
    return result


def process_one(provider: Optional[str] = None) -> bool:
    wid = _worker_id()
    job = claim_next_job(worker_id=wid, provider=provider)
    if not job:
        return False

    job_id = int(job["id"])
    prov = job.get("provider")

    try:
        job_type = job.get("job_type") or ""
        if job_type == "DK_INGEST":
            meta = _run_dk_ingest_job(job)
            mark_job_done(job_id, meta=meta)
            print(f"[sync-worker] DONE DK_INGEST job={job_id} meta={meta}")
        elif prov == "fanduel":
            meta = _run_fanduel_job(job)
            mark_job_done(job_id, meta=meta)
            print(f"[sync-worker] DONE job={job_id} provider={prov} meta={meta}")
        elif prov == "draftkings":
            meta = _run_draftkings_job(job)
            mark_job_done(job_id, meta=meta)
            print(f"[sync-worker] DONE job={job_id} provider={prov} meta={meta}")
        else:
            raise RuntimeError(f"Unknown provider/job_type: {prov}/{job_type}")
        return True

    except Exception as e:
        msg = str(e)
        needs_auth = (
            "NeedsHumanAuth" in type(e).__name__
            or "login timeout" in msg.lower()
            or "login wall" in msg.lower()
            or "please log in" in msg.lower()
            or "login required" in msg.lower()
            or "needs_auth" in msg.lower()
        )
        if needs_auth:
            mark_job_needs_login(job_id, message=msg)
            print(f"[sync-worker] NEEDS_LOGIN job={job_id} err={msg}")
        else:
            mark_job_error(job_id, msg)
            print(f"[sync-worker] ERROR job={job_id} err={msg}")
        return True


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--provider", default=None, help="draftkings|fanduel")
    ap.add_argument("--sleep", type=float, default=10.0)
    args = ap.parse_args()

    if not args.once and not args.loop:
        args.loop = True

    if args.once:
        process_one(provider=args.provider)
        return

    while True:
        did = process_one(provider=args.provider)
        if not did:
            time.sleep(args.sleep)


if __name__ == "__main__":
    main()
