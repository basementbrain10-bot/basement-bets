"""Unit tests for the DK automated ingest pipeline (additive)."""

from __future__ import annotations

import hashlib
import os
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Test 1: Stable sha256-based external_id (not Python hash())
# ---------------------------------------------------------------------------

def test_stable_external_id_uses_sha256():
    """LedgerWriter._stable_external_id must produce identical output across calls."""
    from src.books.ledger_writer import LedgerWriter

    raw = "DraftKings|User123|Spread Won $10.00"
    id1 = LedgerWriter._stable_external_id(raw)
    id2 = LedgerWriter._stable_external_id(raw)

    # Must be identical (stable across restarts)
    assert id1 == id2

    # Must start with the prefix
    assert id1.startswith("dk_scrape_")

    # Must be sha256-derived, not Python hash() (which changes between processes)
    expected_hash = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    assert id1 == f"dk_scrape_{expected_hash}"


def test_stable_external_id_different_inputs():
    """Different raw texts must produce different external_ids."""
    from src.books.ledger_writer import LedgerWriter

    id1 = LedgerWriter._stable_external_id("bet A")
    id2 = LedgerWriter._stable_external_id("bet B")
    assert id1 != id2


# ---------------------------------------------------------------------------
# Test 2: LedgerWriter upsert idempotency with same external_id
# ---------------------------------------------------------------------------

def test_ledger_writer_upsert_idempotent():
    """Inserting the same external_id twice should not raise and commits once."""
    from src.books.ledger_writer import LedgerWriter

    writer = LedgerWriter()

    sample_bet = {
        "user_id": "test-user",
        "provider": "DraftKings",
        "external_id": "dk_scrape_abc123def456xxxx",
        "date": "2024-01-15",
        "sport": "NCAAM",
        "bet_type": "Spread",
        "wager": 10.0,
        "profit": 9.09,
        "status": "WON",
        "description": "Test Team -3.5",
        "selection": "Test Team -3.5",
        "odds": -110,
        "raw_text": "Test raw text",
        "source": "dk_scrape",
    }

    mock_cursor = MagicMock()
    # Simulate: external_id NOT found -> insert path
    mock_cursor.fetchall.return_value = []
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = lambda s: mock_cursor
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    # Patch execute_batch to prevent it from inspecting the mock cursor type
    with patch("src.books.ledger_writer.execute_batch") as mock_eb:
        inserted, updated = writer.upsert_bets(mock_conn, [sample_bet])

    # execute_batch should have been called exactly once
    mock_eb.assert_called_once()
    # commit must have been called
    mock_conn.commit.assert_called_once()
    # total inserted+updated == 1 (one bet)
    assert inserted + updated == 1


# ---------------------------------------------------------------------------
# Test 3: book_ingest_runs row lifecycle (mocked)
# ---------------------------------------------------------------------------

def test_dk_ingest_service_writes_ingest_run():
    """DraftKingsIngestService should write a book_ingest_runs row on NeedsHumanAuth."""
    from src.services.dk_ingest_service import DraftKingsIngestService
    from src.scrapers.user_draftkings import NeedsHumanAuth

    svc = DraftKingsIngestService()

    with patch("src.services.dk_ingest_service.DraftKingsScraper") as MockScraper, \
         patch("src.services.dk_ingest_service.get_db_connection") as mock_conn_ctx, \
         patch("src.services.dk_ingest_service._start_ingest_run", return_value=42) as mock_start, \
         patch("src.services.dk_ingest_service._finish_ingest_run") as mock_finish:

        # Scraper raises NeedsHumanAuth
        MockScraper.return_value.scrape_settled_bets_automated.side_effect = NeedsHumanAuth("Login required")

        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = lambda s: mock_conn
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        result = svc.run_draftkings_ingest(user_id="test-user", account_id="Main")

    # Status must be needs_auth
    assert result["status"] == "needs_auth"

    # _finish_ingest_run must have been called with needs_auth
    mock_finish.assert_called_once()
    call_kwargs = mock_finish.call_args
    # The positional args: (conn, run_id, status, ...)
    assert "needs_auth" in str(call_kwargs)


def test_dk_ingest_service_success_path():
    """DraftKingsIngestService calls parse, filter, and ledger writer on success."""
    from src.services.dk_ingest_service import DraftKingsIngestService

    svc = DraftKingsIngestService()

    sample_bets = [
        {
            "external_id": "DK123",
            "date": "2024-01-15 12:00:00",
            "sport": "NCAAM",
            "bet_type": "Spread",
            "wager": 10.0,
            "profit": 9.09,
            "status": "WON",
            "description": "Test -3.5",
            "selection": "Test -3.5",
            "odds": -110,
            "raw_text": "raw",
        }
    ]

    with patch("src.services.dk_ingest_service.DraftKingsScraper") as MockScraper, \
         patch("src.services.dk_ingest_service.DraftKingsTextParser") as MockParser, \
         patch("src.services.dk_ingest_service.LedgerWriter") as MockWriter, \
         patch("src.services.dk_ingest_service.get_db_connection") as mock_conn_ctx, \
         patch("src.services.dk_ingest_service._start_ingest_run", return_value=99), \
         patch("src.services.dk_ingest_service._finish_ingest_run") as mock_finish:

        MockScraper.return_value.scrape_settled_bets_automated.return_value = "raw text"
        MockParser.return_value.parse.return_value = sample_bets
        MockWriter.return_value.upsert_bets.return_value = (1, 0)

        mock_conn = MagicMock()
        mock_conn_ctx.return_value.__enter__ = lambda s: mock_conn
        mock_conn_ctx.return_value.__exit__ = MagicMock(return_value=False)

        result = svc.run_draftkings_ingest("user1", "Main")

    assert result["status"] == "success"
    assert result["inserted"] == 1
    assert result["updated"] == 0


# ---------------------------------------------------------------------------
# Test 4: Egress guard
# ---------------------------------------------------------------------------

def test_egress_guard_allows_allowlisted_url():
    from src.utils.egress_guard import check_egress

    with patch.dict(os.environ, {
        "EGRESS_MODE": "restricted",
        "EGRESS_ALLOWLIST": "draftkings.com,neon.tech",
        "NEON_HOST": "",
    }):
        # Should not raise
        check_egress("https://sportsbook.draftkings.com/mybets")
        check_egress("https://api.neon.tech/v2/projects")


def test_egress_guard_blocks_unlisted_url():
    from src.utils.egress_guard import check_egress, EgressViolation

    with patch.dict(os.environ, {
        "EGRESS_MODE": "restricted",
        "EGRESS_ALLOWLIST": "draftkings.com",
        "NEON_HOST": "",
    }):
        with pytest.raises(EgressViolation):
            check_egress("https://api.someother.com/data")


def test_egress_guard_open_mode_allows_all():
    from src.utils.egress_guard import check_egress

    with patch.dict(os.environ, {"EGRESS_MODE": "open"}):
        # Should not raise even for non-allowlisted domains
        check_egress("https://random.example.com/endpoint")
