"""Egress guard — additive Python HTTP allowlist.

Usage:
    from src.utils.egress_guard import check_egress
    check_egress("https://api.draftkings.com/...")  # OK if allowlisted

This guard protects Python HTTP client calls (requests, urllib, etc.).
It does NOT control browser/Selenium asset fetching; use OS firewall for that.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse


class EgressViolation(Exception):
    """Raised when an outbound URL is not on the allowlist in restricted mode."""


def _get_allowlist() -> list[str]:
    """
    Build allowlist from EGRESS_ALLOWLIST env var.
    Supports ${NEON_HOST} expansion.
    """
    raw = os.environ.get("EGRESS_ALLOWLIST", "")
    # Expand ${NEON_HOST} token
    neon_host = os.environ.get("NEON_HOST", "")
    raw = raw.replace("${NEON_HOST}", neon_host)
    entries = [e.strip() for e in raw.split(",") if e.strip()]
    return entries


def check_egress(url: str) -> None:
    """
    Check whether *url* is allowed under the current egress policy.

    - If EGRESS_MODE != 'restricted', this is a no-op.
    - If restricted, the host of *url* must equal or end-with one of the
      domains in EGRESS_ALLOWLIST.

    Raises:
        EgressViolation if the URL is not allowlisted.
    """
    mode = os.environ.get("EGRESS_MODE", "open").lower()
    if mode != "restricted":
        return  # No restriction

    allowlist = _get_allowlist()
    if not allowlist:
        # If mode is restricted but list is empty, block everything
        raise EgressViolation(
            f"EGRESS_MODE=restricted but EGRESS_ALLOWLIST is empty. "
            f"Blocked: {url}"
        )

    try:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
    except Exception as exc:
        raise EgressViolation(f"Could not parse URL for egress check: {url}") from exc

    for allowed in allowlist:
        allowed_lower = allowed.lower().lstrip("*.")
        if host == allowed_lower or host.endswith("." + allowed_lower):
            return  # Allowed

    raise EgressViolation(
        f"Egress blocked: '{host}' is not in EGRESS_ALLOWLIST. "
        f"URL: {url}  Allowlist: {allowlist}"
    )
