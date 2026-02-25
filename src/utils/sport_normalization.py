from __future__ import annotations

import re

# Canonical sport codes used across the app.
CANONICAL_SPORTS = {
    "NFL",
    "NBA",
    "NCAAM",
    "NCAAF",
    "MLB",
    "NHL",
    "EPL",
    "UNKNOWN",
}

# Common aliases -> canonical
ALIASES = {
    "NCAAB": "NCAAM",
    "NCAA M": "NCAAM",
    "NCAA MEN": "NCAAM",
    "NCAA MEN'S": "NCAAM",
    "NCAA MEN’S": "NCAAM",
    "COLLEGE BASKETBALL": "NCAAM",
    "CBB": "NCAAM",

    "SOCCER": "EPL",  # app uses EPL as the soccer bucket
    "EPL SOCCER": "EPL",

    "UNK": "UNKNOWN",
    "": "UNKNOWN",
    None: "UNKNOWN",  # type: ignore
}


def normalize_sport(value) -> str:
    """Normalize sport labels to canonical codes.

    Goal: prevent duplicates like 'World Cup' vs 'WORLD CUP' and keep DB consistent.
    We intentionally map soccer to EPL as the app's canonical soccer bucket.
    """
    if value is None:
        return "UNKNOWN"

    s = str(value).strip()
    if not s:
        return "UNKNOWN"

    # Collapse whitespace and upper.
    s = re.sub(r"\s+", " ", s).strip().upper()

    # Direct alias mapping
    if s in ALIASES:
        return ALIASES[s]

    # If already canonical
    if s in CANONICAL_SPORTS:
        return s

    # Heuristic cleanup
    if "WORLD CUP" in s:
        return "EPL"  # treat as soccer bucket for now

    return s
