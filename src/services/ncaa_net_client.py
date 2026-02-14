"""NCAA NET rankings client (source: NCAA.com).

We use the NCAA.com NET rankings page as the canonical source for:
- NET rank
- Overall record
- Location splits (road/neutral/home)
- Quadrant records (Q1-Q4)

This is intended for UI context in the Research → Details modal.

Note: NCAA.com markup can change. Parser is defensive and stores raw row text.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import re
import requests


NET_URL = "https://www.ncaa.com/rankings/basketball-men/d1/ncaa-mens-basketball-net-rankings"


@dataclass
class NetRow:
    rank: int | None
    school: str
    record: str | None = None
    conf: str | None = None
    road: str | None = None
    neutral: str | None = None
    home: str | None = None
    prev: int | None = None
    quad1: str | None = None
    quad2: str | None = None
    quad3: str | None = None
    quad4: str | None = None
    raw: str | None = None


class NcaamNetClient:
    def fetch(self) -> dict[str, Any]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        r = requests.get(NET_URL, headers=headers, timeout=20)
        r.raise_for_status()
        return {
            "fetched_at": datetime.utcnow().isoformat() + "Z",
            "html": r.text,
        }

    def parse(self, html: str) -> tuple[str | None, list[NetRow]]:
        """Parse NET table from NCAA.com HTML.

        Returns:
          - through_games (e.g. "Through Games Feb. 13 2026") if found
          - list of rows
        """
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html or "", "html.parser")

        # Attempt to find "Through Games" banner
        through_games = None
        try:
            txt = soup.get_text(" ", strip=True)
            m = re.search(r"Through Games\s+([A-Za-z]{3,}\.?)\s+(\d{1,2})\s+(\d{4})", txt)
            if m:
                through_games = f"Through Games {m.group(1)} {m.group(2)} {m.group(3)}"
        except Exception:
            through_games = None

        # Find a table that includes headers 'Rank' and 'School'
        table = None
        for t in soup.find_all("table"):
            h = t.get_text(" ", strip=True)
            if "Rank" in h and "School" in h and "Quad 1" in h:
                table = t
                break

        if table is None:
            # Fallback: extract from readability-like plain text blocks
            text = soup.get_text("\n", strip=True)
            return through_games, self._parse_text_fallback(text)

        rows: list[NetRow] = []
        for tr in table.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            if not cells:
                continue
            # Skip header rows
            if any(x.lower() == "rank" for x in cells[:2]):
                continue

            # Expected order:
            # Rank, School, Record, Conf, Road, Neutral, Home, Non-Div I, Prev, Quad 1..4
            # But NCAA occasionally varies. We'll map by position with guards.
            try:
                rank = int(re.sub(r"[^0-9]", "", cells[0]) or "")
            except Exception:
                rank = None

            school = cells[1] if len(cells) > 1 else ""
            if not school:
                continue

            def g(i):
                return cells[i] if len(cells) > i and cells[i] not in ("—", "-") else None

            # Some tables include Non-Div I column before Prev.
            record = g(2)
            conf = g(3)
            road = g(4)
            neutral = g(5)
            home = g(6)
            prev = None
            # prev is usually at 9, but may be at 8 depending on whether Non-Div I exists.
            prev_candidate = g(8) or g(9)
            try:
                prev = int(prev_candidate) if prev_candidate is not None else None
            except Exception:
                prev = None

            # Quads: last 4 columns
            quad1 = cells[-4] if len(cells) >= 4 else None
            quad2 = cells[-3] if len(cells) >= 3 else None
            quad3 = cells[-2] if len(cells) >= 2 else None
            quad4 = cells[-1] if len(cells) >= 1 else None

            rows.append(
                NetRow(
                    rank=rank,
                    school=school,
                    record=record,
                    conf=conf,
                    road=road,
                    neutral=neutral,
                    home=home,
                    prev=prev,
                    quad1=quad1,
                    quad2=quad2,
                    quad3=quad3,
                    quad4=quad4,
                    raw=" | ".join(cells),
                )
            )

        return through_games, rows

    def _parse_text_fallback(self, text: str) -> list[NetRow]:
        """Very defensive fallback parser from plain text.

        This is less reliable but better than nothing if NCAA changes markup.
        """
        lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
        # Find header line index
        try:
            hdr = lines.index("Rank")
        except Exception:
            hdr = 0
        # After header, rows tend to be blocks of 1 rank + 12 columns.
        # We'll greedily scan for rank lines.
        rows: list[NetRow] = []
        i = 0
        while i < len(lines):
            if re.fullmatch(r"\d{1,3}", lines[i]):
                try:
                    rank = int(lines[i])
                    school = lines[i + 1]
                    record = lines[i + 2] if i + 2 < len(lines) else None
                    conf = lines[i + 3] if i + 3 < len(lines) else None
                    road = lines[i + 4] if i + 4 < len(lines) else None
                    neutral = lines[i + 5] if i + 5 < len(lines) else None
                    home = lines[i + 6] if i + 6 < len(lines) else None
                    prev = None
                    try:
                        prev = int(lines[i + 8])
                    except Exception:
                        prev = None
                    quad1 = lines[i + 9] if i + 9 < len(lines) else None
                    quad2 = lines[i + 10] if i + 10 < len(lines) else None
                    quad3 = lines[i + 11] if i + 11 < len(lines) else None
                    quad4 = lines[i + 12] if i + 12 < len(lines) else None
                    rows.append(NetRow(rank=rank, school=school, record=record, conf=conf, road=road, neutral=neutral, home=home, prev=prev, quad1=quad1, quad2=quad2, quad3=quad3, quad4=quad4, raw=None))
                    i += 13
                    continue
                except Exception:
                    pass
            i += 1
        return rows
