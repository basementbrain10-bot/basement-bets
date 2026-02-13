"""
Backfill sport detection for existing bets with sport='Unknown'.
Uses the shared sport_detection module with 300+ NCAAM teams.

DRY RUN by default. Pass --apply to write to DB.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from src.database import get_db_connection, _exec
from src.parsers.sport_detection import detect_sport

def main():
    apply = "--apply" in sys.argv

    with get_db_connection() as conn:
        rows = _exec(conn, """
            SELECT id, provider, COALESCE(selection,'') as selection,
                   COALESCE(description,'') as description,
                   COALESCE(raw_text,'') as raw_text
            FROM bets
            WHERE sport = 'Unknown' OR sport IS NULL OR sport = ''
            ORDER BY id
        """).fetchall()

        print(f"Found {len(rows)} bets with Unknown sport")

        updated = 0
        still_unknown = 0

        for row in rows:
            bid = row["id"]
            # Build text to scan from all available fields
            text = f"{row['selection']} {row['description']} {row['raw_text']}"
            sport = detect_sport(text)

            if sport == "Unknown":
                still_unknown += 1
                continue

            if apply:
                _exec(conn, "UPDATE bets SET sport = %s WHERE id = %s", (sport, bid))

            updated += 1
            if updated <= 20:
                sel_preview = (row['selection'] or '')[:50]
                print(f"  id={bid} [{row['provider']}] → {sport}  sel='{sel_preview}'")

        if apply:
            conn.commit()
            print(f"\n✅ APPLIED: {updated} bets updated, {still_unknown} still Unknown")
        else:
            print(f"\n🔍 DRY RUN: {updated} would be updated, {still_unknown} still Unknown")
            print("   Run with --apply to write changes to DB")

if __name__ == "__main__":
    main()
