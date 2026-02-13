"""
Backfill script: re-parse raw_text for all DraftKings/FanDuel bets
to fix selection (remove odds/scores/bet-type labels) and correct bet_type.

DRY RUN by default. Pass --apply to actually write to DB.

Guards:
  - Skip CSV-imported bets (raw_text starts with "Imported from CSV")
  - Skip bets where raw_text is too short to be a real paste (<20 chars)
  - Only update if new value is non-empty and different from old
  - Don't downgrade bet_type from a specific type to generic "ML"/"Straight"
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))

from src.database import get_db_connection, _exec
from src.parsers.fanduel import FanDuelParser
from src.parsers.draftkings_text import DraftKingsTextParser

# Bet types considered "generic" — don't overwrite a specific type with these
GENERIC_BET_TYPES = {"ML", "Straight", ""}

def is_better_selection(old, new):
    """Return True if new selection is an improvement over old."""
    if not new or len(new) < 2:
        return False
    if not old:  # Old is empty/null, anything is better
        return True
    # If new is shorter AND doesn't contain noise tokens, it's likely cleaner
    # Count noise tokens in old vs new
    noise_tokens = ["Spread", "Winner (ML)", "Moneyline", "+", "-", "SPREAD BETTING",
                    "VIEW PICKS", "OVER", "UNDER", "Finished"]
    old_noise = sum(1 for t in noise_tokens if t.upper() in old.upper())
    new_noise = sum(1 for t in noise_tokens if t.upper() in new.upper())
    if new_noise < old_noise:
        return True
    # If both are clean, prefer old (don't change what works)
    if old_noise == 0 and new_noise == 0:
        return False
    return new_noise < old_noise

def main():
    apply = "--apply" in sys.argv

    fd_parser = FanDuelParser()
    dk_parser = DraftKingsTextParser()

    with get_db_connection() as conn:
        rows = _exec(conn, """
            SELECT id, provider, raw_text, selection, bet_type
            FROM bets
            WHERE provider IN ('FanDuel', 'DraftKings')
              AND raw_text IS NOT NULL AND raw_text != ''
            ORDER BY id
        """).fetchall()

        print(f"Found {len(rows)} bets with raw_text")

        updated = 0
        skipped = 0
        errors = 0

        for row in rows:
            bid = row["id"]
            provider = row["provider"]
            raw = row["raw_text"] or ""
            old_sel = (row["selection"] or "").strip()
            old_bt = (row["bet_type"] or "").strip()

            # Guard: skip CSV-imported bets
            if raw.startswith("Imported from CSV"):
                skipped += 1
                continue

            # Guard: skip if raw_text is too short to be real paste
            if len(raw) < 20:
                skipped += 1
                continue

            try:
                if provider == "FanDuel":
                    results = fd_parser.parse(raw)
                elif provider == "DraftKings":
                    results = dk_parser.parse(raw)
                else:
                    skipped += 1
                    continue

                if not results:
                    skipped += 1
                    continue

                parsed = results[0]
                new_sel = (parsed.get("selection") or "").strip()
                new_bt = (parsed.get("bet_type") or "").strip()

                # Decide what to update
                update_sel = is_better_selection(old_sel, new_sel)
                
                # Don't downgrade bet_type from specific to generic
                update_bt = False
                if new_bt and new_bt != old_bt:
                    if old_bt in GENERIC_BET_TYPES or new_bt not in GENERIC_BET_TYPES:
                        update_bt = True

                if not update_sel and not update_bt:
                    skipped += 1
                    continue

                final_sel = new_sel if update_sel else old_sel
                final_bt = new_bt if update_bt else old_bt

                if apply:
                    _exec(conn, """
                        UPDATE bets SET selection = %s, bet_type = %s WHERE id = %s
                    """, (final_sel, final_bt, bid))

                updated += 1

                # Log interesting changes (first 30)
                if updated <= 30:
                    changes = []
                    if update_sel:
                        changes.append(f"sel: '{old_sel[:50]}' → '{new_sel[:50]}'")
                    if update_bt:
                        changes.append(f"bt: '{old_bt}' → '{new_bt}'")
                    print(f"  [{provider}] id={bid}  {' | '.join(changes)}")

            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  ERROR id={bid}: {e}")

        if apply:
            conn.commit()
            print(f"\n✅ APPLIED: {updated} bets updated, {skipped} unchanged, {errors} errors")
        else:
            print(f"\n🔍 DRY RUN: {updated} would be updated, {skipped} unchanged, {errors} errors")
            print("   Run with --apply to write changes to DB")

if __name__ == "__main__":
    main()
