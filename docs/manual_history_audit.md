# Manual History Import Audit

Date: 2026-02-16

File audited:
- `data/imports/manual_history.txt`

## Summary
This file uses `Bet #` as a *grouping key* for parent bets. Rows with a blank `Bet #` are **legs** of the most recent parent bet and should not be counted as standalone bets.

## Counts (confirmed)
- Total data rows (excluding header): **1162**
- Parent bet rows (non-empty `Bet #`): **982**
- Distinct `Bet #` values (parent bet groups): **873**
- Leg rows (blank `Bet #`): **180**
- Legs with non-zero stake: **0** (all legs have empty/zero stake)
- `Bet #` integer range: **1 → 874**

## Implications
- For any import pipeline, treat **one parent bet per distinct `Bet #`** as the betting unit.
- Rows with blank `Bet #` are legs/components and must be ignored for performance rollups.

## Notes
- The file’s `Profit / Loss` field does not parse cleanly for parent rows using a naive tab-separated column mapping (values appear missing/blank in the parsed column). Use the file primarily as **structural ground truth** (counts + parent/leg grouping), unless/ until a better column mapping is defined.
