# Agent Council + Prediction Ops — Roles & Responsibilities (NCAAM)

This document defines the **agent team as “employees”** working to create the best NCAAM predictions.

Goals:
- Produce **stable, auditable** daily picks
- Create **actionable outputs** (decisions, evidence, confidence, escalation)
- Accumulate structured data so agents can **improve the model over time**

Non-goals (for now):
- Agents directly overriding numerical model outputs (we are in **explanations-only mode**)

---

## Operating Principles

### 1) Actionable outputs
Every agent should return four things whenever possible:
1. **Decision** — what to do (e.g., “bet”, “pass”, “wait”, “escalate”)
2. **Evidence** — facts and links supporting the decision
3. **Confidence** — numeric + plain-language
4. **Escalation** — explicit conditions that require human review

### 2) Determinism + auditability
- The system must be able to answer: **“why did this pick exist?”** and **“why did it change?”**
- Prefer structured JSON (signals) over prose.

### 3) Separation of concerns
- **Model** generates quantitative fair prices / probabilities.
- **Council** generates narrative + structured signals.
- **Risk** controls bankroll exposure and operational constraints.

### 4) Measured improvement
No agent influences picks until we can demonstrate improvement via offline evaluation:
- CLV
- ROI
- Brier score / calibration

---

## System Overview (current pipeline)

**DecisionOrchestrator** (manager) runs:
1. EventOpsAgent → events
2. MarketDataAgent → offers
3. PricingAgentNCAAM → fairs
4. EdgeEVAgent → edges
5. RiskManagerAgent → recommendations
6. BetBuilderAgent → final picks
7. (Optional) Council: ResearchAgent + MemoryAgent + OracleAgent → `council_narrative`
8. JournalAgent persists the run

---

## Roles (the “employee roster”)

### DecisionOrchestrator (Manager)
**Mission:** coordinate all agents into a single `DecisionRun` with strict error handling + idempotency.

**Inputs:** parameters (league, days_ahead, etc.)

**Outputs:** `DecisionRun`

**Success metrics:**
- Produces a run within total timeout budget
- Stable idempotency behavior during retries

**Escalation:**
- Any fatal agent error → fail fast with `status=FAILED`

---

### EventOpsAgent (Scheduler / Slate Operations)
**Mission:** provide a prioritized list of events to analyze.

**Inputs:** league, days_ahead

**Outputs:** `EventContext[]`

**What “actionable” means here:**
- Flag events as `ready_for_pricing` vs `waiting_on_markets` vs `ignore`

**Success metrics:**
- High recall of relevant games (today’s slate)
- Low noise (no stale / cancelled games)

---

### MarketDataAgent (Market Data Engineer)
**Mission:** fetch/normalize the available market offers (spread/total, book, line, odds).

**Inputs:** events

**Outputs:** `MarketOffer[]`

**Actionable add-ons (desired):**
- `market_quality` per event: book count, freshness, missing markets, consensus vs best
- `movement_flags`: steam/line drift vs open

**Success metrics:**
- Completeness (spread + total coverage)
- Freshness (stale detection)

---

### PricingAgentNCAAM (Quant / Forecaster)
**Mission:** produce fair probabilities/lines for each offer using `NCAAMMarketFirstModelV2`.

**Inputs:** offers + events

**Outputs:** `FairPrice[]`

**Actionable add-ons (desired):**
- Attribution summary: what drove the fair line (Torvik vs KenPom vs market anchor)
- Explicit uncertainty reasons (missing signals, conflicting tempo, etc.)

**Success metrics:**
- Calibration of probabilities
- CLV relative to close

---

### EdgeEVAgent (Value Investor)
**Mission:** compute EV + edge vs the available offers.

**Inputs:** fairs + offers

**Outputs:** `EdgeResult[]`

**Actionable add-ons (desired):**
- Consistent edge sign convention
- Sensitivity to half-point changes and price changes

**Success metrics:**
- Rank ordering correlates with realized EV/CLV

---

### RiskManagerAgent (Risk Officer)
**Mission:** apply gates + sizing + correlation/exposure controls.

**Inputs:** edges

**Outputs:** `BetRecommendation[]`

**Actionable requirements:**
- For each rejected candidate, record **why** (threshold, correlation, exposure, etc.)
- Tie sizing to bankroll (percent → dollars) when bankroll is available

**Success metrics:**
- Avoids overexposure
- Improves risk-adjusted ROI

---

### BetBuilderAgent (Portfolio Manager)
**Mission:** rank, trim, and finalize the day’s pick list.

**Inputs:** recommendations

**Outputs:** ranked picks (same type)

**Actionable add-ons (desired):**
- Diversification constraints (optional)
- Provide alternates + reasons

---

## Council (Explanations-Only Team)

### ResearchAgent (Beat Reporter)
**Mission:** gather real-time news/injuries/lineup context.

**Inputs:** events

**Outputs:** per-event research summary (should become structured citations)

**Actionable requirements:**
- Include URLs + freshness where possible
- Separate facts from inference

---

### MemoryAgent (Institutional Knowledge / RAG)
**Mission:** retrieve relevant prior lessons and failure modes.

**Inputs:** events

**Outputs:** per-event top lessons + similarity scores

**Actionable requirements:**
- Prefer retrieving by *archetype/signals* (pace mismatch, injury misread, steam fade) not just team name matches

---

### OracleAgent (Chair / Synthesizer)
**Mission:** turn quant + research + memory into a debate transcript and structured `signals`.

**Inputs:** edges (text), research, memories, events

**Outputs:**
- `debate[]`
- `oracle_verdict` (narrative)
- `signals` (structured)

**Actionable requirements:**
- Provide citations in `signals.sources`
- Provide `signals.confidence`
- Emit `signals.red_flags` and `recommended_followups`

**Important:** explanations-only means these signals **do not modify picks yet**.

---

## Learning & Evaluation Team

### JournalAgent (Ledger / Data Clerk)
**Mission:** persist runs + ensure idempotency to avoid duplicate work.

**Outputs:** DB rows in `decision_runs`, `decision_recommendations`, optional HITL.

**Actionable add-ons (desired):**
- Store council signals in a dedicated table for analysis/training.

---

### PerformanceAuditorAgent (QA / Evaluator)
**Mission:** grade performance and produce daily reports.

**Gaps to fix:**
- Grade using model probability (`p_fair`) not implied odds probability.
- Fix spread grading sign conventions (home/away line logic).

**Outputs:** `performance_reports`.

---

### PostMortemAgent (After Action Review)
**Mission:** generate compact “lessons learned” after results finalize and store them as memory.

**Gaps to fix:**
- Fix embedding persistence bug (variable name mismatch).
- Make lessons structured (mistake_type, missed_signal, prevention).

---

## Proposed near-term refactors (high leverage)

1) **Council signals persistence**
- Add `council_signals` table keyed by (event_id, run_id, created_at) storing `signals_json`.
- Enables offline analysis and future feature engineering.

2) **Correct evaluation (CLV + calibration)**
- Update PerformanceAuditorAgent to grade against `p_fair` and compute CLV from closing lines.

3) **Data quality gates**
- Add a Market Quality report (books count, stale odds, missing markets) that can force `WAIT`.

4) **Operational transparency**
- Store “rejection reasons” for every candidate edge.

---

## Phase plan for agents “adding value” later

Phase 1 (now):
- Council is explanations-only; gather structured evidence.

Phase 2:
- Offline evaluation shows which signals correlate with CLV/ROI.

Phase 3:
- Introduce gated model adjustments (only in narrow conditions with proven uplift).

