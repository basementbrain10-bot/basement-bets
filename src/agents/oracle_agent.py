import os
import json
from typing import Any, Dict, List
from src.utils.gemini_rest import generate_content
from src.agents.base import BaseAgent
from src.agents.contracts import EventContext, MarketOffer, FairPrice

class OracleAgent(BaseAgent):
    """
    Synthesizes numerical edges, web research, and retrieved RAG memories 
    into a structured debate and final prediction decision.

    In addition to returning the council narrative, it persists each event's
    `signals` to the `council_signals` table so downstream consumers and
    offline evaluation can audit every qualitative claim.
    """
    def __init__(self):
        super().__init__()

    def execute(self, context: Dict[str, Any], *args, **kwargs) -> Dict[str, Any]:
        events: List[EventContext] = context.get('events', [])
        quantitative_edges = context.get('edges', {})      # By event_id
        research_data = context.get('research', {})         # By event_id
        memory_data = context.get('memories', {})           # By event_id
        # run_id passed in so we can key council_signals rows
        run_id = context.get('run_id', None)
        
        results = {}
        if not events:
            return results

        matchups_data = []
        for ev in events:
            ev_id = ev.event_id
            quant = quantitative_edges.get(ev_id, "No quantitative edge data available.")
            
            # Build evidence: include extracted facts from ResearchAgent if available
            research = research_data.get(ev_id, {})
            citations = research.get("citations", [])
            facts = research.get("facts", [])
            evidence_str = json.dumps({
                "raw_snippets": citations,
                "extracted_facts": facts
            }) if (citations or facts) else "No news snippets available."
            
            memories = memory_data.get(ev_id, [])
            memory_str = "\n".join([f"- (Sim: {m['similarity']}) {m['lesson']}" for m in memories]) if memories else "No relevant historical lessons found."
            
            matchups_data.append({
                "event_id": ev_id,
                "matchup": f"{ev.away_team} at {ev.home_team}",
                "quantitative_data": quant,
                "evidence": evidence_str,
                "historical_lessons": memory_str
            })

        system_prompt = f"""
        You are 'The Oracle', a meta-agent overseeing a council of specialized AI sports betting algorithms.
        You are evaluating a batch of {len(events)} college basketball matchups.
        
        Here is the FACTUAL DATA gathered for each matchup:
        {json.dumps(matchups_data, indent=2)}
         
        TASK:
        1. For EACH matchup, analyze the `evidence` section. Use `extracted_facts` for verifiable roster/injury claims; use `raw_snippets` for broader context only.
        2. Combine these facts with the `quantitative_data` and `historical_lessons`.
        3. CRITICAL: Pay special attention to `historical_lessons`. If the system has repeatedly missed on a specific team or matchup archetype, apply a qualitative correction and flag it in red_flags.
        4. Provide strictly data-backed executive summaries and final verdicts.
        5. Emit `signals.red_flags` for any major risks, contradictions, or recurring lesson patterns that require human review.
        
        ANTI-HALLUCINATION INSTRUCTIONS:
        - Do NOT invent storylines or unverified dynamics.
        - If no actionable news is in the snippets, state "No actionable news found" and rely on the quantitative data + historical lessons.
        - You MUST explicitly evaluate Spread, Moneyline, and Game Totals.
        
        OUTPUT FORMAT MUST BE VALID JSON:
        Return a single dictionary where keys are the exact `event_id` strings.
        Each entry must include a "debate" with these four specific agent roles:
        - "Executive Summary": High-level view combining quant edge + news context.
        - "Research & Roster": Factual news and injury analysis from extracted_facts only.
        - "Historical Context": Deep dive into historical_lessons and how they apply here.
        - "Verdict": Final synthesizing decision across Spread, Moneyline, and Totals.
        {{
            "event_id_1": {{
                "debate": [
                    {{"agent": "Executive Summary", "message": "..."}},
                    {{"agent": "Research & Roster", "message": "..."}},
                    {{"agent": "Historical Context", "message": "..."}},
                    {{"agent": "Verdict", "message": "..."}}
                ],
                "oracle_verdict": "...",
                "signals": {{
                    "confidence": 0.0,
                    "market_lean": {{
                        "spread": {{"side": "HOME|AWAY|NONE", "points": 0.0, "reason": ""}},
                        "total": {{"side": "OVER|UNDER|NONE", "points": 0.0, "reason": ""}},
                        "moneyline": {{"side": "HOME|AWAY|NONE", "reason": ""}}
                    }},
                    "data_points": [
                        {{"type": "injury|rotation|travel|fatigue|matchup|pace|other", "team": "", "detail": "", "source_url": ""}}
                    ],
                    "sources": ["https://..."],
                    "red_flags": ["<brief description of any major risk or contradiction, or \\"\\" if none>"]
                }}
            }}
        }}
        """

        try:
            self.log_trace(f"Synthesizing debate for {len(events)} matchups", {"event_ids": [ev.event_id for ev in events]})
            response_text = generate_content(
                model="gemini-2.0-flash",
                system_prompt=system_prompt,
                json_mode=True,
                max_tokens=8192
            )
            clean_text = response_text.strip()
            if clean_text.startswith("```json"):
                clean_text = clean_text[7:]
            if clean_text.endswith("```"):
                clean_text = clean_text[:-3]
            clean_text = clean_text.strip()
            
            self.log_trace("Oracle response received", {"raw_json": clean_text})
            council_output = json.loads(clean_text)
            
            # Map back to results ensuring all events have an entry
            for ev in events:
                if ev.event_id in council_output:
                    results[ev.event_id] = council_output[ev.event_id]
                    trace_data = {
                        "verdict": results[ev.event_id].get('oracle_verdict'),
                        "confidence": results[ev.event_id].get('signals', {}).get('confidence')
                    }
                    self.log_trace(f"Oracle verdict generated for {ev.away_team} at {ev.home_team}", trace_data)
                else:
                    results[ev.event_id] = {
                        "debate": [{"agent": "System", "message": "Oracle omitted this event in batch response."}],
                        "oracle_verdict": "Omitted from batch.",
                        "signals": {"confidence": 0.0, "data_points": [], "sources": [], "red_flags": []}
                    }
                    self.log_trace(f"Oracle omitted event: {ev.event_id}")

            # Persist structured signals to council_signals table
            self._persist_signals(events, results, run_id)

        except Exception as e:
            print(f"[OracleAgent] Synthesis failed for batch: {e}")
            for ev in events:
                results[ev.event_id] = {
                    "debate": [{"agent": "System", "message": f"Error generating debate: {e}"}],
                    "oracle_verdict": "Could not synthesize due to error.",
                    "signals": {"confidence": 0.0, "data_points": [], "sources": [], "red_flags": []}
                }

        return results

    def _persist_signals(self, events: List[EventContext], results: Dict[str, Any], run_id: str) -> None:
        """
        Write each event's signals to council_signals for offline auditing.
        Idempotent: uses ON CONFLICT DO UPDATE so re-runs are safe.
        """
        if not run_id:
            return  # Can't key the row without a run_id; skip silently

        try:
            from src.database import get_db_connection, _exec

            with get_db_connection() as conn:
                for ev in events:
                    ev_id = ev.event_id
                    nar = results.get(ev_id) or {}
                    signals = nar.get('signals') if isinstance(nar, dict) else None
                    if not signals:
                        continue
                    sources = signals.get('sources') if isinstance(signals, dict) else None
                    _exec(conn, """
                        INSERT INTO council_signals (run_id, event_id, league, signals_json, sources)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (run_id, event_id) DO UPDATE SET
                          signals_json = EXCLUDED.signals_json,
                          sources = EXCLUDED.sources,
                          created_at = NOW()
                    """, (
                        run_id, ev_id, ev.league,
                        json.dumps(signals),
                        json.dumps(sources) if sources is not None else None,
                    ))
                conn.commit()
            print(f"[OracleAgent] Persisted signals for {len(events)} events to council_signals.")
        except Exception as e:
            print(f"[OracleAgent] Signal persistence failed (non-fatal): {e}")
