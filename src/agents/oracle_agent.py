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
    """
    def __init__(self):
        super().__init__()

    def execute(self, context: Dict[str, Any], *args, **kwargs) -> Dict[str, Any]:
        events: List[EventContext] = context.get('events', [])
        quantitative_edges = context.get('edges', {}) # By event_id
        research_data = context.get('research', {}) # By event_id
        memory_data = context.get('memories', {}) # By event_id
        
        results = {}
        if not events:
            return results

        matchups_data = []
        for ev in events:
            ev_id = ev.event_id
            quant = quantitative_edges.get(ev_id, "No quantitative edge data available.")
            
            # RAW RESEARCH HANDLE
            research = research_data.get(ev_id, {})
            citations = research.get("citations", [])
            cite_str = json.dumps(citations) if citations else "No news snippets available."
            
            memories = memory_data.get(ev_id, [])
            memory_str = "\n".join([f"- (Sim: {m['similarity']}) {m['lesson']}" for m in memories]) if memories else "No relevant historical lessons found."
            
            matchups_data.append({
                "event_id": ev_id,
                "matchup": f"{ev.away_team} at {ev.home_team}",
                "quantitative_data": quant,
                "raw_news_snippets": cite_str,
                "historical_lessons": memory_str
            })

        system_prompt = f"""
        You are 'The Oracle', a meta-agent overseeing a council of specialized AI sports betting algorithms.
        You are evaluating a batch of {len(events)} college basketball matchups.
        
        Here is the FACTUAL DATA gathered for each matchup:
        {json.dumps(matchups_data, indent=2)}
         
        TASK:
        1. For EACH matchup, analyze the `raw_news_snippets`. Extract ONLY verifiable roster/news facts (injuries, confirmed lineup changes).
        2. Combine these facts with the `quantitative_data` and the `historical_lessons` (RAG memories).
        3. CRITICAL: Pay special attention to `historical_lessons`. If the system has repeatedly missed on a specific team or matchup archetype, apply a qualitative correction.
        4. Provide strictly data-backed executive summaries and final verdicts.
        
        ANTI-HALLUCINATION INSTRUCTIONS:
        - Do NOT invent storylines or unverified dynamics.
        - If no actionable news is in the snippets, state "No actionable news found" and rely on the quantitative data + historical lessons.
        - You MUST explicitly evaluate Spread, Moneyline, and Game Totals.
        
        OUTPUT FORMAT MUST BE VALID JSON:
        Return a single dictionary where keys are the exact `event_id` strings.
        Each entry must include a "debate" with these specific agent roles:
        - "Executive Summary": High-level view.
        - "Research & Roster": Factual news and injury analysis.
        - "Historical Context": Deep dive into `historical_lessons` and how they apply here.
        - "Verdict": Final synthesizing decision.
        
        Example:
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
                    "sources": ["https://..."]
                }}
            }}
        }}
        """
        
        try:
            self.log_trace(f"Synthesizing debate for {len(events)} matchups", {"event_ids": [ev.event_id for ev in events]})
            response_text = generate_content(
                model="gemini-2.0-flash", # Use 2.0 flash
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
                        "oracle_verdict": "Omitted from batch."
                    }
                    self.log_trace(f"Oracle omitted event: {ev.event_id}")
                    
        except Exception as e:
            print(f"[OracleAgent] Synthesis failed for batch: {e}")
            for ev in events:
                results[ev.event_id] = {
                    "debate": [{"agent": "System", "message": f"Error generating debate: {e}"}],
                    "oracle_verdict": "Could not synthesize due to error."
                }

        return results

