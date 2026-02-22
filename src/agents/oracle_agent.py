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
        pass

    def execute(self, context: Dict[str, Any], *args, **kwargs) -> Dict[str, Any]:
        """
        Receives data from EdgeEVAgent, ResearchAgent, and MemoryAgent.
        Returns the Council Transcript and Oracle Prediction for each event.
        """
        events: List[EventContext] = context.get('events', [])
        quantitative_edges = context.get('edges', {}) # By event_id
        research_data = context.get('research', {}) # By event_id
        memory_data = context.get('memories', {}) # By event_id
        
        results = {}

        for ev in events:
            ev_id = ev.event_id
            
            # Gather context
            quant = quantitative_edges.get(ev_id, "No quantitative edge data available.")
            news = research_data.get(ev_id, {}).get("summary", "No news found.")
            memories = memory_data.get(ev_id, [])
            
            memory_str = "\n".join([f"- (Sim: {m['similarity']}) {m['lesson']}" for m in memories]) if memories else "No relevant historical lessons found."

            system_prompt = f"""
            You are 'The Oracle', a meta-agent overseeing a council of specialized AI sports betting algorithms.
            A new college basketball matchup is on the slate: {ev.away_team} at {ev.home_team}.
            
            Here is the factual data gathered:
            
            1. [Quantitative Data]:
            {quant}
            
            2. [Injury & Roster News]:
            {news}
            
            3. [Historical Matchups/Spot Memories]:
            {memory_str}
            
            Your job is to provide a strictly data-backed executive summary of the edge on this game.
            
            ANTI-HALLUCINATION INSTRUCTIONS:
            - Do NOT invent storylines, momentum shifts, injuries, locker room dynamics, or unverified fatigue.
            - Do NOT simulate a "debate". Speak directly with the facts provided above in a clinical, objective tone.
            - If no actionable news is provided under [Injury & Roster News], firmly state "No actionable news found" and rely entirely on the quantitative data.
            - You MUST explicitly evaluate Spread, Moneyline, and Game Totals based ONLY on the numbers and verified text provided to you.

            
            OUTPUT FORMAT MUST BE VALID JSON with exactly these keys:
            {{
                "debate": [
                    {{"agent": "Executive Summary", "message": "..."}},
                    {{"agent": "Quantitative Edge", "message": "..."}},
                    {{"agent": "Qualitative Factors", "message": "..."}}
                ],
                "oracle_verdict": "...",
                "signals": {{
                    "confidence": 0.0,
                    "market_lean": {{
                        "spread": {{"side": "HOME|AWAY|NONE", "points": 0.0, "reason": ""}},
                        "total": {{"side": "OVER|UNDER|NONE", "points": 0.0, "reason": ""}},
                        "moneyline": {{"side": "HOME|AWAY|NONE", "reason": ""}}
                    }},
                    "key_factors": ["..."],
                    "data_points": [
                        {{"type": "injury|rotation|travel|fatigue|matchup|pace|referee|motivation|other", "team": "", "detail": "", "source_url": ""}}
                    ],
                    "recommended_followups": ["..."],
                    "red_flags": ["..."],
                    "sources": ["https://..."]
                }}
            }}

            Notes:
            - This is EXPLANATIONS-ONLY mode. Do NOT override the quantitative model; focus on structuring evidence.
            - Put any URLs you used in signals.sources (and per-item source_url when applicable).
            """
            
            try:
                response_text = generate_content(
                    model="gemini-2.5-flash",
                    system_prompt=system_prompt,
                    json_mode=True,
                    max_tokens=8192
                )
                # Strip markdown blocks if present
                clean_text = response_text.strip()
                if clean_text.startswith("```json"):
                    clean_text = clean_text[7:]
                if clean_text.endswith("```"):
                    clean_text = clean_text[:-3]
                clean_text = clean_text.strip()
                
                council_output = json.loads(clean_text)
                results[ev_id] = council_output
            except Exception as e:
                print(f"[OracleAgent] Synthesis failed for {ev_id}: {e}")
                results[ev_id] = {
                    "debate": [{"agent": "System", "message": f"Error generating debate: {e}"}],
                    "oracle_verdict": "Could not synthesize due to error."
                }

        return results
