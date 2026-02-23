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
            news = research_data.get(ev_id, {}).get("summary", "No news found.")
            memories = memory_data.get(ev_id, [])
            memory_str = "\n".join([f"- (Sim: {m['similarity']}) {m['lesson']}" for m in memories]) if memories else "No relevant historical lessons found."
            
            matchups_data.append({
                "event_id": ev_id,
                "matchup": f"{ev.away_team} at {ev.home_team}",
                "quantitative_data": quant,
                "injury_and_roster_news": news,
                "historical_lessons": memory_str
            })

        system_prompt = f"""
        You are 'The Oracle', a meta-agent overseeing a council of specialized AI sports betting algorithms.
        You are evaluating a batch of {len(events)} college basketball matchups.
        
        Here is the factual data gathered for each matchup:
        {json.dumps(matchups_data, indent=2)}
        
        Your job is to provide strictly data-backed executive summaries of the edge for EVERY game in the batch.
        
        ANTI-HALLUCINATION INSTRUCTIONS:
        - Do NOT invent storylines, momentum shifts, injuries, locker room dynamics, or unverified fatigue.
        - Do NOT simulate a "debate". Speak directly with the facts provided above in a clinical, objective tone.
        - If no actionable news is provided under [injury_and_roster_news], firmly state "No actionable news found" and rely entirely on the quantitative data.
        - You MUST explicitly evaluate Spread, Moneyline, and Game Totals based ONLY on the numbers and verified text provided to you.
        
        OUTPUT FORMAT MUST BE VALID JSON:
        Return a single dictionary where the keys are the exact `event_id` strings, and the values are the evaluation object for that game.
        Example:
        {{
            "event_id_1": {{
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
            }},
            "event_id_2": {{ ... }}
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
            clean_text = response_text.strip()
            if clean_text.startswith("```json"):
                clean_text = clean_text[7:]
            if clean_text.endswith("```"):
                clean_text = clean_text[:-3]
            clean_text = clean_text.strip()
            
            council_output = json.loads(clean_text)
            
            # Map back to results ensuring all events have an entry
            for ev in events:
                if ev.event_id in council_output:
                    results[ev.event_id] = council_output[ev.event_id]
                else:
                    results[ev.event_id] = {
                        "debate": [{"agent": "System", "message": "Oracle omitted this event in batch response."}],
                        "oracle_verdict": "Omitted from batch."
                    }
                    
        except Exception as e:
            print(f"[OracleAgent] Synthesis failed for batch: {e}")
            for ev in events:
                results[ev.event_id] = {
                    "debate": [{"agent": "System", "message": f"Error generating debate: {e}"}],
                    "oracle_verdict": "Could not synthesize due to error."
                }

        return results

