import json
from typing import Any, Dict, List
from duckduckgo_search import DDGS
from src.agents.base import BaseAgent
from src.agents.contracts import EventContext

class ResearchAgent(BaseAgent):
    """
    Performs real-time web research for an event to find breaking news,
    injuries, or lineup changes.
    """
    def execute(self, context: Dict[str, Any], *args, **kwargs) -> Dict[str, Any]:
        events: List[EventContext] = context.get("events", [])
        if not events:
            return {}

        results = {}
        
        # In a real heavy-load scenario we'd be careful with rate limits for DDG,
        # but for small daily slates (max 25 plays), this is fine.
        with DDGS() as ddgs:
            for ev in events:
                query = f"{ev.away_team} vs {ev.home_team} basketball news injuries 2026"
                try:
                    # Fetch top 5 news results for better coverage
                    search_results = list(ddgs.text(query, max_results=5))
                    
                    summaries = []
                    for r in search_results:
                        summaries.append(f"- {r.get('title', '')}: {r.get('body', '')}")
                        
                    results[ev.event_id] = {
                        "query": query,
                        "articles_found": len(search_results),
                        "summary": "\n".join(summaries) if summaries else "No notable breaking news found."
                    }
                except Exception as e:
                    print(f"[ResearchAgent] Failed to search for {ev.event_id}: {e}")
                    results[ev.event_id] = {
                        "query": query,
                        "articles_found": 0,
                        "summary": f"Search failed: {str(e)}"
                    }

        return results
