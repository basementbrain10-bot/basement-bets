import json
import requests
from bs4 import BeautifulSoup
from typing import Any, Dict, List
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

        research_results = {}
        
        # DDG HTML search URL
        search_url = "https://html.duckduckgo.com/html/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) width/1920",
        }
        
        for ev in events:
            query = f"{ev.away_team} vs {ev.home_team} basketball news injuries 2026"
            try:
                # Fetch top news results via HTML
                resp = requests.post(search_url, data={"q": query}, headers=headers, timeout=10)
                resp.raise_for_status()
                
                soup = BeautifulSoup(resp.text, 'html.parser')
                results = soup.find_all('a', class_='result__snippet', limit=5)
                
                summaries = []
                for r in results:
                    text = r.get_text(strip=True)
                    if text:
                        summaries.append(f"- {text}")
                        
                research_results[ev.event_id] = {
                    "query": query,
                    "articles_found": len(summaries),
                    "summary": "\n".join(summaries) if summaries else "No notable breaking news found."
                }
            except Exception as e:
                print(f"[ResearchAgent] Failed to search for {ev.event_id}: {e}")
                research_results[ev.event_id] = {
                    "query": query,
                    "articles_found": 0,
                    "summary": f"Search failed: {str(e)}"
                }

        return research_results
