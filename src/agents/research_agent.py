import json
import requests
from bs4 import BeautifulSoup
from typing import Any, Dict, List
from src.agents.base import BaseAgent
from src.agents.contracts import EventContext
from src.utils.gemini_rest import generate_content

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
        
        batch_citations = {}
        for ev in events:
            query = f"{ev.away_team} vs {ev.home_team} basketball news injuries 2026"
            try:
                resp = requests.post(search_url, data={"q": query}, headers=headers, timeout=10)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, 'html.parser')

                citations = []
                for res_el in soup.select('.result')[:5]:
                    a = res_el.select_one('a.result__a')
                    sn = res_el.select_one('.result__snippet')
                    if not a:
                        continue
                    url = a.get('href')
                    title = a.get_text(strip=True)
                    snippet = sn.get_text(strip=True) if sn else ''
                    if url and title:
                        citations.append({
                            'title': title,
                            'url': url,
                            'snippet': snippet
                        })
                batch_citations[ev.event_id] = citations
                research_results[ev.event_id] = {
                    "query": query,
                    "articles_found": len(citations),
                    "summary": "No notable breaking news found.",
                    "citations": citations,
                    "facts": []
                }
            except Exception as e:
                print(f"[ResearchAgent] Failed to search for {ev.event_id}: {e}")
                research_results[ev.event_id] = {
                    "query": query,
                    "articles_found": 0,
                    "summary": f"Search failed: {str(e)}"
                }

        # 2. RAW RETURN: Skip LLM extraction to save on Rate Limits. 
        # The OracleAgent will handle the raw snippets in its consolidated prompt.
        for eid in events_ids := [ev.event_id for ev in events]:
            if eid in research_results:
                research_results[eid]["summary"] = f"Found {research_results[eid]['articles_found']} raw search results for analysis."
        
        return research_results
