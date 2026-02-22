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
                
                raw_snippets = "\n".join(summaries)
                if not raw_snippets:
                    final_summary = "No notable breaking news found."
                else:
                    # NLP Extraction layer to sanitize the raw web noise
                    prompt = f"""
                    You are a strict data extraction tool. Review the following raw web search snippets for the game {ev.away_team} vs {ev.home_team}.
                    Extract ONLY factual injuries, suspensions, or confirmed lineup changes.
                    DO NOT invent or assume any information.
                    If the snippets just contain betting noise, picks, or generic previews, rigidly state "No verifiable roster news found."
                    
                    Raw text:
                    {raw_snippets}
                    """
                    clean_res = generate_content(model="gemini-2.5-flash", system_prompt=prompt, max_tokens=150)
                    final_summary = clean_res.strip()
                        
                research_results[ev.event_id] = {
                    "query": query,
                    "articles_found": len(summaries),
                    "summary": final_summary
                }
            except Exception as e:
                print(f"[ResearchAgent] Failed to search for {ev.event_id}: {e}")
                research_results[ev.event_id] = {
                    "query": query,
                    "articles_found": 0,
                    "summary": f"Search failed: {str(e)}"
                }

        return research_results
