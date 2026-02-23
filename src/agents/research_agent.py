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

        # 2. Batch extraction via LLM
        games_with_news = {eid: citations for eid, citations in batch_citations.items() if citations}
        
        if games_with_news:
            prompt = f"""
You are a strict information extraction tool. You are evaluating news citations for {len(games_with_news)} college basketball matchups.

Matchups and Citations (keyed by event_id):
{json.dumps(games_with_news, ensure_ascii=False)}

Task:
- For each matchup, extract ONLY verifiable roster/news facts (injuries, suspensions, confirmed lineup changes).
- Do NOT invent or assume anything.
- If nothing verifiable exists for a matchup, return an empty facts list and a summary that says: No verifiable roster news found.

Return VALID JSON:
A dictionary where keys are the exact event_id strings.
Example:
{{
    "event_id_1": {{
        "summary": "string",
        "facts": [
            {{"type":"injury|suspension|lineup", "team":"", "detail":"", "source_url":""}}
        ]
    }}
}}
            """
            try:
                clean_res = generate_content(model="gemini-2.5-flash", system_prompt=prompt, json_mode=True, max_tokens=1500)
                clean_text = clean_res.strip()
                if clean_text.startswith("```json"):
                    clean_text = clean_text[7:]
                if clean_text.endswith("```"):
                    clean_text = clean_text[:-3]
                
                parsed = json.loads(clean_text.strip())
                for eid, res in parsed.items():
                    if eid in research_results:
                        research_results[eid]["summary"] = str(res.get('summary') or '').strip() or 'No verifiable roster news found.'
                        research_results[eid]["facts"] = res.get('facts') or []
            except Exception as e:
                print(f"[ResearchAgent] Failed batch extraction: {e}")
                for eid in games_with_news:
                    research_results[eid]["summary"] += f" (Extraction failed: {str(e)})"

        return research_results
