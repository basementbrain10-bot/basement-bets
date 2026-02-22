"""
Force the Agent Council to debate a specific live game.
Fetches real-time odds from The Odds API and pipes them through
ResearchAgent -> MemoryAgent -> OracleAgent.
"""
import sys, os, json, requests
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import google.generativeai as genai
from src.agents.contracts import EventContext
from src.agents.research_agent import ResearchAgent
from src.agents.memory_agent import MemoryAgent
from src.agents.oracle_agent import OracleAgent
from src.services.kenpom_client import KenPomClient
from src.services.torvik_projection import TorvikProjectionService

# ── Config ──────────────────────────────────────────────
AWAY_TEAM = "BYU"
HOME_TEAM = "Iowa State"
EVENT_ID  = "NCAAB_BYU_at_Iowa_State_20260221"
ODDS_API_KEY = os.environ.get("ODDS_API_KEY", "")

# ── 1. Fetch Real Stats & Rankings ──────────────────────
print(f"\n{'='*60}")
print(f"  AGENT COUNCIL — {AWAY_TEAM} @ {HOME_TEAM}")
print(f"{'='*60}\n")

print("[1/5] Fetching official team stats from database...")
kenpom = KenPomClient()
torvik = TorvikProjectionService()

kp_h = kenpom.get_team_rating(HOME_TEAM) or {}
kp_a = kenpom.get_team_rating(AWAY_TEAM) or {}
bt_h = torvik._get_latest_metrics(HOME_TEAM) or {}
bt_a = torvik._get_latest_metrics(AWAY_TEAM) or {}

stats_summary = f"""REAL-TIME TEAM DATA (2026 Season):
- {HOME_TEAM}: KenPom Rank #{kp_h.get('rank', 'N/A')}, Torvik Rank #{bt_h.get('torvik_rank', 'N/A')}, AdjEM: {kp_h.get('adj_em', 'N/A')}
- {AWAY_TEAM}: KenPom Rank #{kp_a.get('rank', 'N/A')}, Torvik Rank #{bt_a.get('torvik_rank', 'N/A')}, AdjEM: {kp_a.get('adj_em', 'N/A')}
- {HOME_TEAM} Record: {bt_h.get('record', 'N/A')} | {AWAY_TEAM} Record: {bt_a.get('record', 'N/A')}
"""
print(stats_summary)

print("[2/5] Fetching live odds from The Odds API...")
odds_url = "https://api.the-odds-api.com/v4/sports/basketball_ncaab/odds/"
params = {
    "apiKey": ODDS_API_KEY,
    "regions": "us",
    "markets": "spreads,totals,h2h",
    "oddsFormat": "american",
}

live_odds_summary = "No live odds data available."
try:
    resp = requests.get(odds_url, params=params, timeout=10)
    games = resp.json()

    # Find the target game
    target = None
    for g in games:
        teams_lower = [t.lower() for t in g.get("home_team", "").lower().split()] + \
                      [t.lower() for t in g.get("away_team", "").lower().split()]
        if AWAY_TEAM.lower() in teams_lower or HOME_TEAM.lower() in teams_lower:
            target = g
            break

    if target:
        lines = [f"Home: {target['home_team']}  |  Away: {target['away_team']}"]
        for bm in target.get("bookmakers", [])[:3]:
            bk = bm["title"]
            for mkt in bm.get("markets", []):
                key = mkt["key"]
                for outcome in mkt.get("outcomes", []):
                    pt = f"  {bk} | {key}: {outcome['name']}"
                    if "point" in outcome:
                        pt += f" {outcome['point']}"
                    pt += f" ({outcome['price']})"
                    lines.append(pt)
        live_odds_summary = "\n".join(lines)
        print(live_odds_summary)
    else:
        print("  Could not find the Auburn/Kentucky game in the API response.")
        live_odds_summary = "Game not found in The Odds API. It may have already tipped off or the API window closed."
except Exception as e:
    print(f"  Odds API error: {e}")
    live_odds_summary = f"Odds API error: {e}"


# ── 2. Web Research ─────────────────────────────────────
print("\n[3/5] Running ResearchAgent (DuckDuckGo news search)...")
research_agent = ResearchAgent()
ev = EventContext(
    event_id=EVENT_ID,
    league="NCAAM",
    home_team=HOME_TEAM,
    away_team=AWAY_TEAM,
    start_time="2026-02-21T22:00:00-05:00"
)
research_results = research_agent.execute({"events": [ev]})
news_summary = research_results.get(EVENT_ID, {}).get("summary", "No news found.")
print(f"  Found {research_results.get(EVENT_ID, {}).get('articles_found', 0)} articles.")


# ── 3. RAG Memory ──────────────────────────────────────
print("\n[4/5] Running MemoryAgent (RAG retrieval)...")
memory_agent = MemoryAgent()
memory_results = memory_agent.execute({"events": [ev]})
memories = memory_results.get(EVENT_ID, [])
print(f"  Retrieved {len(memories)} relevant memories.")


# ── 4. The Oracle ──────────────────────────────────────
print("\n[5/5] Running OracleAgent (Gemini synthesis)...")
oracle_agent = OracleAgent()

# Build a rich context string combining odds + stats
quant_context = f"""{stats_summary}

LIVE ODDS DATA:
{live_odds_summary}
"""

oracle_results = oracle_agent.execute({
    "events": [ev],
    "edges": {EVENT_ID: quant_context},
    "research": research_results,
    "memories": memory_results
})

council = oracle_results.get(EVENT_ID, {})


# ── 5. Print The Debate ───────────────────────────────
print(f"\n{'='*60}")
print(f"  THE AGENT COUNCIL DEBATE")
print(f"{'='*60}\n")

if "debate" in council:
    for msg in council["debate"]:
        agent = msg.get("agent", "???")
        message = msg.get("message", "")
        print(f"  [{agent}]")
        print(f"  {message}\n")

print(f"{'='*60}")
print(f"  THE ORACLE'S VERDICT")
print(f"{'='*60}\n")
print(f"  {council.get('oracle_verdict', 'No verdict generated.')}\n")

# Save to a JSON file for the frontend
output_path = os.path.join(os.path.dirname(__file__), "..", "data", "last_council_debate.json")
os.makedirs(os.path.dirname(output_path), exist_ok=True)
with open(output_path, "w") as f:
    json.dump({
        "event_id": EVENT_ID,
        "matchup": f"{AWAY_TEAM} @ {HOME_TEAM}",
        "live_odds": live_odds_summary,
        "news": news_summary,
        "memories": memories,
        "council": council
    }, f, indent=2)
print(f"[5/5] Saved debate to {output_path}")
