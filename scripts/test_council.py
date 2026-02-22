import sys
import os
import json
from pprint import pprint

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.agents.orchestrator import DecisionOrchestrator
from src.agents.event_ops_agent import EventOpsAgent
from src.agents.market_data_agent import MarketDataAgent
from src.agents.pricing_agent_ncaam import PricingAgentNCAAM
from src.agents.edge_ev_agent import EdgeEVAgent
from src.agents.risk_manager_agent import RiskManagerAgent
from src.agents.bet_builder_agent import BetBuilderAgent
from src.agents.research_agent import ResearchAgent
from src.agents.memory_agent import MemoryAgent
from src.agents.oracle_agent import OracleAgent
from src.agents.journal_agent import JournalAgent
from src.agents.post_mortem_agent import PostMortemAgent

def run_test():
    print("Initializing Agent Council Pipeline...")
    
    # 1. Test PostMortem to seed some memory
    print("\n--- Running PostMortem Agent (Mock Game) ---")
    post_mortem = PostMortemAgent()
    pm_res = post_mortem.run({
        "completed_games": [
            {
                "away_team": "Purdue",
                "home_team": "Indiana",
                "oracle_prediction": "Take Indiana +3.5 at home. The crowd energy in Assembly Hall combined with Purdue's recent struggles against high-pressure defense should keep this close.",
                "actual_result": "Indiana wins outright 74-68."
            }
        ]
    })
    print(f"Memory Seed Result: {pm_res[0] if not pm_res[1] else pm_res[1]}")
    
    # 2. Test the full Decision Pipeline
    print("\n--- Running Decision Orchestrator ---")
    orchestrator = DecisionOrchestrator(league="NCAAM", model_version="agent_test_v1")
    
    # Override settings to force it to run immediately and not rely on cache for the test
    import src.agents.settings as settings
    settings.AGENTS_MAX_EVENTS_PER_RUN = 2 # Keep it small to save time/tokens
    settings.AGENTS_IDEMPOTENCY_WINDOW_SECONDS = 0 
    
    try:
        run_payload = orchestrator.run_pipeline(
            event_ops_agent=EventOpsAgent(),
            market_data_agent=MarketDataAgent(),
            pricing_agent=PricingAgentNCAAM(),
            edge_ev_agent=EdgeEVAgent(),
            risk_manager_agent=RiskManagerAgent(),
            bet_builder_agent=BetBuilderAgent(),
            research_agent=ResearchAgent(),
            memory_agent=MemoryAgent(),
            oracle_agent=OracleAgent(),
            journal_agent=JournalAgent(),
            parameters={"days_ahead": 1}
        )
        
        print(f"Pipeline Completed! Status: {run_payload.status}")
        print(f"Found {len(run_payload.recommendations)} Recommendations.")
        
        if run_payload.council_narrative:
            print("\n--- The Agent Council Narrative ---")
            print(json.dumps(run_payload.council_narrative, indent=2))
        else:
            print("\n--- No Council Narrative Generated (Likely no edges found) ---")
            
        print("\nErrors:", run_payload.errors)
        
    except Exception as e:
        print(f"Pipeline crashed horizontally: {e}")

if __name__ == "__main__":
    run_test()
