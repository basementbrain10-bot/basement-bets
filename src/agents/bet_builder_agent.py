from typing import Any, Dict, List
from src.agents.base import BaseAgent
from src.agents.contracts import BetRecommendation
from src.agents.settings import AGENTS_MAX_PLAYS_PER_DAY

class BetBuilderAgent(BaseAgent):
    """
    Ranks processed recommendations, trims down output caps, and assigns
    finalized layout structs for the orchestration tier.
    """
    def execute(self, context: Dict[str, Any], *args, **kwargs) -> List[BetRecommendation]:
        recs: List[BetRecommendation] = context.get("recommendations", [])
        if not recs:
            return []
            
        # Hard sort mathematically by ev_pct float property 
        # Resolves previous EV sorting string bug 
        recs.sort(key=lambda r: float(r.ev_pct), reverse=True)
        
        # Apply Daily Limit 
        trimmed_recs = recs[:AGENTS_MAX_PLAYS_PER_DAY]
        
        # Assign formal ranks
        for idx, rec in enumerate(trimmed_recs):
            rec.rank = idx + 1
            
        return trimmed_recs
