from typing import Any, Dict, List, Optional
from src.agents.base import BaseAgent
from src.agents.contracts import FairPrice, MarketOffer, EventContext
from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2

class PricingAgentNCAAM(BaseAgent):
    """
    Wraps the existing NCAAM Market First Model v2 without rewriting its internals.
    Maps internal model predictions back to the FairPrice[] contract.
    """
    def __init__(self):
        self.model = NCAAMMarketFirstModelV2()
        
    def execute(self, context: Dict[str, Any], *args, **kwargs) -> List[FairPrice]:
        offers: List[MarketOffer] = context.get("offers", [])
        events: List[EventContext] = context.get("events", [])
        
        if not offers or not events:
            return []
            
        # Group offers by event for processing
        event_dict = {str(ev.event_id): ev for ev in events}
        offers_by_event = {}
        for o in offers:
            offers_by_event.setdefault(o.event_id, []).append(o)
            
        fair_prices: List[FairPrice] = []
        
        for event_id_str, event_offers in offers_by_event.items():
            ev_context = event_dict.get(event_id_str)
            if not ev_context:
                continue
                
            # Synthesize generic snapshot for the model
            # Note: For safety, instead of relying exclusively on the orchestrator's snapshot,
            # we invoke analyze() natively which does its own odds fetch if needed,
            # ensuring backward compatibility. However, if analyze() can accept market_snapshot 
            # we could inject it. analyze() currently defaults to its own fetch.
            try:
                res = self.model.analyze(event_id_str, relax_gates=True, persist=False)
                if not res or 'error' in res:
                    continue
                    
                debug = res.get('debug', {})
                mu_spread_home = debug.get('mu_spread_final')
                mu_total = debug.get('mu_total_final')
                
                # We need win probs per side
                spread_win_prob_home = debug.get('win_prob_spread_home')
                spread_win_prob_away = debug.get('win_prob_spread_away')
                total_win_prob_over = debug.get('win_prob_total_over')
                total_win_prob_under = debug.get('win_prob_total_under')
                
                confidence_spread = debug.get('win_prob_spread_home_lb10') or spread_win_prob_home or 0.5
                confidence_total = debug.get('win_prob_total_over_lb10') or total_win_prob_over or 0.5
                
                # Pair the offers back up strictly to the event model output
                for offer in event_offers:
                    p_fair = None
                    conf = 0.5
                    f_line = None
                    
                    if offer.market_type == "SPREAD":
                        f_line = mu_spread_home if offer.side == "HOME" else (-mu_spread_home if mu_spread_home else None)
                        if offer.side == "HOME":
                            p_fair = spread_win_prob_home
                            conf = confidence_spread
                        else:
                            p_fair = spread_win_prob_away
                            conf = debug.get('win_prob_spread_away_lb10') or spread_win_prob_away or 0.5
                            
                    elif offer.market_type == "TOTAL":
                        f_line = mu_total
                        if offer.side == "OVER":
                            p_fair = total_win_prob_over
                            conf = confidence_total
                        else:
                            p_fair = total_win_prob_under
                            conf = debug.get('win_prob_total_under_lb10') or total_win_prob_under or 0.5

                    if p_fair is not None:
                        # Extract the narrative reasoning straight from the model's generated narrative
                        rationale = []
                        rec_list = res.get("recommendations", [])
                        for r in rec_list:
                            # Attempt to find the specific narrative matching this offer
                            if r.get('bet_type') == offer.market_type and str(r.get('selection_side')).upper() == offer.side:
                                if r.get("narrative"):
                                    rationale.append(r.get("narrative"))
                                    
                        # Guard against pydantic validation using float()
                        p_fair = float(p_fair)
                        
                        fair_prices.append(
                            FairPrice(
                                event_id=offer.event_id,
                                market_type=offer.market_type,
                                side=offer.side,
                                p_fair=max(0.001, min(0.999, float(p_fair))),
                                confidence=max(0.001, min(0.999, abs(float(conf)))),
                                model_sources=["NCAAMMarketFirstModelV2"],
                                rationale=rationale or ["Derived from primary market consensus metrics"],
                                line=offer.line,
                                fair_line=float(f_line) if f_line is not None else None,
                                fair_odds_american=None # Derive in Edge Agent
                            )
                        )
            except Exception as e:
                # Capture cleanly under the executor base
                raise Exception(f"Failed to price {event_id_str}: {e}")
                
        return fair_prices
