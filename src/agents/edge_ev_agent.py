from typing import Any, Dict, List
from src.agents.base import BaseAgent
from src.agents.contracts import EdgeResult, FairPrice, MarketOffer
import src.utils.ev as ev_utils

class EdgeEVAgent(BaseAgent):
    """
    Standardizes all EV formulations requiring strict Float logic.
    Calculates numerical expected values removing unstable sorting states cleanly.
    """
    def execute(self, context: Dict[str, Any], *args, **kwargs) -> List[EdgeResult]:
        fairs: List[FairPrice] = context.get("fairs", [])
        offers: List[MarketOffer] = context.get("offers", [])
        
        if not fairs or not offers:
            return []
            
        edge_results: List[EdgeResult] = []
        
        # Match offers to fair prices
        offer_dict = {f"{o.event_id}:{o.market_type}:{o.side}": o for o in offers}
        
        for fair in fairs:
            key = f"{fair.event_id}:{fair.market_type}:{fair.side}"
            offer = offer_dict.get(key)
            
            if not offer:
                continue
                
            # Perform strict math logic
            implied_p = self._american_to_implied(offer.odds_american)
            ev_per_unit = self._calc_ev_per_unit(fair.p_fair, offer.odds_american)
            ev_pct = ev_per_unit # Percentage is just per-unit scale
            
            # Edge points calculation
            edge_points = 0.0
            if offer.line is not None and fair.fair_line is not None:
                # Depends on perspective. We need points of value in our favor.
                if fair.market_type == "TOTAL":
                    if fair.side == "OVER":
                        edge_points = offer.line - fair.fair_line
                    else:
                        edge_points = fair.fair_line - offer.line
                else: # SPREAD
                    # E.g. Fair is -6, Offer is -4. Diff = 2 points.
                    # Negative means favoring.
                    if fair.side == "HOME":
                        edge_points = offer.line - fair.fair_line
                    else:
                        edge_points = offer.line - fair.fair_line
            
            flags = []
            if ev_pct < 0:
                flags.append("NEGATIVE_EV")
                
            # Reconstruct legacy UI display items
            ev_display = f"{max(0, ev_pct * 100):.2f}% EV"
            edge_display = f"{edge_points:.1f} pts" if edge_points else None

            edge_results.append(EdgeResult(
                offer=offer,
                fair=fair,
                implied_p=float(implied_p),
                edge_points=round(float(edge_points), 2),
                ev_per_unit=round(float(ev_per_unit), 5),
                ev_pct=round(float(ev_pct), 5),
                flags=flags,
                rationale=fair.rationale,
                ev_display=ev_display,
                edge_display=edge_display
            ))
            
        return edge_results

    def _american_to_implied(self, odds: int) -> float:
        if odds == 0:
            return 0.0
        if odds > 0:
            return 100.0 / (odds + 100.0)
        return abs(odds) / (abs(odds) + 100.0)

    def _calc_ev_per_unit(self, win_prob: float, odds: int) -> float:
        """ Calculates expected profit (units) per 1 unit wagered """
        if odds == 0:
            return 0.0
        
        # Profit if win
        profit_if_win = (odds / 100.0) if odds > 0 else (100.0 / abs(odds))
        loss_if_loss = -1.0 # 1 unit
        
        return (win_prob * profit_if_win) + ((1.0 - win_prob) * loss_if_loss)
