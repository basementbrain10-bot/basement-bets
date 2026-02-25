from typing import Any, Dict, List, Tuple
import uuid
from src.agents.base import BaseAgent
from src.agents.contracts import BetRecommendation, EdgeResult, RejectedOffer
from src.agents.settings import (
    AGENTS_MIN_EDGE, AGENTS_MIN_EV_PER_UNIT, AGENTS_SIZING_MODE,
    AGENTS_KELLY_FRACTION, AGENTS_MAX_KELLY_PCT, AGENTS_MAX_EVENT_EXPOSURE_PCT,
    AGENTS_CORRELATION_HAIRCUT_ENABLED
)

class RiskManagerAgent(BaseAgent):
    """
    Applies bankroll sizing rules, min-EV gates, and rudimentary
    correlation haircuts to guard over-exposure on specific events.

    Returns a tuple of (recommendations, rejections) so every filtered
    edge carries a traceable gate reason.
    """
    def execute(self, context: Dict[str, Any], *args, **kwargs) -> Tuple[List[BetRecommendation], List[RejectedOffer]]:
        edges: List[EdgeResult] = context.get("edges", [])
        if not edges:
            return [], []
            
        recs: List[BetRecommendation] = []
        rejections: List[RejectedOffer] = []
        
        # Sieve 1: Hard thresholds — track why each edge is dropped
        filtered_edges = []
        for e in edges:
            if "NEGATIVE_EV" in e.flags:
                rejections.append(RejectedOffer(
                    event_id=e.offer.event_id,
                    market_type=e.offer.market_type,
                    side=e.offer.side,
                    line=e.offer.line,
                    ev_per_unit=float(e.ev_per_unit),
                    reason="negative_ev"
                ))
                continue
            if e.ev_per_unit < AGENTS_MIN_EV_PER_UNIT:
                rejections.append(RejectedOffer(
                    event_id=e.offer.event_id,
                    market_type=e.offer.market_type,
                    side=e.offer.side,
                    line=e.offer.line,
                    ev_per_unit=float(e.ev_per_unit),
                    reason=f"ev_threshold(min={AGENTS_MIN_EV_PER_UNIT})"
                ))
                continue
            if abs(e.edge_points) < AGENTS_MIN_EDGE:
                rejections.append(RejectedOffer(
                    event_id=e.offer.event_id,
                    market_type=e.offer.market_type,
                    side=e.offer.side,
                    line=e.offer.line,
                    ev_per_unit=float(e.ev_per_unit),
                    reason=f"edge_threshold(min={AGENTS_MIN_EDGE},actual={e.edge_points:.2f})"
                ))
                continue
            filtered_edges.append(e)

        # Sieve 2: Base Sizing
        for e in filtered_edges:
            stake_pct = 0.01  # Flat 1% base
            
            if AGENTS_SIZING_MODE == "fractional_kelly":
                implied_p = 100.0 / (abs(e.offer.odds_american) + 100.0) if e.offer.odds_american < 0 else 100.0 / (e.offer.odds_american + 100.0)
                # odds decimal
                b = (e.offer.odds_american / 100.0) if e.offer.odds_american > 0 else (100.0 / abs(e.offer.odds_american))
                # Full kelly fraction
                f_star = (e.fair.p_fair * b - (1 - e.fair.p_fair)) / b
                # Fractional
                f_frac = f_star * AGENTS_KELLY_FRACTION
                # Caps
                stake_pct = min(max(f_frac, 0.0), AGENTS_MAX_KELLY_PCT)

            # Assign correlation groups
            c_group = f"{e.offer.event_id}:{e.offer.period or 'game'}"

            recs.append(BetRecommendation(
                id=str(uuid.uuid4()),
                offer=e.offer,
                stake=round(float(stake_pct), 4),
                sizing_method=AGENTS_SIZING_MODE,
                rank=0, # Built in next agent
                confidence=e.fair.confidence,
                expected_value=e.ev_per_unit,
                ev_pct=e.ev_pct,
                ev_per_unit=e.ev_per_unit,
                implied_p=e.implied_p,
                p_fair=e.fair.p_fair,
                edge_points=e.edge_points,
                risk_flags=e.flags,
                rationale=e.rationale,
                correlation_group=c_group
            ))

        # Sieve 3: Correlation Haircuts
        if AGENTS_CORRELATION_HAIRCUT_ENABLED:
            c_groups = {}
            for r in recs:
                c_groups.setdefault(r.correlation_group, []).append(r)
                
            for grp, grp_recs in c_groups.items():
                total_grp_stake = sum(r.stake for r in grp_recs)
                if total_grp_stake > AGENTS_MAX_EVENT_EXPOSURE_PCT:
                    # Prorate and tag haircut reason
                    ratio = AGENTS_MAX_EVENT_EXPOSURE_PCT / total_grp_stake
                    for r in grp_recs:
                        r.stake = round(r.stake * ratio, 4)
                        r.risk_flags.append(f"HAIRCUT_CORRELATION:{grp}")

        return recs, rejections
