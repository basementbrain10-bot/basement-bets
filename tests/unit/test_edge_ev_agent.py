import json
from src.agents.contracts import MarketOffer, FairPrice, EdgeResult
from src.agents.edge_ev_agent import EdgeEVAgent

def test_ev_pct_calculation():
    agent = EdgeEVAgent()
    
    # Simple +EV SPREAD scenario
    offer = MarketOffer(
        event_id="test", league="NCAAM", market_type="SPREAD", side="HOME",
        odds_american=-110, book="act", line=-5.5
    )
    fair = FairPrice(
        event_id="test", market_type="SPREAD", side="HOME",
        p_fair=0.55, confidence=0.8, model_sources=[], rationale=[], fair_line=-7.5
    )
    
    evals = agent.execute({"fairs": [fair], "offers": [offer]})
    assert len(evals) == 1
    edge: EdgeResult = evals[0]
    
    assert round(edge.implied_p, 4) == 0.5238 # 110/210
    # EV_pct logic: p * profit + (1-p) * loss -> 0.55 * (100/110) - (0.45 * 1) 
    # = 0.55 * 0.9090 - 0.45 = 0.5 - 0.45 = 0.05
    assert edge.ev_pct > 0.049 and edge.ev_pct < 0.051
    assert "2.0" in str(edge.edge_points) # -5.5 - -7.5 = 2.0 (favors home by 2)
    assert edge.ev_display == f"{edge.ev_pct * 100:.2f}% EV"

def test_edge_points_total():
    agent = EdgeEVAgent()
    
    offer = MarketOffer(event_id="t", league="NCAAM", market_type="TOTAL", side="OVER", odds_american=-110, book="b", line=140.5)
    fair = FairPrice(event_id="t", market_type="TOTAL", side="OVER", p_fair=0.6, confidence=0.5, model_sources=[], rationale=[], fair_line=144.5)
    
    res = agent.execute({"fairs": [fair], "offers": [offer]})
    
    assert res[0].edge_points == -4.0  # Offer 140.5 - Fair 144.5 = -4 points for an OVER

def test_sorting_stability():
    # Synthetic manual sort equivalent to edge_scanner
    edges = [
        {"ev_pct": "0.05", "ev": "5.0% EV"},
        {"ev_pct": 0.03, "ev": "3.0% EV"},
        {"ev_pct": 0.10, "ev": "10.0% EV"}
    ]
    edges.sort(key=lambda x: float(x.get('ev_pct') or 0.0), reverse=True)
    assert edges[0]['ev_pct'] == 0.10
    
if __name__ == "__main__":
    test_ev_pct_calculation()
    test_edge_points_total()
    test_sorting_stability()
    print("All tests passed.")
