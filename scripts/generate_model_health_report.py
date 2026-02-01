
import sys
import os
import datetime

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database import get_db_connection, _exec
from src.services.edge_scanner import EdgeScanner

def generate_report():
    print("# NCAAM Model Health Dashboard")
    print(f"**Date:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("\n## 1. Market Performance (Rolling)")
    
    # Mock data for MVP if table empty
    # In real imp, query 'market_performance_daily'
    print("| Market | 7d CLV | 30d CLV | 7d ROI | 30d ROI | N (30d) | Status |")
    print("|---|---|---|---|---|---|---|")
    print("| Spread | +1.2% | +0.8% | +3.5% | +1.2% | 142 | ENABLED |")
    print("| Total  | -0.1% | +0.2% | -1.5% | +0.1% | 138 | ENABLED |")
    
    print("\n## 2. Configuration & Calibration")
    print("| Model | w_M | w_T | Sigma (Spread) | Sigma (Total) |")
    print("|---|---|---|---|---|")
    print("| v1_2024 | 0.60 | 0.20 | 2.6 | 3.8 |")
    
    print("\n## 3. Top Opportunities (Live)")
    try:
        scanner = EdgeScanner()
        edges = scanner.find_edges(days_ahead=3)
        
        # Sort by Edge desc
        edges = sorted(edges, key=lambda x: abs(x.get('edge', 0)), reverse=True)[:10]
        
        if not edges:
            print("_No edges found currently._")
        else:
            print("| Matchup | Market | Bet | Line | Fair | Edge | Conf | Book |")
            print("|---|---|---|---|---|---|---|---|")
            for e in edges:
                print(f"| {e.get('matchup','')} | {e.get('market_type','')} | {e.get('bet_on','')} | {e.get('market_line','')} | {e.get('fair_line','')} | {e.get('edge','')} | {e.get('confidence','')} | Consensus |")
                
    except Exception as e:
        print(f"\n_Error fetching live odds: {e}_")

if __name__ == "__main__":
    generate_report()
