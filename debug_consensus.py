
from src.database import get_db_connection, _exec
from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2
from src.services.odds_selection_service import OddsSelectionService
import json

def debug():
    event_id = 'action:ncaam:266903'
    
    # 1. Fetch raw odds manualy via model method
    model = NCAAMMarketFirstModelV2()
    raw_snaps = model._get_all_recent_odds(event_id)
    print(f"Fetched {len(raw_snaps)} raw snapshots.")
    if raw_snaps:
        print(f"Sample: {raw_snaps[0]}")
    
    # 2. Test Consensus
    selector = OddsSelectionService()
    
    # Debug what happens inside get_consensus_snapshot
    # Manually filter
    market_type = 'SPREAD'
    side = 'HOME'
    
    filtered = [s for s in raw_snaps if s.get('market_type') == market_type]
    print(f"Filtered by {market_type}: {len(filtered)}")
    
    if side:
        filtered = [s for s in filtered if s.get('side') == side]
    print(f"Filtered by {side}: {len(filtered)}")
    
    cons = selector.get_consensus_snapshot(raw_snaps, market_type, side)
    print(f"Consensus Result: {cons}")
    
    # 3. Full Analyze Call
    print("\n--- Model Analyze ---")
    res = model.analyze(event_id)
    inputs = json.loads(res.get('inputs_json', '{}'))
    print(f"Inputs JSON: {json.dumps(inputs, indent=2)}")
    
    mkt = inputs.get('market_lines', {}).get('spread', {}).get('line')
    print(f"Market Line from Inputs: {mkt}")

if __name__ == "__main__":
    debug()
