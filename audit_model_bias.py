
from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2
from src.database import get_db_connection, _exec
from datetime import datetime
import json

def audit_bias():
    print("--- Auditing Model Bias (NCAAM) ---")
    model = NCAAMMarketFirstModelV2()
    # Mocking news service to avoid API spam/latency
    class MockNews:
        def fetch_game_context(self, h, a): return {}
        def summarize_impact(self, c): return "No News"
    model.news_service = MockNews()
    
    # Fetch today's games
    dates = ['2026-01-31', '2026-02-01']
    games = []
    
    with get_db_connection() as conn:
        for d in dates:
             q = """
             SELECT id, home_team, away_team, start_time 
             FROM events 
             WHERE league='NCAAM' 
               AND start_time >= :d::timestamp 
               AND start_time < (:d::timestamp + INTERVAL '24 hours')
             """
             rows = _exec(conn, q, {'d': d}).fetchall()
             games.extend([dict(r) for r in rows])
    
    # LIMIT to 20 for speed
    games = games[:20]
             
    print(f"Analyzing {len(games)} games...")
    
    fav_bets = 0
    dog_bets = 0
    total_bets = 0
    
    results = []

    for g in games:
        try:
            res = model.analyze(g['id'])
            rec = res.get('recommendations', [])
            
            # Find Spread Bet
            spread_bet = next((r for r in rec if r['bet_type'] == 'SPREAD'), None)
            
            if spread_bet:
                total_bets += 1
                side = spread_bet['selection'].split(' ')[0] # 'Duke'
                line_str = spread_bet['selection'].split(' ')[-1] # '+13.0'
                
                # Determine if Fav or Dog
                # Need market spread to know who is fav.
                # Inspect logic: if line > 0, usually Dog. If line < 0, Fav.
                # Exception: Pick'em (0).
                
                try:
                    line_val = float(line_str)
                    is_dog = line_val > 0
                    is_fav = line_val < 0
                    
                    if is_dog:
                        dog_bets += 1
                        tag = "DOG"
                    elif is_fav:
                        fav_bets += 1
                        tag = "FAV"
                    else:
                        tag = "PK"
                        
                    results.append({
                        "game": f"{g['away_team']} @ {g['home_team']}",
                        "pick": f"{side} {line_val}",
                        "tag": tag,
                        "mu_market": res.get("mu_market"), # Home spread
                        "mu_final": res.get("mu_final"),   # Home spread
                        "delta": (res.get("mu_final") or 0) - (res.get("mu_market") or 0)
                    })
                    
                except:
                    pass
        except Exception as e:
            # print(f"Error analyzing {g['id']}: {e}")
            pass
            
            
    print(f"\nResults Summary:")
    print(f"Total Spread Bets: {total_bets}")
    print(f"Favorites: {fav_bets} ({(fav_bets/total_bets*100) if total_bets else 0:.1f}%)")
    print(f"Underdogs: {dog_bets} ({(dog_bets/total_bets*100) if total_bets else 0:.1f}%)")
    
    print("\n--- Deep Dive: Why are we fading Favorites? ---")
    # Show delta for top 5 Favorite Fades (where we bet Dog)
    # i.e., games where we picked DOG.
    # If we picked Dog, it usually means Model spread > Market spread (for home dog) or Model < Market (for away dog).
    # Easier: Just show huge deltas.
    
    sorted_res = sorted(results, key=lambda x: abs(x['delta']), reverse=True)
    for r in sorted_res[:10]:
        print(f"{r['game']} | Pick: {r['pick']} ({r['tag']})")
        print(f"   Market: {r['mu_market']:.1f} | Model: {r['mu_final']:.1f} | Delta: {r['delta']:.1f}")

if __name__ == "__main__":
    audit_bias()
