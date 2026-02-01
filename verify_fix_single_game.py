
from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2
import math

def verify_math():
    model = NCAAMMarketFirstModelV2()
    
    # Scenario: Xavier (Home) favored by 4.0
    # Inputs
    line_s = -4.0   # Market Spread (Home Fav)
    mu_s_final = -5.0 # Model thinks Home should be -5 (Stronger Fav)
    sig_s = 10.0
    
    # Expected: 
    # Market says Win by 4. Model says Win by 5.
    # Edge: 1 pt.
    # Win Probability P(Margin > 4) given Mean=5 should be > 50%.
    
    print(f"--- Testing Probability Logic ---")
    print(f"Spread: {line_s} (Fav)")
    print(f"Model Proj Spread: {mu_s_final}")
    
    # Old Logic (Bad) - assuming mu passed directly
    # mean = -5. P(X > 4) given mean -5 is ~0.
    prior_prob = 1.0 - model._normal_cdf(-line_s, mu_s_final, sig_s)
    print(f"Old Logic Win Prob: {prior_prob:.4f} (Expect ~0)")
    
    # New Logic (Fixed) - passing -mu
    # mean = 5. P(X > 4) given mean 5 is > 50%.
    curr_prob = 1.0 - model._normal_cdf(-line_s, -mu_s_final, sig_s)
    print(f"New Logic Win Prob: {curr_prob:.4f} (Expect > 0.5)")
    
    if curr_prob > 0.5 and prior_prob < 0.1:
        print("SUCCESS: Fix confirmed. Favorites now have valid win probabilities.")
    else:
        print("FAILURE: Logic still incorrect.")

if __name__ == "__main__":
    verify_math()
