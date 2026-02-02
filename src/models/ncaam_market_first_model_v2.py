
import math
import json
from datetime import datetime
from typing import Dict, List, Any, Optional

from src.services.torvik_projection import TorvikProjectionService
from src.services.odds_selection_service import OddsSelectionService
from src.services.kenpom_client import KenPomClient
from src.services.news_service import NewsService
from src.services.geo_service import GeoService
from src.database import get_db_connection, _exec, insert_model_prediction

from src.models.base_model import BaseModel
from src.utils.naming import standardize_team_name

class NCAAMMarketFirstModelV2(BaseModel):
    """
    NCAAM Market-First Model v2.
    - Market base with corrective signals.
    - CLV-first gating.
    - Structured narratives.
    """
    
    VERSION = "2.1.1-sniper"
    
    # Optimized Model Weights (2026-02-01 Sniper Plan)
    # Target: 75% Market / 25% Torvik+KenPom
    # Formula: mu = market + base*(torvik-market) + kenpom*(kenpom-market)
    # Weights apply to the DIFF. 0.25 on diff means 25% projection / 75% market anchor.
    DEFAULT_W_BASE = 0.25  # Torvik Weight (Increased from 0.20)
    DEFAULT_W_SCHED = 0.00 # Disabled (Season Stats removal)
    DEFAULT_W_KENPOM = 0.00 # KenPom Weight (Integrated into base/torvik or handled separately? V1 plan said 25% total. Let's stick effectively to Torvik dominance for simplicity)
    # Wait, implementation uses multiple diffs. Let's set Base 0.25 and KenPom 0.0 for now to match V1 exactly.
    # Actually V2 has separate KenPom logic. Keep KenPom small or 0 if V1 disabled it? V1 ENABLED KenPom at 10% in plan.
    # Re-reading plan: "Shift from 20% to 25% (Torvik)". "Increase KenPom Weight: 10%".
    DEFAULT_W_KENPOM = 0.10
    
    # Default Caps
    DEFAULT_CAP_SPREAD = 2.0
    DEFAULT_CAP_TOTAL = 3.0
    
    # Aggressive Mode Caps
    AGGRESSIVE_CAP_SPREAD = 4.0
    AGGRESSIVE_CAP_TOTAL = 5.0

    def __init__(self, aggressive: bool = False, cap_spread: float = None, cap_total: float = None, manual_adjustments: dict = None):
        """
        Initialize model with configurable parameters.
        """
        super().__init__(sport_key="ncaam") # BaseModel init
        self.torvik_service = TorvikProjectionService()
        self.odds_selector = OddsSelectionService()
        self.kenpom_client = KenPomClient()
        self.news_service = NewsService()
        self.geo_service = GeoService()
        
        self.manual_adjustments = manual_adjustments or {}
        
        # Set weights (Sniper Config)
        self.W_BASE = self.DEFAULT_W_BASE
        self.W_SCHED = self.DEFAULT_W_SCHED
        self.W_KENPOM = self.DEFAULT_W_KENPOM
        
        # Set caps (priority: explicit > aggressive > default)
        if cap_spread is not None:
            self.CAP_SPREAD = cap_spread
        elif aggressive:
            self.CAP_SPREAD = self.AGGRESSIVE_CAP_SPREAD
        else:
            self.CAP_SPREAD = self.DEFAULT_CAP_SPREAD
            
        if cap_total is not None:
            self.CAP_TOTAL = cap_total
        elif aggressive:
            self.CAP_TOTAL = self.AGGRESSIVE_CAP_TOTAL
        else:
            self.CAP_TOTAL = self.DEFAULT_CAP_TOTAL

    def fetch_data(self):
        """Pre-load data (No-op for V2, as it fetches per-request)."""
        pass

    def evaluate(self):
        """Evaluate performance (No-op)."""
        pass

    def _get_dynamic_weights(self, game_date) -> float:
        """
        Returns a weight (W_BASE) that scales based on the time of year.
        Early season: Trust Market heavily (projections are preseason-based).
        Late season: Trust Projections more (based on actual games).
        """
        from datetime import datetime
        
        if game_date is None:
            game_date = datetime.now()
        elif isinstance(game_date, str):
            try:
                game_date = datetime.fromisoformat(game_date.replace('Z', '+00:00'))
            except:
                game_date = datetime.now()
        
        month = game_date.month
        
        # Off-season/Early Season (Nov/Dec) - Preseason projections
        if month == 11: return 0.12  # Trust Market heavily
        if month == 12: return 0.18  
        # Conference Play (Jan/Feb) - Real data accumulating
        if month == 1: return 0.25   # Standard Sniper Weight
        if month == 2: return 0.32   # Trust Projections more
        # Tournament Time (March) - Peak sample size
        if month == 3: return 0.38   # Peak Alpha
        # April+ (Rare, post-tournament)
        return 0.25  # Default

    def _calculate_shooting_regression(self, team_name: str) -> float:
        """
        3-Point Variance Signal: Fade teams shooting hot from 3.
        If a team is shooting >8% above their season average over last 3 games,
        apply a negative regression penalty.
        """
        from src.database import get_team_recent_shooting
        
        try:
            stats = get_team_recent_shooting(team_name, num_games=3)
            delta = stats.get('delta', 0.0)
            
            if delta is None:
                return 0.0
                
            # For every 2% above season average, subtract 0.5 points
            # Only apply if shooting significantly hot (>4% above avg)
            if delta > 0.04:  # 4% threshold
                penalty = (delta * 100) / 2 * 0.5  # Convert to points
                return -min(penalty, 2.0)  # Cap at -2 points
            elif delta < -0.04:  # Shooting cold, bonus (regression to mean UP)
                bonus = abs(delta * 100) / 2 * 0.25  # Smaller bonus
                return min(bonus, 1.0)  # Cap at +1 point
                
        except Exception as e:
            print(f"[MODEL] Shooting regression error for {team_name}: {e}")
            
        return 0.0

    def _calculate_situational_spots(self, team_name: str, event: dict) -> float:
        """
        Spot Modeling: Lookahead and Letdown situations.
        - Lookahead: Team plays weak opponent before a big game → flat performance
        - Letdown: Team just upset a ranked opponent → hangover
        """
        from src.database import get_team_last_game, get_db_connection, _exec
        from datetime import datetime
        
        adj = 0.0
        current_date = event.get('start_time', datetime.now())
        if isinstance(current_date, str):
            try:
                current_date = datetime.fromisoformat(current_date.replace('Z', '+00:00'))
            except:
                current_date = datetime.now()
        
        try:
            # 1. LETDOWN SPOT: Check if last game was an upset win
            last_game = get_team_last_game(team_name)
            if last_game:
                margin = last_game.get('margin', 0) or 0
                opponent_rank = last_game.get('opponent_rank', 0) or 0
                
                # Upset win = Won (margin > 0) against ranked team (#1-25)
                if margin > 0 and 1 <= opponent_rank <= 25:
                    # Big upset against Top 10 = bigger letdown
                    if opponent_rank <= 10:
                        adj -= 1.5  # Major letdown risk
                        print(f"[SPOT] {team_name} LETDOWN: Just beat Top 10 team (#{opponent_rank})")
                    else:
                        adj -= 1.0  # Standard letdown
                        print(f"[SPOT] {team_name} LETDOWN: Just beat ranked team (#{opponent_rank})")
            
            # 2. LOOKAHEAD SPOT: Check if NEXT game is against a Top 25/rival
            with get_db_connection() as conn:
                query = """
                SELECT e.home_team, e.away_team, 
                       COALESCE(m1.adj_o, 0) + COALESCE(m1.adj_d, 0) as home_rank,
                       COALESCE(m2.adj_o, 0) + COALESCE(m2.adj_d, 0) as away_rank
                FROM events e
                LEFT JOIN bt_team_metrics_daily m1 ON LOWER(m1.team_text) LIKE LOWER(CONCAT('%', e.home_team, '%'))
                LEFT JOIN bt_team_metrics_daily m2 ON LOWER(m2.team_text) LIKE LOWER(CONCAT('%', e.away_team, '%'))
                WHERE (LOWER(e.home_team) LIKE LOWER(:t) OR LOWER(e.away_team) LIKE LOWER(:t))
                  AND e.start_time > :now
                ORDER BY e.start_time ASC LIMIT 1
                """
                # Use named parameters to avoid %s conflicts and improve safety
                result = _exec(conn, query, {"t": f"%{team_name}%", "now": current_date}).fetchone()
                
                # This is complex - for MVP, just apply a small lookahead penalty
                # if next opponent is highly ranked. Full implementation needs rivalry DB.
                # Skipping for now, will log when data available.
                
        except Exception as e:
            print(f"[MODEL] Situational spot error for {team_name}: {e}")
            
        return adj

    def _apply_referee_signal(self, event_id: str, mu_total: float) -> float:
        """
        Referee Signal: Adjust total based on officiating crew tendencies.
        Crews that call more fouls = more free throws = higher totals.
        """
        from src.database import get_referee_assignment
        
        NCAA_AVG_FOULS = 36.0  # National average fouls per game
        
        try:
            ref_data = get_referee_assignment(event_id)
            if ref_data and ref_data.get('crew_avg_fouls'):
                crew_fouls = float(ref_data['crew_avg_fouls'])
                
                # Every 1 foul above average adds ~0.8 points to total
                if crew_fouls > NCAA_AVG_FOULS:
                    over_adj = (crew_fouls - NCAA_AVG_FOULS) * 0.8
                    mu_total += min(over_adj, 4.0)  # Cap at +4 points
                    print(f"[REF] Crew avg {crew_fouls:.1f} fouls → Total +{over_adj:.1f}")
                elif crew_fouls < NCAA_AVG_FOULS - 2:
                    under_adj = (NCAA_AVG_FOULS - crew_fouls) * 0.5
                    mu_total -= min(under_adj, 2.0)  # Cap at -2 points
                    print(f"[REF] Tight crew {crew_fouls:.1f} fouls → Total -{under_adj:.1f}")
                    
        except Exception as e:
            print(f"[MODEL] Referee signal error: {e}")
            
        return mu_total

    def _calculate_fatigue_penalty(self, team_name: str, event_id: str, current_game_date) -> float:
        """
        Checks the schedule for 'Short Rest' spots.
        -2.5 pts for '3rd game in 6 days' (The 'Tired Legs' Spot)
        -1.5 pts for 'Back-to-back'
        """
        from src.database import get_db_connection, _exec
        from datetime import datetime, timedelta
        
        if current_game_date is None:
            return 0.0
        if isinstance(current_game_date, str):
            try:
                current_game_date = datetime.fromisoformat(current_game_date.replace('Z', '+00:00'))
            except:
                return 0.0
        
        try:
            # Query recent games, excluding the current one
            query = """
            SELECT id, start_time FROM events 
            WHERE (LOWER(home_team) LIKE LOWER(:t) OR LOWER(away_team) LIKE LOWER(:t))
            AND start_time < :now
            AND id != :eid  -- Critical fix: Exclude current game to prevent self-matching
            ORDER BY start_time DESC LIMIT 2
            """
            
            # Use named parameters
            params = {
                "t": f"%{team_name}%",
                "now": current_game_date,
                "eid": event_id
            }
            
            with get_db_connection() as conn:
                rows = _exec(conn, query, params).fetchall()
                
            if not rows: 
                return 0.0
            
            last_game_date = rows[0]['start_time']
            if isinstance(last_game_date, str):
                last_game_date = datetime.fromisoformat(last_game_date)
            
            days_rest = (current_game_date - last_game_date).days
            
            # Only count >0 days to avoid weird intraday header issues, 
            # though id exclusion handles the main bug.
            if days_rest == 0:
                 # Sanity check: if same day and not same ID, double header? 
                 # Unlikely in NCAA. Assume data noise.
                 pass

            if days_rest <= 1: 
                print(f"[FATIGUE] {team_name}: Back-to-back (-1.5)")
                return -1.5  # Back-to-back
            
            if len(rows) > 1:
                two_games_ago = rows[1]['start_time']
                if isinstance(two_games_ago, str):
                    two_games_ago = datetime.fromisoformat(two_games_ago)
                if (current_game_date - two_games_ago).days <= 6:
                    print(f"[FATIGUE] {team_name}: 3rd game in 6 days (-2.5)")
                    return -2.5  # 3rd game in 6 days
                    
        except Exception as e:
            print(f"[MODEL] Fatigue calc error for {team_name}: {e}")
            
        return 0.0

    def _generate_bell_curve(self, mu: float, sigma: float, line: float) -> Dict:
        """
        Generates 50 points representing the Normal Distribution curve.
        Used by Frontend (Chart.js) to render the 'Cover Zone'.
        """
        import math
        
        points = []
        # Generate points from -3 sigma to +3 sigma
        start = mu - (3 * sigma)
        end = mu + (3 * sigma)
        step = (end - start) / 50
        
        for i in range(51):
            x = start + (i * step)
            # Standard Normal PDF Formula
            y = (1 / (sigma * math.sqrt(2 * math.pi))) * math.exp(-0.5 * ((x - mu) / sigma)**2)
            points.append({"x": round(x, 2), "y": round(y, 6)})
        
        # Calculate probability of covering (CDF at line)
        z = (line - mu) / sigma
        cover_prob = 0.5 * (1 + math.erf(z / math.sqrt(2)))
        
        return {
            "points": points,
            "mu": round(mu, 2),
            "sigma": round(sigma, 2),
            "line": round(line, 2),
            "cover_prob": round(cover_prob, 4),
            "is_under_line": mu < line  # Tells UI which side is favorable
        }

    def predict(self, game_id: str, home_team: str, away_team: str, market_total: float = 0) -> Dict[str, Any]:
        """
        Satisfy BaseModel interface by wrapping analyze().
        """
        from datetime import datetime
        # Create minimal context
        event = {
            "id": game_id,
            "home_team": standardize_team_name(home_team),
            "away_team": standardize_team_name(away_team),
            "sport": "NCAAM",
            "league": "NCAAM",
            "start_time": datetime.now()  # Default to now to prevent luck check failure
        }
        # Minimal market snap
        snap = {
            "total": market_total or 145.0,
            "spread_home": 0.0
        }
        return self.analyze(game_id, market_snapshot=snap, event_context=event)

    def analyze(self, event_id: str, market_snapshot: Optional[Dict] = None, event_context: Optional[Dict] = None) -> Dict:
        """
        On-demand analysis for one game.
        """
        # 1. Load Event (Use context if provided, else DB)
        event = event_context
        if not event:
            event = self._get_event(event_id)
        
        if not event:
            return {"error": f"Event {event_id} not found."}

        # 2. Market Snapshot - Use CONSENSUS for model input (not best-line outlier)
        raw_snaps = []
        if not market_snapshot:
            raw_snaps = self._get_all_recent_odds(event_id)
            
            # Step A: Get CONSENSUS lines (median of all books) for model input
            consensus_spread = self.odds_selector.get_consensus_snapshot(raw_snaps, 'SPREAD', 'HOME')
            consensus_total = self.odds_selector.get_consensus_snapshot(raw_snaps, 'TOTAL', 'OVER')
            
            # Step B: Get BEST PRICES for later (after we find an edge)
            best_spread_home = self.odds_selector.get_best_price_for_side(raw_snaps, 'SPREAD', 'HOME')
            best_spread_away = self.odds_selector.get_best_price_for_side(raw_snaps, 'SPREAD', 'AWAY')
            best_total_over = self.odds_selector.get_best_price_for_side(raw_snaps, 'TOTAL', 'OVER')
            best_total_under = self.odds_selector.get_best_price_for_side(raw_snaps, 'TOTAL', 'UNDER')
            
            # Composite Snapshot for Analysis (uses CONSENSUS for model math)
            market_snapshot = {
                # Consensus for model input
                'spread_home': consensus_spread['line_value'] if consensus_spread else None,
                'spread_price_home': consensus_spread['price'] if consensus_spread else -110,
                'total': consensus_total['line_value'] if consensus_total else None,
                'total_over_price': consensus_total['price'] if consensus_total else -110,
                'book_spread': 'Consensus',
                'book_total': 'Consensus',
                # Best prices for betting (after edge identified)
                '_best_spread_home': best_spread_home,
                '_best_spread_away': best_spread_away,
                '_best_total_over': best_total_over,
                '_best_total_under': best_total_under,
                # Raw for narrative
                '_raw_snaps': raw_snaps
            }
        else:
            raw_snaps = market_snapshot.get('_raw_snaps', [])

        # VALIDATION: Abort if Critical Market Data is Missing
        if market_snapshot.get('spread_home') is None or market_snapshot.get('total') is None:
            return {
                "headline": "Market Data Waiting",
                "recommendation": "No Line",
                "rationale": ["Market odds not yet fully populated."],
                "is_actionable": False
            }

        mu_market_spread = float(market_snapshot['spread_home'])
        mu_market_total = float(market_snapshot['total'])

        # 3. External Projections & Signals
        # Torvik - PRIMARY SIGNAL
        game_date_obj = event.get('start_time')
        game_date_str = None
        if game_date_obj:
            try:
                # Ensure we have a string YYYYMMDD for Torvik service
                # If it's a string, try to parse it first (ISO format from earlier fix)
                if isinstance(game_date_obj, str):
                    game_date_obj = datetime.fromisoformat(game_date_obj.replace('Z', '+00:00'))
                game_date_str = game_date_obj.strftime("%Y%m%d")
            except Exception:
                pass

        torvik_view = self.torvik_service.get_projection(event['home_team'], event['away_team'], date=game_date_str)
        
        # VALIDATION: Abort if Torvik is missing (Stop the "0.0 edge" bug)
        if not torvik_view or torvik_view.get('lean') == 'No Data':
             return {
                "headline": "Data Unavailable",
                "recommendation": "Pass",
                "rationale": ["Primary projection source (Torvik) unavailable for this game."],
                "is_actionable": False
            }

        # Other Signals
        torvik_team_stats = self.torvik_service.get_matchup_team_stats(event['home_team'], event['away_team'], date=game_date_str)
        kenpom_adj = self.kenpom_client.calculate_kenpom_adjustment(event['home_team'], event['away_team'])
        news_context = self.news_service.fetch_game_context(event['home_team'], event['away_team'])
        
        # 4. Dynamic Weight Scaling
        self.W_BASE = self._get_dynamic_weights(game_date_obj)
        w_base = self.W_BASE

        # 5. Blend Math (Strict mu_final)
        # Defensive None handling
        mu_torvik_spread = -(torvik_view.get('margin') or 0.0)
        mu_sched_spread = -(torvik_view.get('official_margin') or -mu_torvik_spread)
        
        # KenPom Integration
        kp_margin = kenpom_adj.get('spread_adj') or 0.0
        mu_kenpom_line = -kp_margin
        
        diff_torvik = (mu_torvik_spread - mu_market_spread)
        diff_sched = (mu_sched_spread - mu_market_spread)
        diff_kenpom = (mu_kenpom_line - mu_market_spread)
        
        # --- Feature: Luck Regression ---
        # If a team has high "Luck" (>0.05), they are overperforming and due for regression.
        # We apply a penalty to their projected margin.
        
        # Volatility Check: In November (Month 11), luck is noisy. Scale down.
        luck_scale = 1.0
        try:
             # event['start_time'] is datetime object
             if event['start_time'].month == 11:
                 luck_scale = 0.5
        except Exception:
             pass

        home_luck = (torvik_team_stats.get('home') or {}).get('luck', 0.0) or 0.0
        away_luck = (torvik_team_stats.get('away') or {}).get('luck', 0.0) or 0.0
        
        luck_adjustment = 0.0
        if home_luck > 0.05:
            luck_adjustment += (1.0 * luck_scale) # Home penalty
        if home_luck < -0.05:
            luck_adjustment -= (1.0 * luck_scale) # Home boost
            
        if away_luck > 0.05:
            luck_adjustment -= (1.0 * luck_scale) # Away penalty
        if away_luck < -0.05:
            luck_adjustment += (1.0 * luck_scale) # Away boost
            
        # --- Feature: Continuity / Transfer Portal Factor ---
        # If teams have low continuity, metrics are less reliable early season.
        # We trust the MARKET more.
        home_continuity = (torvik_team_stats.get('home') or {}).get('continuity', 1.0) or 0.7
        away_continuity = (torvik_team_stats.get('away') or {}).get('continuity', 1.0) or 0.7
        
        avg_continuity = (home_continuity + away_continuity) / 2.0
        
        # Dynamic Weights
        w_base = self.W_BASE
        if avg_continuity < 0.5:
             # Low continuity -> Trust Market More (increase W_BASE from 0.20 to 0.40?)
             # Effectively reduces weight of Torvik/KenPom
             # Wait, equation is: mu_market + W * (diff)
             # If we want to stay closer to market, we REDUCE the weights of projections?
             # Or we treat Market as anchor. 
             # Current formula: market + 0.2*(torvik-market).
             # If we want to trust market more, we should LOWER W_BASE.
             # "increase W_BASE (Market Weight)" - wait, W_BASE in my code is strictly applied to Torvik diff?
             # Let's check line 102: mu_spread_final = mu_market + (W_BASE * diff_torvik) ...
             # YES. To trust market MORE, we need SMALLER weights on the diffs.
             w_base = 0.10 # Reduced from 0.20
        
        
        mu_spread_final = mu_market_spread + (w_base * diff_torvik) + (self.W_SCHED * diff_sched) + (self.W_KENPOM * diff_kenpom) + luck_adjustment
        
        # Apply Caps
        if abs(mu_spread_final - mu_market_spread) > self.CAP_SPREAD:
             mu_spread_final = mu_market_spread + (self.CAP_SPREAD * math.copysign(1, mu_spread_final - mu_market_spread))

        # Total - with None-safe handling
        mu_torvik_total = torvik_view.get('total') or 145.0
        kenpom_total_adj = kenpom_adj.get('total_adj') or 0.0
        market_total = market_snapshot.get('total') or 145.0
        mu_kenpom_total = market_total + kenpom_total_adj
        
        # Ensure mu_market_total is not None
        if mu_market_total is None:
            mu_market_total = 145.0
        
        mu_total_final = mu_market_total + (w_base * (mu_torvik_total - mu_market_total)) + (self.W_KENPOM * (mu_kenpom_total - mu_market_total))
        
        if abs(mu_total_final - mu_market_total) > self.CAP_TOTAL:
             mu_total_final = mu_market_total + (self.CAP_TOTAL * math.copysign(1, mu_total_final - mu_market_total))

        # 6. Pace-Adjusted Sigma (Refined)
        # Higher tempo = more possessions = higher variance = wider sigma
        # Scaling: sqrt(tempo / avg) is theoretically correct for variance scaling.
        game_tempo = (torvik_team_stats or {}).get('game_tempo', 68.0) or 68.0
        tempo_factor = math.sqrt(game_tempo / 68.0)
        
        base_sigma_spread = 10.5
        base_sigma_total = 15.0
        
        
        sigma_spread = (base_sigma_spread * tempo_factor) + 0.1 * abs(diff_torvik)
        sigma_total = (base_sigma_total * tempo_factor) + 0.1 * abs(mu_torvik_total - mu_market_total)
        
        # --- Feature: Advanced Home Court (Geo) ---
        # Travel Fatigue & Altitude
        # Neutral Site Detection
        is_neutral = False
        if event_context and event_context.get('neutral_site'):
            is_neutral = True
        elif ' vs ' in f"{event['away_team']} {event['home_team']}" or ' vs. ' in f"{event['away_team']} {event['home_team']}":
             # Extremely crude heuristic: some feeds use "vs" for neutral, "@" for home.
             # But our event['home_team'] is structured.
             # Better: Check if event['site_key'] or similar exists.
             # For now, rely on manual_adjustments or updated parsers.
             pass
             
        if self.manual_adjustments.get('is_neutral'):
            is_neutral = True

        # Altitude
        altitude_adj = self.geo_service.get_altitude_adjustment(event['home_team'], neutral_site=is_neutral)
        if altitude_adj > 0:
            mu_spread_final -= altitude_adj
            
        # Travel
        dist = self.geo_service.calculate_distance(event['home_team'], event['away_team'])
        if dist > 1000 and not is_neutral:
            mu_spread_final -= 0.5

        # --- Feature: Live Injury Impact Toggle ---
        if self.manual_adjustments:
            h_inj = self.manual_adjustments.get('home_injury', 0.0)
            a_inj = self.manual_adjustments.get('away_injury', 0.0)
            mu_spread_final += h_inj
            mu_spread_final -= a_inj

        # --- Feature: Basement Line (Power Rating) ---
        raw_basement_line = (mu_torvik_spread + mu_kenpom_line) / 2.0
        raw_basement_line += luck_adjustment
        if altitude_adj > 0: raw_basement_line -= altitude_adj
        if dist > 1000 and not is_neutral: raw_basement_line -= 0.5
        if self.manual_adjustments:
            raw_basement_line += self.manual_adjustments.get('home_injury', 0.0)
            raw_basement_line -= self.manual_adjustments.get('away_injury', 0.0)
        
        # 6.5 Advanced Situational Signals
        # -- Shooting Regression (3PT Variance) --
        shooting_regr_home = self._calculate_shooting_regression(event['home_team'])
        shooting_regr_away = self._calculate_shooting_regression(event['away_team'])
        shooting_adj = shooting_regr_home - shooting_regr_away
        mu_spread_final += shooting_adj
        
        # -- Situational Spots (Letdown/Lookahead) --
        spot_adj_home = self._calculate_situational_spots(event['home_team'], event)
        spot_adj_away = self._calculate_situational_spots(event['away_team'], event)
        spot_adj = spot_adj_home - spot_adj_away
        mu_spread_final += spot_adj
        
        # -- Fatigue / Short Rest Signal --
        game_date = event.get('start_time')
        event_id = event.get('id')
        h_fatigue = self._calculate_fatigue_penalty(event['home_team'], event_id, game_date)
        a_fatigue = self._calculate_fatigue_penalty(event['away_team'], event_id, game_date)
        fatigue_adj = h_fatigue - a_fatigue
        mu_spread_final += fatigue_adj
        
        # -- Referee Signal (Totals) --
        mu_total_final = self._apply_referee_signal(event['id'], mu_total_final)
        
        # 6.6 Generate Bell Curve Data for UI Visualization
        bell_curve_spread = self._generate_bell_curve(mu_spread_final, sigma_spread, mu_market_spread)
        bell_curve_total = self._generate_bell_curve(mu_total_final, sigma_total, mu_market_total)
            
        # 7. Recommendations & EV
        recs = self._generate_recommendations(mu_spread_final, sigma_spread, mu_total_final, sigma_total, market_snapshot, event)
        
        debug_info = {
            "mu_spread_final": mu_spread_final,
            "sigma_spread": sigma_spread,
            "tempo_factor": tempo_factor,
            "luck_adj": luck_adjustment,
            "geo_adj": altitude_adj if 'altitude_adj' in locals() else 0.0,
            "is_neutral": is_neutral,
            "basement_line": raw_basement_line,
            "w_base": self.W_BASE,
            "shooting_adj": shooting_adj,
            "spot_adj": spot_adj,
            "fatigue_adj": fatigue_adj,
            "bell_curve_spread": bell_curve_spread,
            "bell_curve_total": bell_curve_total,
            "torvik_refresh": datetime.now().strftime('%Y-%m-%d %H:%M'),  # When Torvik data was fetched
        }
        
        # 8. Narrative (UI MATCH)
        # Pass raw odds so we can generate matchup-specific key factors (e.g., line movement)
        narrative = self._generate_narrative(event, market_snapshot, torvik_view, kenpom_adj, news_context, recs, raw_snaps=raw_snaps, debug_info=debug_info)
        
        # 9. Result Object
        ui_recs = []
        best_rec = None

        # Basic game script (team-efficiency driven; Torvik)
        game_script = []
        try:
            h = (torvik_team_stats or {}).get('home') or {}
            a = (torvik_team_stats or {}).get('away') or {}
            tempo = (torvik_team_stats or {}).get('game_tempo')
            if tempo is not None:
                pace_label = 'fast' if tempo >= 71 else 'average' if tempo >= 67 else 'slow'
                game_script.append(f"Expected pace: {pace_label} (~{tempo} possessions).")

            # Mismatch style explanations (adj_off vs opp adj_def)
            # Note: adj_def is points allowed per 100 (lower is better).
            if h.get('adj_off') is not None and a.get('adj_def') is not None:
                game_script.append(f"Home offense ({h['adj_off']:.1f} AdjO) vs away defense ({a['adj_def']:.1f} AdjD) drives home scoring projection.")
            if a.get('adj_off') is not None and h.get('adj_def') is not None:
                game_script.append(f"Away offense ({a['adj_off']:.1f} AdjO) vs home defense ({h['adj_def']:.1f} AdjD) drives away scoring projection.")

            # Late-game / variance callouts
            game_script.append("Spread outcomes are most sensitive to turnover margin and late free throws (end-game fouling).")
        except Exception:
            game_script = []

        
        for r in recs:
            # Provide side-relative market + fair lines so UI can explain meaning.
            market_line_side = None
            fair_line_side = None
            edge_points_side = None
            win_prob = r.get('win_prob')

            if r['market'] == 'SPREAD':
                market_home = market_snapshot.get('spread_home')
                fair_home = mu_spread_final

                # Convert to the side being bet (home vs away)
                if r['side'] == event['home_team']:
                    market_line_side = market_home
                    fair_line_side = fair_home
                else:
                    market_line_side = (-market_home) if market_home is not None else None
                    fair_line_side = (-fair_home) if fair_home is not None else None

                if (market_line_side is not None) and (fair_line_side is not None):
                    # Points you are getting vs model fair (positive = better for bettor)
                    edge_points_side = round(float(market_line_side) - float(fair_line_side), 1)

            elif r['market'] == 'TOTAL':
                market_total = market_snapshot.get('total')
                fair_total = mu_total_final
                market_line_side = market_total
                fair_line_side = fair_total
                if (market_line_side is not None) and (fair_line_side is not None):
                    # For totals, interpret edge points as difference in line (direction depends on OVER/UNDER)
                    edge_points_side = round(float(fair_line_side) - float(market_line_side), 1)

            ui_recs.append({
                "bet_type": r['market'],
                "selection": r['side'] + (f" {r['line']}" if r['line'] is not None else ""),
                # Keep legacy key name for UI, but this is EV% not points.
                "edge": f"{(r['ev']*100):.2f}%",
                "win_prob": round(float(win_prob), 3) if win_prob is not None else None,
                "market_line": (round(float(market_line_side), 1) if market_line_side is not None else None),
                "fair_line": (round(float(fair_line_side), 1) if fair_line_side is not None else None),
                "edge_points": edge_points_side,
                "confidence": "High" if r['ev'] * 100 * 5 > 80 else "Medium" if r['ev'] * 100 * 5 > 50 else "Low", # Using new confidence calc
                "book": r['book']
            })
            if not best_rec or r['ev'] > best_rec['ev']:
                best_rec = r

        res = {
            "id": None, 
            "event_id": event_id,
            "home_team": event['home_team'],
            "away_team": event['away_team'],
            "analyzed_at": datetime.now().isoformat(),
            "model_version": self.VERSION,
            "market_type": best_rec['market'] if best_rec else "AUTO",
            "pick": best_rec['side'] if best_rec else "NONE",
            "bet_line": best_rec['line'] if best_rec else None,
            "bet_price": best_rec['price'] if best_rec else None,
            "book": best_rec['book'] if best_rec else None,
            "mu_market": mu_market_spread,
            "mu_torvik": mu_torvik_spread,
            "mu_final": mu_spread_final,
            "sigma": sigma_spread,
            "win_prob": best_rec['win_prob'] if best_rec else 0.5,
            "ev_per_unit": best_rec['ev'] if best_rec else 0.0,
            "kelly": best_rec['kelly'] if best_rec else 0.0,
            "confidence_0_100": int(best_rec['ev'] * 100 * 5) if best_rec else 0, # Crude scale
            "inputs_json": json.dumps({"market": market_snapshot, "torvik": torvik_view, "kenpom": kenpom_adj, "news": news_context}, default=str),
            "outputs_json": json.dumps({"mu_spread": mu_spread_final, "mu_total": mu_total_final, "recommendations": recs, "debug": debug_info}, default=str),
            "narrative": narrative, 
            "narrative_json": json.dumps(narrative, default=str),
            "recommendations": ui_recs,
            "torvik_view": torvik_view,
            "torvik_team_stats": torvik_team_stats,
            "game_script": game_script,
            "kenpom_data": kenpom_adj,
            "news_summary": self.news_service.summarize_impact(news_context),
            "key_factors": narrative.get('key_factors') or [],
            "risks": narrative.get('risks') or [],
            "selection": best_rec['side'] if best_rec else None,
            "price": best_rec['price'] if best_rec else None,
            "basement_line": mu_spread_final if (best_rec and best_rec['market'] == 'SPREAD') else mu_total_final,
            "edge_points": abs((best_rec['line'] if best_rec else 0) - (mu_spread_final if (best_rec and best_rec['market'] == 'SPREAD') else mu_total_final)), 
            "open_line": best_rec['line'] if best_rec else None,
            "open_price": best_rec['price'] if best_rec else None,
            "clv_method": "odds_selector_v1",
            "debug": debug_info
        }
        
        if not res['id']:
            import uuid
            res['id'] = str(uuid.uuid4())

        # 10. Persist
        insert_model_prediction(res)
        
        return res

    def _generate_recommendations(self, mu_s, sig_s, mu_t, sig_t, snap, event) -> List[Dict]:
        recs = []
        if not snap: return recs
        
        # Helper: Push probability for whole-number lines
        def get_push_prob(line, sigma):
            """Estimate push probability for whole-number lines."""
            if line is None:
                return 0.0
            # If half-point line (e.g., -5.5), no push possible
            if line % 1 != 0:
                return 0.0
            # For whole-number lines, estimate P(margin == line) using PDF approximation
            # Approximate: push_prob ≈ 0.05 for common numbers, 0.03 otherwise
            key_numbers = {2, 3, 4, 5, 6, 7, 10, 14}
            if abs(line) in key_numbers:
                return 0.05
            return 0.03
        
        # --- Spread ---
        line_s = snap.get('spread_home')
        
        # Use CONSENSUS line for all recommendations (user prefers single line display)
        price_home = snap.get('spread_price_home', -110)
        # Use actual best away price if available (user feedback: don't hardcode -110)
        best_away = snap.get('_best_spread_away', {})
        price_away = best_away.get('price', -110) if best_away else -110
        book_consensus = snap.get('book_spread', 'Consensus')
        
        if line_s is not None:
            # Calculate Home Side
            # Correct formula: P(Home Covers) = P(Margin > -Spread)
            # mu_s is Expected SPREAD (e.g. -5). Expected Margin is -mu_s (e.g. +5).
            prob_home_raw = 1.0 - self._normal_cdf(-line_s, -mu_s, sig_s)
            
            push_prob = get_push_prob(line_s, sig_s)
            # Adjust: subtract half of push probability
            prob_home = prob_home_raw - (push_prob / 2)
            
            # EV for Home
            ev_home = self._calculate_ev(prob_home, price_home)
            kelly_home = self._calculate_kelly(prob_home, price_home)
            
            # Away side (mirrored consensus line)
            prob_away = (1.0 - prob_home_raw) - (push_prob / 2)
            ev_away = self._calculate_ev(prob_away, price_away)
            kelly_away = self._calculate_kelly(prob_away, price_away)
            
            # Filter: Sniper Logic (Edge >= 2.5 pts)
            market_line_s = float(line_s)
            fair_line_s = float(mu_s) 
            # Fair line is expected spread (e.g. -5).
            # Market line is spread (e.g. -2).
            # Edge = abs(-5 - -2) = 3pts.
            edge_pts = abs(fair_line_s - market_line_s)
            
            threshold = 0.01
            # "Sniper" Mode: Only return actionable bets if Edge >= 2.5
            is_actionable = (edge_pts >= 2.5)

            if ev_home > threshold and is_actionable:
                # Home Bet - use consensus line
                recs.append({
                    "market": "SPREAD",
                    "side": event['home_team'],
                    "line": round(market_line_s, 1),
                    "price": price_home,
                    "prob": round(prob_home, 3),
                    "win_prob": round(prob_home, 3),
                    "ev": round(ev_home, 3),
                    "kelly": round(kelly_home, 3),
                    "book": book_consensus,
                    "edge_points": round(edge_pts, 1)
                })
            elif ev_away > threshold and is_actionable:
                # Away Bet
                recs.append({
                    "market": "SPREAD",
                    "side": event['away_team'],
                    "line": round(-market_line_s, 1),
                    "price": price_away,
                    "prob": round(prob_away, 3),
                    "win_prob": round(prob_away, 3),
                    "ev": round(ev_away, 3),
                    "kelly": round(kelly_away, 3),
                    "book": book_consensus,
                    "edge_points": round(edge_pts, 1)
                })

        # --- Total ---
        line_t = snap.get('total')
        
        # Get best prices for totals
        best_over = snap.get('_best_total_over')
        best_under = snap.get('_best_total_under')
        
        price_over = best_over['price'] if best_over else snap.get('total_over_price', -110)
        price_under = best_under['price'] if best_under else -110
        book_over = best_over['book'] if best_over else snap.get('book_total', 'Consensus')
        book_under = best_under['book'] if best_under else snap.get('book_total', 'Consensus')
        
        if line_t is not None:
            # Prob Over = P(score > line)
            prob_over_raw = 1.0 - self._normal_cdf(line_t, mu_t, sig_t)
            push_prob_t = get_push_prob(line_t, sig_t)
            prob_over = prob_over_raw - (push_prob_t / 2)
            prob_under = (1.0 - prob_over_raw) - (push_prob_t / 2)
            
            ev_over = self._calculate_ev(prob_over, price_over)
            kelly_over = self._calculate_kelly(prob_over, price_over)
            ev_under = self._calculate_ev(prob_under, price_under)
            kelly_under = self._calculate_kelly(prob_under, price_under)
            
            # Filter: Sniper Logic Totals (Edge >= 3.0 pts)
            # Re-enabled per user request but gated for quality.
            market_line_t = float(line_t)
            fair_line_t = float(mu_t)
            edge_pts_t = abs(fair_line_t - market_line_t)
            
            threshold = 0.01
            is_actionable_t = (edge_pts_t >= 3.0)
            
            if ev_over > threshold and is_actionable_t:
                # Over Bet
                recs.append({
                    "market": "TOTAL",
                    "side": "OVER",
                    "line": best_over['line_value'] if best_over else line_t,
                    "price": price_over,
                    "prob": round(prob_over, 3),
                    "win_prob": round(prob_over, 3),
                    "ev": round(ev_over, 3),
                    "kelly": round(kelly_over, 3),
                    "book": book_over,
                    "edge_points": round(edge_pts_t, 1)
                })
            elif ev_under > threshold and is_actionable_t:
                # Under Bet
                recs.append({
                    "market": "TOTAL",
                    "side": "UNDER",
                    "line": best_under['line_value'] if best_under else line_t,
                    "price": price_under,
                    "prob": round(prob_under, 3),
                    "win_prob": round(prob_under, 3),
                    "ev": round(ev_under, 3),
                    "kelly": round(kelly_under, 3),
                    "book": book_under,
                    "edge_points": round(edge_pts_t, 1)
                })
                 
        return recs

    def _normal_cdf(self, x, mu, sigma):
        return 0.5 * (1 + math.erf((x - mu) / (sigma * math.sqrt(2))))

    def _calculate_ev(self, win_prob, price):
        """
        Calculate Estimated Value (ROI)
        EV = (Win_Prob * Profit) - (Loss_Prob * Wager)
        Normalized to 1 unit wager.
        """
        if price > 0:
            payout = price / 100.0
        else:
            payout = 100.0 / abs(price)
            
        ev = (win_prob * payout) - (1.0 - win_prob)
        return ev

    def _calculate_kelly(self, win_prob, price):
        """
        Calculate Kelly Criterion optimal stake fraction.
        f = (bp - q) / b
        b = net odds received (decimal - 1)
        p = win probability
        q = loss probability
        """
        if price > 0:
            b = price / 100.0
        else:
            b = 100.0 / abs(price)
            
        p = win_prob
        q = 1.0 - p
        
        fraction = (b * p - q) / b
        
        # Quarter Kelly for safety
        return max(0.0, fraction * 0.25)

    def _generate_narrative(self, event, snap, torvik, kenpom, news, recs, raw_snaps: Optional[List[Dict]] = None, debug_info: Optional[Dict] = None) -> Dict:
        """Generate matchup-specific narrative + factors.

        IMPORTANT: key_factors/risks must be specific to this matchup, not generic labels.
        """
        headline = "No Edge Detected"
        rationale: List[str] = []
        key_factors: List[str] = []
        risks: List[str] = []

        spread_mkt = snap.get('spread_home', None)
        total_mkt = snap.get('total', None)
        
        debug_info = debug_info or {}

        # --- Line movement (best effort) ---
        line_move = None
        try:
            if raw_snaps:
                home_spreads = [s for s in raw_snaps if s.get('market_type') == 'SPREAD' and s.get('side') == 'HOME' and s.get('line_value') is not None]
                if home_spreads:
                    latest = home_spreads[0].get('line_value')
                    earliest = home_spreads[-1].get('line_value')
                    if latest is not None and earliest is not None:
                        line_move = float(latest) - float(earliest)
        except Exception:
            line_move = None

        # --- Core rationale strings (still specific) ---
        if spread_mkt is not None:
            rationale.append(f"Market spread (home): {spread_mkt:+.1f}")
        if total_mkt is not None:
            rationale.append(f"Market total: {float(total_mkt):.1f}")

        # Torvik
        if torvik:
            try:
                margin = float(torvik.get('margin', 0.0))
                # torvik margin is (home - away). fair spread is -margin.
                fair_spread_torvik = -margin
                if spread_mkt is not None:
                    delta = fair_spread_torvik - float(spread_mkt)
                    key_factors.append(f"Torvik margin {margin:+.1f} → fair spread {fair_spread_torvik:+.1f} (Δ vs market {delta:+.1f})")
            except Exception:
                pass
            if torvik.get('projected_score'):
                rationale.append(f"Torvik projected score: {torvik.get('projected_score')}")
            if torvik.get('lean'):
                rationale.append(f"Torvik lean: {torvik.get('lean')}")

        # KenPom
        try:
            kp_margin = kenpom.get('spread_adj', None)
            if kp_margin is not None:
                kp_margin = float(kp_margin)
                fair_spread_kp = -kp_margin
                if spread_mkt is not None:
                    delta = fair_spread_kp - float(spread_mkt)
                    key_factors.append(f"KenPom expected margin {kp_margin:+.1f} → fair spread {fair_spread_kp:+.1f} (Δ vs market {delta:+.1f})")
            if kenpom.get('total_adj') is not None and total_mkt is not None:
                key_factors.append(f"KenPom total adj {float(kenpom.get('total_adj')):+.1f} (market {float(total_mkt):.1f})")
        except Exception:
            pass

        # === SIGNAL-BASED FACTORS (from debug_info) ===
        
        # Luck Regression
        luck_adj = debug_info.get('luck_adj', 0.0)
        if luck_adj and abs(luck_adj) >= 0.5:
            direction = "inflated" if luck_adj > 0 else "deflated"
            key_factors.append(f"Luck regression: Team performance {direction} by recent good fortune ({luck_adj:+.1f} pts adj)")
        
        # Fatigue / Short Rest
        fatigue_adj = debug_info.get('fatigue_adj', 0.0)
        if fatigue_adj and abs(fatigue_adj) >= 1.0:
            if fatigue_adj < 0:
                key_factors.append(f"Fatigue signal: Home team on short rest ({fatigue_adj:+.1f} pts)")
            else:
                key_factors.append(f"Fatigue signal: Away team on short rest ({fatigue_adj:+.1f} pts to home)")
        
        # Shooting Regression (3PT variance)
        shooting_adj = debug_info.get('shooting_adj', 0.0)
        if shooting_adj and abs(shooting_adj) >= 0.5:
            if shooting_adj < 0:
                key_factors.append(f"Shooting regression: Team shooting hot from 3, expect regression ({shooting_adj:+.1f} pts)")
            else:
                key_factors.append(f"Shooting bounce-back: Team shooting cold from 3, expect improvement ({shooting_adj:+.1f} pts)")
        
        # Situational Spots (Letdown/Lookahead)
        spot_adj = debug_info.get('spot_adj', 0.0)
        if spot_adj and abs(spot_adj) >= 0.5:
            if spot_adj > 0:
                key_factors.append(f"Situational spot: Away team in letdown spot after big win ({spot_adj:+.1f} pts to home)")
            else:
                key_factors.append(f"Situational spot: Home team in letdown spot ({spot_adj:+.1f} pts)")
        
        # Dynamic Weights (Season Progression)
        w_base = debug_info.get('w_base', 0.25)
        if w_base != 0.25:
            trust_level = "high" if w_base >= 0.32 else "low"
            key_factors.append(f"Model trust: {trust_level} projection confidence ({w_base:.0%} weight)")

        # News
        if news:
            if news.get('has_injury_news'):
                key_factors.append(f"Injury/news: {news.get('summary')}")
            else:
                # Still matchup-specific: we looked and found none
                risks.append("No meaningful injury/rotation news detected (risk: late scratches)")

        # Market movement risk (if any)
        if line_move is not None and abs(line_move) >= 1.0:
            risks.append(f"Line moved {line_move:+.1f} pts recently (market disagreement / timing risk)")

        # Recommendation framing
        if recs:
            main = recs[0]
            headline = f"Bet: {main['side']} {main['line'] or ''}".strip()
            try:
                ev_pct = float(main.get('ev', 0.0)) * 100.0
                rationale.append(f"Model EV vs price: {ev_pct:+.1f}%")
            except Exception:
                pass

        # Data quality risk
        if not snap:
            risks.append("Missing market snapshot for this matchup (defaults used)")

        # If still empty, make it explicit but not generic
        if not key_factors:
            key_factors.append("No strong model-vs-market discrepancies detected for this matchup")

        return {
            "headline": headline,
            "market_summary": f"Line: {snap.get('spread_home','N/A')}  •  Total: {snap.get('total','N/A')}",
            "recommendation": headline,
            "rationale": rationale,
            "key_factors": key_factors,
            "risks": risks,
            "torvik_view": torvik.get('lean', 'Computed projections only') if torvik else 'Computed projections only',
            "kenpom_view": kenpom.get('summary', 'No Data') if kenpom else 'No Data',
            "news_view": news.get('summary', 'No News') if news else 'No News',
            "data_quality": "High" if snap else "Low"
        }

    def _get_event(self, event_id: str) -> Optional[Dict]:
        query = "SELECT * FROM events WHERE id = :id"
        with get_db_connection() as conn:
            row = _exec(conn, query, {"id": event_id}).fetchone()
            if row: return dict(row)
        return None

    def _get_all_recent_odds(self, event_id: str) -> List[Dict]:
        query = """
        SELECT market_type, side, line_value, price, book, captured_at
        FROM odds_snapshots 
        WHERE event_id = :eid 
        ORDER BY captured_at DESC
        LIMIT 200
        """
        with get_db_connection() as conn:
            rows = _exec(conn, query, {"eid": event_id}).fetchall()
            return [dict(r) for r in rows]
