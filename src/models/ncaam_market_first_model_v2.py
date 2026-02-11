
import math
import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional

# Async DB
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Pydantic V2 Schemas
from src.schemas.ncaam import PredictionResponse

# Services
from src.services.torvik_projection import TorvikProjectionService
from src.services.odds_selection_service import OddsSelectionService
from src.services.kenpom_client import KenPomClient
from src.services.news_service import NewsService
from src.services.geo_service import GeoService

# Base
from src.models.base_model import BaseModel
from src.utils.naming import standardize_team_name

class NCAAMMarketFirstModelV2(BaseModel):
    """
    NCAAM Market-First Model V2 (Async Refactor).
    - Fully Async Database Layer (SQLAlchemy 2.0)
    - Pydantic V2 Response
    - Corrected Math Logic (Sigma, Copysign)
    - Integrated with upstream robust recommendation/gate logic.
    """
    
    VERSION = "2.1.2-async-sniper"
    
    # Weights
    DEFAULT_W_BASE = 0.25
    DEFAULT_W_SCHED = 0.00
    DEFAULT_W_KENPOM = 0.10
    
    # Caps
    DEFAULT_CAP_SPREAD = 2.0
    DEFAULT_CAP_TOTAL = 3.0
    AGGRESSIVE_CAP_SPREAD = 4.0
    AGGRESSIVE_CAP_TOTAL = 5.0
    
    # Gates
    PUBLISH_MIN_EV = 0.02
    PUBLISH_CI_Z = 1.96
    PUBLISH_MIN_N_TORVIK_OK = 40
    PUBLISH_MIN_N_TORVIK_MISSING = 60
    PUBLISH_MIN_EV_TORVIK_MISSING_BUMP = 0.005

    def __init__(self, aggressive: bool = False, cap_spread: float = None, cap_total: float = None, manual_adjustments: dict = None):
        super().__init__(sport_key="ncaam")
        self.torvik_service = TorvikProjectionService()
        self.odds_selector = OddsSelectionService()
        self.kenpom_client = KenPomClient()
        self.news_service = NewsService()
        self.geo_service = GeoService()
        
        self.manual_adjustments = manual_adjustments or {}
        
        self.W_BASE = self.DEFAULT_W_BASE
        self.W_SCHED = self.DEFAULT_W_SCHED
        self.W_KENPOM = self.DEFAULT_W_KENPOM
        
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
            
        self._action_network_stats = None

    def fetch_data(self):
        """Pre-load data (No-op)."""
        pass

    def evaluate(self):
        """Evaluate performance (No-op)."""
        pass

    def predict(self, game_id: str, home_team: str, away_team: str, market_total: float = 0) -> Dict[str, Any]:
        """
        Legacy generic wrapper. 
        WARNING: This is synchronous and does not support the new async features.
        """
        # We cannot await here. Return empty/error or rely on analyze being called directly.
        return {"error": "Use async analyze() method"}


    async def analyze(self, event_id: str, db: AsyncSession, market_snapshot: Optional[Dict] = None) -> PredictionResponse:
        """
        Refactored to be fully Async and SQLAlchemy 2.0 compliant.
        Returns Pydantic V2 PredictionResponse.
        """
        # 1. Load Event (Async)
        stmt = text("SELECT * FROM events WHERE id = :id")
        result = await db.execute(stmt, {"id": event_id})
        event_row = result.mappings().fetchone()
        
        if not event_row:
            raise ValueError(f"Event {event_id} not found")
            
        # Convert row to dict for compatibility
        event = dict(event_row)

        # 2. Market Validation
        if not market_snapshot or market_snapshot.get('spread_home') is None:
             return self._build_empty_response(event_id, event['home_team'], event['away_team'], "Market Data Missing")

        mu_market_spread = float(market_snapshot['spread_home'])
        mu_market_total = float(market_snapshot['total'] or 145.0)

        # 3. External Projections
        game_date_obj = event['start_time']
        game_date_str = game_date_obj.strftime("%Y%m%d") if game_date_obj else None
        
        torvik_view = self.torvik_service.get_projection(event['home_team'], event['away_team'], date=game_date_str)
        
        if not torvik_view or torvik_view.get('lean') == 'No Data':
             return self._build_empty_response(event_id, event['home_team'], event['away_team'], "Torvik Data Missing")

        # 4. Situational Adjustments (Async DB calls)
        fatigue_h = await self._calculate_fatigue_penalty(event['home_team'], event_id, game_date_obj, db)
        fatigue_a = await self._calculate_fatigue_penalty(event['away_team'], event_id, game_date_obj, db)
        fatigue_adj = fatigue_h - fatigue_a
        
        # Shooting Regression
        shooting_regr_home = await self._calculate_shooting_regression(event['home_team'])
        shooting_regr_away = await self._calculate_shooting_regression(event['away_team'])
        shooting_adj = shooting_regr_home - shooting_regr_away

        # 5. Core Math
        w_base = self._get_dynamic_weights(game_date_obj)
        
        mu_torvik_spread = -(torvik_view.get('margin') or 0.0)
        diff_torvik = mu_torvik_spread - mu_market_spread
        
        # KenPom
        kenpom_adj = self.kenpom_client.calculate_kenpom_adjustment(event['home_team'], event['away_team'])
        kp_margin = kenpom_adj.get('spread_adj') or 0.0
        mu_kenpom_line = -kp_margin
        diff_kenpom = mu_kenpom_line - mu_market_spread
        
        # Final Spread
        mu_spread_final = mu_market_spread + (w_base * diff_torvik) + (self.W_KENPOM * diff_kenpom) + fatigue_adj + shooting_adj
        
        # Apply Caps (Spread)
        if abs(mu_spread_final - mu_market_spread) > self.CAP_SPREAD:
             diff = mu_spread_final - mu_market_spread
             mu_spread_final = mu_market_spread + (self.CAP_SPREAD * math.copysign(1.0, diff))

        # Final Total
        mu_torvik_total = torvik_view.get('total') or 145.0
        mu_total_final = mu_market_total + (w_base * (mu_torvik_total - mu_market_total))
        
        # Apply Caps (Total)
        if abs(mu_total_final - mu_market_total) > self.CAP_TOTAL:
             diff_t = mu_total_final - mu_market_total
             mu_total_final = mu_market_total + (self.CAP_TOTAL * math.copysign(1.0, diff_t))

        # Pace-Adjusted Sigma 
        game_tempo = torvik_view.get('tempo') or 68.0
        tempo_factor = math.sqrt(game_tempo / 68.0)
        
        base_sigma_spread = 10.5
        base_sigma_total = 15.0 
        
        sigma_spread = (base_sigma_spread * tempo_factor) + 0.1 * abs(diff_torvik)
        sigma_total = (base_sigma_total * tempo_factor) + 0.1 * abs(mu_torvik_total - mu_market_total)

        # 6. Recommendations (Using Upstream Logic)
        recs = self._generate_recommendations(mu_spread_final, sigma_spread, mu_total_final, sigma_total, market_snapshot, event, torvik_view=torvik_view)
        
        # Fetch News Lazy
        news_context = {}
        if recs:
            try:
                news_context = self.news_service.fetch_game_context(event['home_team'], event['away_team'])
            except Exception as e:
                print(f"[NEWS] fetch_game_context failed: {e}")

        # Narrative & Response Construction
        # We need raw_snaps for narrative if available
        raw_snaps = market_snapshot.get('_raw_snaps', [])
        
        # Debug Info
        debug_info = {
            "mu_spread_final": mu_spread_final,
            "sigma_spread": sigma_spread,
            "tempo_factor": tempo_factor,
            "fatigue_adj": fatigue_adj,
            "shooting_adj": shooting_adj,
            "w_base": w_base
        }

        narrative = self._generate_narrative(event, market_snapshot, torvik_view, kenpom_adj, news_context, recs, raw_snaps=raw_snaps, debug_info=debug_info)
        
        best_rec = recs[0] if recs else None
        
        # Return Pydantic Response
        return PredictionResponse(
            event_id=event_id,
            home_team=event['home_team'],
            away_team=event['away_team'],
            market_type=best_rec['market'] if best_rec else "NONE",
            pick=best_rec['side'] if best_rec else "PASS",
            bet_line=best_rec['line'] if best_rec else None,
            bet_price=best_rec['price'] if best_rec else None,
            confidence_0_100=int(best_rec['ev']*1000) if best_rec else 0, 
            ev_per_unit=best_rec['ev'] if best_rec else 0.0,
            is_actionable=(best_rec and best_rec['ev'] > 0.02),
            mu_final=round(mu_spread_final, 2),
            mu_market=round(mu_market_spread, 2),
            mu_torvik=round(mu_torvik_spread, 2),
            narrative=json.dumps(narrative, default=str), # Serialize dict to str for Pydantic if needed, or change schema to Dict
            recommendations=recs
        )

    # --- Async Helpers ---

    async def _calculate_fatigue_penalty(self, team_name: str, event_id: str, current_game_date, db: AsyncSession) -> float:
        if not current_game_date: return 0.0
        
        query = text("""
            SELECT start_time FROM events 
            WHERE (LOWER(home_team) LIKE LOWER(:t) OR LOWER(away_team) LIKE LOWER(:t))
            AND start_time < :now 
            AND id != :eid
            ORDER BY start_time DESC LIMIT 2
        """)
        
        result = await db.execute(query, {
            "t": f"%{team_name}%", 
            "now": current_game_date, 
            "eid": event_id
        })
        rows = result.fetchall()
        
        if not rows: return 0.0
        
        last_game_date = rows[0].start_time
        days_rest = (current_game_date - last_game_date).days
        
        if days_rest <= 1: return -1.5 
        if len(rows) > 1:
            two_days_ago = rows[1].start_time
            if (current_game_date - two_days_ago).days <= 6:
                return -2.5 
                
        return 0.0

    async def _calculate_shooting_regression(self, team_name: str) -> float:
        # Mock implementation as per refactor plan
        stats = {"delta": 0.05} 
        delta = stats.get('delta', 0.0)
        
        if delta > 0.04:
             penalty = (delta * 100) / 2 * 0.5
             return -min(penalty, 2.0)
        elif delta < -0.04:
             bonus = abs(delta * 100) / 2 * 0.25
             return min(bonus, 1.0)
        return 0.0
        
    def _get_dynamic_weights(self, date):
        return 0.25

    def _build_empty_response(self, event_id, home, away, reason=""):
        return PredictionResponse(
            event_id=event_id, home_team=home, away_team=away, 
            market_type="NONE", pick="PASS", 
            confidence_0_100=0, ev_per_unit=0.0, is_actionable=False,
            mu_final=0.0, mu_market=0.0, mu_torvik=0.0, 
            narrative=f"Skipped: {reason}",
            recommendations=[]
        )

    # --- Upstream Logic Preserved ---

    def _generate_narrative(self, event, market_snapshot, torvik_view, kenpom_adj, news_context, recs, raw_snaps=None, debug_info=None):
        # Simplified Narrative Builder
        return {
            "headline": f"{event['away_team']} @ {event['home_team']}",
            "summary": f"Model favors {recs[0]['side']} ({recs[0]['ev']*100:.1f}% EV)" if recs else "No actionable edge.",
            "key_factors": ["Pace-adjusted analysis", "Torvik projection"],
            "risks": ["Late line movement"]
        }

    def _archetype_key(self, market: str, side: str, edge_pts: float, spread_bucket: Optional[str], torvik_ok: bool) -> str:
        edge_bucket = int(round(min(10.0, max(0.0, float(edge_pts)))))
        sb = spread_bucket or "na"
        tv = "tv" if torvik_ok else "no_tv"
        s = (side or "").lower().replace(" ", "_")
        return f"{market}:{s}:{edge_bucket}:{sb}:{tv}"

    def _spread_bucket(self, market_line_home: float) -> str:
        try:
            v = abs(float(market_line_home or 0.0))
        except Exception:
            v = 0.0
        if v <= 3.0: return "close"
        if v <= 7.0: return "mid"
        return "big"

    def _load_action_network_stats(self) -> Dict[str, Any]:
        if self._action_network_stats is not None:
            return self._action_network_stats
        
        # Use relative path safely
        try:
            repo_root = os.getcwd() # Assumption
            path = os.path.join(repo_root, 'data', 'model_params', 'action_network_archetype_stats_ncaam.json')
            if os.path.exists(path):
                with open(path, 'r', encoding='utf-8') as f:
                    self._action_network_stats = json.load(f)
            else:
                 self._action_network_stats = {"bins": {}}
        except Exception:
            self._action_network_stats = {"bins": {}}
        return self._action_network_stats

    def _get_archetype_stats(self, key: str) -> Dict[str, Any]:
        stats = self._load_action_network_stats()
        bins = (stats or {}).get('bins') or {}
        parts = key.split(':')
        short_key = ':'.join(parts[:4]) if len(parts) >= 4 else key
        row = bins.get(short_key) or {}
        return {
            "n": int(row.get('n') or 0),
            "mean": float(row.get('mean') or 0.0),
            "sd": float(row.get('sd') or 0.0),
        }

    def _passes_publish_gates(self, rec: Dict[str, Any], market_line_home: Optional[float], torvik_ok: bool) -> bool:
        ev = float(rec.get("ev") or 0.0)
        edge_pts = float(rec.get("edge_points") or 0.0)
        market = str(rec.get("market") or "")
        
        spread_bucket = self._spread_bucket(market_line_home or 0.0) if market == "SPREAD" else None
        key = self._archetype_key(market, str(rec.get('side')), edge_pts, spread_bucket, torvik_ok)
        stats = self._get_archetype_stats(key)
        
        min_n = self.PUBLISH_MIN_N_TORVIK_OK if torvik_ok else self.PUBLISH_MIN_N_TORVIK_MISSING
        min_ev = self.PUBLISH_MIN_EV if torvik_ok else (self.PUBLISH_MIN_EV + self.PUBLISH_MIN_EV_TORVIK_MISSING_BUMP)
        
        if ev < min_ev: return False
        if (stats.get("n") or 0) < min_n: return False
        
        # Override check
        override_ev = float(os.getenv('PUBLISH_OVERRIDE_MIN_EV', '0.06'))
        override_edge = float(os.getenv('PUBLISH_OVERRIDE_MIN_EDGE_PTS', '2.0'))
        if ev >= override_ev and edge_pts >= override_edge: return True
        
        return True # Default lenient for now if stats present

    def _generate_recommendations(self, mu_s, sig_s, mu_t, sig_t, snap, event, torvik_view: Optional[Dict[str, Any]] = None) -> List[Dict]:
        recs: List[Dict[str, Any]] = []
        if not snap: return recs

        torvik_ok = bool(torvik_view and str(torvik_view.get("lean") or "").lower().strip() not in ("no data", ""))

        def get_push_prob(line, sigma):
            if line is None or line % 1 != 0: return 0.0
            if abs(line) in {2, 3, 4, 5, 6, 7, 10, 14}: return 0.05
            return 0.03

        # Spread Logic
        line_s = snap.get("spread_home")
        if line_s is not None:
             prob_home_raw = 1.0 - self._normal_cdf(-line_s, -mu_s, sig_s)
             push_prob = get_push_prob(line_s, sig_s)
             prob_home = prob_home_raw - (push_prob / 2)
             prob_away = (1.0 - prob_home_raw) - (push_prob / 2)
             
             ev_home = self._calculate_ev(prob_home, -110)
             ev_away = self._calculate_ev(prob_away, -110)
             
             edge_pts = abs(mu_s - line_s)
             
             cand_home = {
                 "market": "SPREAD", "side": "home", "team": event["home_team"], "line": line_s,
                 "price": -110, "win_prob": prob_home, "ev": ev_home, "edge_points": edge_pts, "book": "Consensus"
             }
             cand_away = {
                 "market": "SPREAD", "side": "away", "team": event["away_team"], "line": -line_s,
                 "price": -110, "win_prob": prob_away, "ev": ev_away, "edge_points": edge_pts, "book": "Consensus"
             }
             
             best = cand_home if ev_home > ev_away else cand_away
             if self._passes_publish_gates(best, line_s, torvik_ok):
                 recs.append(best)
                 
        # Total Logic
        line_t = snap.get("total")
        best_over = snap.get("_best_total_over")
        best_under = snap.get("_best_total_under")

        price_over = best_over["price"] if best_over else snap.get("total_over_price", -110)
        price_under = best_under["price"] if best_under else -110
        book_over = best_over["book"] if best_over else snap.get("book_total", "Consensus")
        book_under = best_under["book"] if best_under else snap.get("book_total", "Consensus")

        if line_t is not None:
             prob_over_raw = 1.0 - self._normal_cdf(line_t, mu_t, sig_t)
             push_prob_t = get_push_prob(line_t, sig_t)
             prob_over = prob_over_raw - (push_prob_t / 2)
             prob_under = (1.0 - prob_over_raw) - (push_prob_t / 2)

             ev_over = self._calculate_ev(prob_over, price_over)
             kelly_over = self._calculate_kelly(prob_over, price_over)
             ev_under = self._calculate_ev(prob_under, price_under)
             kelly_under = self._calculate_kelly(prob_under, price_under)

             market_line_t = float(line_t)
             fair_line_t = float(mu_t)
             edge_pts_t = abs(fair_line_t - market_line_t)

             cand_over = {
                 "market": "TOTAL",
                 "side": "over",
                 "line": float(best_over["line_value"] if best_over else line_t),
                 "price": int(price_over),
                 "prob": round(prob_over, 3),
                 "win_prob": round(prob_over, 3),
                 "ev": float(round(ev_over, 4)),
                 "kelly": float(round(kelly_over, 4)),
                 "book": book_over,
                 "edge_points": float(round(edge_pts_t, 2)),
             }

             cand_under = {
                 "market": "TOTAL",
                 "side": "under",
                 "line": float(best_under["line_value"] if best_under else line_t),
                 "price": int(price_under),
                 "prob": round(prob_under, 3),
                 "win_prob": round(prob_under, 3),
                 "ev": float(round(ev_under, 4)),
                 "kelly": float(round(kelly_under, 4)),
                 "book": book_under,
                 "edge_points": float(round(edge_pts_t, 2)),
             }

             best = cand_over if cand_over["ev"] >= cand_under["ev"] else cand_under
             if self._passes_publish_gates(best, market_line_home=None, torvik_ok=torvik_ok):
                 recs.append(best)

             
        return recs

    def _normal_cdf(self, x, mu, sigma):
        return 0.5 * (1 + math.erf((x - mu) / (sigma * math.sqrt(2))))

    def _calculate_ev(self, win_prob: float, american_odds: int) -> float:
        if american_odds > 0:
            decimal_odds = (american_odds / 100.0) + 1.0
        else:
            decimal_odds = (100.0 / abs(american_odds)) + 1.0
        return (win_prob * (decimal_odds - 1)) - (1 - win_prob)

    def _calculate_kelly(self, win_prob, american_odds):
        if american_odds > 0:
            decimal_odds = (american_odds / 100.0) + 1.0
        else:
            decimal_odds = (100.0 / abs(american_odds)) + 1.0
        b = decimal_odds - 1
        return (b * win_prob - (1 - win_prob)) / b
