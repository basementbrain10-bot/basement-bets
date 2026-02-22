import datetime
from typing import Any, Dict, List, Optional
from src.agents.base import BaseAgent
from src.agents.contracts import MarketOffer, EventContext
from src.services.odds_fetcher_service import OddsFetcherService

class MarketDataAgent(BaseAgent):
    """
    Normalizes existing Odds Fetcher data into strict Pydantic models.
    """
    def __init__(self):
        self.fetcher = OddsFetcherService()
        
    def execute(self, context: Dict[str, Any], *args, **kwargs) -> List[MarketOffer]:
        events: List[EventContext] = context.get("events", [])
        if not events:
            return []
            
        league = events[0].league if events else "NCAAM"
        
        # Determine the date span to fetch bulk odds from action network natively
        dates_to_fetch = set()
        for ev in events:
            if ev.start_time:
                # Naive parse to YYYYMMDD
                try:
                    dt = datetime.datetime.fromisoformat(ev.start_time.replace('Z', '+00:00'))
                    dates_to_fetch.add(dt.strftime("%Y%m%d"))
                except ValueError:
                    pass
        
        if not dates_to_fetch:
            dates_to_fetch.add(datetime.date.today().strftime("%Y%m%d"))
            
        raw_games = []
        for dt_str in dates_to_fetch:
            # We call existing service wrapper instead of raw REST avoiding refactors
            games = self.fetcher.fetch_odds(league=league, start_date=dt_str)
            raw_games.extend(games)
            
        offers: List[MarketOffer] = []
        now_str = datetime.datetime.now(datetime.timezone.utc).isoformat()
        
        # Link fetched blocks with db event elements via id formatting
        event_dict = {str(ev.event_id): ev for ev in events}
        
        for g in raw_games:
            game_id = str(g.get("game_id"))
            action_id = f"action:{league.lower()}:{game_id}"
            
            if action_id not in event_dict:
                # Depending on how events are keyed, try naked game_id just in case
                if game_id in event_dict:
                    action_id = game_id
                else:
                    continue
            
            ev_context = event_dict[action_id]
            
            # Formulate Spread Offers
            if g.get("home_spread") is not None and g.get("home_spread_odds") is not None:
                offers.append(MarketOffer(
                    event_id=ev_context.event_id,
                    league=ev_context.league,
                    market_type="SPREAD",
                    side="HOME",
                    odds_american=int(g.get("home_spread_odds", -110)),
                    book="action_network",
                    start_time=ev_context.start_time,
                    period="game",
                    line=float(g.get("home_spread")),
                    captured_at=now_str
                ))
            
            if g.get("away_spread") is not None and g.get("away_spread_odds") is not None:
                offers.append(MarketOffer(
                    event_id=ev_context.event_id,
                    league=ev_context.league,
                    market_type="SPREAD",
                    side="AWAY",
                    odds_american=int(g.get("away_spread_odds", -110)),
                    book="action_network",
                    start_time=ev_context.start_time,
                    period="game",
                    line=float(g.get("away_spread")),
                    captured_at=now_str
                ))
                
            # Formulate Total Offers
            total_score = g.get("total_score")
            if total_score is not None:
                if g.get("over_odds") is not None:
                    offers.append(MarketOffer(
                        event_id=ev_context.event_id,
                        league=ev_context.league,
                        market_type="TOTAL",
                        side="OVER",
                        odds_american=int(g.get("over_odds", -110)),
                        book="action_network",
                        start_time=ev_context.start_time,
                        period="game",
                        line=float(total_score),
                        captured_at=now_str
                    ))
                if g.get("under_odds") is not None:
                    offers.append(MarketOffer(
                        event_id=ev_context.event_id,
                        league=ev_context.league,
                        market_type="TOTAL",
                        side="UNDER",
                        odds_american=int(g.get("under_odds", -110)),
                        book="action_network",
                        start_time=ev_context.start_time,
                        period="game",
                        line=float(total_score),
                        captured_at=now_str
                    ))
                    
        return offers
