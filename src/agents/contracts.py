from typing import List, Optional, Any, Dict
from pydantic import BaseModel, ConfigDict, StrictFloat, field_validator
from datetime import datetime

class AgentError(BaseModel):
    model_config = ConfigDict(strict=True)
    agent: str
    code: str
    message: str
    detail: Optional[Dict[str, Any]] = None

class EventContext(BaseModel):
    model_config = ConfigDict(strict=True)
    event_id: str
    league: str
    home_team: str
    away_team: str
    start_time: Optional[str] = None
    neutral: Optional[bool] = None

class MarketOffer(BaseModel):
    model_config = ConfigDict(strict=True)
    event_id: str
    league: str
    market_type: str
    side: str
    odds_american: int
    book: str
    start_time: Optional[str] = None
    period: Optional[str] = None
    line: Optional[float] = None
    captured_at: Optional[str] = None

class FairPrice(BaseModel):
    model_config = ConfigDict(strict=True)
    event_id: str
    market_type: str
    side: str
    p_fair: StrictFloat
    confidence: StrictFloat
    model_sources: List[str]
    rationale: List[str]
    line: Optional[float] = None
    fair_odds_american: Optional[int] = None
    fair_line: Optional[float] = None

class EdgeResult(BaseModel):
    model_config = ConfigDict(strict=True)
    offer: MarketOffer
    fair: FairPrice
    implied_p: StrictFloat
    edge_points: StrictFloat
    ev_per_unit: StrictFloat
    ev_pct: StrictFloat
    flags: List[str]
    rationale: List[str]
    
    # Optional display properties for UI compat
    ev_display: Optional[str] = None
    edge_display: Optional[str] = None

class BetRecommendation(BaseModel):
    model_config = ConfigDict(strict=True)
    id: str
    offer: MarketOffer
    stake: float
    sizing_method: str
    rank: int
    confidence: StrictFloat
    expected_value: StrictFloat
    ev_pct: StrictFloat
    ev_per_unit: StrictFloat
    implied_p: StrictFloat
    edge_points: StrictFloat
    risk_flags: List[str]
    rationale: List[str]
    correlation_group: Optional[str] = None

class DecisionRun(BaseModel):
    model_config = ConfigDict(strict=True)
    run_id: str
    created_at: str
    league: str
    status: str  # OK | NO_BET | FAILED | STAGED_FOR_REVIEW
    inputs_hash: str
    offers_count: int
    recommendations: List[BetRecommendation]
    rejected_offers: List[Dict[str, Any]]
    notes: List[str]
    errors: List[AgentError]
    model_version: str
    council_narrative: Optional[Dict[str, Any]] = None
