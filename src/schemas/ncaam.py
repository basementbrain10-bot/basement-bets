from pydantic import BaseModel, Field
from typing import List, Optional, Any

class PredictionResponse(BaseModel):
    """
    Standardized response for NCAAM Model predictions.
    """
    event_id: str
    home_team: str
    away_team: str
    market_type: str
    pick: str
    bet_line: Optional[float]
    bet_price: Optional[int]
    confidence_0_100: int
    ev_per_unit: float
    is_actionable: bool
    
    # Detailed components
    mu_final: float
    mu_market: float
    mu_torvik: float
    
    narrative: str
    recommendations: List[Any] = Field(default_factory=list)
