import datetime
from typing import Any, Dict, List

from src.agents.base import BaseAgent
from src.agents.contracts import EventContext
from src.agents.settings import AGENTS_MAX_EVENTS_PER_RUN
from src.database import get_db_connection, _exec

class EventOpsAgent(BaseAgent):
    """
    Fetches upcoming Events from the database up to the max run capacity.
    """
    def execute(self, context: Dict[str, Any], *args, **kwargs) -> List[EventContext]:
        league = context.get("league", "NCAAM")
        params = context.get("params", {})
        days_ahead = int(params.get("days_ahead", 3))
        
        query = """
        SELECT id, home_team, away_team, start_time, league
        FROM events
        WHERE league = %s 
          AND start_time BETWEEN NOW() AND NOW() + INTERVAL '%s days'
        ORDER BY start_time ASC
        LIMIT %s
        """
        
        events_ret: List[EventContext] = []
        with get_db_connection() as conn:
            rows = _exec(conn, query, (league, days_ahead, AGENTS_MAX_EVENTS_PER_RUN)).fetchall()
            for row in rows:
                events_ret.append(
                    EventContext(
                        event_id=str(row['id']),
                        league=str(row['league']),
                        home_team=str(row['home_team']),
                        away_team=str(row['away_team']),
                        start_time=row['start_time'].isoformat() if isinstance(row['start_time'], datetime.datetime) else str(row['start_time']),
                        neutral=bool(row.get('neutral_site')) if 'neutral_site' in row else False
                    )
                )
                
        return events_ret
