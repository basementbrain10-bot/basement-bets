import datetime
import traceback
from typing import Any, Dict, Optional, Tuple

from src.agents.contracts import AgentError

class BaseAgent:
    """
    Base Agent class that enforces non-exceptional error returns
    and standardized telemetry/timeboxing wrapping.
    """
    
    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def version(self) -> str:
        return "1.0.0"

    def run(self, context: Dict[str, Any], *args, **kwargs) -> Tuple[Optional[Any], Optional[AgentError]]:
        """
        Main execution wrapper that blocks exceptions from surfacing 
        and instead captures runtime failures natively as AgentError.
        """
        try:
            # Enforce deadlines here if implemented synchronously
            # Currently wrapping pure execution
            result = self.execute(context, *args, **kwargs)
            return result, None

        except Exception as ex:
            # Standardized exception capture to never swallow failures silently.
            err = AgentError(
                agent=self.name,
                code="UNHANDLED_EXCEPTION",
                message=str(ex),
                detail={"traceback": traceback.format_exc()}
            )
            return None, err

    def execute(self, context: Dict[str, Any], *args, **kwargs) -> Any:
        """
        Abstract property to be strictly implemented by the worker agent.
        Raises Exceptions directly to be captured by run().
        """
        raise NotImplementedError("Agents must implement execute().")
