import json
import datetime
from typing import Any, Dict, Tuple, Optional
from src.agents.base import BaseAgent
from src.agents.contracts import DecisionRun, BetRecommendation
from src.agents.settings import AGENTS_IDEMPOTENCY_WINDOW_SECONDS
from src.database import get_db_connection

class JournalAgent(BaseAgent):
    """
    Critical Serverless Agent: 
    Enforces a single DB connection and transaction per Orchestrator run.
    Uses bulk insertions to preserve Postgres pooling limits.
    """
    def execute(self, context: Dict[str, Any], *args, **kwargs) -> Any:
        action = context.get("action")
        
        if action == "check_cache":
            return self._check_cache(context.get("inputs_hash"))
        elif action == "persist":
            return self._persist(context.get("decision_run"))
        else:
            raise ValueError(f"Unknown JournalAgent action: {action}")

    def _check_cache(self, inputs_hash: str) -> Optional[DecisionRun]:
        if not inputs_hash:
            return None
            
        now = datetime.datetime.now(datetime.timezone.utc)
        cutoff = now - datetime.timedelta(seconds=AGENTS_IDEMPOTENCY_WINDOW_SECONDS)
        
        query = """
        SELECT payload_json FROM decision_runs
        WHERE inputs_hash = %s AND created_at >= %s
        ORDER BY created_at DESC LIMIT 1
        """
        
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Catch exception if table fundamentally doesn't exist yet on first deploy
                try:
                    cur.execute(query, (inputs_hash, cutoff))
                    row = cur.fetchone()
                    if row and row['payload_json']:
                        # Deserialize back to the strict Pydantic contract
                        return DecisionRun.model_validate_json(row['payload_json'])
                except Exception:
                    pass
                    
        return None

    def _persist(self, decision: DecisionRun) -> bool:
        if not decision:
            return False
            
        payload_str = decision.model_dump_json()
        
        queries = []
        params = []
        
        # 1. Main Run Ledger
        queries.append("""
            INSERT INTO decision_runs (run_id, created_at, league, status, inputs_hash, payload_json, model_version, council_narrative)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO NOTHING
        """)
        
        cn_json = json.dumps(decision.council_narrative) if decision.council_narrative else None
        
        params.append((
            decision.run_id, decision.created_at, decision.league, 
            decision.status, decision.inputs_hash, payload_str, decision.model_version, cn_json
        ))

        # 2. Extract specific recommendations
        if decision.recommendations:
            # We use manual batch execution structure inside the single connection block
            rec_sql = """
                INSERT INTO decision_recommendations 
                (run_id, rec_id, event_id, market_type, side, line, odds, stake, ev_pct, confidence, payload_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (rec_id) DO NOTHING
            """
            for r in decision.recommendations:
                queries.append(rec_sql)
                params.append((
                    decision.run_id, r.id, r.offer.event_id, r.offer.market_type,
                    r.offer.side, r.offer.line, r.offer.odds_american, r.stake,
                    r.ev_pct, r.confidence, r.model_dump_json()
                ))

        # 3. Handle STAGED_FOR_REVIEW queue if applicable
        if decision.status == "STAGED_FOR_REVIEW":
            hitl_sql = """
                INSERT INTO pending_decisions (run_id, created_at, status, inputs_hash, payload_json, reason, expires_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            expires = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=6)
            queries.append(hitl_sql)
            params.append((
                decision.run_id, decision.created_at, "PENDING", decision.inputs_hash, 
                payload_str, "Flagged for manual review by Orchestrator", expires
            ))

        # 4. Agent Traces
        if hasattr(decision, 'agent_traces') and decision.agent_traces:
            trace_sql = """
                INSERT INTO agent_traces (run_id, agent_name, task_description, details, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """
            for t in decision.agent_traces:
                queries.append(trace_sql)
                params.append((
                    decision.run_id, t.agent_name, t.task_description,
                    json.dumps(t.details) if t.details else None,
                    t.timestamp or datetime.datetime.now(datetime.timezone.utc).isoformat()
                ))

        # 5. SINGLE Pool Connection Block
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                for q, p in zip(queries, params):
                    cur.execute(q, p)
            conn.commit()
            
        return True
