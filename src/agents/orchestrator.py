import datetime
import hashlib
import json
from typing import Any, Dict, List, Optional
from src.agents.settings import (
    AGENTS_IDEMPOTENCY_WINDOW_SECONDS,
    AGENTS_REVIEW_CONFIDENCE_THRESHOLD,
    AGENTS_REVIEW_ON_FLAGS,
    AGENTS_MAX_EVENTS_PER_RUN
)
from src.agents.contracts import (
    AgentError, BetRecommendation, DecisionRun, EdgeResult, EventContext,
    FairPrice, MarketOffer
)

class DecisionOrchestrator:
    """
    Coordinates the pipeline of Agents into a singular DecisionRun payload.
    Employs an idempotency key (inputs_hash) to short-circuit repeated Vercel retries.
    """
    def __init__(self, league: str, model_version: str):
        self.league = league
        self.model_version = model_version
        self.db_conn = None # To be injected by callers or JournalAgent
        
    def build_inputs_hash(self, parameters: Dict[str, Any]) -> str:
        """
        Produce a deterministic hash serving as our concurrency/idempotency lock.
        Based on league, parameter footprints, and current rounded time buckets.
        """
        # Round time to idempotency window blocks so retries fall in same bucket
        now = datetime.datetime.now(datetime.timezone.utc).timestamp()
        bucket = int(now // AGENTS_IDEMPOTENCY_WINDOW_SECONDS)
        
        payload = json.dumps({
            "league": self.league,
            "version": self.model_version,
            "params": parameters,
            "bucket": bucket
        }, sort_keys=True)
        return hashlib.sha256(payload.encode('utf-8')).hexdigest()

    def run_pipeline(
        self, 
        event_ops_agent,
        market_data_agent,
        pricing_agent,
        edge_ev_agent,
        risk_manager_agent,
        bet_builder_agent,
        research_agent,
        memory_agent,
        oracle_agent,
        journal_agent,
        parameters: Dict[str, Any]
    ) -> DecisionRun:
        
        run_id = "DR-" + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d%H%M%S')
        inputs_hash = self.build_inputs_hash(parameters)
        
        # 0. Check history DB for existing cached outputs matching inputs_hash
        # (Deferring strict DB check implementation to JournalAgent for pooled connections)
        cached_run, err = journal_agent.run({"inputs_hash": inputs_hash, "action": "check_cache"})
        if cached_run:
            return cached_run
            
        errors: List[AgentError] = []
        if err:
            errors.append(err)

        # 1. Pipeline Agents
        # Note: If any agent returns an error tuple, capturing and aborting gracefully.
        
        # A. Event Contexts (cap to AGENTS_MAX_EVENTS_PER_RUN inside agent wrapper)
        events, err = event_ops_agent.run({"league": self.league, "params": parameters})
        if err:
            return self._fail_fast(run_id, inputs_hash, errors + [err])
            
        # B. Offers
        offers, err = market_data_agent.run({"events": events})
        if err:
            return self._fail_fast(run_id, inputs_hash, errors + [err])
            
        # C. Pricing 
        fairs, err = pricing_agent.run({"offers": offers, "events": events})
        if err:
            return self._fail_fast(run_id, inputs_hash, errors + [err])
            
        # D. Edge & EV Computation
        edges, err = edge_ev_agent.run({"fairs": fairs, "offers": offers})
        if err:
            return self._fail_fast(run_id, inputs_hash, errors + [err])
            
        # E. Risk Sizing & Constraints
        recommendations, err = risk_manager_agent.run({"edges": edges})
        if err:
            return self._fail_fast(run_id, inputs_hash, errors + [err])
            
        # F. Bet Builder Formatting
        final_picks, err = bet_builder_agent.run({"recommendations": recommendations})
        if err:
            return self._fail_fast(run_id, inputs_hash, errors + [err])
            
        # 2. Hard Gates & Status Formatting
        status = "OK"
        if not final_picks:
            status = "NO_BET"
        else:
            # Check Human In The Loop rules
            for pick in final_picks:
                if pick.confidence < AGENTS_REVIEW_CONFIDENCE_THRESHOLD:
                    status = "STAGED_FOR_REVIEW"
                    break
                if AGENTS_REVIEW_ON_FLAGS and len(pick.risk_flags) > 0:
                    status = "STAGED_FOR_REVIEW"
                    break

        # G. The Agent Council (Research & RAG)
        # We only run the council on the specific events that made it through as recommendations
        # to save API costs and LLM tokens. Or we can do it for all events. Let's do it for recommended matches.
        recommended_event_ids = {r.offer.event_id for r in final_picks} if final_picks else set()
        target_events = [ev for ev in events if ev.event_id in recommended_event_ids]
        
        council_narrative = {}
        if target_events:
            research, err = research_agent.run({"events": target_events})
            if err: errors.append(err)
            
            memories, err = memory_agent.run({"events": target_events})
            if err: errors.append(err)
            
            # Map edges by event_id for the oracle
            edges_by_ev = {}
            if edges:
                for e in edges:
                    ev_id = e.offer.event_id
                    if ev_id not in edges_by_ev:
                        edges_by_ev[ev_id] = []
                    edges_by_ev[ev_id].append(f"{e.offer.market_type} {e.offer.side} {e.offer.line} ({e.ev_display})")
                    
            oracle_outputs, err = oracle_agent.run({
                "events": target_events,
                "edges": {k: ", ".join(v) for k, v in edges_by_ev.items()},
                "research": research,
                "memories": memories
            })
            if err: errors.append(err)
            council_narrative = oracle_outputs

        # 3. Formulate the final DecisionRun contract
        decision_run = DecisionRun(
            run_id=run_id,
            created_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            league=self.league,
            status=status,
            inputs_hash=inputs_hash,
            offers_count=len(offers) if offers else 0,
            recommendations=final_picks or [],
            rejected_offers=[], # Collect rejections actively in agents later
            notes=["Completed orchestrator pipeline."],
            errors=errors,
            model_version=self.model_version,
            council_narrative=council_narrative
        )
        
        # 4. Journaling (Guarded single DB transaction)
        _, err = journal_agent.run({"decision_run": decision_run, "action": "persist"})
        if err:
            # Even if Journaling fails, we log it and return the Decision Payload.
            decision_run.errors.append(err)
            
        return decision_run

    def _fail_fast(self, run_id: str, inputs_hash: str, errors: List[AgentError]) -> DecisionRun:
        return DecisionRun(
            run_id=run_id,
            created_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
            league=self.league,
            status="FAILED",
            inputs_hash=inputs_hash,
            offers_count=0,
            recommendations=[],
            rejected_offers=[],
            notes=["Pipeline aborted due to fatal agent error."],
            errors=errors,
            model_version=self.model_version
        )
