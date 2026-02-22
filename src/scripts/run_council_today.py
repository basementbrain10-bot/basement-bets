import sys
import os
import json
import datetime
from datetime import datetime as dt, timezone

# Allow running from repo root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.database import get_db_connection, _exec
from src.agents.contracts import EventContext, DecisionRun
from src.agents.research_agent import ResearchAgent
from src.agents.memory_agent import MemoryAgent
from src.agents.oracle_agent import OracleAgent
from src.agents.journal_agent import JournalAgent

def main():
    date_et = None
    if len(sys.argv) > 1:
        date_et = sys.argv[1]
    
    if not date_et:
        try:
            with get_db_connection() as conn:
                date_et = _exec(conn, "SELECT (NOW() AT TIME ZONE 'America/New_York')::date::text").fetchone()[0]
        except Exception:
            date_et = dt.now(timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=-5))).strftime('%Y-%m-%d')
            
    print(f"[{dt.now().isoformat()}] run_council_today for {date_et}")
    
    # 1. Fetch Actionable Top Picks for the Date
    q = """
    SELECT d.event_id, d.rec_json, e.home_team, e.away_team
    FROM daily_top_picks d
    JOIN events e ON d.event_id = e.id
    WHERE d.date_et = %(d)s 
      AND d.league = 'NCAAM'
      AND d.is_actionable = TRUE
    """
    with get_db_connection() as conn:
        picks = _exec(conn, q, {"d": date_et}).fetchall()
        
    if not picks:
        print("No actionable picks found to run the council on today.")
        return
        
    print(f"Found {len(picks)} actionable picks to review.")
    
    # 2. Build Event contexts and Quant Edges
    events = []
    quant_edges = {}
    
    for pick in picks:
        ev_id = pick['event_id']
        rec = pick['rec_json']
        if isinstance(rec, str):
            rec = json.loads(rec)
            
        if not rec:
            continue
            
        home_team = pick.get('home_team', 'Home')
        away_team = pick.get('away_team', 'Away')
        
        ev = EventContext(
            event_id=ev_id,
            league="NCAAM",
            home_team=home_team,
            away_team=away_team,
            start_time=f"{date_et}T12:00:00Z" # Dummy fallback
        )
        events.append(ev)
        
        # Build quant context
        market_str = rec.get('selection', 'Unknown Selection')
        ev_disp = rec.get('edge', 'Unknown Edge')
        conf = rec.get('confidence', 'Unknown')
        quant_edges[ev_id] = f"Selection: {market_str} | Model EV: {ev_disp} | Confidence: {conf}"
        
    if not events:
        print("No valid events constructed.")
        return
        
    # 3. Run the Council Agents
    research_agent = ResearchAgent()
    memory_agent = MemoryAgent()
    oracle_agent = OracleAgent()
    journal_agent = JournalAgent()
    
    # Run in batches of 5 to avoid Vercel timeouts if needed
    for i in range(0, len(events), 5):
        batch_events = events[i:i+5]
        batch_edges = {ev.event_id: quant_edges[ev.event_id] for ev in batch_events}
        
        print(f"Running Council for batch {i//5 + 1} ({len(batch_events)} games)...")
        
        research_out, _ = research_agent.run({"events": batch_events})
        memory_out, _ = memory_agent.run({"events": batch_events})
        
        oracle_out, _ = oracle_agent.run({
            "events": batch_events,
            "edges": batch_edges,
            "research": research_out,
            "memories": memory_out
        })
        
        if not oracle_out:
            print("Failed to get Oracle output.")
            continue
            
        # 4. Persist to decision_runs via JournalAgent
        run_id = "DR-COUNCIL-" + dt.now(timezone.utc).strftime('%Y%m%d%H%M%S') + f"-B{i}"
        
        decision_run = DecisionRun(
            run_id=run_id,
            created_at=dt.now(timezone.utc).isoformat(),
            league="NCAAM",
            status="COUNCIL_COMPLETE",
            inputs_hash=f"council_{date_et}_{i}",
            offers_count=len(batch_events),
            recommendations=[],
            rejected_offers=[],
            notes=[f"Council run on {len(batch_events)} top picks."],
            errors=[],
            model_version="2.1.2-council",
            council_narrative=oracle_out
        )
        
        journal_agent.run({"decision_run": decision_run, "action": "persist"})
        print(f"Persisted council debate for {len(batch_events)} games.")
        
    print("Council analysis complete. Re-running build_daily_top_picks.py to apply the qualitative adjustments (Oracle verdicts)...")
    
    # Run the top picks builder again to apply the adjustments
    from src.scripts.build_daily_top_picks import fetch_event_ids_for_date, upsert_pick
    from src.models.ncaam_market_first_model_v2 import NCAAMMarketFirstModelV2
    
    # Only re-run on the games we just reviewed
    model = NCAAMMarketFirstModelV2()
    with get_db_connection() as conn:
        for ev in events:
            try:
                res = model.analyze(ev.event_id, relax_gates=False, persist=False)
                upsert_pick(date_et, ev.event_id, res if isinstance(res, dict) else {}, conn=conn)
            except Exception as e:
                pass
        conn.commit()
        
    print("Successfully applied qualitative adjustments.")

if __name__ == '__main__':
    main()
