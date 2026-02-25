import sys
import os
import json
import datetime
from datetime import datetime as dt, timezone

# Allow running from repo root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from src.config import settings
from src.database import get_db_connection, _exec
from src.agents.contracts import EventContext, DecisionRun
from src.agents.research_agent import ResearchAgent
from src.agents.memory_agent import MemoryAgent
from src.agents.oracle_agent import OracleAgent
from src.agents.journal_agent import JournalAgent

def get_recently_analyzed(conn, event_ids):
    """
    Returns a dict mapping event_id to (last_analyzed_at, analyzed_line)
    leveraging the council_narrative JSONB column.
    """
    if not event_ids:
        return {}
    
    # We look for runs in the last 24 hours to be safe, though we only care about the last 2.
    q = """
    SELECT created_at, council_narrative 
    FROM decision_runs 
    WHERE created_at > NOW() - INTERVAL '24 hours'
      AND council_narrative IS NOT NULL
    ORDER BY created_at DESC
    """
    cur = _exec(conn, q)
    recent = {}
    for row in cur.fetchall():
        nar = row['council_narrative']
        if isinstance(nar, str):
            try:
                nar = json.loads(nar)
            except:
                continue
        if not isinstance(nar, dict):
            continue
        
        for eid in event_ids:
            if eid in nar and eid not in recent:
                line = None
                game_data = nar[eid]
                if isinstance(game_data, dict):
                    sig = game_data.get('signals', {})
                    if isinstance(sig, dict):
                        line = sig.get('market_line')
                
                recent[eid] = (row['created_at'], line)
    return recent

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
    
    q = """
    SELECT d.event_id, d.rec_json, e.home_team, e.away_team
    FROM daily_top_picks d
    JOIN events e ON d.event_id = e.id
    WHERE d.date_et = %(d)s 
      AND d.league = 'NCAAM'
      AND d.rec_json IS NOT NULL
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
        
        if isinstance(rec, list) and len(rec) > 0:
            rec = rec[0]
            
        # Build quant context
        if isinstance(rec, dict):
            market_str = rec.get('selection', rec.get('market', 'Unknown Selection'))
            ev_disp = rec.get('edge', rec.get('ev_per_unit', 'Unknown Edge'))
            conf = rec.get('confidence', 'Unknown')
            quant_edges[ev_id] = f"Selection: {market_str} | Model EV: {ev_disp} | Confidence: {conf}"
        else:
            quant_edges[ev_id] = "No explicit quantitative recommendation found."
        
    print(f"Total events constructed: {len(events)}")
    if not events:
        print("No valid events constructed.")
        return
        
    # 3. Check for recently analyzed games (Smart Idempotency)
    # Skip if analyzed within 120 minutes AND the line hasn't changed.
    print("Checking recently analyzed games...")
    with get_db_connection() as conn:
        recent_map = get_recently_analyzed(conn, [ev.event_id for ev in events])

    active_events = []
    line_map = {} # Store current line per event_id for comparison
    
    for pick in picks:
        eid = pick['event_id']
        rec = pick['rec_json']
        if isinstance(rec, str): rec = json.loads(rec)
        if isinstance(rec, list) and len(rec) > 0: rec = rec[0]
        current_line = rec.get('market_line', rec.get('line')) if isinstance(rec, dict) else None
        line_map[eid] = current_line

    for ev in events:
        eid = ev.event_id
        current_line = line_map.get(eid)
        
        recent_info = recent_map.get(eid)
        if not recent_info:
            print(f"Game {eid} has no recent analysis. Adding to active.")
            active_events.append(ev)
            continue
            
        last_time, last_line = recent_info
        # last_time is likely a datetime object from psycopg2
        if last_time.tzinfo is None:
            last_time = last_time.replace(tzinfo=timezone.utc)
            
        age_mins = (dt.now(timezone.utc) - last_time).total_seconds() / 60
        
        # Criteria for re-running:
        # 1. Age > 120 minutes
        # 2. OR Market Line has shifted (comparing as strings/floats)
        line_moved = False
        if current_line is not None and last_line is not None:
            try:
                if abs(float(current_line) - float(last_line)) >= 0.1:
                    line_moved = True
            except:
                if str(current_line) != str(last_line):
                    line_moved = True
        elif current_line != last_line:
            line_moved = True

        if age_mins > 120 or line_moved:
            print(f"Game {eid} needs re-analysis (age: {int(age_mins)}m, line_moved: {line_moved}).")
            active_events.append(ev)
        else:
            print(f"Skipping {eid} (analyzed {int(age_mins)}m ago, line {last_line} -> {current_line})")

    print(f"Active events to run: {len(active_events)}")
    if not active_events:
        print("All candidate games were recently analyzed. Nothing to do.")
        return

    # 4. Run the Council Agents
    print("Initializing Council Agents...")
    research_agent = ResearchAgent()
    memory_agent = MemoryAgent()
    oracle_agent = OracleAgent()
    journal_agent = JournalAgent()
    
    # Run in batches of 30 (Full slate usually falls into 1-2 calls max)
    batch_size = 30
    for i in range(0, len(active_events), batch_size):
        if i > 0:
            print(f"Waiting 45s for rate limit reset between batches...")
            time.sleep(45)
            
        batch_events = active_events[i:i+batch_size]
        batch_edges = {ev.event_id: quant_edges[ev.event_id] for ev in batch_events}
        
        print(f"Running Council for batch {i//batch_size + 1} ({len(batch_events)} games)...")
        
        try:
            research_out, _ = research_agent.run({"events": batch_events})
            memory_out, _ = memory_agent.run({"events": batch_events})
            
            oracle_out, _ = oracle_agent.run({
                "events": batch_events,
                "edges": batch_edges,
                "research": research_out,
                "memories": memory_out
            })
            
            if not oracle_out:
                print("Failed to get Oracle output for this batch.")
                continue
                
            # 4. Persist to decision_runs via JournalAgent
            run_id = "DR-COUNCIL-" + dt.now(timezone.utc).strftime('%Y%m%d%H%M%S') + f"-B{i}"
            
            # Aggregate Traces
            all_traces = []
            all_traces.extend(research_agent.get_traces())
            all_traces.extend(memory_agent.get_traces())
            all_traces.extend(oracle_agent.get_traces())

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
                council_narrative=oracle_out,
                agent_traces=all_traces
            )
            
            journal_agent.run({"decision_run": decision_run, "action": "persist"})

            # Persist structured council signals separately
            with get_db_connection() as conn:
                for ev in batch_events:
                    ev_id = ev.event_id
                    nar = (oracle_out or {}).get(ev_id) or {}
                    signals = nar.get('signals') if isinstance(nar, dict) else None
                    if not signals:
                        continue
                    sources = None
                    try:
                        sources = signals.get('sources') if isinstance(signals, dict) else None
                    except Exception:
                        sources = None
                    _exec(conn, """
                        INSERT INTO council_signals (run_id, event_id, league, signals_json, sources)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (run_id, event_id) DO UPDATE SET
                          signals_json = EXCLUDED.signals_json,
                          sources = EXCLUDED.sources,
                          created_at = NOW()
                    """, (
                        run_id, ev_id, 'NCAAM',
                        json.dumps(signals),
                        json.dumps(sources) if sources is not None else None,
                    ))
                conn.commit()
            print(f"Persisted council debate for {len(batch_events)} games.")
            
        except RuntimeError as re:
            if "QUOTA_EXHAUSTED" in str(re):
                print(f"CRITICAL: Quota exhausted. Aborting run. Details: {re}")
                # Persist a marker run so the UI knows why debates are missing
                try:
                    fail_run = DecisionRun(
                        run_id="DR-FAIL-" + dt.now(timezone.utc).strftime('%Y%m%d%H%M%S'),
                        created_at=dt.now(timezone.utc).isoformat(),
                        league="NCAAM",
                        status="RATE_LIMITED",
                        inputs_hash=f"fail_{date_et}_{i}",
                        offers_count=len(batch_events),
                        recommendations=[],
                        rejected_offers=[],
                        notes=[f"Quota exhausted during batch {i//batch_size + 1}"],
                        errors=[],
                        model_version="2.1.2-fail",
                        council_narrative={ev.event_id: {"oracle_verdict": "Rate Limit Exceeded. Check API Quotas."} for ev in batch_events}
                    )
                    journal_agent.run({"decision_run": fail_run, "action": "persist"})
                except:
                    pass
                break
            print(f"Batch failed with RuntimeError: {re}")
        except Exception as e:
            print(f"Batch failed with unexpected error: {e}")
            import traceback
            traceback.print_exc()
        
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
