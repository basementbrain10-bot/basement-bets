"""
Clean up duplicate games in the events table.
Strategy:
1. Remove test/smoke events (test_evt_, smoke_evt_)
2. For games with both ESPN and Action Network entries, keep Action Network (canonical)
3. Migrate any odds_snapshots and game_results to the canonical event before deletion
"""
from src.database import get_db_connection

def cleanup_duplicates():
    print("--- Cleaning Up Duplicate Events ---")
    
    with get_db_connection() as conn:
        cur = conn.cursor()
        
        # 1. Remove test and smoke events
        print("\n1. Removing test/smoke events...")
        cur.execute("""
        DELETE FROM odds_snapshots WHERE event_id LIKE 'test_evt_%' OR event_id LIKE 'smoke_evt_%'
        """)
        print(f"   Deleted {cur.rowcount} test odds_snapshots")
        
        cur.execute("""
        DELETE FROM game_results WHERE event_id LIKE 'test_evt_%' OR event_id LIKE 'smoke_evt_%'
        """)
        print(f"   Deleted {cur.rowcount} test game_results")
        
        cur.execute("""
        DELETE FROM model_predictions WHERE event_id LIKE 'test_evt_%' OR event_id LIKE 'smoke_evt_%'
        """)
        print(f"   Deleted {cur.rowcount} test model_predictions")
        
        cur.execute("""
        DELETE FROM events WHERE id LIKE 'test_evt_%' OR id LIKE 'smoke_evt_%'
        """)
        print(f"   Deleted {cur.rowcount} test events")
        
        # 2. Find duplicates (same teams, same date) where both ESPN and Action exist
        print("\n2. Finding ESPN/Action duplicates...")
        cur.execute("""
        SELECT 
            home_team, away_team,
            DATE(start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') as game_date,
            array_agg(id) as event_ids
        FROM events
        WHERE league = 'NCAAM'
        GROUP BY home_team, away_team, DATE(start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
        HAVING COUNT(*) > 1
        """)
        
        duplicates = cur.fetchall()
        print(f"   Found {len(duplicates)} sets of duplicates")
        
        espn_to_delete = []
        migrated = 0
        
        for row in duplicates:
            home_team, away_team, game_date, event_ids = row
            
            # Find the Action Network event (canonical)
            action_id = None
            espn_ids = []
            
            for eid in event_ids:
                if eid.startswith('action:'):
                    action_id = eid
                elif eid.startswith('espn:'):
                    espn_ids.append(eid)
            
            if action_id and espn_ids:
                # Migrate odds_snapshots from ESPN to Action (if any don't exist)
                for espn_id in espn_ids:
                    # Check if Action event already has odds
                    cur.execute("SELECT COUNT(*) FROM odds_snapshots WHERE event_id = %s", (action_id,))
                    action_has_odds = cur.fetchone()[0] > 0
                    
                    if not action_has_odds:
                        # Migrate ESPN odds to Action
                        cur.execute("""
                        UPDATE odds_snapshots SET event_id = %s WHERE event_id = %s
                        """, (action_id, espn_id))
                        if cur.rowcount > 0:
                            migrated += 1
                    
                    # Delete ESPN odds (duplicates)
                    cur.execute("DELETE FROM odds_snapshots WHERE event_id = %s", (espn_id,))
                    
                    # Delete ESPN game_results (we already have Action scores)
                    cur.execute("DELETE FROM game_results WHERE event_id = %s", (espn_id,))
                    
                    espn_to_delete.append(espn_id)
        
        # 3. Delete ESPN duplicate events (and their FK dependencies)
        print(f"\n3. Deleting {len(espn_to_delete)} ESPN duplicate events...")
        for espn_id in espn_to_delete:
            # Delete from all FK-dependent tables first
            cur.execute("DELETE FROM event_providers WHERE event_id = %s", (espn_id,))
            cur.execute("DELETE FROM model_predictions WHERE event_id = %s", (espn_id,))
            cur.execute("DELETE FROM events WHERE id = %s", (espn_id,))
        
        print(f"   Migrated {migrated} odds snapshots to Action events")
        print(f"   Deleted {len(espn_to_delete)} ESPN duplicate events")
        
        conn.commit()
        
        # 4. Verify remaining duplicates
        print("\n4. Verifying remaining duplicates...")
        cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT home_team, away_team, DATE(start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
            FROM events WHERE league = 'NCAAM'
            GROUP BY home_team, away_team, DATE(start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York')
            HAVING COUNT(*) > 1
        ) x
        """)
        remaining = cur.fetchone()[0]
        print(f"   Remaining duplicate sets: {remaining}")
        
        # 5. Count final totals
        cur.execute("SELECT COUNT(*) FROM events WHERE league = 'NCAAM'")
        total_events = cur.fetchone()[0]
        print(f"\n✓ Final NCAAM event count: {total_events}")

if __name__ == "__main__":
    cleanup_duplicates()
