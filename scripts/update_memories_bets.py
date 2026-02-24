import os
import json
from src.database import get_db_connection, _exec

def update_memories_with_bets():
    with get_db_connection() as conn:
        # 1. Fetch all model predictions with their bet details and date
        pred_query = "SELECT event_id, market_type, selection, bet_line, analyzed_at FROM model_predictions"
        preds = _exec(conn, pred_query).fetchall()
        
        # Build lookup map: event_id -> (bet_string, date_string)
        bet_map = {}
        for p in preds:
            eid = p['event_id']
            market = p['market_type']
            side = p['selection']
            line = p['bet_line']
            date_str = p['analyzed_at'].strftime("%Y-%m-%d") if p['analyzed_at'] else "Historical"
            
            bet_str = f"{side} {market}"
            if market == 'Spread':
                bet_str = f"{side} {line}"
            elif market == 'Total':
                bet_str = f"{side} {line}"
            elif market == 'Moneyline':
                bet_str = f"{side} ML"
            
            bet_map[eid] = (bet_str, date_str)

        # 2. Fetch events to map event_id to team names
        event_query = "SELECT id, home_team, away_team FROM events"
        events = _exec(conn, event_query).fetchall()
        
        team_to_eid = {}
        for e in events:
            team_to_eid[(e['away_team'], e['home_team'])] = e['id']
            team_to_eid[(e['home_team'], e['away_team'])] = e['id']

        # 3. Fetch memories that don't have the date prepended yet
        # Heuristic: lesson doesn't contain a parenthesis after the bracket
        mem_query = "SELECT id, team_a, team_b, lesson FROM agent_memories WHERE lesson NOT LIKE '%%(%%-%%-%%)%%'"
        memories = _exec(conn, mem_query).fetchall()
        
        print(f"Found {len(memories)} memories to potentially update with dates/bets.")
        
        updated_count = 0
        for m in memories:
            key = (m['team_a'], m['team_b'])
            eid = team_to_eid.get(key)
            
            if eid and eid in bet_map:
                bet_info, date_info = bet_map[eid]
                old_lesson = m['lesson']
                
                if old_lesson.startswith('['):
                    closing_bracket = old_lesson.find(']')
                    if closing_bracket != -1:
                        status = old_lesson[:closing_bracket+1]
                        # Remove existing bet info if we were halfway through a previous update
                        # Looking for ": " separator
                        lesson_payload = old_lesson[closing_bracket+1:].strip()
                        if ": " in lesson_payload:
                            lesson_payload = lesson_payload.split(": ", 1)[1]
                            
                        new_lesson = f"{status} ({date_info}) {bet_info}: {lesson_payload}"
                        
                        update_query = "UPDATE agent_memories SET lesson = %s WHERE id = %s"
                        _exec(conn, update_query, (new_lesson, m['id']))
                        updated_count += 1
        
        conn.commit()
        print(f"Successfully updated {updated_count} memories with bet details and dates.")

if __name__ == "__main__":
    update_memories_with_bets()
