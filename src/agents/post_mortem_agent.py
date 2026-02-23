import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List
from src.utils.gemini_rest import generate_content, embed_content
from src.agents.base import BaseAgent
from src.agents.contracts import EventContext
from src.database import get_db_connection, _exec

class PostMortemAgent(BaseAgent):
    """
    Runs after games complete. Analyzes the Oracle's prediction vs reality
    and saves a 'lesson learned' back into the vector memory table.
    """
    def __init__(self):
        pass

        results_list = context.get('completed_games', []) # List of dicts with outcome details
        if not results_list:
            return {"status": "No games to analyze"}

        memories_added = 0
        with get_db_connection() as conn:
            unreviewed = {}
            unreviewed_raw = {}
            for game in results_list:
                team_a = game.get('away_team')
                team_b = game.get('home_team')
                prediction = game.get('oracle_prediction', 'Unknown')
                actual_result = game.get('actual_result', 'Unknown')
                
                # Check if we already reviewed this
                exists_query = "SELECT id FROM agent_memories WHERE team_a = %s AND team_b = %s AND DATE(timestamp) = CURRENT_DATE"
                row = _exec(conn, exists_query, (team_a, team_b)).fetchone()
                if row:
                    continue
                
                key = f"{team_a} vs {team_b}"
                unreviewed[key] = f"Oracle Prediction/Edge: {prediction}, Actual Outcome: {actual_result}"
                unreviewed_raw[key] = game

            if not unreviewed:
                return {"status": "success", "memories_generated": 0}

            full_prompt = f"""
You are the Post-Mortem Auditor for a sports betting system.
You are evaluating {len(unreviewed)} completed college basketball matchups.

Data (keyed by matchup):
{json.dumps(unreviewed, ensure_ascii=False, indent=2)}

Task:
For EACH matchup, reflect on the prediction vs actual outcome.

Return VALID JSON where keys are the exact matchup strings, and values are objects with these keys:
{{
  "matchup_key": {{
      "result": "correct|incorrect|unknown",
      "lesson": "1-2 sentences, factual; no made-up stats",
      "missed_signal": "" ,
      "followup_check": "" ,
      "tags": ["injury","market_move","tempo","matchup","variance","data_gap","other"]
  }}
}}

Rules:
- DO NOT invent game stats.
- If the outcome is ambiguous, set result=unknown.
"""

            try:
                response_text = generate_content(
                    model="gemini-2.5-flash",
                    system_prompt=full_prompt,
                    json_mode=True,
                    max_tokens=4000
                )
                clean_text = response_text.strip()
                if clean_text.startswith("```json"):
                    clean_text = clean_text[7:]
                if clean_text.endswith("```"):
                    clean_text = clean_text[:-3]

                parsed_lessons = json.loads(clean_text.strip())

                for key, lesson_obj in parsed_lessons.items():
                    if key not in unreviewed_raw:
                        continue
                    
                    team_a = unreviewed_raw[key].get('away_team')
                    team_b = unreviewed_raw[key].get('home_team')
                    
                    lesson_str = json.dumps(lesson_obj, ensure_ascii=False)
                    
                    # 2. Get Embedding for the lesson (cheap/fast)
                    embed_res = embed_content(
                        model="models/gemini-embedding-001",
                        title="Reflection",
                        task_type="RETRIEVAL_DOCUMENT",
                        content=lesson_str
                    )
                    
                    # 3. Save to memory DB
                    insert_query = """
                        INSERT INTO agent_memories (team_a, team_b, context, lesson, timestamp, embedding_json)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    _exec(conn, insert_query, (
                        team_a, team_b, "Post-game reflection", lesson_str, 
                        datetime.now(timezone.utc), json.dumps(embed_res)
                    ))
                    conn.commit()
                    memories_added += 1

            except Exception as e:
                print(f"[PostMortemAgent] Failed to process batch: {e}")

        return {"status": "success", "memories_generated": memories_added}
