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

    def execute(self, context: Dict[str, Any], *args, **kwargs) -> Dict[str, Any]:
        """
        Runs post-game analysis and embedding generation.
        """
        results_list = context.get('completed_games', []) # List of dicts with outcome details
        if not results_list:
            return {"status": "No games to analyze"}

        memories_added = 0
        with get_db_connection() as conn:
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

                # 1. Ask Gemini to reflect (structured)
                reflection_payload = f"Matchup: {team_a} vs {team_b}, Oracle Prediction/Edge: {prediction}, Actual Outcome: {actual_result}"
                full_prompt = f"""
You are the Post-Mortem Auditor for a sports betting system.

Data:
{reflection_payload}

Return VALID JSON with exactly these keys:
{{
  "result": "correct|incorrect|unknown",
  "lesson": "1-2 sentences, factual; no made-up stats",
  "missed_signal": "" ,
  "followup_check": "" ,
  "tags": ["injury","market_move","tempo","matchup","variance","data_gap","other"]
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
                        max_tokens=400
                    )
                    # Store the structured JSON as the lesson payload for retrieval.
                    lesson_obj = None
                    try:
                        lesson_obj = json.loads(response_text)
                    except Exception:
                        lesson_obj = None

                    if isinstance(lesson_obj, dict) and lesson_obj.get('lesson'):
                        lesson = json.dumps(lesson_obj, ensure_ascii=False)
                    else:
                        lesson = response_text.strip()
                    
                    # 2. Get Embedding for the lesson
                    embed_res = embed_content(
                        model="models/gemini-embedding-001",
                        title="Reflection",
                        task_type="RETRIEVAL_DOCUMENT",
                        content=lesson
                    )
                    embedding_vector = embed_res
                    
                    # 3. Save to memory DB
                    insert_query = """
                        INSERT INTO agent_memories (team_a, team_b, context, lesson, timestamp, embedding_json)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    _exec(conn, insert_query, (
                        team_a, team_b, "Post-game reflection", lesson, 
                        datetime.now(timezone.utc), json.dumps(embedding_vector)
                    ))
                    conn.commit()
                    memories_added += 1

                    # Gentle rate-limit backoff (configurable)
                    import time
                    sleep_s = float(os.getenv('POST_MORTEM_SLEEP_SECONDS', '2'))
                    if sleep_s > 0:
                        time.sleep(sleep_s)
                except Exception as e:
                    print(f"[PostMortemAgent] Failed to process {team_a} vs {team_b}: {e}")

        return {"status": "success", "memories_generated": memories_added}
