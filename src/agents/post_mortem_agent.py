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

                # 1. Ask Gemini to reflect
                reflection_payload = f"Matchup: {team_a} vs {team_b}, Oracle Prediction: {prediction}, Actual Outcome: {actual_result}"
                full_prompt = f"Analyze this sports betting prediction vs exactly what happened. What went right or wrong? Keep it to 2 sentences.\n\n{reflection_payload}"
                
                try:
                    response_text = generate_content(
                        model="gemini-2.5-flash",
                        system_prompt=full_prompt,
                        max_tokens=250
                    )
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
                        datetime.now(timezone.utc), json.dumps(embedding)
                    ))
                    conn.commit()
                    memories_added += 1
                except Exception as e:
                    print(f"[PostMortemAgent] Failed to process {team_a} vs {team_b}: {e}")

        return {"status": "success", "memories_generated": memories_added}
