import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List
import google.generativeai as genai
from src.agents.base import BaseAgent
from src.database import get_db_connection, _exec

class PostMortemAgent(BaseAgent):
    """
    Evaluates completed games where the Oracle made a prediction.
    Generates a 'lesson learned' and saves it to the agent_memories table.
    """
    def __init__(self):
        genai.configure(api_key=os.environ.get('GEMINI_API_KEY'))
        self.model = genai.GenerativeModel('gemini-2.5-flash')

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

                # 1. Generate Lesson using LLM
                prompt = f"""
                You are a sports betting analyst reviewing a past game.
                Matchup: {team_a} vs {team_b}
                Oracle Prediction: {prediction}
                Actual Outcome: {actual_result}
                
                Write a concise, 2-3 sentence 'lesson learned' from this outcome. 
                Focus on betting logic, team archetypes, or why the prediction succeeded or failed.
                """
                
                try:
                    response = self.model.generate_content(
                        prompt,
                        generation_config=genai.GenerationConfig(max_output_tokens=150)
                    )
                    lesson = response.text.strip()
                    
                    # 2. Get Embedding for the lesson
                    embed_res = genai.embed_content(
                        model="models/gemini-embedding-001",
                        title="Reflection",
                        task_type="RETRIEVAL_DOCUMENT",
                        content=lesson
                    )
                    embedding = embed_res['embedding']
                    
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
