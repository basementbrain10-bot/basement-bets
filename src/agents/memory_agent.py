import os
import json
import json
import math
from typing import Any, Dict, List, Optional
import google.generativeai as genai

from src.agents.base import BaseAgent
from src.agents.contracts import EventContext
from src.database import get_db_connection, _exec

class MemoryAgent(BaseAgent):
    """
    RAG (Retrieval-Augmented Generation) Agent for historical post-mortem retrieval.
    Queries the 'agent_memories' table via cosine similarity on embeddings.
    """
    def __init__(self):
        genai.configure(api_key=os.environ.get('GEMINI_API_KEY'))

    def execute(self, context: Dict[str, Any], *args, **kwargs) -> Dict[str, Any]:
        """
        Expects a list of events. Returns a dict mapping event_id -> list of lessons learned.
        """
        events: List[EventContext] = context.get("events", [])
        if not events:
            return {}

        retrieved_memories = {}
        
        # For serverless environments without pgvector, we load memories and compute cosine similarity in python
        # Since memories are sparse (only games we bet on), this is extremely fast.
        all_memories = self._load_all_memories()
        
        if not all_memories:
            for ev in events:
                retrieved_memories[ev.event_id] = []
            return retrieved_memories

        # No need for matrix creation in pure python
        # We will iterate through all_memories directly
        
        for ev in events:
            query = f"Lessons learned betting on {ev.away_team} vs {ev.home_team} tight matchups"
            try:
                # 1. Embed the target query
                res = genai.embed_content(
                    model="models/gemini-embedding-001",
                    content=query,
                    task_type="RETRIEVAL_QUERY"
                )
                # We can use the raw list directly for pure python dot product
                target_emb = res['embedding']
                
                # 2. Compute cosine similarities (dot product)
                # Embeddings are normalized by Gemini, so dot product = cosine similarity
                scored_memories = []
                for mem in all_memories:
                    emb = mem['embedding']
                    # Pure python dot product
                    dot_product = sum(a * b for a, b in zip(emb, target_emb))
                    scored_memories.append((dot_product, mem))
                
                # 3. Sort by similarity descending
                scored_memories.sort(key=lambda x: x[0], reverse=True)
                
                relevant_lessons = []
                for similarity, mem in scored_memories[:3]:
                    if similarity > 0.4:
                        relevant_lessons.append({
                            "similarity": round(float(similarity), 3),
                            "teams": f"{mem['team_a']} vs {mem['team_b']}",
                            "lesson": mem['lesson'],
                            "date": mem['timestamp']
                        })
                        
                retrieved_memories[ev.event_id] = relevant_lessons

            except Exception as e:
                print(f"[MemoryAgent] Error retrieving memories for {ev.event_id}: {e}")
                retrieved_memories[ev.event_id] = []

        return retrieved_memories
        
    def _load_all_memories(self) -> List[Dict]:
        """Loads all memories from DB."""
        try:
            with get_db_connection() as conn:
                # Ensure the table exists
                cur = _exec(conn, "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'agent_memories')")
                exists = cur.fetchone()[0]
                if not exists:
                    return []
                    
                rows = _exec(conn, "SELECT team_a, team_b, lesson, timestamp, embedding_json FROM agent_memories").fetchall()
                memories = []
                for r in rows:
                    if r['embedding_json']:
                        memories.append({
                            'team_a': r['team_a'],
                            'team_b': r['team_b'],
                            'lesson': r['lesson'],
                            'timestamp': str(r['timestamp']),
                            'embedding': json.loads(r['embedding_json'])
                        })
                return memories
        except Exception as e:
            print(f"[MemoryAgent] Could not load memories: {e}")
            return []
