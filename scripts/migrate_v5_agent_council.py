import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.database import get_admin_db_connection

def migrate():
    print("Running Multi-Agent Phase 2 Migration (Council Narrative + RAG)...")
    
    schema = """
    -- Add memory embeddings table
    CREATE TABLE IF NOT EXISTS agent_memories (
        id SERIAL PRIMARY KEY,
        team_a TEXT NOT NULL,
        team_b TEXT NOT NULL,
        context TEXT,
        lesson TEXT NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        embedding_json TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_agent_memories_teams ON agent_memories(team_a, team_b);

    -- Add council_narrative JSON col to decision_runs
    ALTER TABLE decision_runs ADD COLUMN IF NOT EXISTS council_narrative JSONB;
    """
    
    with get_admin_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(schema)
        conn.commit()
    print("Migration successful: Added Council narrative column and Agent Memories table.")

if __name__ == '__main__':
    migrate()
