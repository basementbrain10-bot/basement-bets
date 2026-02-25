
import os
import json
from src.database import get_db_connection, _exec

def inspect_memories():
    with get_db_connection() as conn:
        query = "SELECT team_a, team_b, lesson, context, timestamp FROM agent_memories WHERE team_a LIKE '%%Purdue%%' OR team_b LIKE '%%Indiana%%' ORDER BY timestamp DESC LIMIT 5"
        rows = _exec(conn, query).fetchall()
        
        print(f"Found {len(rows)} memories:\n")
        for r in rows:
            print(f"Matchup: {r['team_a']} vs {r['team_b']}")
            print(f"Lesson: {r['lesson']}")
            print(f"Context: {r['context']}")
            print("-" * 40)

if __name__ == "__main__":
    inspect_memories()
