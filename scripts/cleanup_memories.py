from src.database import get_db_connection, _exec

def cleanup():
    with get_db_connection() as conn:
        _exec(conn, "DELETE FROM agent_memories WHERE team_a = 'Purdue' AND team_b = 'Indiana'")
        conn.commit()
    print("Cleaned up Purdue/Indiana memories.")

if __name__ == "__main__":
    cleanup()
