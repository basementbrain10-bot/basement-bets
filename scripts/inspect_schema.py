import os
from src.database import get_db_connection, _exec

def inspect_schema():
    with get_db_connection() as conn:
        for table in ["model_predictions", "agent_memories"]:
            print(f"\n--- Columns in {table} ---")
            query = f"SELECT * FROM {table} LIMIT 1"
            row = _exec(conn, query).fetchone()
            if row:
                print(list(row.keys()))
            else:
                print("Table is empty.")

if __name__ == "__main__":
    inspect_schema()
