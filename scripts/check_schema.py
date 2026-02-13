
import sys
import os
sys.path.append(os.getcwd())
from src.database import get_db_connection, _exec

def check():
    with get_db_connection() as conn:
        # Postgres specific
        q = """
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'transactions';
        """
        cur = _exec(conn, q)
        for row in cur.fetchall():
            print(f"{row['column_name']}: {row['data_type']}")

if __name__ == "__main__":
    check()
