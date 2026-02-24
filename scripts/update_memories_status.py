import os
import sys
import json

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database import get_db_connection, _exec

def update_status():
    print("--- Memory Status Update (Retroactive) ---")
    
    updates = [
        (5, "WON", "The prediction was technically correct as the total exceeded 138.5, but Rhode Island's defensive lapses in the second half made the cover closer than expected."),
        (4, "LOST", "The prediction for Arizona State +12.5 failed because Baylor's superior perimeter depth overwhelmed ASU in the final ten minutes of the game."),
        (3, "WON", "This prediction was entirely successful, as the bet on the OVER hit comfortably due to both teams shooting above 45% from the field."),
        (2, "LOST", "The prediction went wrong because Indiana State failed to protect the paint, allowing Belmont to record high-percentage shots late in the shot clock."),
        (1, "WON", "The Oracle successfully identified key defensive advantages for Indiana at home, proving that Assembly Hall's environment continues to be a significant factor against top-ranked opponents.")
    ]

    with get_db_connection() as conn:
        for mid, status, lesson in updates:
            print(f"Updating ID {mid} to {status}...")
            
            # Prepend status to lesson text
            display_lesson = f"[{status}] {lesson}"
            
            # Create metadata JSON for context
            metadata = {
                "result": status,
                "lesson": lesson,
                "tags": ["retroactive_fix"]
            }
            context_json = json.dumps(metadata)
            
            q = "UPDATE agent_memories SET lesson = %s, context = %s WHERE id = %s"
            _exec(conn, q, (display_lesson, context_json, mid))
            conn.commit()

    print("\nStatus update complete.")

if __name__ == '__main__':
    update_status()
