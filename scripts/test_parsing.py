
from datetime import datetime, timezone

def test():
    s = "2025-11-20T23:00:00.000Z"
    print(f"Testing string: {s}")
    try:
        dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
        print(f"Parsed: {dt}")
        print(f"Type: {type(dt)}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test()
