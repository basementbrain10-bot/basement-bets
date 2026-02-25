#!/usr/bin/env python3
"""Open DraftKings in a persistent Chrome profile and keep it open for manual login.

Usage:
  DK_PROFILE_PATH=./chrome_profile python scripts/dk_login_helper.py

This is intentionally dumb: it just opens the browser and waits forever.
Once you're logged in, close the terminal (or Ctrl+C) but keep the profile.
Then run the worker/ingest.
"""

import os
import sys
import time

# Allow running from repo root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.scrapers.user_driver import UserDriver


def main():
    profile = os.environ.get("DK_PROFILE_PATH") or "./chrome_profile"
    url = os.environ.get("DK_LOGIN_URL") or "https://sportsbook.draftkings.com/mybets"

    d = UserDriver()
    driver = d.launch(profile_path=profile)
    driver.get(url)

    print("\n[dk_login_helper] Browser opened.")
    print(f"- Profile: {profile}")
    print(f"- URL: {url}")
    print("\nLog into DraftKings in that window (2FA, location, etc.).")
    print("Leave this terminal running while you log in. Ctrl+C to exit when done.\n")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n[dk_login_helper] Exiting (closing browser). Profile is preserved on disk.")
    finally:
        d.close()


if __name__ == "__main__":
    main()
