#!/usr/bin/env python3
"""
GitHub Actions Torvik Scraper

Standalone script for GitHub Actions to scrape BartTorvik data.
Uses Selenium (available in GitHub Actions Linux environment).
Writes directly to Postgres bt_team_metrics_daily table.

Usage:
  python scripts/github_scrape_torvik.py
  
Environment:
  DATABASE_URL - Postgres connection string (set in GitHub Secrets)
"""
import os
import sys
import json
import psycopg2
import psycopg2.extras
from datetime import datetime
from contextlib import contextmanager

# Attempt Selenium import (available in GitHub Actions)
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False
    print("[WARNING] Selenium not available, using requests fallback")

import requests

DATABASE_URL = os.environ.get('DATABASE_URL')
TORVIK_URL = "https://barttorvik.com/trank.php"

@contextmanager
def get_db_connection():
    """Get database connection."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.DictCursor)
        yield conn
    finally:
        if conn:
            conn.close()


def fetch_with_selenium():
    """Fetch Torvik data using Selenium (full browser)."""
    print("[SELENIUM] Starting Chrome...")
    
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        # Fetch team rankings page
        year = datetime.now().year if datetime.now().month >= 11 else datetime.now().year
        url = f"{TORVIK_URL}?year={year}&json=1"
        
        print(f"[SELENIUM] Fetching {url}")
        driver.get(url)
        
        # Wait for content
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "pre"))
        )
        
        # Get JSON content
        pre = driver.find_element(By.TAG_NAME, "pre")
        content = pre.text
        
        driver.quit()
        
        return json.loads(content)
        
    except Exception as e:
        print(f"[SELENIUM] Error: {e}")
        if 'driver' in locals():
            driver.quit()
        return None


def fetch_with_requests():
    """Fallback: try simple requests (may be blocked)."""
    print("[REQUESTS] Fetching Torvik data...")
    
    year = datetime.now().year if datetime.now().month >= 11 else datetime.now().year
    url = f"https://barttorvik.com/trank.php?year={year}&json=1"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }
    
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200 and resp.text.strip().startswith('['):
            return resp.json()
    except Exception as e:
        print(f"[REQUESTS] Error: {e}")
    
    return None


def save_to_database(teams: list):
    """Insert/update team metrics in bt_team_metrics_daily."""
    if not teams:
        print("[DB] No data to save")
        return 0
    
    today = datetime.now().strftime('%Y%m%d')
    
    # Map Torvik fields to our table
    # Torvik JSON format: [rank, team, conf, record, adj_oe, adj_de, ...]
    # Actual format varies - inspect and adapt
    
    query = """
    INSERT INTO bt_team_metrics_daily 
        (team_text, date, adj_o, adj_d, adj_t, luck, sos, created_at)
    VALUES 
        (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (team_text, date) DO UPDATE SET
        adj_o = EXCLUDED.adj_o,
        adj_d = EXCLUDED.adj_d,
        adj_t = EXCLUDED.adj_t,
        luck = EXCLUDED.luck,
        sos = EXCLUDED.sos
    """
    
    count = 0
    with get_db_connection() as conn:
        cur = conn.cursor()
        
        for team in teams:
            try:
                # Adapt based on actual Torvik JSON structure
                # This is a common format from trank.php
                if isinstance(team, list):
                    # Array format: [rank, name, conf, record, adjO, adjD, adjT, luck, sos, ...]
                    team_name = team[1] if len(team) > 1 else None
                    adj_o = team[4] if len(team) > 4 else None
                    adj_d = team[5] if len(team) > 5 else None
                    adj_t = team[6] if len(team) > 6 else None
                    luck = team[7] if len(team) > 7 else None
                    sos = team[8] if len(team) > 8 else None
                elif isinstance(team, dict):
                    # Object format
                    team_name = team.get('team')
                    adj_o = team.get('adj_o') or team.get('adjoe')
                    adj_d = team.get('adj_d') or team.get('adjde')
                    adj_t = team.get('adj_t') or team.get('tempo')
                    luck = team.get('luck')
                    sos = team.get('sos')
                else:
                    continue
                
                if not team_name:
                    continue
                
                cur.execute(query, (
                    team_name,
                    today,
                    float(adj_o) if adj_o else None,
                    float(adj_d) if adj_d else None,
                    float(adj_t) if adj_t else None,
                    float(luck) if luck else None,
                    float(sos) if sos else None,
                    datetime.now()
                ))
                count += 1
                
            except Exception as e:
                print(f"[DB] Error saving {team}: {e}")
                continue
        
        conn.commit()
    
    return count


def main():
    print("=" * 50)
    print("GitHub Actions Torvik Scraper")
    print(f"Time: {datetime.now().isoformat()}")
    print("=" * 50)
    
    if not DATABASE_URL:
        print("[ERROR] DATABASE_URL not set")
        sys.exit(1)
    
    # Try Selenium first, then fallback
    teams = None
    
    if HAS_SELENIUM:
        teams = fetch_with_selenium()
    
    if not teams:
        teams = fetch_with_requests()
    
    if not teams:
        print("[ERROR] Failed to fetch data from Torvik")
        sys.exit(1)
    
    print(f"[DATA] Fetched {len(teams)} teams")
    
    # Save to database
    saved = save_to_database(teams)
    print(f"[DB] Saved {saved} team records")
    
    print("=" * 50)
    print("✅ Complete!")


if __name__ == "__main__":
    main()
