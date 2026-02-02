#!/usr/bin/env python3
"""
GitHub Actions Torvik Scraper

Standalone script for GitHub Actions to scrape BartTorvik data.
Writes directly to Postgres bt_team_metrics_daily table.
"""
import os
import sys
import json
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta
from contextlib import contextmanager
import requests
import time

DATABASE_URL = None
TORVIK_SCHEDULE_URL = "https://barttorvik.com/schedule.php"

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


def fetch_schedule_data():
    """
    Fetch team data from BartTorvik schedule.php endpoint.
    This endpoint works better than trank.php for scraping.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://barttorvik.com/',
    }
    
    all_teams = {}
    
    # Fetch multiple days to get team stats
    for days_offset in range(0, 7):
        target_date = datetime.now() + timedelta(days=days_offset)
        date_str = target_date.strftime('%Y%m%d')
        url = f"{TORVIK_SCHEDULE_URL}?date={date_str}&json=1"
        
        print(f"[FETCH] Trying {url}")
        
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            
            if resp.status_code == 200:
                content = resp.text.strip()
                
                # Check if it's JSON
                if content.startswith('[') or content.startswith('{'):
                    data = json.loads(content)
                    
                    for game in data:
                        # Extract team stats from game projections
                        if isinstance(game, dict):
                            # Home team
                            home = game.get('home', '')
                            if home and home not in all_teams:
                                all_teams[home] = {
                                    'team': home,
                                    'adj_o': game.get('home_adjoe'),
                                    'adj_d': game.get('home_adjde'),
                                    'adj_t': game.get('home_tempo'),
                                }
                            
                            # Away team
                            away = game.get('away', '')
                            if away and away not in all_teams:
                                all_teams[away] = {
                                    'team': away,
                                    'adj_o': game.get('away_adjoe'),
                                    'adj_d': game.get('away_adjde'),
                                    'adj_t': game.get('away_tempo'),
                                }
                    
                    print(f"[FETCH] Found {len(data)} games, {len(all_teams)} unique teams so far")
                else:
                    print(f"[FETCH] Non-JSON response for {date_str}")
                    
        except Exception as e:
            print(f"[FETCH] Error for {date_str}: {e}")
        
        time.sleep(0.5)  # Be polite
    
    return list(all_teams.values())


def fetch_trank_data():
    """Fallback: Try trank.php with various user agents."""
    headers_list = [
        {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/json',
        },
        {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/537.36',
            'Accept': '*/*',
        },
    ]
    
    year = datetime.now().year
    url = f"https://barttorvik.com/trank.php?year={year}&json=1"
    
    for headers in headers_list:
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code == 200 and resp.text.strip().startswith('['):
                return resp.json()
        except:
            continue
    
    return None


def save_to_database(teams: list):
    """Insert/update team metrics in bt_team_metrics_daily."""
    if not teams:
        print("[DB] No data to save")
        return 0
    
    today = datetime.now().strftime('%Y%m%d')
    
    query = """
    INSERT INTO bt_team_metrics_daily 
        (team_text, date, adj_o, adj_d, adj_t, created_at)
    VALUES 
        (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (team_text, date) DO UPDATE SET
        adj_o = EXCLUDED.adj_o,
        adj_d = EXCLUDED.adj_d,
        adj_t = EXCLUDED.adj_t
    """
    
    count = 0
    with get_db_connection() as conn:
        cur = conn.cursor()
        
        for team in teams:
            try:
                if isinstance(team, dict):
                    team_name = team.get('team')
                    adj_o = team.get('adj_o')
                    adj_d = team.get('adj_d')
                    adj_t = team.get('adj_t')
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
                    datetime.now()
                ))
                count += 1
                
            except Exception as e:
                print(f"[DB] Error saving {team}: {e}")
                continue
        
        conn.commit()
    
    return count


def main():
    global DATABASE_URL
    
    print("=" * 50)
    print("GitHub Actions Torvik Scraper")
    print(f"Time: {datetime.now().isoformat()}")
    print("=" * 50)
    
    # Get DATABASE_URL
    DATABASE_URL = (
        os.environ.get('DATABASE_URL') or 
        os.environ.get('POSTGRES_URL') or 
        os.environ.get('DB_URL')
    )
    
    if not DATABASE_URL:
        print("[ERROR] No database URL found!")
        sys.exit(1)
    
    print(f"[OK] Database URL configured")
    
    # Try schedule.php first (more reliable)
    print("\n[STEP 1] Fetching from schedule.php...")
    teams = fetch_schedule_data()
    
    # Fallback to trank.php
    if not teams or len(teams) < 50:
        print("\n[STEP 2] Trying trank.php fallback...")
        trank_teams = fetch_trank_data()
        if trank_teams:
            # Parse trank format
            for t in trank_teams:
                if isinstance(t, list) and len(t) > 6:
                    teams.append({
                        'team': t[1],
                        'adj_o': t[4],
                        'adj_d': t[5],
                        'adj_t': t[6]
                    })
    
    if not teams:
        print("[WARNING] Could not fetch team data from Torvik (likely blocked)")
        print("[INFO] The model will use cached data from the database")
        print("\n" + "=" * 50)
        print("⚠️ Scrape skipped - Torvik blocked this IP")
        sys.exit(0)  # Exit success so workflow doesn't fail
    
    print(f"\n[DATA] Collected {len(teams)} teams")
    
    # Save to database
    saved = save_to_database(teams)
    print(f"[DB] Saved {saved} team records")
    
    print("\n" + "=" * 50)
    print("✅ Complete!")
    sys.exit(0)


if __name__ == "__main__":
    main()
