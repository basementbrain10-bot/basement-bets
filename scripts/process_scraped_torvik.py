
import sys
import os
import json
import re
from datetime import datetime

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.database import upsert_bt_daily_schedule

def parse_line(line_text, home_team, away_team):
    # Example: "Michigan -1.9 73-71 (58%)"
    # Example: "Saint Louis -13.8 82-68 (89%)"
    # Format: [FavoredTeam] [Spread] [ScoreFav]-[ScoreDog] (...)
    
    # Extract numbers
    # -1.9, 73, 71
    # Note: Using regex to find the spread (neg float) and scores (ints)
    
    try:
        # Regex for Spread: " -1.9" or " -13.8"
        # Look for negative number after team name?
        # Or just split by space?
        # Teams can have spaces (e.g. "Saint Louis").
        
        # Strategy: Valid Torvik lines in this view always seem to list the FAVORED team first with a negative spread.
        # "Michigan -1.9" -> Michigan is favored by 1.9.
        # "Saint Louis -13.8" -> SLU favored by 13.8.
        
        # 1. Find the spread (negative number)
        spread_match = re.search(r' (-[\d\.]+)', line_text)
        if not spread_match:
            return 0.0, 0.0, 0, 0
            
        spread_val = float(spread_match.group(1)) # -1.9
        
        # 2. Identify who is favored (String before the spread)
        # "Michigan -1.9" -> "Michigan"
        favored_team_text = line_text[:spread_match.start()].strip()
        
        # 3. Scores
        # Look for "73-71"
        score_match = re.search(r' (\d+)-(\d+) ', line_text)
        if not score_match:
             # Try end of string logic if (58%) isn't there?
             pass
        
        score_fav = float(score_match.group(1))
        score_dog = float(score_match.group(2))
        
        total = score_fav + score_dog
        
        # 4. Perspective: We need home_spread (Spread for Home Team)
        # If Home Team is Favored: home_spread = spread_val (e.g. -13.8)
        # If Away Team is Favored: home_spread = -spread_val (e.g. +1.9)
        
        # We need to match `favored_team_text` to `home_team` or `away_team`.
        # Simple string inclusion or fuzzy?
        # The scraper returns names from the table. Usually reliable.
        
        is_home_favored = False
        
        # 4a. Exact Match (Prioritize this!)
        if favored_team_text == home_team:
             is_home_favored = True
        elif favored_team_text == away_team:
             is_home_favored = False
             
        # 4b. Substring Match (Secondary)
        elif favored_team_text in home_team and favored_team_text not in away_team:
            is_home_favored = True
        elif favored_team_text in away_team and favored_team_text not in home_team:
            is_home_favored = False
            
        # 4c. Ambiguous Substring (e.g. "Michigan" in "Michigan" AND "Michigan St.")
        # If it's in BOTH, picking the SHORTER one usually is correct? 
        # No, strict equality handled above.
        # If we are here, it means it's NOT exact match for either.
        # e.g. "Mich" vs "Michigan" and "Michigan St."
        
        else:
             # Fallback: Assume if it starts with...
             if home_team.startswith(favored_team_text):
                 is_home_favored = True
             elif away_team.startswith(favored_team_text):
                 is_home_favored = False


        if is_home_favored:
            home_spread = spread_val
        else:
            home_spread = -spread_val
            
        return home_spread, total, score_fav, score_dog
            
    except Exception as e:
        print(f"Parse Error '{line_text}': {e}")
        return 0.0, 0.0, 0, 0

def parse_row(row_str):
    # Example: "01:00 PM | 28 Texas A&M at 34 Georgia SEC Network | Georgia -2.7 90-87 (59%) | 91 |"
    parts = [p.strip() for p in row_str.split('|')]
    if len(parts) < 3:
        return None, None, None
        
    matchup_text = parts[1] # "28 Texas A&M at 34 Georgia SEC Network"
    line_text = parts[2]    # "Georgia -2.7 90-87 (59%)"
    
    # Parse Home/Away
    # Split by " at "
    if " at " in matchup_text:
        away_part, home_part = matchup_text.split(" at ", 1)
    else:
        return None, None, None
        
    # Clean Team Names (Remove leading ranks like "28 ")
    # Regex: Remove leading digits and space
    away = re.sub(r'^\d+\s+', '', away_part).strip()
    
    # Home part has rank AND network often: "34 Georgia SEC Network"
    # Logic: Remove leading rank first.
    home = re.sub(r'^\d+\s+', '', home_part).strip()
    
    # Network cleanup is hard because networks vary wildly.
    # But parse_line logic fuzzy matches "Favored Team" inside "Home Team".
    # Favored: "Georgia"
    # Home: "Georgia SEC Network"
    # "Georgia" in "Georgia SEC Network" -> True.
    # So we can leave the network junk in 'home' for now, usually safe.
    
    return away, home, line_text

def main():
    date_str = "20260131"
    file_path = "torvik_scraped.json"
    
    print(f"Reading {file_path}...")
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    with open(file_path, 'r') as f:
        scraped = json.load(f)
        
    payload = []
    
    for item in scraped:
        # Handle string input
        if isinstance(item, str):
            away, home, line = parse_row(item)
            if not away: continue
        else:
            # Fallback for dict (old format)
            away = item.get('away')
            home = item.get('home')
            line = item.get('line')
            
        h_spread, total, s_fav, s_dog = parse_line(line, home, away)
        
        # Calculate implied scores
        if h_spread < 0:
            h_score = s_fav
            a_score = s_dog
        else:
            h_score = s_dog
            a_score = s_fav
            
        payload.append({
            "date": date_str,
            "away": away,
            "home": home,
            "home_spread": h_spread,
            "total": total,
            "home_score": h_score,
            "away_score": a_score,
            "line_text": line
        })
        
    print(f"Transformed {len(payload)} items.")
    if len(payload) > 0:
        print(f"Sample: {payload[0]}")
        upsert_bt_daily_schedule(payload, date_str)
        print("Persisted to DB.")

if __name__ == "__main__":
    main()
