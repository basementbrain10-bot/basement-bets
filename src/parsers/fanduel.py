import re
from datetime import datetime

class FanDuelParser:
    def parse(self, raw_text):
        """
        Parses raw text from FanDuel 'Card View' copy-paste.
        Returns a list of bet dictionaries.
        """
        bets = []
        # Split into blocks based on "BET ID:" which is a reliable delimiter at the bottom of each card
        # Strategy: The "BET ID" line is near the end. We validly assume one file contains multiple bets.
        # We can regex split.
        
        # Normalize text
        raw_text = raw_text.strip()
        
        # Split by BET ID lines to separate bets approximately
        # Actually, "BET ID" is at the bottom. The text before it belongs to that bet.
        # Let's split by regex that captures the BET ID line, then iterate.
        
        # Pattern: BET ID: O/xxxx/xxxx
        # We find all end-markers, then slice the text? 
        # Or just split by "BET ID:" and reconstruct?
        
        # Let's try splitting by the PLACED date line, which usually follows BET ID.
        # Actually, let's look at the structure:
        # [Bet Details]
        # [Wager/Return]
        # BET ID: ...
        # PLACED: ...
        
        # We can split by "BET ID: O/" regex.
        chunks = re.split(r'(BET ID: O/\S+)', raw_text)
        
        # chunks[0] is text before first bet id (the body of first bet)
        # chunks[1] is the first bet id
        # chunks[2] is text after (includes PLACED line of first bet, then body of second bet)
        
        # We need to recombine carefully.
        # Actually, "PLACED: ..." follows "BET ID: ..." immediately.
        # So a bet block ends with "PLACED: ... ET".
        
        # Let's try a block-based approach using regex finditer.
        # We look for the "footer" of a bet and grab everything before it up to the previous footer.
        
        # But simpler: Split by "BET ID:".
        # segment `i` contains the body of bet `i`.
        # segment `i+1` starts with the ID, then PLACED, then body of bet `i+1`.
        
        # Let's clean this up. A bet ends essentially after the "PLACED: <date>" line.
        # Let's just iterate line by line to build blocks.
        
        lines = [l.strip() for l in raw_text.split('\n') if l.strip()]
        current_block = []
        parsed_bets = []
        
        for i, line in enumerate(lines):
            current_block.append(line)
            if line.startswith("PLACED:"):
                # End of a bet block
                bet_data = self._parse_single_bet(current_block)
                if bet_data:
                    parsed_bets.append(bet_data)
                current_block = []
                
        return parsed_bets

    def _parse_single_bet(self, block):
        # Join for regex searching
        full_text = "\n".join(block)
        
        # 1. Date
        # Line: PLACED: 1/11/2026 4:28PM ET
        date_match = re.search(r'PLACED:\s+(\d{1,2}/\d{1,2}/\d{4}.*?ET)', full_text)
        if not date_match:
            return None
        date_str = date_match.group(1).replace("ET", "").strip()
        # Parse date buffer
        try:
            # 1/11/2026 4:28PM
            dt = datetime.strptime(date_str, "%m/%d/%Y %I:%M%p")
            formatted_date = dt.strftime("%Y-%m-%d")
        except:
            formatted_date = date_str

        # 2. Wager and Return
        wager = 0.0
        return_amount = 0.0
        status = "LOST" # Default
        
        # Find "TOTAL WAGER" and look at line before
        for i, line in enumerate(block):
            if "TOTAL WAGER" in line:
                wager_line = block[i-1]
                wager = float(wager_line.replace('$', '').replace(',', ''))
            
            if "WON ON FANDUEL" in line or "RETURNED" in line:
                ret_line = block[i-1]
                return_amount = float(ret_line.replace('$', '').replace(',', ''))
        
        profit = return_amount - wager
        
        # Status Logic
        if return_amount > 0.01:
             if abs(profit) < 0.01:
                 status = "PUSH"
             elif profit > 0:
                 status = "WON"
             else:
                 status = "LOST"
        else:
             status = "LOST"

        # 3. Bet Type — scan ALL lines for keywords (not just block[0])
        bet_type = None
        bet_type_keywords = {
            "SPREAD BETTING": "Spread", "SPREAD": "Spread", "POINT SPREAD": "Spread",
            "MONEYLINE": "ML", "MONEY LINE": "ML",
            "TOTAL POINTS": "Over/Under", "OVER/UNDER": "Over/Under",
            "OVER / UNDER": "Over/Under",
            "PROP": "Prop", "PLAYER PROP": "Prop",
            "PARLAY": None,  # handled separately for leg count
            "ROUND ROBIN": "Round Robin",
        }
        # Also check for parlay in first line
        first_line_up = block[0].upper()
        if "PARLAY" in first_line_up or "LEG" in first_line_up:
            match = re.search(r'(\d+)', block[0])
            bet_type = f"{match.group(1)} leg parlay" if match else "parlay"
        elif "ROUND ROBIN" in first_line_up:
            bet_type = "Round Robin"

        if not bet_type:
            # Scan all lines for bet type keywords
            for line in block:
                line_up = line.strip().upper()
                for keyword, bt in bet_type_keywords.items():
                    if keyword in line_up and bt:
                        bet_type = bt
                        break
                if bet_type:
                    break

        if not bet_type:
            bet_type = "ML"  # Final fallback
        
        # Matchup Detection
        matchup = block[0] 
        for line in block:
            if "@" in line or " vs " in line.lower():
                matchup = line
                break
        
        description = matchup

        # Sport Inference — use shared detector
        from src.parsers.sport_detection import detect_sport
        text_to_scan = full_text + " " + matchup
        sport = detect_sport(text_to_scan)
        
        # 4. Odds
        odds = None
        for line in block[0:6]:
            if re.match(r'^[+-]\d+$', line):
                odds = int(line)
        
        # 5. Selection — only team name(s) + bet line. No odds, no bet type labels, no scores.
        # Noise patterns to exclude from selection
        _noise_patterns = [
            r'^[+-]\d+$',              # Odds like +13.5, -120 (standalone)
            r'^BET ID:',               # Footer
            r'^PLACED:',               # Footer
            r'TOTAL WAGER',            # Footer
            r'WON ON FANDUEL',         # Footer
            r'RETURNED',               # Footer
            r'^\$',                    # Dollar amounts
            r'^Finished$',             # Game finished marker
            r'^\d+$',                  # Bare numbers (scores)
            r'^profit boost',          # Promo label
            r'^\d+%$',                 # Boost percent
        ]
        # Bet type labels to exclude
        _bet_type_labels = [
            "SPREAD BETTING", "SPREAD", "POINT SPREAD",
            "MONEYLINE", "MONEY LINE", "ML",
            "TOTAL POINTS", "OVER/UNDER", "OVER / UNDER",
            "PROP", "PLAYER PROP",
            "PARLAY", "ROUND ROBIN",
        ]
        
        selection_parts = []
        for line in block:
            line_s = line.strip()
            if not line_s:
                continue
            line_up = line_s.upper()

            # Skip noise
            skip = False
            for p in _noise_patterns:
                if re.search(p, line_s, re.IGNORECASE):
                    skip = True
                    break
            if skip:
                continue

            # Skip bet type labels
            if line_up in _bet_type_labels or any(line_up == lbl for lbl in _bet_type_labels):
                continue

            # Skip if it's just a standalone odds value like "-120"
            if re.match(r'^[+-]\d{2,}$', line_s):
                continue

            selection_parts.append(line_s)
        
        # Build selection: team name + line (e.g. "La Salle +13.5")
        # For spreads/totals, combine team name with the spread/total line
        selection = ""
        if selection_parts:
            # Check if second part looks like a spread/total line (+13.5, -3, O 45.5, etc.)
            if len(selection_parts) >= 2 and re.match(r'^[+-]?\d+\.?\d*$', selection_parts[1]):
                # Team + line (e.g. "La Salle" + "+13.5" → "La Salle +13.5")
                selection = f"{selection_parts[0]} {selection_parts[1]}"
            else:
                # Just use first selection part (team name for ML, or full text)
                selection = selection_parts[0]

        if not selection:
            selection = matchup

        description = matchup

        # 6. Live / Bonus
        is_live = "Live" in full_text
        is_bonus = "Bonus" in full_text or "Free Bet" in full_text or "profit boost" in text_to_scan

        return {
            "provider": "FanDuel",
            "date": formatted_date,
            "sport": sport,
            "bet_type": bet_type,
            "wager": wager,
            "profit": profit,
            "status": status,
            "description": description, 
            "selection": selection,
            "odds": odds,
            "is_live": is_live,
            "is_bonus": is_bonus,
            "raw_text": full_text
        }

