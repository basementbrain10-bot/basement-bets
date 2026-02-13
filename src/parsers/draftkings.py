import re
from typing import List, Dict

class DraftKingsParser:
    def __init__(self):
        pass

    def parse_text_dump(self, text: str) -> List[Dict]:
        """
        Parses the 'Card View' copy-paste dump from DraftKings.
        Returns a list of dicts ready for database insertion.
        """
        # Normalize newlines
        text = text.replace('\r\n', '\n')
        
        # Split by the DK ID at the end of blocks (DK + 15+ digits)
        blocks = re.split(r"DK\d{15,}", text)
        
        parsed_bets = []
        for block in blocks:
            if not block.strip():
                continue
            if "Wager:" in block:
                parsed_bets.append(self._parse_single_bet(block.strip()))
                
        return parsed_bets

    def _parse_single_bet(self, raw_text: str) -> Dict:
        bet = {
            "provider": "DraftKings",
            "raw_text": raw_text,
            "description": raw_text.split('\n')[0][:50], # Brief desc
            "date": "Unknown",
            "wager": 0.0,
            "profit": 0.0,
            "status": "UNKNOWN",
            "sport": "Unknown",
            "bet_type": "Unknown",
            "selection": "Unknown",
            "odds": None,
            "is_live": False,
            "is_bonus": False
        }

        lines = [l.strip() for l in raw_text.split('\n') if l.strip()]
        first_line = lines[0] if lines else ""

        # --- New Parsers ---
        
        # Selection: Usually the 2nd line
        if len(lines) > 1:
            bet["selection"] = lines[1]
            
        # Odds: Look for + or - digits at end of first line
        # e.g. "SGP2 PICKS+232+278" -> +278
        # e.g. "OHIO STATE-140" -> -140
        odds_match = re.search(r"([+-]\d+)$", first_line)
        if odds_match:
            try:
                bet["odds"] = int(odds_match.group(1))
            except:
                pass
        
        # Live Bet Check
        if "Live" in raw_text:
            bet["is_live"] = True
            
        # Bonus/Boost Check
        lower_text = raw_text.lower()
        if any(x in lower_text for x in ["bonus", "boost", "no sweat", "free bet", "profit boost"]):
            bet["is_bonus"] = True

        # --- End New Parsers ---

        # 1. Financials
        wager_match = re.search(r"Wager:\s*\$([\d\.]+)", raw_text)
        if wager_match:
            bet["wager"] = float(wager_match.group(1))
            
        payout = 0.0
        paid_match = re.search(r"Paid:\s*\$([\d\.]+)", raw_text)
        if paid_match:
            payout = float(paid_match.group(1))
        
        # 2. Status
        if "WON" in raw_text.split('\n'):
            bet["status"] = "WON"
        elif "LOST" in raw_text.split('\n'):
            bet["status"] = "LOST"
        elif "CASHED OUT" in raw_text:
            bet["status"] = "CASHED_OUT"
        
        bet["profit"] = payout - bet["wager"]

        # 3. Date
        date_match = re.search(r"([A-Z][a-z]{2} \d{1,2}, \d{4})", raw_text)
        if date_match:
             try:
                 from datetime import datetime
                 dt = datetime.strptime(date_match.group(1), "%b %d, %Y")
                 bet["date"] = dt.strftime("%Y-%m-%d")
             except:
                 bet["date"] = date_match.group(1)

        # 4. Bet Type
        
        if "SGP" in first_line:
            bet["bet_type"] = "Same Game Parlay"
        elif "PARLAY" in first_line:
            bet["bet_type"] = "Parlay"
        elif "Moneyline" in raw_text:
            bet["bet_type"] = "Moneyline"
        elif "Over" in first_line or "Under" in first_line or "Total" in raw_text:
            bet["bet_type"] = "Total (Over/Under)"
        elif "Anytime TD" in raw_text:
            bet["bet_type"] = "Prop (Player)"
        elif "Runs -" in raw_text:
            bet["bet_type"] = "Prop (Game)"
        else:
            bet["bet_type"] = "Straight/Other"

        # 5. Sport Inference
        # Use shared detector (more consistent + handles soccer/EPL)
        try:
            from src.parsers.sport_detection import detect_sport
            bet["sport"] = detect_sport(raw_text + " " + bet.get("selection", "") + " " + bet.get("description", ""))
        except Exception:
            # Fallback to unknown if detector not available
            bet["sport"] = bet.get("sport") or "Unknown"

        return bet
