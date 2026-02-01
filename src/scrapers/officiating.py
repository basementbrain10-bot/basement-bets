"""
Officiating Assignments Scraper

Fetches referee assignments and their historical foul rates.
Used to populate referee_assignments table for total adjustment signal.

Data Sources:
- Official NCAA assignments (usually posted 1-2 hours before tip)
- StatBroadcast 
- CBBRef historical data
"""
import requests
from datetime import datetime
from typing import List, Dict, Optional
from src.database import get_db_connection, _exec

# Historical referee foul data (manually curated from CBBRef)
# Format: {referee_name: avg_fouls_per_game}
REFEREE_FOUL_RATES = {
    # High-foul crews (favor OVER)
    "John Higgins": 39.2,
    "Karl Hess": 40.1,
    "Ted Valentine": 38.8,
    "Doug Shows": 38.5,
    "Pat Adams": 39.0,
    
    # Average crews
    "Roger Ayers": 36.5,
    "Bo Boroski": 36.2,
    "Mike Eades": 35.8,
    "Tony Greene": 36.0,
    "Doug Sirmons": 36.3,
    
    # Tight crews (favor UNDER)
    "Brian O'Connell": 33.5,
    "Kipp Kissinger": 34.2,
    "Jeff Anderson": 33.8,
    "Bill Ek": 34.0,
}

NCAA_AVG_FOULS = 36.0


class OfficiatingScraper:
    """
    Scrapes and manages officiating assignments for CBB games.
    """
    
    STATBROADCAST_URL = "https://statbroadcast.com"
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def lookup_referee_rate(self, referee_name: str) -> Optional[float]:
        """Look up historical foul rate for a referee."""
        # Exact match first
        if referee_name in REFEREE_FOUL_RATES:
            return REFEREE_FOUL_RATES[referee_name]
        
        # Partial match
        for ref, rate in REFEREE_FOUL_RATES.items():
            if referee_name.lower() in ref.lower() or ref.lower() in referee_name.lower():
                return rate
        
        return None
    
    def calculate_crew_avg(self, referees: List[str]) -> float:
        """Calculate average foul rate for a crew of referees."""
        rates = []
        for ref in referees:
            rate = self.lookup_referee_rate(ref)
            if rate:
                rates.append(rate)
        
        if rates:
            return sum(rates) / len(rates)
        return NCAA_AVG_FOULS  # Default to average
    
    def fetch_assignments_from_conference(self, conference: str = 'big12') -> List[Dict]:
        """
        Try to fetch referee assignments from conference sites.
        Many conferences post assignments day-of.
        """
        # This would require conference-specific scrapers
        # For MVP, we'll use manual entry or the hardcoded rates
        return []
    
    def save_assignment(self, event_id: str, referees: List[str], source: str = 'manual'):
        """Save a referee assignment to the database."""
        if not referees:
            return False
        
        crew_avg = self.calculate_crew_avg(referees)
        
        query = """
        INSERT INTO referee_assignments 
            (event_id, referee_1, referee_2, referee_3, crew_avg_fouls, source, fetched_at)
        VALUES 
            (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO UPDATE SET
            referee_1 = EXCLUDED.referee_1,
            referee_2 = EXCLUDED.referee_2,
            referee_3 = EXCLUDED.referee_3,
            crew_avg_fouls = EXCLUDED.crew_avg_fouls,
            source = EXCLUDED.source,
            fetched_at = EXCLUDED.fetched_at
        """
        
        with get_db_connection() as conn:
            cur = conn.cursor()
            cur.execute(query, (
                event_id,
                referees[0] if len(referees) > 0 else None,
                referees[1] if len(referees) > 1 else None,
                referees[2] if len(referees) > 2 else None,
                crew_avg,
                source,
                datetime.now()
            ))
            conn.commit()
        
        print(f"[REF] Saved: {event_id} -> {referees} (Avg: {crew_avg:.1f})")
        return True
    
    def bulk_assign_by_pattern(self):
        """
        Assign referees to upcoming games based on historical patterns.
        This is a heuristic - certain conferences tend to use certain crews.
        """
        # Get upcoming NCAAM events without referee assignments
        query = """
        SELECT e.id, e.home_team, e.away_team
        FROM events e
        LEFT JOIN referee_assignments r ON e.id = r.event_id
        WHERE e.league = 'NCAAM' 
          AND e.start_time > NOW()
          AND e.start_time < NOW() + INTERVAL '2 days'
          AND r.id IS NULL
        """
        
        with get_db_connection() as conn:
            events = _exec(conn, query).fetchall()
        
        print(f"[REF] Found {len(events)} events without referee assignments")
        
        # For now, assign average crews (neutral signal)
        # In production, this would be updated day-of with actual assignments
        for event in events:
            # Default to neutral crew
            self.save_assignment(event['id'], ['Unknown Crew'], source='default')
        
        return len(events)


def manual_entry():
    """CLI for manual referee entry."""
    scraper = OfficiatingScraper()
    
    print("Officiating Assignment Entry")
    print("Available referees with known foul rates:")
    for ref, rate in sorted(REFEREE_FOUL_RATES.items(), key=lambda x: -x[1]):
        marker = "🔴 HIGH" if rate > 38 else ("🟢 LOW" if rate < 35 else "")
        print(f"  {ref}: {rate:.1f} {marker}")
    
    print("\n")
    event_id = input("Event ID: ")
    refs_str = input("Referees (comma-separated): ")
    
    if event_id and refs_str:
        refs = [r.strip() for r in refs_str.split(',')]
        scraper.save_assignment(event_id, refs, source='manual')
        print("Saved!")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == 'manual':
        manual_entry()
    else:
        scraper = OfficiatingScraper()
        scraper.bulk_assign_by_pattern()
