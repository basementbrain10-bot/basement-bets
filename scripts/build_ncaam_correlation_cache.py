
import sys
import os
import json
import logging
from datetime import datetime

# Allow imports from src
sys.path.append(os.getcwd())

from src.services.correlation.ncaam_correlation_engine import NCAAMCorrelationEngine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_FILE = "data/correlation_cache_2025_2026.json"

def build_cache():
    logger.info("Starting Correlation Cache Build (2025-2026)...")
    
    engine = NCAAMCorrelationEngine()
    
    # 1. Fetch Data
    logger.info("Fetching season data...")
    df = engine.fetch_season_data()
    logger.info(f"Fetched {len(df)} games.")
    
    # 2. Build Bins
    df = engine.build_archetype_bins(df)
    
    # 3. Compute Metrics
    logger.info("Computing metrics...")
    metrics = engine.compute_metrics(df)
    
    # 4. Save
    output = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "season": "2025-2026",
            "games_count": len(df)
        },
        "archetypes": metrics
    }
    
    with open(CACHE_FILE, "w") as f:
        json.dump(output, f, indent=2)
        
    logger.info(f"Cache saved to {CACHE_FILE}")

if __name__ == "__main__":
    build_cache()
