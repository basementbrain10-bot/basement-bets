"""
Naming Standardization Utilities
Consolidated from legacy models to ensure consistent matching across services.
"""

def standardize_team_name(team_name: str) -> str:
    """
    Standardize team name for consistent matching across data sources.
    Source: Legacy NCAAMModel.
    """
    if not team_name:
        return team_name
        
    # Common normalizations
    normalized = team_name.strip()
    
    # Known aliases
    aliases = {
        "uconn": "Connecticut",
        "ole miss": "Mississippi",
        "lsu": "LSU",
        "ucla": "UCLA",
        "usc": "USC",
        "smu": "SMU",
        "tcu": "TCU",
        "byu": "BYU",
        "uncw": "UNC Wilmington",
        "unc": "North Carolina",
        "umass": "Massachusetts",
        "unlv": "UNLV",
        "vcu": "VCU",
        "utep": "UTEP",
    }
    
    normalized_lower = normalized.lower()
    for alias, full_name in aliases.items():
        if normalized_lower == alias:
            return full_name
            
    # Clean up common suffixes/prefixes
    normalized = normalized.replace(".", "")
    normalized = normalized.replace(" St", " State")
    normalized = normalized.replace(" (FL)", "")
    normalized = normalized.replace(" (PA)", "")
    normalized = normalized.replace(" (OH)", "")
    
    return normalized
