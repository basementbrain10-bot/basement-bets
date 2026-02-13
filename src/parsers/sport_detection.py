"""
Shared sport detection utility for all bet parsers.
Centralizes team/keyword lists so FanDuel and DraftKings parsers stay consistent.
"""

# ─── NCAAM (College Basketball) ─── 
# Power conferences + mid-majors that commonly appear on DraftKings/FanDuel
NCAAM_TEAMS = [
    # Big East
    "butler", "creighton", "depaul", "georgetown", "marquette",
    "providence", "seton hall", "st. john's", "st johns", "uconn",
    "villanova", "xavier", "connecticut",
    # ACC
    "boston college", "clemson", "duke", "florida state", "georgia tech",
    "louisville", "miami", "nc state", "north carolina", "notre dame",
    "pittsburgh", "pitt", "smu", "stanford", "syracuse", "virginia",
    "virginia tech", "wake forest", "california",
    # SEC
    "alabama", "arkansas", "auburn", "florida", "georgia", "kentucky",
    "lsu", "mississippi state", "missouri", "ole miss", "south carolina",
    "tennessee", "texas a&m", "vanderbilt", "oklahoma",
    # Big Ten
    "illinois", "indiana", "iowa", "maryland", "michigan",
    "michigan state", "minnesota", "nebraska", "northwestern",
    "ohio state", "penn state", "purdue", "rutgers", "wisconsin",
    "ucla", "usc", "oregon", "washington",
    # Big 12
    "arizona", "arizona state", "baylor", "byu", "brigham young",
    "cincinnati", "colorado", "houston", "iowa state", "kansas",
    "kansas state", "oklahoma state", "tcu", "texas tech",
    "ucf", "west virginia",
    # AAC
    "east carolina", "fau", "florida atlantic", "memphis", "north texas",
    "rice", "south florida", "usf", "temple", "tulane", "tulsa",
    "uab", "utsa", "wichita state", "charlotte",
    # Mountain West
    "air force", "boise state", "colorado state", "fresno state",
    "nevada", "new mexico", "san diego state", "san jose state",
    "unlv", "utah state", "wyoming",
    # WCC
    "gonzaga", "saint mary's", "san francisco", "loyola marymount",
    "pepperdine", "santa clara", "pacific", "portland",
    # MAC / Mid-Majors
    "akron", "ball state", "bowling green", "buffalo", "central michigan",
    "eastern michigan", "kent state", "miami (oh)", "miami oh",
    "northern illinois", "ohio", "toledo", "western michigan",
    # Atlantic 10
    "dayton", "davidson", "duquesne", "fordham", "george mason",
    "george washington", "la salle", "loyola chicago", "massachusetts",
    "umass", "rhode island", "richmond", "saint louis", "st. louis",
    "saint joseph's", "st. joseph's", "st. bonaventure", "vcu",
    # CAA / Horizon / MAAC / Patriot / etc.
    "drexel", "hofstra", "northeastern", "towson", "william & mary",
    "william and mary", "delaware", "stony brook", "charleston",
    "uncw", "unc wilmington", "elon", "james madison",
    "cleveland state", "detroit mercy", "iu indianapolis",
    "wright state", "youngstown state", "oakland", "green bay",
    "milwaukee", "robert morris", "fairleigh dickinson",
    "marist", "manhattan", "niagara", "canisius", "iona",
    "rider", "quinnipiac", "fairfield", "siena", "monmouth",
    # Ivy League
    "brown", "columbia", "cornell", "dartmouth", "harvard",
    "penn", "princeton", "yale",
    # MEAC / SWAC / misc
    "howard", "norfolk state", "morgan state", "coppin state",
    "hampton", "delaware state", "grambling", "grambling state",
    "southern", "prairie view", "jackson state",
    "alcorn state", "texas southern", "arkansas pine bluff",
    # Summit / Horizon / misc
    "south dakota state", "oral roberts", "north dakota state",
    "denver", "south dakota", "north dakota", "western illinois",
    "omaha",
    # SoCon / Big South / OVC / ASUN
    "chattanooga", "etsu", "furman", "mercer", "samford",
    "unc greensboro", "uncg", "wofford", "western carolina",
    "high point", "campbell", "liberty", "lipscomb", "kennesaw state",
    "jacksonville state", "bellarmine", "queens",
    "austin peay", "eastern kentucky", "morehead state",
    "murray state", "southeast missouri", "tennessee state",
    "tennessee tech", "ut martin", "lindenwood",
    # WAC / Southland / etc.
    "abilene christian", "lamar", "incarnate word",
    "mcneese", "nicholls", "northwestern state",
    "sam houston", "sam houston state", "southeastern louisiana",
    "stephen f. austin", "stephen f austin", "tarleton state",
    "texas state", "ut arlington", "ut rio grande valley",
    # Sun Belt
    "app state", "appalachian state", "arkansas state",
    "coastal carolina", "georgia southern", "georgia state",
    "james madison", "louisiana", "Louisiana-Lafayette",
    "louisiana tech", "marshall", "old dominion",
    "south alabama", "southern miss", "troy", "texas arlington",
    # NEC / America East / etc.
    "central connecticut", "long island", "liu",
    "merrimack", "mount st. mary's", "sacred heart",
    "st. francis", "wagner", "albany", "binghamton",
    "maine", "new hampshire", "umbc", "vermont",
    # Misc commonly bet teams
    "east texas a&m", "new mexico state", "uc irvine",
    "uc davis", "uc santa barbara", "uc riverside",
    "cal poly", "cal state fullerton", "long beach state",
    "hawaii", "utep", "drake",
    "loyola", "belmont", "valparaiso",
    "northern iowa", "southern illinois", "indiana state",
    "bradley", "evansville", "illinois state", "missouri state",
    # Common DraftKings display names (abbreviated)
    "bonaventure", "st. bonny", "bonnies",
    "tarheels", "tar heels", "wildcats", "bulldogs",
    "jayhawks", "hoosiers", "mountaineers", "volunteers",
    "crimson tide", "wolverines", "spartans", "buckeyes",
    "badgers", "hawkeyes", "nittany lions", "terrapins",
    "blue devils", "cavaliers", "hokies", "wolfpack",
    "seminoles", "yellow jackets", "cardinals", "hurricanes",
    "orange", "demon deacons", "fighting irish", "panthers",
    "mustangs", "bruins", "trojans", "ducks", "huskies",
    "gators", "dawgs", "volunteers", "razorbacks",
]

# ─── NBA ───
NBA_TEAMS = [
    "nba", "lakers", "celtics", "warriors", "bucks", "76ers", "sixers",
    "nets", "knicks", "heat", "bulls", "cavaliers", "cavs",
    "mavericks", "mavs", "rockets", "spurs", "clippers",
    "nuggets", "jazz", "timberwolves", "wolves", "pelicans",
    "thunder", "blazers", "trail blazers", "kings", "suns",
    "hornets", "hawks", "magic", "wizards", "pistons",
    "pacers", "raptors", "grizzlies",
    "points", "assists", "rebounds", "threes", "3-pointers",
    "steals", "blocks",
]

# ─── NFL ───
NFL_TEAMS = [
    "nfl", "chiefs", "bills", "49ers", "ravens", "lions",
    "packers", "bears", "cowboys", "eagles", "giants",
    "seahawks", "steelers", "bengals", "browns", "vikings",
    "saints", "falcons", "buccaneers", "bucs", "jaguars",
    "titans", "colts", "texans", "broncos", "chargers",
    "raiders", "patriots", "jets", "dolphins", "rams",
    "cardinals", "commanders", "panthers",
    "quarterback", "passing", "rushing", "touchdown", "receptions",
    "qb", "yardage", "interception", "receiving yards",
    "anytime td", "td scorer",
    # Short-form team names used by DK
    "sea seahawks", "ne patriots", "sf 49ers", "gb packers",
    "kc chiefs", "buf bills", "bal ravens", "det lions",
    "dal cowboys", "phi eagles", "nyg giants", "nyj jets",
    "ari cardinals", "atl falcons", "tb buccaneers",
    "chi bears", "cin bengals", "cle browns", "den broncos",
    "hou texans", "ind colts", "jax jaguars", "lac chargers",
    "lar rams", "lv raiders", "mia dolphins", "min vikings",
    "no saints", "pit steelers", "ten titans", "was commanders",
]

# ─── NCAAF ───
NCAAF_KEYWORDS = [
    "ncaaf", "cfb", "bowl game", "college football",
    "cfp", "cotton bowl", "rose bowl", "sugar bowl",
    "peach bowl", "orange bowl", "fiesta bowl",
]

# ─── Other Sports ───
MLB_KEYWORDS = [
    "mlb", "dodgers", "yankees", "red sox", "mets", "astros",
    "braves", "phillies", "padres", "mariners", "orioles",
    "guardians", "twins", "brewers", "cardinals", "cubs",
    "reds", "pirates", "diamondbacks", "rockies", "royals",
    "rays", "athletics", "white sox", "tigers", "nationals",
    "marlins", "angels", "rangers", "blue jays",
    "runs", "innings", "strikeouts", "stolen base", "home run",
    "rbi", "hits", "pitcher", "batting",
]

NHL_KEYWORDS = [
    "nhl", "puck line", "bruins", "leafs", "rangers", "goals",
    "goalie", "slapshot", "icing", "penguins", "capitals",
    "hurricanes", "panthers", "lightning", "maple leafs",
    "canadiens", "senators", "sabres", "red wings", "flyers",
    "islanders", "devils", "blue jackets", "blackhawks", "blues",
    "predators", "stars", "wild", "avalanche", "jets",
    "flames", "oilers", "canucks", "kraken", "ducks",
    "sharks", "knights", "golden knights", "coyotes",
]

SOCCER_KEYWORDS = [
    "soccer", "epl", "chelsea", "liverpool", "arsenal",
    "man city", "manchester city", "man united", "manchester united",
    "tottenham", "crystal palace", "west ham", "everton",
    "newcastle", "brighton", "wolverhampton", "wolves",
    "aston villa", "fulham", "bournemouth", "brentford",
    "nottingham forest", "leicester", "ipswich", "southampton",
    "champions league", "premier league", "la liga",
    "bundesliga", "serie a", "ligue 1", "mls",
    "real madrid", "barcelona", "atletico madrid",
    "bayern munich", "borussia dortmund", "juventus",
    "inter milan", "ac milan", "psg", "benfica",
]


def detect_sport(text: str) -> str:
    """
    Detect the sport from free text (selection, description, matchup, raw_text).
    Returns one of: NFL, NBA, NCAAM, NCAAF, MLB, NHL, SOCCER, Unknown.
    """
    t = text.lower()
    
    # Check NCAAM first (most specific — many team names overlap with NCAAF/NBA)
    if any(team in t for team in NCAAM_TEAMS):
        return "NCAAM"
    
    # NFL
    if any(kw in t for kw in NFL_TEAMS):
        return "NFL"
    
    # NBA
    if any(kw in t for kw in NBA_TEAMS):
        return "NBA"
    
    # NCAAF
    if any(kw in t for kw in NCAAF_KEYWORDS):
        return "NCAAF"
    
    # MLB
    if any(kw in t for kw in MLB_KEYWORDS):
        return "MLB"
    
    # NHL
    if any(kw in t for kw in NHL_KEYWORDS):
        return "NHL"
    
    # Soccer
    if any(kw in t for kw in SOCCER_KEYWORDS):
        return "SOCCER"
    
    return "Unknown"
