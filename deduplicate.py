"""Deduplication and fuzzy matching for cross-provider event comparison.

Improvements over v1:
- Comprehensive team alias dictionary (abbreviations → canonical names)
  covering Basketball (primary), Volleyball, Handball, Ice Hockey, Baseball
- External alias file support (alias_overrides.xlsx) for ops team to maintain
- Robust women's fixture detection (multi-language patterns)
- Gender mismatch blocks matching entirely (no false positives)
- Alias lookup runs BEFORE fuzzy matching for accurate scoring
"""

import logging
import os
import re
import unicodedata
from dataclasses import dataclass

import pandas as pd
from rapidfuzz.fuzz import token_sort_ratio

from config import PipelineConfig

logger = logging.getLogger("pipeline.deduplicate")

# ---------------------------------------------------------------------------
# TEAM ALIAS DICTIONARY
# Maps known abbreviations / nicknames → canonical lowercase name
# Basketball is the primary focus but all sports are covered.
# ---------------------------------------------------------------------------
_BUILTIN_ALIASES: dict[str, str] = {
    # ── BASKETBALL — NBA ────────────────────────────────────────────────────
    "lakers": "los angeles lakers",
    "la lakers": "los angeles lakers",
    "lal": "los angeles lakers",
    "celtics": "boston celtics",
    "bos": "boston celtics",
    "warriors": "golden state warriors",
    "gsw": "golden state warriors",
    "gs warriors": "golden state warriors",
    "bulls": "chicago bulls",
    "chi": "chicago bulls",
    "heat": "miami heat",
    "mia": "miami heat",
    "nets": "brooklyn nets",
    "bkn": "brooklyn nets",
    "bk nets": "brooklyn nets",
    "knicks": "new york knicks",
    "nyk": "new york knicks",
    "ny knicks": "new york knicks",
    "sixers": "philadelphia 76ers",
    "76ers": "philadelphia 76ers",
    "phi": "philadelphia 76ers",
    "bucks": "milwaukee bucks",
    "mil": "milwaukee bucks",
    "raptors": "toronto raptors",
    "tor": "toronto raptors",
    "nuggets": "denver nuggets",
    "den": "denver nuggets",
    "suns": "phoenix suns",
    "phx": "phoenix suns",
    "phx suns": "phoenix suns",
    "clippers": "los angeles clippers",
    "lac": "los angeles clippers",
    "la clippers": "los angeles clippers",
    "mavs": "dallas mavericks",
    "mavericks": "dallas mavericks",
    "dal": "dallas mavericks",
    "grizzlies": "memphis grizzlies",
    "mem": "memphis grizzlies",
    "jazz": "utah jazz",
    "uta": "utah jazz",
    "pelicans": "new orleans pelicans",
    "nop": "new orleans pelicans",
    "no pelicans": "new orleans pelicans",
    "thunder": "oklahoma city thunder",
    "okc": "oklahoma city thunder",
    " okc thunder": "oklahoma city thunder",
    "blazers": "portland trail blazers",
    "trail blazers": "portland trail blazers",
    "por": "portland trail blazers",
    "spurs": "san antonio spurs",
    "sas": "san antonio spurs",
    "sa spurs": "san antonio spurs",
    "rockets": "houston rockets",
    "hou": "houston rockets",
    "kings": "sacramento kings",
    "sac": "sacramento kings",
    "hawks": "atlanta hawks",
    "atl": "atlanta hawks",
    "hornets": "charlotte hornets",
    "cha": "charlotte hornets",
    "pistons": "detroit pistons",
    "det": "detroit pistons",
    "pacers": "indiana pacers",
    "ind": "indiana pacers",
    "magic": "orlando magic",
    "orl": "orlando magic",
    "wizards": "washington wizards",
    "was": "washington wizards",
    "wiz": "washington wizards",
    "cavaliers": "cleveland cavaliers",
    "cavs": "cleveland cavaliers",
    "cle": "cleveland cavaliers",
    "timberwolves": "minnesota timberwolves",
    "wolves": "minnesota timberwolves",
    "min": "minnesota timberwolves",

    # ── BASKETBALL — EUROLEAGUE / EUROCUP ───────────────────────────────────
    "rm": "real madrid",
    "rma": "real madrid",
    "real madrid basketball": "real madrid",
    "barca": "barcelona",
    "fcb": "barcelona",
    "fc barcelona": "barcelona",
    "barca basket": "barcelona",
    "barça": "barcelona",
    "cska": "cska moscow",
    "cska moscow basketball": "cska moscow",
    "psg": "paris saint germain",
    "psg basket": "paris saint germain",
    "paris sg": "paris saint germain",
    "oly": "olympiacos",
    "olympiakos": "olympiacos",
    "pana": "panathinaikos",
    "panathinaikos aktor": "panathinaikos",
    "fcbb": "fc barcelona",
    "alba": "alba berlin",
    "alba berlin basketball": "alba berlin",
    "Bayern": "bayern munich",
    "fcb basketball": "bayern munich",
    "fc bayern": "bayern munich",
    "fc bayern basketball": "bayern munich",
    "maccabi": "maccabi tel aviv",
    "maccabi tlv": "maccabi tel aviv",
    "maccabi ta": "maccabi tel aviv",
    "efes": "anadolu efes",
    "anadolu": "anadolu efes",
    "zal": "zalgiris",
    "zalgiris kaunas": "zalgiris",
    "brose": "brose bamberg",
    "bamberg": "brose bamberg",
    "uni": "unicaja",
    "unicaja malaga": "unicaja",
    "asv": "as monaco",
    "monaco": "as monaco",
    "virtus": "virtus bologna",
    "virtus bo": "virtus bologna",
    "vbo": "virtus bologna",
    "red star": "crvena zvezda",
    "red star belgrade": "crvena zvezda",
    "cz": "crvena zvezda",
    "fenerbahce": "fenerbahce beko",
    "fener": "fenerbahce beko",
    "fb": "fenerbahce beko",
    "galatasaray": "galatasaray nef",
    "gala": "galatasaray nef",
    "gs": "galatasaray nef",
    "paok": "paok thessaloniki",
    "aris": "aris thessaloniki",
    "bam": "brose bamberg",
    "ldlc": "ldlc asvel",
    "asvel": "ldlc asvel",
    "lyon": "ldlc asvel",
    "reth": "rethymno",
    "lavrio": "lavrio megabolt",
    "promitheas": "promitheas patras",

    # ── BASKETBALL — Turkish BSL ─────────────────────────────────────────────
    "ana efes": "anadolu efes",
    "besiktas": "besiktas basketball",
    "bjk": "besiktas basketball",
    "tofas": "tofas bursa",
    "bursa": "tofas bursa",
    "pinar": "pinar karsiyaka",
    "karsi": "pinar karsiyaka",

    # ── BASKETBALL — Spanish ACB ─────────────────────────────────────────────
    "joventut": "joventut badalona",
    "badalona": "joventut badalona",
    "obradoiro": "obradoiro cab",
    "breogan": "rio breogan",
    "gran canaria": "gran canaria basketball",
    "herbalife": "gran canaria basketball",
    "bilbao": "bilbao basket",
    "surne bilbao": "bilbao basket",
    "estudiantes": "real madrid estudiantes",
    "manresa": "baxi manresa",

    # ── BASKETBALL — Italian Lega Basket ─────────────────────────────────────
    "olimpia": "olimpia milano",
    "milan": "olimpia milano",
    "ax armani": "olimpia milano",
    "ea7": "olimpia milano",
    "trento": "dolomiti energia trento",
    "dolomiti": "dolomiti energia trento",
    "brindisi": "happy casa brindisi",
    "happy casa": "happy casa brindisi",
    "sassari": "dinamo sassari",
    "dinamo": "dinamo sassari",
    "venezia": "umana reyer venezia",
    "reyer": "umana reyer venezia",
    "tortona": "bertram derthona tortona",
    "derthona": "bertram derthona tortona",

    # ── BASKETBALL — Greek HEBA ──────────────────────────────────────────────
    "panathinaikos": "panathinaikos",
    "pao": "panathinaikos",
    "olympiacos": "olympiacos",
    "piraeus": "olympiacos",
    "aek": "aek athens",
    "aek athens basketball": "aek athens",
    "peristeri": "peristeri basketball",
    "kolossos": "kolossos rhodos",

    # ── BASKETBALL — Russian VTB / General ──────────────────────────────────
    "khimki": "khimki moscow",
    "lokomotiv": "lokomotiv kuban",
    "loko": "lokomotiv kuban",
    "unics": "unics kazan",
    "nizhny": "nizhny novgorod",
    "nnov": "nizhny novgorod",
    "zenit": "zenit saint petersburg",
    "zenit spb": "zenit saint petersburg",

    # ── ICE HOCKEY — NHL ─────────────────────────────────────────────────────
    "leafs": "toronto maple leafs",
    "tor maple leafs": "toronto maple leafs",
    "tml": "toronto maple leafs",
    "habs": "montreal canadiens",
    "mtl": "montreal canadiens",
    "canadiens": "montreal canadiens",
    "bruins": "boston bruins",
    "bos bruins": "boston bruins",
    "rangers": "new york rangers",
    "nyr": "new york rangers",
    "ny rangers": "new york rangers",
    "flyers": "philadelphia flyers",
    "phi flyers": "philadelphia flyers",
    "penguins": "pittsburgh penguins",
    "pens": "pittsburgh penguins",
    "pit": "pittsburgh penguins",
    "caps": "washington capitals",
    "capitals": "washington capitals",
    "wsh": "washington capitals",
    "lightning": "tampa bay lightning",
    "tbl": "tampa bay lightning",
    "tb lightning": "tampa bay lightning",
    "panthers": "florida panthers",
    "fla": "florida panthers",
    "fl panthers": "florida panthers",
    "hurricanes": "carolina hurricanes",
    "canes": "carolina hurricanes",
    "car": "carolina hurricanes",
    "senators": "ottawa senators",
    "sens": "ottawa senators",
    "ott": "ottawa senators",
    "sabres": "buffalo sabres",
    "buf": "buffalo sabres",
    "red wings": "detroit red wings",
    "drw": "detroit red wings",
    "det red wings": "detroit red wings",
    "blackhawks": "chicago blackhawks",
    "chi blackhawks": "chicago blackhawks",
    "chw": "chicago blackhawks",
    "blues": "st louis blues",
    "stl": "st louis blues",
    "st. louis": "st louis blues",
    "wild": "minnesota wild",
    "mnw": "minnesota wild",
    "jets": "winnipeg jets",
    "wpg": "winnipeg jets",
    "avalanche": "colorado avalanche",
    "avs": "colorado avalanche",
    "col": "colorado avalanche",
    "sharks": "san jose sharks",
    "sjs": "san jose sharks",
    "sj sharks": "san jose sharks",
    "ducks": "anaheim ducks",
    "ana": "anaheim ducks",
    "kings": "los angeles kings",
    "lak": "los angeles kings",
    "la kings": "los angeles kings",
    "coyotes": "arizona coyotes",
    "ari": "arizona coyotes",
    "flames": "calgary flames",
    "cgy": "calgary flames",
    "oilers": "edmonton oilers",
    "edm": "edmonton oilers",
    "canucks": "vancouver canucks",
    "van": "vancouver canucks",
    "golden knights": "vegas golden knights",
    "vgk": "vegas golden knights",
    "vegas": "vegas golden knights",
    "kraken": "seattle kraken",
    "sea": "seattle kraken",
    "blue jackets": "columbus blue jackets",
    "cbj": "columbus blue jackets",
    "predators": "nashville predators",
    "preds": "nashville predators",
    "nsh": "nashville predators",
    "stars": "dallas stars",
    "dal stars": "dallas stars",
    "cougars": "prince george cougars",

    # ── HANDBALL ─────────────────────────────────────────────────────────────
    "thw": "thw kiel",
    "kiel": "thw kiel",
    "rhein neckar": "rhein neckar lowen",
    "lowen": "rhein neckar lowen",
    "flensburg": "sg flensburg handewitt",
    "sg flensburg": "sg flensburg handewitt",
    "magdeburg": "sc magdeburg",
    "barcelona handball": "fc barcelona handball",
    "barca handball": "fc barcelona handball",
    "paris handball": "paris saint germain handball",
    "psg handball": "paris saint germain handball",
    "montpellier": "montpellier hb",
    "nantes": "hbc nantes",
    "chambery": "chambery savoie mont blanc",
    "pick szeged": "pick szeged",
    "pick": "pick szeged",
    "veszprem": "telekom veszprem",
    "porto handball": "fc porto handball",
    "sporting handball": "sporting cp handball",
    "vardar": "rk vardar",
    "zeleznicar": "rk zeleznicar",
    "nexe": "rk nexe",
    "ppd zagreb": "ppd zagreb",
    "ppd": "ppd zagreb",
    "meshkov": "meshkov brest",
    "brest handball": "meshkov brest",

    # ── VOLLEYBALL ───────────────────────────────────────────────────────────
    "trentino": "itas trentino",
    "modena": "leo shoes modena",
    "perugia": "sir safety perugia",
    "sir": "sir safety perugia",
    "lube": "cucine lube civitanova",
    "civitanova": "cucine lube civitanova",
    "monza": "vero volley monza",
    "milano volley": "allianz milano",
    "allianz": "allianz milano",
    "zenit kazan": "zenit kazan volleyball",
    "lokomotiv novosibirsk": "lokomotiv novosibirsk volleyball",
    "dinamo moscow": "dinamo moscow volleyball",

    # ── BASEBALL — MLB ───────────────────────────────────────────────────────
    "yanks": "new york yankees",
    "yankees": "new york yankees",
    "nyy": "new york yankees",
    "ny yankees": "new york yankees",
    "sox": "boston red sox",
    "red sox": "boston red sox",
    "bos red sox": "boston red sox",
    "mets": "new york mets",
    "nym": "new york mets",
    "ny mets": "new york mets",
    "dodgers": "los angeles dodgers",
    "lad": "los angeles dodgers",
    "la dodgers": "los angeles dodgers",
    "cubs": "chicago cubs",
    "chc": "chicago cubs",
    "white sox": "chicago white sox",
    "chw": "chicago white sox",
    "chi white sox": "chicago white sox",
    "cards": "st louis cardinals",
    "cardinals": "st louis cardinals",
    "stl cardinals": "st louis cardinals",
    "braves": "atlanta braves",
    "atl braves": "atlanta braves",
    "giants": "san francisco giants",
    "sfg": "san francisco giants",
    "sf giants": "san francisco giants",
    "astros": "houston astros",
    "hou astros": "houston astros",
    "phillies": "philadelphia phillies",
    "phi phillies": "philadelphia phillies",
    "nationals": "washington nationals",
    "nats": "washington nationals",
    "was nationals": "washington nationals",
    "mariners": "seattle mariners",
    "sea mariners": "seattle mariners",
    "athletics": "oakland athletics",
    "a's": "oakland athletics",
    "oak": "oakland athletics",
    "rangers baseball": "texas rangers",
    "tex": "texas rangers",
    "angels": "los angeles angels",
    "laa": "los angeles angels",
    "la angels": "los angeles angels",
    "twins": "minnesota twins",
    "min twins": "minnesota twins",
    "tigers": "detroit tigers",
    "det tigers": "detroit tigers",
    "royals": "kansas city royals",
    "kc": "kansas city royals",
    "kcr": "kansas city royals",
    "blue jays": "toronto blue jays",
    "tbj": "toronto blue jays",
    "tor blue jays": "toronto blue jays",
    "rays": "tampa bay rays",
    "tbr": "tampa bay rays",
    "tb rays": "tampa bay rays",
    "orioles": "baltimore orioles",
    "bal": "baltimore orioles",
    "bal orioles": "baltimore orioles",
    "pirates": "pittsburgh pirates",
    "pit pirates": "pittsburgh pirates",
    "reds": "cincinnati reds",
    "cin": "cincinnati reds",
    "brewers": "milwaukee brewers",
    "mil brewers": "milwaukee brewers",
    "rockies": "colorado rockies",
    "col rockies": "colorado rockies",
    "padres": "san diego padres",
    "sdp": "san diego padres",
    "sd padres": "san diego padres",
    "diamondbacks": "arizona diamondbacks",
    "dbacks": "arizona diamondbacks",
    "ari dbacks": "arizona diamondbacks",
    "marlins": "miami marlins",
    "mia marlins": "miami marlins",
    "guardians": "cleveland guardians",
    "cle guardians": "cleveland guardians",
}

# ---------------------------------------------------------------------------
# WOMEN'S DETECTION — comprehensive multi-language patterns
# ---------------------------------------------------------------------------
_WOMEN_PATTERNS = re.compile(
    r"""
    \bw\b           |   # standalone "W" — "Barcelona W", "Atletico W"
    \(w\)           |   # "(W)"
    \bwomen\b       |   # English
    \bwomens\b      |
    \bwoman\b       |
    \bladies\b      |   # English
    \blady\b        |
    \bgirls\b       |
    \bfemenino\b    |   # Spanish
    \bfemenina\b    |
    \bfemeni\b      |   # Catalan
    \bfeminino\b    |   # Portuguese
    \bfeminina\b    |
    \bfrauen\b      |   # German
    \bdamen\b       |
    \bdames\b       |   # Dutch / French
    \bvrouwen\b     |   # Dutch
    \bfemmes\b      |   # French
    \bdonna\b       |   # Italian
    \bdonne\b       |
    \bfemminile\b   |
    \bkvinder\b     |   # Danish
    \bkvinnor\b     |   # Swedish
    \bnaiset\b      |   # Finnish
    \bnaisten\b     |
    \bkobiet\b      |   # Polish
    \bzenanki\b     |   # Croatian/Serbian
    \bzenicka\b     |
    \bwfc\b         |   # Women's FC
    \bwbc\b         |
    \(women\)       |
    women\'s        |
    woman\'s
    """,
    re.IGNORECASE | re.VERBOSE,
)

_MEN_PATTERNS = re.compile(
    r"""
    \bmen\b         |   # English men's (careful — also in "women")
    \bgents\b       |
    \bherren\b      |   # German
    \bheren\b       |   # Dutch
    \bhommes\b      |   # French
    \buomini\b      |   # Italian
    \bmasculino\b   |   # Spanish
    \bmasculina\b   |
    \bmasculin\b    |   # French/Romanian
    \bmeski\b       |   # Polish
    \bmuzski\b          # Croatian/Serbian
    """,
    re.IGNORECASE | re.VERBOSE,
)


def detect_gender(text: str) -> str:
    """Detect if a team/event name refers to a women's or men's fixture.

    Returns:
        "women" if women's indicators found
        "men"   if men's indicators found (and no women's)
        "unknown" if neither found
    """
    # Check women's FIRST — "women" contains "men" so order matters
    if _WOMEN_PATTERNS.search(text):
        return "women"
    # Exclude the "men" in "women" — only flag pure men's indicators
    clean = re.sub(r'\bwomen\b', '', text, flags=re.IGNORECASE)
    if _MEN_PATTERNS.search(clean):
        return "men"
    return "unknown"


# ---------------------------------------------------------------------------
# Alias loading — built-in dict + optional external Excel file
# ---------------------------------------------------------------------------
_LOADED_ALIASES: dict[str, str] = {}
_ALIASES_LOADED = False


def _load_aliases(alias_file: str | None = None) -> dict[str, str]:
    """Load alias dictionary. Merges built-in + external file overrides."""
    global _LOADED_ALIASES, _ALIASES_LOADED

    if _ALIASES_LOADED:
        return _LOADED_ALIASES

    aliases = dict(_BUILTIN_ALIASES)

    # Look for alias_overrides.xlsx in common locations
    search_paths = [
        alias_file,
        "alias_overrides.xlsx",
        "alias_overrides.csv",
        os.path.join("pipeline", "alias_overrides.xlsx"),
        os.path.join("pipeline", "alias_overrides.csv"),
    ]

    for path in search_paths:
        if not path or not os.path.isfile(path):
            continue
        try:
            if path.endswith(".csv"):
                df = pd.read_csv(path, dtype=str)
            else:
                df = pd.read_excel(path, dtype=str)

            df.columns = [str(c).strip().lower() for c in df.columns]
            if "alias" in df.columns and "canonical name" in df.columns:
                for _, row in df.iterrows():
                    alias = str(row["alias"]).strip().lower()
                    canonical = str(row["canonical name"]).strip().lower()
                    if alias and canonical:
                        aliases[alias] = canonical
                logger.info(
                    "Loaded %d alias overrides from '%s'", len(df), path
                )
            break
        except Exception as exc:
            logger.warning("Could not load alias file '%s': %s", path, exc)

    _LOADED_ALIASES = aliases
    _ALIASES_LOADED = True
    return aliases


def resolve_alias(name: str) -> str:
    """Resolve a team name through the alias dictionary.

    Returns the canonical name if found, otherwise the original name.
    Looks up both the full name and progressively shorter tokens.
    """
    aliases = _load_aliases()
    cleaned = name.strip().lower()

    # Direct lookup
    if cleaned in aliases:
        return aliases[cleaned]

    # Try without punctuation
    no_punct = re.sub(r"[^\w\s]", "", cleaned).strip()
    if no_punct in aliases:
        return aliases[no_punct]

    return cleaned


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

# Gender suffixes to strip AFTER gender detection
_GENDER_SUFFIXES = re.compile(
    r"""\s*(
        \bw\b | \(w\) | \bwomen\b | \bwomens\b | \bfemenino\b |
        \bfemenina\b | \bfrauen\b | \bdames\b | \bladies\b |
        \bgirls\b | \bfeminino\b | \bfeminina\b | \bdamen\b |
        \bvrouwen\b | \bfemminile\b | \bwfc\b |
        \bherren\b | \bhommes\b | \bmasculino\b
    )\s*$""",
    re.IGNORECASE | re.VERBOSE,
)

# Club affixes to strip
_CLUB_AFFIXES = re.compile(
    r"\b(fc|bc|sc|cd|ca|hc|h\.c\.|hb|rk|bm|ck|vk|ok|kk|zk|nk|sk|tk|ak|gk|pk|fk|bk|ik|if|aik|bf)\b",
    re.IGNORECASE,
)

# Country prefix: "Germany - " or "Spain: "
_COUNTRY_PREFIX = re.compile(r"^[a-z][a-z\s]+\s*[-:]\s+", re.IGNORECASE)

# Punctuation
_PUNCTUATION = re.compile(r"[^\w\s]", re.UNICODE)

# Possessive gender
_GENDER_POSSESSIVE = re.compile(r"\b(women|men)['\u2019]s\b", re.IGNORECASE)

# Whitespace collapse
_MULTI_SPACE = re.compile(r"\s{2,}")


def normalize_team_name(name: str) -> str:
    """Normalise a team name for fuzzy comparison.

    Pipeline:
    1. Lowercase + unicode
    2. Detect & strip dotted club prefixes (H.C., etc.)
    3. Strip gender suffixes (after detecting gender separately)
    4. Strip club affixes (FC, BC, etc.)
    5. Resolve alias → canonical name
    6. Collapse whitespace
    """
    result = str(name).lower()
    result = re.sub(r'\bh\.c\.?\s*', '', result, flags=re.IGNORECASE)
    result = _GENDER_SUFFIXES.sub("", result)
    result = _CLUB_AFFIXES.sub("", result)
    result = unicodedata.normalize("NFKC", result)
    result = _MULTI_SPACE.sub(" ", result).strip()

    # Resolve alias AFTER stripping affixes/suffixes
    result = resolve_alias(result)
    result = _MULTI_SPACE.sub(" ", result).strip()
    return result


def normalize_competition_name(name: str) -> str:
    """Normalise a competition / league name for fuzzy comparison."""
    result = str(name).lower()
    result = _COUNTRY_PREFIX.sub("", result)
    result = _GENDER_POSSESSIVE.sub(lambda m: m.group(1).lower(), result)
    result = _PUNCTUATION.sub("", result)
    result = unicodedata.normalize("NFKC", result)
    result = _MULTI_SPACE.sub(" ", result)
    return result.strip()


# ---------------------------------------------------------------------------
# DuplicateSet dataclass
# ---------------------------------------------------------------------------

@dataclass
class DuplicateSet:
    """A group of events identified as duplicates across providers."""
    events: list[dict]
    retained_event: dict
    similarity_scores: list[float]
    retained_provider: str
    discarded_event_ids: list[str]


# ---------------------------------------------------------------------------
# Match scoring
# ---------------------------------------------------------------------------

def compute_match_score(event_a: pd.Series, event_b: pd.Series) -> float:
    """Compute a composite similarity score between two events.

    Steps before scoring:
    1. Detect gender for both events — if mismatch, return 0.0 immediately
    2. Normalize + alias-resolve team names
    3. Composite: team(60%) + competition(20%) + time(20%)
    """
    # --- Gender mismatch check (hard block) ---
    name_a = str(event_a.get("event_name", ""))
    name_b = str(event_b.get("event_name", ""))
    league_a_raw = str(event_a.get("league", ""))
    league_b_raw = str(event_b.get("league", ""))

    gender_a = detect_gender(f"{name_a} {league_a_raw}")
    gender_b = detect_gender(f"{name_b} {league_b_raw}")

    # If one is known women's and other is known men's → completely different events
    if gender_a != "unknown" and gender_b != "unknown" and gender_a != gender_b:
        return 0.0

    # --- Team name similarity (alias-resolved) ---
    home_a = normalize_team_name(str(event_a.get("home_team", "")))
    home_b = normalize_team_name(str(event_b.get("home_team", "")))
    away_a = normalize_team_name(str(event_a.get("away_team", "")))
    away_b = normalize_team_name(str(event_b.get("away_team", "")))

    # Normal order
    home_sim = token_sort_ratio(home_a, home_b) / 100.0
    away_sim = token_sort_ratio(away_a, away_b) / 100.0
    score_normal = (home_sim + away_sim) / 2.0

    # Swapped order (some providers list teams differently)
    home_sim_sw = token_sort_ratio(home_a, away_b) / 100.0
    away_sim_sw = token_sort_ratio(away_a, home_b) / 100.0
    score_swapped = (home_sim_sw + away_sim_sw) / 2.0

    team_score = max(score_normal, score_swapped)

    # --- Competition similarity ---
    comp_a = normalize_competition_name(league_a_raw)
    comp_b = normalize_competition_name(league_b_raw)
    comp_score = token_sort_ratio(comp_a, comp_b) / 100.0

    # --- Time proximity ---
    # Use a soft penalty rather than a hard cliff so that events with
    # identical team names and league but slightly different scheduled times
    # (which is common across BR / BG feeds) still dedup correctly.
    # The score decays from 1.0 (0 min diff) to 0.0 at ≥60 min, giving a
    # maximum composite penalty of 0.20 — not enough to sink an otherwise
    # perfect team+comp match below the 0.85 threshold.
    time_a = pd.Timestamp(event_a.get("start_datetime"))
    time_b = pd.Timestamp(event_b.get("start_datetime"))
    try:
        diff_minutes = abs((time_a - time_b).total_seconds()) / 60.0
    except Exception:
        diff_minutes = 999.0
    # Hard reject if times are more than 60 minutes apart — different events
    if diff_minutes > 60.0:
        return 0.0
    time_score = max(0.0, 1.0 - (diff_minutes / 60.0))

    return 0.6 * team_score + 0.2 * comp_score + 0.2 * time_score


# ---------------------------------------------------------------------------
# find_duplicates
# ---------------------------------------------------------------------------

def find_duplicates(
    df: pd.DataFrame,
    config: PipelineConfig,
) -> list[DuplicateSet]:
    """Identify duplicate events across providers using time-window grouping
    and composite fuzzy scoring with alias resolution and gender detection."""
    if df.empty:
        return []

    # Use a wider window for candidate grouping so events with the same
    # teams but slightly different scheduled start times across providers
    # are still compared against each other.  The actual time penalty is
    # applied inside compute_match_score; here we just need to ensure the
    # two events end up in the same candidate group.
    # time_tolerance_minutes (default 5) is for reporting accuracy, NOT
    # for dedup grouping — using it here caused BR/BG pairs with a 6-min
    # difference to never be evaluated as duplicates at all.
    _DEDUP_WINDOW = pd.Timedelta(minutes=60)
    duplicate_sets: list[DuplicateSet] = []
    consumed: set[int] = set()

    df = df.sort_values(["sport", "start_datetime"]).reset_index(drop=True)

    for sport in df["sport"].unique():
        sport_df = df[df["sport"] == sport]
        indices = sport_df.index.tolist()

        # Build time-proximity groups (sliding window)
        groups: list[list[int]] = []
        i = 0
        while i < len(indices):
            group = [indices[i]]
            j = i + 1
            anchor_time = pd.Timestamp(sport_df.loc[indices[i], "start_datetime"])
            while j < len(indices):
                cand_time = pd.Timestamp(sport_df.loc[indices[j], "start_datetime"])
                if (cand_time - anchor_time) <= _DEDUP_WINDOW:
                    group.append(indices[j])
                    j += 1
                else:
                    break
            groups.append(group)
            i = j if j > i + 1 else i + 1

        for group in groups:
            if len(group) < 2:
                continue

            pair_clusters: dict[int, set[int]] = {}
            for a_pos in range(len(group)):
                for b_pos in range(a_pos + 1, len(group)):
                    idx_a = group[a_pos]
                    idx_b = group[b_pos]
                    if idx_a in consumed or idx_b in consumed:
                        continue
                    row_a = df.loc[idx_a]
                    row_b = df.loc[idx_b]
                    if row_a["source"] == row_b["source"]:
                        continue
                    score = compute_match_score(row_a, row_b)
                    if score >= config.fuzzy_threshold:
                        cluster_a = pair_clusters.get(idx_a)
                        cluster_b = pair_clusters.get(idx_b)
                        if cluster_a is None and cluster_b is None:
                            new_cluster = {idx_a, idx_b}
                            pair_clusters[idx_a] = new_cluster
                            pair_clusters[idx_b] = new_cluster
                        elif cluster_a is not None and cluster_b is None:
                            cluster_a.add(idx_b)
                            pair_clusters[idx_b] = cluster_a
                        elif cluster_a is None and cluster_b is not None:
                            cluster_b.add(idx_a)
                            pair_clusters[idx_a] = cluster_b
                        else:
                            merged = cluster_a | cluster_b
                            for k in merged:
                                pair_clusters[k] = merged

            seen_clusters: list[set[int]] = []
            seen_ids: set[int] = set()
            for idx, cluster in pair_clusters.items():
                if id(cluster) not in seen_ids:
                    seen_clusters.append(cluster)
                    seen_ids.add(id(cluster))

            for cluster in seen_clusters:
                cluster_indices = sorted(cluster)
                events = [df.loc[ci].to_dict() for ci in cluster_indices]
                scores: list[float] = []
                for a_pos in range(len(cluster_indices)):
                    for b_pos in range(a_pos + 1, len(cluster_indices)):
                        scores.append(
                            compute_match_score(
                                df.loc[cluster_indices[a_pos]],
                                df.loc[cluster_indices[b_pos]],
                            )
                        )

                retained = events[0]
                discarded_ids = [
                    str(e.get("original_event_id", ""))
                    for e in events[1:]
                ]

                dup_set = DuplicateSet(
                    events=events,
                    retained_event=retained,
                    similarity_scores=scores,
                    retained_provider=str(retained.get("source", "")),
                    discarded_event_ids=discarded_ids,
                )
                duplicate_sets.append(dup_set)
                consumed.update(cluster_indices)

                logger.info(
                    "Duplicate set: providers=%s ids=%s scores=%s",
                    [e.get("source") for e in events],
                    [e.get("original_event_id") for e in events],
                    [round(s, 3) for s in scores],
                )

    return duplicate_sets


# ---------------------------------------------------------------------------
# deduplicate (main entry point)
# ---------------------------------------------------------------------------

def deduplicate(
    df: pd.DataFrame,
    config: PipelineConfig,
) -> tuple[pd.DataFrame, list[DuplicateSet]]:
    """Run deduplication and return cleaned DataFrame plus duplicate records.

    THE KEY FIX: After finding duplicate clusters, we resolve priority BEFORE
    deciding which row to keep.  The old code always kept events[0] (arbitrary
    sort order), which could be a lower-priority provider — then assign_channel
    tried to annotate the priority winner but that row had already been dropped.

    Now we:
    1. Find duplicate clusters (unchanged).
    2. Call resolve_priority on each cluster to identify the true winner.
    3. Keep ONLY the winner's row in the DataFrame; discard the rest.
    4. assign_channel (called later in check_engine) will find the winner's
       row still present and correctly annotate it.
    """
    from pipeline.priority import resolve_priority, _sport_from_str, _source_to_key

    dup_sets = find_duplicates(df, config)

    if not dup_sets:
        return df.copy(), dup_sets

    indices_to_drop: set[int] = set()

    for ds in dup_sets:
        cluster_events = ds.events          # list of dicts, all events in the cluster
        if not cluster_events:
            continue

        sport_str = cluster_events[0].get("sport", "")
        sport_enum = _sport_from_str(sport_str)

        # Build candidate list for priority resolution
        candidates = []
        for ev in cluster_events:
            candidates.append({
                "source":             ev.get("source", ""),
                "info":               ev.get("info", ""),
                "sport":              sport_str,
                "feed_type":          ev.get("feed_type", ""),
                "original_event_id":  ev.get("original_event_id", ""),
            })

        # Resolve priority — pick the true winner
        if sport_enum and len(candidates) > 1:
            winner = resolve_priority(sport_enum, candidates)
        else:
            winner = candidates[0] if candidates else {}

        winning_source = winner.get("source", "")
        winning_eid    = winner.get("original_event_id", "")

        # Update DuplicateSet metadata so assign_channel knows who won
        ds.retained_provider   = winning_source
        ds.retained_event      = winner
        ds.discarded_event_ids = [
            str(ev.get("original_event_id", ""))
            for ev in cluster_events
            if ev.get("original_event_id") != winning_eid
            or ev.get("source") != winning_source
        ]

        # Mark every non-winner row for deletion
        for ev in cluster_events:
            if ev.get("original_event_id") == winning_eid and ev.get("source") == winning_source:
                continue   # keep the winner
            eid    = ev.get("original_event_id")
            source = ev.get("source")
            matches = df[
                (df["original_event_id"] == eid) & (df["source"] == source)
            ].index
            indices_to_drop.update(matches.tolist())

        logger.info(
            "Dedup winner for %s cluster: %s (%s) — discarded %s",
            sport_str, winning_source, winning_eid, ds.discarded_event_ids,
        )

    deduped = df.drop(index=list(indices_to_drop)).reset_index(drop=True)
    return deduped, dup_sets
