"""Blacklist filtering for the Live Scheduling ETL Pipeline.

The Blacklisted.xlsx has per-sport tabs. Each tab has Region and League columns.
Matching rules:
- If League says "All leagues" or "All Leagues" → block ALL events from that Region
- Otherwise → block events matching that specific Region + League combination
- Some entries have notes/exceptions in the League column — we extract the core pattern
"""

import logging
import pandas as pd
from rapidfuzz.fuzz import token_sort_ratio

logger = logging.getLogger("pipeline.blacklist")

_FUZZY_THRESHOLD = 75.0


def load_blacklist(filepath: str) -> dict[str, pd.DataFrame]:
    """Parse Blacklisted file — each sheet is a sport name."""
    if not filepath:
        return {}
    try:
        sheets = pd.read_excel(filepath, sheet_name=None, engine="openpyxl", dtype=str)
    except Exception:
        try:
            sheets = pd.read_excel(filepath, sheet_name=None, engine="xlrd", dtype=str)
        except Exception:
            logger.warning("Could not read blacklist file: %s", filepath, exc_info=True)
            return {}

    blacklist = {}
    for sport_name, df in sheets.items():
        df.columns = [str(c).strip() for c in df.columns]
        df = df.fillna("")
        blacklist[str(sport_name).strip()] = df
        logger.info("Loaded %d blacklist entries for '%s'", len(df), sport_name)
    return blacklist


def _normalize(val: str) -> str:
    return str(val).strip().lower()


def _is_all_leagues(league_val: str) -> bool:
    """Check if the blacklist entry means 'all leagues' for that region."""
    val = _normalize(league_val)
    return (
        val.startswith("all league")
        or val == ""
        or "all leagues" in val
    )


def _region_matches(event_region: str, bl_region: str) -> bool:
    """Check if event region matches blacklist region (fuzzy)."""
    er = _normalize(event_region)
    br = _normalize(bl_region)
    if not er or not br:
        return False
    # Exact match first
    if er == br:
        return True
    # Check if one contains the other
    if er in br or br in er:
        return True
    # Fuzzy match
    return token_sort_ratio(er, br) >= _FUZZY_THRESHOLD


def _league_matches(event_league: str, bl_league: str) -> bool:
    """Check if event league matches blacklist league entry (fuzzy)."""
    el = _normalize(event_league)
    bl = _normalize(bl_league)
    if not el or not bl:
        return False
    if el == bl:
        return True
    if el in bl or bl in el:
        return True
    return token_sort_ratio(el, bl) >= _FUZZY_THRESHOLD


def _find_region_col(df: pd.DataFrame) -> str | None:
    """Find the Region column in the blacklist DataFrame."""
    for col in df.columns:
        if _normalize(col) == "region":
            return col
    return None


def _find_league_col(df: pd.DataFrame) -> str | None:
    """Find the League column in the blacklist DataFrame."""
    for col in df.columns:
        if _normalize(col) == "league":
            return col
    return None


def apply_blacklist(
    events: pd.DataFrame,
    blacklist: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Filter events against the sport-specific blacklist.

    For each event:
    1. Find the matching sport tab in the blacklist
    2. Check if the event's Region matches any blacklist Region
    3. If the blacklist League is "All leagues" → remove the event
    4. If the blacklist League is specific → check if event League matches
    """
    if not blacklist or events.empty:
        return events.copy(), pd.DataFrame(columns=events.columns)

    keep_mask = pd.Series(True, index=events.index)

    for idx, row in events.iterrows():
        sport = str(row.get("sport", "")).strip()
        event_region = str(row.get("region", "")).strip()
        event_league = str(row.get("league", "")).strip()

        # Find matching sport tab (case-insensitive)
        bl_df = None
        for bl_sport, bl_data in blacklist.items():
            if _normalize(bl_sport) == _normalize(sport):
                bl_df = bl_data
                break

        if bl_df is None or bl_df.empty:
            continue

        region_col = _find_region_col(bl_df)
        league_col = _find_league_col(bl_df)

        if region_col is None:
            continue

        for _, bl_row in bl_df.iterrows():
            bl_region = str(bl_row.get(region_col, "")).strip()
            bl_league = str(bl_row.get(league_col, "")).strip() if league_col else ""

            if not _region_matches(event_region, bl_region):
                continue

            # Region matches — check league rule
            if _is_all_leagues(bl_league):
                # All leagues blocked for this region
                keep_mask.at[idx] = False
                logger.info(
                    "Blacklisted (region=%s, all leagues): %s - %s",
                    bl_region, event_region, row.get("event_name", ""),
                )
                break
            elif _league_matches(event_league, bl_league):
                # Specific league blocked
                keep_mask.at[idx] = False
                logger.info(
                    "Blacklisted (region=%s, league=%s): %s - %s",
                    bl_region, bl_league, event_region, row.get("event_name", ""),
                )
                break

    filtered = events[keep_mask].reset_index(drop=True)
    removed = events[~keep_mask].reset_index(drop=True)
    return filtered, removed
