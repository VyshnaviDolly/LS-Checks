"""Provider priority engine for sport-specific feed/channel assignment.

Implements the priority resolution logic described in Requirements 5.1–5.10:
- Default sport priority orders
- Info column (TV vs Venue) override rules
- Basketball all-TV BR exception
- Ops-feed Venue override
- Ice Hockey auto>ops special case
"""

import logging
from enum import Enum

import pandas as pd

from config import Sport

logger = logging.getLogger("pipeline.priority")


class FeedType(Enum):
    AUTO = "auto"
    OPS = "ops"


# ---------------------------------------------------------------------------
# Mapping between DataFrame source names and priority key abbreviations.
# The normalised DataFrame uses "Rball" as the source; priority tables use "RB".
# ---------------------------------------------------------------------------
_SOURCE_TO_KEY: dict[str, str] = {
    "BR": "BR",
    "BG": "BG",
    "Rball": "RB",
}

_KEY_TO_SOURCE: dict[str, str] = {v: k for k, v in _SOURCE_TO_KEY.items()}


# ---------------------------------------------------------------------------
# Provider priority order per sport (highest priority first).
# Keys are the short abbreviations used internally.
# ---------------------------------------------------------------------------
SPORT_PRIORITY: dict[Sport, list[tuple[str, FeedType]]] = {
    Sport.BASKETBALL: [("BG", FeedType.AUTO), ("BR", FeedType.AUTO), ("RB", FeedType.OPS)],
    Sport.VOLLEYBALL: [("BR", FeedType.AUTO), ("BG", FeedType.AUTO)],
    Sport.HANDBALL:   [("BR", FeedType.AUTO), ("BG", FeedType.AUTO)],
    Sport.ICE_HOCKEY: [("BR", FeedType.AUTO), ("RB", FeedType.OPS), ("BG", FeedType.OPS)],
    Sport.BASEBALL:   [("BR", FeedType.AUTO), ("BG", FeedType.AUTO)],
}


# ---------------------------------------------------------------------------
# Feed classification: (provider_key, sport) → FeedType
# ---------------------------------------------------------------------------
FEED_CLASSIFICATION: dict[tuple[str, Sport], FeedType] = {
    # Basketball
    ("BG", Sport.BASKETBALL): FeedType.AUTO,
    ("BR", Sport.BASKETBALL): FeedType.AUTO,
    ("RB", Sport.BASKETBALL): FeedType.OPS,
    # Volleyball
    ("BR", Sport.VOLLEYBALL): FeedType.AUTO,
    ("BG", Sport.VOLLEYBALL): FeedType.AUTO,
    # Handball
    ("BR", Sport.HANDBALL): FeedType.AUTO,
    ("BG", Sport.HANDBALL): FeedType.AUTO,
    # Ice Hockey
    ("BR", Sport.ICE_HOCKEY): FeedType.AUTO,
    ("RB", Sport.ICE_HOCKEY): FeedType.OPS,
    ("BG", Sport.ICE_HOCKEY): FeedType.OPS,
    # Baseball
    ("BR", Sport.BASEBALL): FeedType.AUTO,
    ("BG", Sport.BASEBALL): FeedType.AUTO,
}


def _source_to_key(source: str) -> str:
    """Convert a DataFrame source name to the priority key abbreviation."""
    return _SOURCE_TO_KEY.get(source, source)


def _key_to_source(key: str) -> str:
    """Convert a priority key abbreviation back to the DataFrame source name."""
    return _KEY_TO_SOURCE.get(key, key)


def _sport_from_str(sport_str: str) -> Sport | None:
    """Resolve a sport string to a Sport enum member, or None."""
    for s in Sport:
        if s.value.lower() == sport_str.strip().lower():
            return s
    return None


def _priority_index(sport: Sport, provider_key: str) -> int:
    """Return the 0-based priority index for *provider_key* in *sport*.

    Lower index = higher priority.  Returns a large number if the provider
    is not listed for the sport.
    """
    order = SPORT_PRIORITY.get(sport, [])
    for i, (key, _ft) in enumerate(order):
        if key == provider_key:
            return i
    return 999


def _get_feed_type(provider_key: str, sport: Sport) -> FeedType:
    """Look up the feed type for a (provider, sport) pair."""
    return FEED_CLASSIFICATION.get((provider_key, sport), FeedType.AUTO)


# ---------------------------------------------------------------------------
# classify_info
# ---------------------------------------------------------------------------

def classify_info(info_value: str | None) -> str:
    """Map an Info column value to ``"Venue"`` or ``"TV"``.

    * ``"Venue"``, ``"Stadium"``, blank, or ``None`` → ``"Venue"``
    * ``"TV"`` → ``"TV"``

    Any other value is treated as ``"Venue"`` (conservative default).
    """
    if info_value is None:
        return "Venue"
    val = str(info_value).strip().lower()
    if val in ("", "venue", "stadium"):
        return "Venue"
    if val == "tv":
        return "TV"
    # Fallback: treat unknown values as Venue
    return "Venue"


# ---------------------------------------------------------------------------
# resolve_priority
# ---------------------------------------------------------------------------

def resolve_priority(sport: Sport, candidates: list[dict]) -> dict:
    """Select the winning candidate from a duplicate set for *sport*.

    Each candidate dict must contain at minimum:
        source          – DataFrame source name ("BR", "BG", "Rball")
        info            – raw Info column value
        sport           – sport string
        feed_type       – "auto" or "ops"
        original_event_id – provider event id

    Resolution rules (applied in order):
    1. Single candidate → return immediately.
    2. Classify each candidate's Info as "Venue" or "TV".
    3. **Ice Hockey special case (Req 5.9):** When all three providers
       (BR, RB, BG) are present, auto-feed BR wins outright.  TV vs Venue
       priority applies only between the two ops-feed providers (RB, BG).
    4. **Ops-feed Venue override (Req 5.8):** If a lower-priority ops-feed
       provider has Venue and all higher-priority auto-feed providers have
       TV, select the ops-feed provider.
    5. **General rule (Req 5.6):** Prefer Venue over TV regardless of
       default priority.
    6. **Basketball all-TV exception (Req 5.7):** When ALL candidates are
       TV for Basketball, prefer BR.
    7. Within the same Info group, fall back to default sport priority order.
    """
    if len(candidates) <= 1:
        return candidates[0] if candidates else {}

    # Annotate each candidate with derived fields.
    for c in candidates:
        c["_key"] = _source_to_key(c.get("source", ""))
        c["_info_class"] = classify_info(c.get("info"))
        c["_feed_type"] = _get_feed_type(c["_key"], sport)
        c["_priority"] = _priority_index(sport, c["_key"])

    provider_keys = {c["_key"] for c in candidates}

    # --- Ice Hockey special case (Req 5.9) ---
    # When all three providers cover an event, auto-feed (BR) wins outright.
    # TV vs Venue priority applies only between RB and BG (both ops-feed).
    if sport == Sport.ICE_HOCKEY and {"BR", "RB", "BG"}.issubset(provider_keys):
        br_candidates = [c for c in candidates if c["_key"] == "BR"]
        if br_candidates:
            winner = br_candidates[0]
            _cleanup(candidates)
            return winner

    # --- Separate into Venue and TV groups ---
    venue_group = [c for c in candidates if c["_info_class"] == "Venue"]
    tv_group = [c for c in candidates if c["_info_class"] == "TV"]

    # --- Basketball all-TV exception (Req 5.7) ---
    if sport == Sport.BASKETBALL and len(tv_group) == len(candidates):
        # All candidates are TV → prefer BR
        br_tv = [c for c in tv_group if c["_key"] == "BR"]
        if br_tv:
            winner = br_tv[0]
            _cleanup(candidates)
            return winner
        # If BR not present, fall back to default priority among TV
        winner = _pick_by_priority(tv_group)
        _cleanup(candidates)
        return winner

    # --- Ops-feed Venue override (Req 5.8) ---
    # If a lower-priority ops-feed has Venue and ALL higher-priority auto-feed
    # providers have TV, select the ops-feed with Venue.
    ops_venue = [c for c in venue_group if c["_feed_type"] == FeedType.OPS]
    auto_candidates = [c for c in candidates if c["_feed_type"] == FeedType.AUTO]
    if ops_venue and auto_candidates:
        all_auto_tv = all(c["_info_class"] == "TV" for c in auto_candidates)
        if all_auto_tv:
            winner = _pick_by_priority(ops_venue)
            _cleanup(candidates)
            return winner

    # --- General rule (Req 5.6): Prefer Venue over TV ---
    if venue_group:
        winner = _pick_by_priority(venue_group)
        _cleanup(candidates)
        return winner

    # All TV — fall back to default priority
    winner = _pick_by_priority(tv_group if tv_group else candidates)
    _cleanup(candidates)
    return winner


def _pick_by_priority(group: list[dict]) -> dict:
    """Return the candidate with the highest (lowest index) sport priority."""
    return min(group, key=lambda c: c["_priority"])


def _cleanup(candidates: list[dict]) -> None:
    """Remove temporary annotation keys from candidate dicts."""
    for c in candidates:
        c.pop("_key", None)
        c.pop("_info_class", None)
        c.pop("_feed_type", None)
        c.pop("_priority", None)


# ---------------------------------------------------------------------------
# assign_channel
# ---------------------------------------------------------------------------

def assign_channel(
    events: pd.DataFrame,
    duplicate_sets: list | None = None,
) -> pd.DataFrame:
    """Resolve provider priority for each duplicate set and annotate winners.

    Parameters
    ----------
    events : pd.DataFrame
        The full normalised events DataFrame (may include non-duplicated events).
    duplicate_sets : list[DuplicateSet] | None
        Duplicate sets produced by :func:`pipeline.deduplicate.find_duplicates`.
        Each set has an ``events`` list of dicts and a ``retained_event`` dict.

    Returns
    -------
    pd.DataFrame
        A copy of *events* with ``assigned_channel`` and ``feed_type`` updated
        for the winning provider in each duplicate set.  Non-duplicate events
        keep their original values.
    """
    from pipeline.deduplicate import DuplicateSet  # avoid circular import

    result = events.copy()

    if not duplicate_sets:
        # No duplicates — just ensure feed_type is correct per classification.
        result = _update_feed_types(result)
        return result

    for dup_set in duplicate_sets:
        if not isinstance(dup_set, DuplicateSet):
            continue

        dup_events = dup_set.events
        if not dup_events:
            continue

        # Determine sport for this group.
        sport_str = dup_events[0].get("sport", "")
        sport = _sport_from_str(sport_str)
        if sport is None:
            logger.warning(
                "Cannot resolve priority — unknown sport '%s' for event ids %s",
                sport_str,
                [e.get("original_event_id") for e in dup_events],
            )
            continue

        # Build candidate list with required fields.
        candidates = []
        for ev in dup_events:
            candidates.append({
                "source": ev.get("source", ""),
                "info": ev.get("info", ""),
                "sport": ev.get("sport", ""),
                "feed_type": ev.get("feed_type", ""),
                "original_event_id": ev.get("original_event_id", ""),
            })

        winner = resolve_priority(sport, candidates)
        winning_source = winner.get("source", "")
        winning_eid = winner.get("original_event_id", "")
        winning_key = _source_to_key(winning_source)
        winning_feed = _get_feed_type(winning_key, sport)

        # Update the DuplicateSet metadata.
        dup_set.retained_provider = winning_source
        dup_set.retained_event = winner

        # Collect all discarded event ids.
        dup_set.discarded_event_ids = [
            str(e.get("original_event_id", ""))
            for e in dup_events
            if e.get("original_event_id") != winning_eid
            or e.get("source") != winning_source
        ]

        # Annotate the winning event row in the DataFrame.
        mask = (
            (result["source"] == winning_source)
            & (result["original_event_id"] == winning_eid)
        )
        if mask.any():
            result.loc[mask, "assigned_channel"] = winning_source
            result.loc[mask, "feed_type"] = winning_feed.value

        logger.info(
            "Priority resolved for %s duplicate set: winner=%s (%s), "
            "discarded=%s",
            sport_str,
            winning_source,
            winning_eid,
            dup_set.discarded_event_ids,
        )

    # Ensure feed_type is correct for all rows per classification.
    result = _update_feed_types(result)
    return result


def _update_feed_types(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure ``feed_type`` matches FEED_CLASSIFICATION for every row."""
    if df.empty:
        return df
    for idx, row in df.iterrows():
        key = _source_to_key(str(row.get("source", "")))
        sport = _sport_from_str(str(row.get("sport", "")))
        if sport is not None:
            ft = FEED_CLASSIFICATION.get((key, sport))
            if ft is not None:
                df.at[idx, "feed_type"] = ft.value
    return df
