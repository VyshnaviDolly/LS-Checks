"""Check engine — orchestrates the full pipeline run.

Correct execution order (matches requirements diagram):
1.  Ingest + Normalize              (done in app.py before calling run_check)
2.  Deduplicate across providers    (deduplicate.py → find_duplicates + deduplicate)
3.  Assign priority / channel       (priority.py → assign_channel)
4.  Compare with LBC reference      (this module)
    a. Time Changes  — same event, different time
    b. Channel Changes — same event, LBC has different provider than priority winner
    c. No Source — LBC event not found in any provider
5.  Identify Additions              — provider events not in LBC
6.  Deduplicate Additions           — cross-provider dupes in additions list
7.  Blacklist filter on Additions   (blacklist.py → apply_blacklist)
8.  Build CheckResult + Summary
"""

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta, timezone

import pandas as pd
from rapidfuzz.fuzz import token_sort_ratio

from config import (
    CHECK_SPORT_SCOPE,
    CHECK_USES_REFERENCE,
    CheckType,
    PipelineConfig,
    Sport,
)
from pipeline.blacklist import apply_blacklist
from pipeline.deduplicate import (
    DuplicateSet,
    deduplicate,
    normalize_team_name,
    normalize_competition_name,
    detect_gender,
)
from pipeline.priority import (
    assign_channel,
    resolve_priority,
    SPORT_PRIORITY,
    _source_to_key,
    _sport_from_str,
)

logger = logging.getLogger("pipeline.check_engine")

UTC_PLUS_2 = timezone(timedelta(hours=2))

# Match threshold for LBC ↔ provider comparison
# Slightly lower than dedup threshold to catch events with different naming
_MATCH_THRESHOLD = 0.35

# Region normalization map
_REGION_MAP: dict[str, str] = {
    "türkiye": "turkiye", "turkey": "turkiye",
    "north macedonia": "macedonia",
    "czech republic": "czechia",
    "south korea": "korea", "korea republic": "korea",
    "usa": "usa", "united states": "usa",
    "uae": "uae", "united arab emirates": "uae",
    "uk": "uk", "united kingdom": "uk", "great britain": "uk",
    "chinese taipei": "chinese taipei", "taiwan": "chinese taipei",
}

_WILDCARD_REGIONS = {"europe", "international", ""}


# ---------------------------------------------------------------------------
# CheckResult
# ---------------------------------------------------------------------------

@dataclass
class CheckResult:
    check_type: CheckType
    target_date: date
    sports_processed: set
    duplicates_removed: pd.DataFrame
    blacklisted_removed: pd.DataFrame
    time_changes: pd.DataFrame
    channel_changes: pd.DataFrame
    additions: pd.DataFrame
    no_source: pd.DataFrame
    summary: dict
    duplicate_sets: list | None = None


# ---------------------------------------------------------------------------
# Match scoring (LBC ↔ provider comparison)
# Uses alias-resolved team names from deduplicate.py
# ---------------------------------------------------------------------------

def _normalize_region(region: str) -> str:
    r = str(region).strip().lower()
    return _REGION_MAP.get(r, r)


def _match_score(row_a: pd.Series, row_b: pd.Series) -> float:
    """Score similarity between two events for LBC ↔ provider matching.

    Uses alias-resolved team names (via normalize_team_name from deduplicate.py)
    and gender detection to block men's vs women's false matches.
    """
    region_a = _normalize_region(str(row_a.get("region", "")))
    region_b = _normalize_region(str(row_b.get("region", "")))

    is_wildcard = region_a in _WILDCARD_REGIONS or region_b in _WILDCARD_REGIONS
    region_match = (region_a == region_b) or is_wildcard

    if not region_match:
        return 0.0

    # Gender mismatch — hard block using improved detect_gender from deduplicate.py.
    # Check event_name + league together so that "Nationale 1, Women" (league only)
    # is correctly detected as women's even when team names are gender-neutral.
    gender_a = detect_gender(f"{row_a.get('event_name', '')} {row_a.get('league', '')}")
    gender_b = detect_gender(f"{row_b.get('event_name', '')} {row_b.get('league', '')}")
    # Hard block when both are known and differ.
    if gender_a != "unknown" and gender_b != "unknown" and gender_a != gender_b:
        return 0.0
    # Also block when ONE side is clearly gendered and the other is neutral (unknown):
    # e.g. BR "Nationale 1" (unknown) vs LBC "Nationale 1, Women" (women) → false match.
    if (gender_a == "women" and gender_b == "unknown") or (gender_b == "women" and gender_a == "unknown"):
        # Only suppress if the clearly-gendered side is women and the league names
        # are similar (meaning they really are the same competition, just different genders).
        league_raw_a = str(row_a.get("league", ""))
        league_raw_b = str(row_b.get("league", ""))
        league_sim_check = token_sort_ratio(
            normalize_competition_name(league_raw_a),
            normalize_competition_name(league_raw_b),
        ) / 100.0
        if league_sim_check >= 0.5:
            return 0.0

    # Alias-resolved team names
    home_a = normalize_team_name(str(row_a.get("home_team", "")))
    home_b = normalize_team_name(str(row_b.get("home_team", "")))
    away_a = normalize_team_name(str(row_a.get("away_team", "")))
    away_b = normalize_team_name(str(row_b.get("away_team", "")))

    min_team = 0.35

    # Normal order
    h1 = token_sort_ratio(home_a, home_b) / 100.0 if home_a and home_b else 0.0
    a1 = token_sort_ratio(away_a, away_b) / 100.0 if away_a and away_b else 0.0
    s1 = (h1 + a1) / 2.0 if (h1 >= min_team and a1 >= min_team) else 0.0

    # Swapped order
    h2 = token_sort_ratio(home_a, away_b) / 100.0 if home_a and away_b else 0.0
    a2 = token_sort_ratio(away_a, home_b) / 100.0 if away_a and home_b else 0.0
    s2 = (h2 + a2) / 2.0 if (h2 >= min_team and a2 >= min_team) else 0.0

    score = max(s1, s2)

    # Wildcard regions need higher threshold
    if is_wildcard and 0.0 < score < 0.50:
        score = 0.0

    # Fallback: one team very strongly matches
    if score == 0.0 and not is_wildcard:
        best = max(h1, a1, h2, a2)
        if best >= 0.80:
            score = best * 0.40

    # Fallback 2: same region + similar league + close time.
    # Guard: skip if leagues differ only by a gender keyword (e.g. "Nationale 1" vs
    # "Nationale 1 Women") — the gender hard-block above handles this, but
    # normalize_competition_name strips punctuation without removing "women", so
    # the league_sim would be high enough to fire falsely without this guard.
    if score == 0.0 and region_a == region_b and region_a not in _WILDCARD_REGIONS:
        league_a = normalize_competition_name(str(row_a.get("league", "")))
        league_b = normalize_competition_name(str(row_b.get("league", "")))
        _gender_words = {"women", "ladies", "girls", "w", "wfc", "femenino", "femenina",
                         "frauen", "damen", "femmes", "feminino", "donne", "kvinder", "naiset"}
        words_a = set(league_a.split())
        words_b = set(league_b.split())
        _only_in_a = words_a - words_b
        _only_in_b = words_b - words_a
        _gender_diff = bool((_only_in_a & _gender_words) or (_only_in_b & _gender_words))
        if not _gender_diff:
            league_sim = token_sort_ratio(league_a, league_b) / 100.0 if league_a and league_b else 0.0
            if league_sim >= 0.5:
                try:
                    t_a = pd.Timestamp(row_a.get("start_datetime"))
                    t_b = pd.Timestamp(row_b.get("start_datetime"))
                    diff = abs((t_a - t_b).total_seconds()) / 60.0
                    if diff <= 30:
                        best_team = max(h1, a1, h2, a2)
                        # Raised from 0.30 → 0.45 to reduce borderline false positives
                        if best_team >= 0.45:
                            score = 0.40
                except Exception:
                    pass

    return score


def _find_best_match(
    event: pd.Series,
    candidates: pd.DataFrame,
) -> tuple[float, int | None]:
    """Find the best matching event in candidates DataFrame."""
    best_score = 0.0
    best_idx = None
    for idx, cand in candidates.iterrows():
        score = _match_score(event, cand)
        if score > best_score:
            best_score = score
            best_idx = idx
        if best_score >= 0.95:
            break
    return best_score, best_idx


# ---------------------------------------------------------------------------
# Additions deduplication using priority resolution
# ---------------------------------------------------------------------------

def _deduplicate_additions_with_priority(
    additions: pd.DataFrame,
    config: PipelineConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Deduplicate additions across providers AND apply priority rules.

    Unlike the old implementation which kept the first encountered event,
    this uses resolve_priority() from priority.py to pick the correct winner
    based on sport-specific feed priority and Venue/TV rules.
    """
    if additions.empty or len(additions) <= 1:
        return additions.copy(), pd.DataFrame(columns=additions.columns)

    keep = pd.Series(True, index=additions.index)
    consumed: set[int] = set()

    # Group by sport for priority resolution
    for sport_str in additions["sport"].unique():
        sport_df = additions[additions["sport"] == sport_str]
        sport_enum = _sport_from_str(sport_str)
        indices = sport_df.index.tolist()

        for i_pos, idx_a in enumerate(indices):
            if idx_a in consumed:
                continue
            row_a = additions.loc[idx_a]
            cluster = [idx_a]

            for idx_b in indices[i_pos + 1:]:
                if idx_b in consumed:
                    continue
                row_b = additions.loc[idx_b]
                if row_a["source"] == row_b["source"]:
                    continue
                score = _match_score(row_a, row_b)
                if score >= _MATCH_THRESHOLD:
                    cluster.append(idx_b)

            if len(cluster) > 1:
                # Build candidates for priority resolution
                candidates = []
                for ci in cluster:
                    r = additions.loc[ci]
                    candidates.append({
                        "source": str(r.get("source", "")),
                        "info": str(r.get("info", "")),
                        "sport": str(r.get("sport", "")),
                        "feed_type": str(r.get("feed_type", "")),
                        "original_event_id": str(r.get("original_event_id", "")),
                        "_cluster_idx": ci,
                    })

                if sport_enum:
                    winner = resolve_priority(sport_enum, candidates)
                else:
                    winner = candidates[0]

                winning_idx = winner.get("_cluster_idx", cluster[0])

                # Discard all except winner
                for ci in cluster:
                    if ci != winning_idx:
                        keep.at[ci] = False
                        consumed.add(ci)
                consumed.add(winning_idx)

    deduped = additions[keep].reset_index(drop=True)
    removed = additions[~keep].reset_index(drop=True)
    return deduped, removed


# ---------------------------------------------------------------------------
# Channel change detection
# ---------------------------------------------------------------------------

def _detect_channel_changes(
    provider_events: pd.DataFrame,
    reference: pd.DataFrame,
    lbc_to_winner: dict[int, int],
    lbc_providers: dict[int, list[tuple[int, float, str]]] | None = None,
) -> list[dict]:
    """Detect events where the priority-resolved provider differs from LBC.

    lbc_to_winner: maps lbc_idx → winning provider event idx
    lbc_providers: maps lbc_idx → list of (prov_idx, score, source) for ALL
                   providers that matched this LBC event.  Used to suppress
                   false-positive channel changes: if the LBC's booked channel
                   also matched this event (dedup just failed to group them),
                   there is no real channel change — the correct provider is
                   present and LBC already has it right.
    """
    channel_changes = []
    lbc_providers = lbc_providers or {}

    for lbc_idx, prov_idx in lbc_to_winner.items():
        lbc_row = reference.loc[lbc_idx]
        prov_row = provider_events.loc[prov_idx]

        lbc_source = str(lbc_row.get("source", "")).strip().upper()
        prov_source = str(prov_row.get("source", "")).strip().upper()
        prov_assigned = str(prov_row.get("assigned_channel", "")).strip().upper()

        # Use assigned_channel if set by priority engine, else source
        winner_channel = prov_assigned if prov_assigned else prov_source

        # lbc_channel must come from assigned_channel (the actual provider BG/BR/Rball
        # stored during reference normalization), NOT from source (which is always "LBC").
        lbc_assigned = str(lbc_row.get("assigned_channel", "")).strip().upper()
        lbc_channel = lbc_assigned if lbc_assigned else lbc_source

        # No difference → nothing to report
        if not winner_channel or not lbc_channel or winner_channel == lbc_channel:
            continue

        # Suppress false positive: if the LBC's booked provider ALSO matched
        # this event (it just wasn't grouped via dedup due to naming differences),
        # the priority engine already has the correct provider available and LBC
        # is already correct — do not flag as a channel change.
        #
        # Additionally: if ALL providers that matched this LBC event agree that
        # the LBC's current channel is the correct priority winner, suppress.
        matched_sources = {
            src.strip().upper()
            for _, _, src in lbc_providers.get(lbc_idx, [])
        }
        if lbc_channel in matched_sources:
            logger.debug(
                "Suppressing false channel change for '%s': LBC has %s, winner=%s "
                "but %s also matched this event — LBC is already correct.",
                lbc_row.get("event_name", ""), lbc_channel, winner_channel, lbc_channel,
            )
            continue

        channel_changes.append({
            "source": prov_row.get("source", ""),
            "original_event_id": prov_row.get("original_event_id", ""),
            "sport": prov_row.get("sport", ""),
            "region": prov_row.get("region", ""),
            "league": prov_row.get("league", ""),
            "event_name": prov_row.get("event_name", ""),
            "start_datetime": prov_row.get("start_datetime"),
            "assigned_channel": winner_channel,
            "lbc_channel": lbc_channel,
            "info": prov_row.get("info", ""),
        })

    return channel_changes


# ---------------------------------------------------------------------------
# Main run_check function
# ---------------------------------------------------------------------------

def run_check(
    check_type: CheckType,
    provider_events: pd.DataFrame,
    reference: pd.DataFrame | None,
    blacklist: dict[str, pd.DataFrame] | None,
    config: PipelineConfig,
) -> CheckResult:
    """Execute a full pipeline check run.

    Pipeline order:
    1. Deduplicate provider events (cross-provider fuzzy + alias matching)
    2. Assign priority/channel to deduplicated events
    3. Compare with LBC reference → time changes, channel changes, no source
    4. Identify additions (provider events not in LBC)
    5. Deduplicate additions with priority resolution
    6. Blacklist filter on additions
    """
    target = date.fromisoformat(config.date_override) if config.date_override else date.today()
    sports = CHECK_SPORT_SCOPE.get(check_type, set())
    uses_ref = CHECK_USES_REFERENCE.get(check_type, False)

    logger.info(
        "=== run_check START: type=%s date=%s uses_ref=%s ===",
        check_type.value, target, uses_ref,
    )
    logger.info(
        "Provider events: %d | Reference events: %d",
        len(provider_events),
        len(reference) if reference is not None else 0,
    )

    empty_df = pd.DataFrame()

    # ── STEP 1: Deduplicate provider events ──────────────────────────────────
    logger.info("Step 1: Deduplicating provider events...")
    if not provider_events.empty:
        deduped_providers, dup_sets = deduplicate(provider_events, config)
        duplicates_removed_df = provider_events[
            ~provider_events.index.isin(deduped_providers.index)
        ].reset_index(drop=True) if len(deduped_providers) < len(provider_events) else pd.DataFrame(columns=provider_events.columns)
    else:
        deduped_providers = provider_events.copy()
        dup_sets = []
        duplicates_removed_df = pd.DataFrame(columns=provider_events.columns if not provider_events.empty else [])

    # Collect all discarded rows for the duplicates_removed sheet
    if dup_sets:
        discarded_rows = []
        for ds in dup_sets:
            for ev in ds.events[1:]:
                discarded_rows.append(ev)
        if discarded_rows:
            duplicates_removed_df = pd.DataFrame(discarded_rows)

    logger.info(
        "Step 1 complete: %d events after dedup, %d duplicates removed",
        len(deduped_providers), len(duplicates_removed_df),
    )

    # ── STEP 2: Assign priority / channel ────────────────────────────────────
    logger.info("Step 2: Assigning priority channels...")
    if not deduped_providers.empty and dup_sets:
        deduped_providers = assign_channel(deduped_providers, dup_sets)
    elif not deduped_providers.empty:
        # No duplicate sets but still update feed_types
        deduped_providers = assign_channel(deduped_providers, [])
    logger.info("Step 2 complete: priority channels assigned")

    # ── STEP 3: LBC comparison ───────────────────────────────────────────────
    time_changes_list: list[dict] = []
    channel_changes_list: list[dict] = []
    no_source_list: list[dict] = []
    matched_provider_indices: set[int] = set()
    lbc_to_winner: dict[int, int] = {}

    if uses_ref and reference is not None and not reference.empty:
        logger.info("Step 3: Comparing with LBC reference (%d events)...", len(reference))

        # Map: provider_idx → (lbc_idx, score)
        provider_to_lbc: dict[int, tuple[int, float]] = {}
        for prov_idx, prov_row in deduped_providers.iterrows():
            best_score, best_lbc_idx = _find_best_match(prov_row, reference)
            if best_score >= _MATCH_THRESHOLD and best_lbc_idx is not None:
                provider_to_lbc[prov_idx] = (best_lbc_idx, best_score)
                matched_provider_indices.add(prov_idx)

        # Group providers per LBC event
        lbc_providers: dict[int, list[tuple[int, float, str]]] = {}
        for prov_idx, (lbc_idx, score) in provider_to_lbc.items():
            source = str(deduped_providers.loc[prov_idx].get("source", ""))
            lbc_providers.setdefault(lbc_idx, []).append((prov_idx, score, source))

        # For each LBC event → pick priority winner → detect time & channel changes
        for lbc_idx, providers in lbc_providers.items():
            lbc_row = reference.loc[lbc_idx]
            sport_str = str(lbc_row.get("sport", "")).strip()
            sport_enum = _sport_from_str(sport_str)

            # Build candidates for priority resolution
            candidates = []
            for prov_idx, score, source in providers:
                prov_row = deduped_providers.loc[prov_idx]
                candidates.append({
                    "source": source,
                    "info": str(prov_row.get("info", "")),
                    "sport": sport_str,
                    "feed_type": str(prov_row.get("feed_type", "")),
                    "original_event_id": str(prov_row.get("original_event_id", "")),
                    "_prov_idx": prov_idx,
                })

            # Resolve priority winner
            if sport_enum and len(candidates) > 1:
                winner = resolve_priority(sport_enum, candidates)
            else:
                winner = candidates[0] if candidates else {}

            winning_prov_idx = winner.get("_prov_idx")
            if winning_prov_idx is None:
                continue

            lbc_to_winner[lbc_idx] = winning_prov_idx
            prov_row = deduped_providers.loc[winning_prov_idx]

            # Time change detection (with tolerance)
            lbc_time = pd.Timestamp(lbc_row.get("start_datetime"))
            prov_time = pd.Timestamp(prov_row.get("start_datetime"))
            try:
                diff_minutes = abs((lbc_time - prov_time).total_seconds()) / 60.0
                # Report if time differs by more than tolerance
                if diff_minutes > config.time_tolerance_minutes:
                    time_changes_list.append({
                        "source": prov_row.get("source", ""),
                        "original_event_id": prov_row.get("original_event_id", ""),
                        "sport": prov_row.get("sport", ""),
                        "region": prov_row.get("region", ""),
                        "league": prov_row.get("league", ""),
                        "event_name": prov_row.get("event_name", ""),
                        "old_time": lbc_time,
                        "new_time": prov_time,
                        "assigned_channel": prov_row.get("assigned_channel", ""),
                        "info": prov_row.get("info", ""),
                    })
                    logger.info(
                        "Time change: %s | LBC=%s → Provider=%s (diff=%.1f min)",
                        prov_row.get("event_name", ""), lbc_time, prov_time, diff_minutes,
                    )
            except Exception:
                pass

        # Channel change detection
        logger.info("Step 3b: Detecting channel changes...")
        channel_changes_list = _detect_channel_changes(
            deduped_providers, reference, lbc_to_winner, lbc_providers
        )
        logger.info("Step 3b complete: %d channel changes", len(channel_changes_list))

        # No Source detection — LBC events with no provider match
        logger.info("Step 3c: Detecting No Source events...")
        matched_lbc_indices = set(lbc_to_winner.keys())
        for lbc_idx, lbc_row in reference.iterrows():
            if lbc_idx not in matched_lbc_indices:
                no_source_list.append({
                    "source": "LBC",
                    "original_event_id": lbc_row.get("original_event_id", ""),
                    "sport": lbc_row.get("sport", ""),
                    "region": lbc_row.get("region", ""),
                    "league": lbc_row.get("league", ""),
                    "event_name": lbc_row.get("event_name", ""),
                    "start_datetime": lbc_row.get("start_datetime"),
                    "info": lbc_row.get("info", ""),
                    "lbc_channel": lbc_row.get("assigned_channel", lbc_row.get("source", "")),
                })
        logger.info("Step 3c complete: %d no-source events", len(no_source_list))

        # Step 4: Provider events not matched to any LBC → potential additions
        logger.info("Step 4: Identifying additions...")
        addition_rows = []
        for prov_idx, prov_row in deduped_providers.iterrows():
            if prov_idx not in matched_provider_indices:
                addition_rows.append(prov_row.to_dict())
        logger.info("Step 4 complete: %d potential additions", len(addition_rows))

    else:
        # Upcoming Day — no LBC reference, all provider events are potential additions
        logger.info("Step 3 skipped: no LBC reference (upcoming day check)")
        addition_rows = [row.to_dict() for _, row in deduped_providers.iterrows()]

    # ── Build intermediate DataFrames ────────────────────────────────────────
    _empty_cols = list(provider_events.columns) if not provider_events.empty else []

    time_changes = (
        pd.DataFrame(time_changes_list)
        if time_changes_list
        else pd.DataFrame(columns=[
            "source", "original_event_id", "sport", "region", "league",
            "event_name", "old_time", "new_time", "assigned_channel", "info",
        ])
    )

    channel_changes = (
        pd.DataFrame(channel_changes_list)
        if channel_changes_list
        else pd.DataFrame(columns=[
            "source", "original_event_id", "sport", "region", "league",
            "event_name", "start_datetime", "assigned_channel", "lbc_channel", "info",
        ])
    )

    no_source = (
        pd.DataFrame(no_source_list)
        if no_source_list
        else pd.DataFrame(columns=[
            "source", "original_event_id", "sport", "region", "league",
            "event_name", "start_datetime", "info", "lbc_channel",
        ])
    )

    additions_raw = (
        pd.DataFrame(addition_rows)
        if addition_rows
        else pd.DataFrame(columns=_empty_cols)
    )

    # ── STEP 5: Deduplicate additions with priority resolution ────────────────
    logger.info("Step 5: Deduplicating additions with priority resolution...")
    if not additions_raw.empty:
        additions_deduped, additions_dupes_removed = _deduplicate_additions_with_priority(
            additions_raw, config
        )
        # Merge additions duplicates into main duplicates removed
        if not additions_dupes_removed.empty:
            duplicates_removed_df = pd.concat(
                [duplicates_removed_df, additions_dupes_removed],
                ignore_index=True,
            )
    else:
        additions_deduped = additions_raw.copy()
    logger.info(
        "Step 5 complete: %d additions after dedup", len(additions_deduped)
    )

    # ── STEP 6: Blacklist filter on additions ────────────────────────────────
    logger.info("Step 6: Applying blacklist to additions...")
    if blacklist and not additions_deduped.empty:
        additions_final, blacklisted_removed = apply_blacklist(
            additions_deduped, blacklist
        )
    else:
        additions_final = additions_deduped.copy()
        blacklisted_removed = pd.DataFrame(
            columns=additions_deduped.columns if not additions_deduped.empty else _empty_cols
        )
    logger.info(
        "Step 6 complete: %d fresh additions, %d blacklisted removed",
        len(additions_final), len(blacklisted_removed),
    )

    # ── Summary ──────────────────────────────────────────────────────────────
    summary = {
        "total_provider_events": len(provider_events),
        "duplicates_removed": len(duplicates_removed_df),
        "blacklisted_removed": len(blacklisted_removed),
        "time_changes": len(time_changes),
        "channel_changes": len(channel_changes),
        "additions": len(additions_final),
        "no_source": len(no_source),
    }

    logger.info("=== run_check COMPLETE: %s ===", summary)

    return CheckResult(
        check_type=check_type,
        target_date=target,
        sports_processed=sports,
        duplicates_removed=duplicates_removed_df,
        blacklisted_removed=blacklisted_removed,
        time_changes=time_changes,
        channel_changes=channel_changes,
        additions=additions_final,
        no_source=no_source,
        summary=summary,
        duplicate_sets=dup_sets,
    )
