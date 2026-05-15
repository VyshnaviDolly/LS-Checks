"""Excel output generation for pipeline check results."""

import logging
import os
from datetime import datetime

import pandas as pd

from config import PipelineConfig
from pipeline.check_engine import CheckResult

logger = logging.getLogger("pipeline.output")

# Provider/LBC format columns for the additions sheet
_PROVIDER_COLUMNS = [
    "Category", "Update", "Day", "Date", "Time", "Channel", "Stream",
    "Region", "Sport", "League", "Info", "Match", "Bookie", "Bookie Group",
]

_BASE_COLUMNS = [
    "source", "original_event_id", "sport", "region", "league",
    "event_name", "start_datetime", "assigned_channel", "info",
]

_TIME_CHANGE_COLUMNS = [
    "source", "original_event_id", "sport", "region", "league",
    "event_name", "old_time", "new_time", "assigned_channel", "info",
]


def _safe_col(df, col):
    if col in df.columns:
        return df[col]
    return pd.Series([""] * len(df), index=df.index)


def _strip_tz(series):
    if series.empty:
        return series
    try:
        dt_series = pd.to_datetime(series, errors="coerce")
        if hasattr(dt_series.dt, "tz") and dt_series.dt.tz is not None:
            return dt_series.dt.tz_localize(None)
        return dt_series
    except Exception:
        return series


def _prepare_base_sheet(df):
    if df.empty:
        return pd.DataFrame(columns=_BASE_COLUMNS)
    out = pd.DataFrame({
        "source": _safe_col(df, "source"),
        "original_event_id": _safe_col(df, "original_event_id"),
        "sport": _safe_col(df, "sport"),
        "region": _safe_col(df, "region"),
        "league": _safe_col(df, "league"),
        "event_name": _safe_col(df, "event_name"),
        "start_datetime": _strip_tz(_safe_col(df, "start_datetime")),
        "assigned_channel": _safe_col(df, "assigned_channel"),
        "info": _safe_col(df, "info"),
    })
    return out.sort_values(["sport", "start_datetime"], ascending=True).reset_index(drop=True)


def _prepare_time_change_sheet(df):
    if df.empty:
        return pd.DataFrame(columns=_TIME_CHANGE_COLUMNS)
    out = pd.DataFrame({
        "source": _safe_col(df, "source"),
        "original_event_id": _safe_col(df, "original_event_id"),
        "sport": _safe_col(df, "sport"),
        "region": _safe_col(df, "region"),
        "league": _safe_col(df, "league"),
        "event_name": _safe_col(df, "event_name"),
        "old_time": _strip_tz(_safe_col(df, "old_time")),
        "new_time": _strip_tz(_safe_col(df, "new_time")),
        "assigned_channel": _safe_col(df, "assigned_channel"),
        "info": _safe_col(df, "info"),
    })
    return out.sort_values(["sport", "new_time"], ascending=True).reset_index(drop=True)


def _prepare_additions_sheet(df):
    """Build additions in the same format as provider/LBC files."""
    if df.empty:
        return pd.DataFrame(columns=_PROVIDER_COLUMNS)

    rows = []
    for _, r in df.iterrows():
        dt = pd.Timestamp(r.get("start_datetime"))
        date_str = ""
        time_str = ""
        day_str = ""
        if dt is not pd.NaT:
            date_str = dt.strftime("%d/%m/%Y")
            time_str = dt.strftime("%H:%M")
            day_str = dt.strftime("%a")[:2]

        source = str(r.get("source", ""))
        # Map source codes back to provider names
        channel_map = {"BR": "Sportradar", "BG": "Genius Sports", "Rball": "Rball", "LBC": ""}
        channel = channel_map.get(source, source)

        rows.append({
            "Category": "",
            "Update": "",
            "Day": day_str,
            "Date": date_str,
            "Time": time_str,
            "Channel": channel,
            "Stream": str(r.get("stream", "")),
            "Region": str(r.get("region", "")),
            "Sport": str(r.get("sport", "")),
            "League": str(r.get("league", "")),
            "Info": str(r.get("info", "")),
            "Match": str(r.get("event_name", "")),
            "Bookie": str(r.get("booking_status", "")),
            "Bookie Group": "",
        })

    return pd.DataFrame(rows, columns=_PROVIDER_COLUMNS).sort_values(
        ["Sport", "Date", "Time"]
    ).reset_index(drop=True)


def _prepare_no_source_sheet(df):
    """Build No Source sheet — LBC events not found in any provider."""
    if df.empty:
        return pd.DataFrame(columns=_PROVIDER_COLUMNS)

    rows = []
    for _, row in df.iterrows():
        dt = pd.Timestamp(row.get("start_datetime"))
        date_str = ""
        time_str = ""
        day_str = ""
        if dt is not pd.NaT:
            if dt.tzinfo is not None:
                dt = dt.tz_localize(None)  # Strip tz without converting
            date_str = dt.strftime("%d/%m/%Y")
            time_str = dt.strftime("%H:%M")
            day_str = dt.strftime("%a")[:2]

        lbc_channel = str(row.get("lbc_channel", row.get("source", "")))
        channel_map = {"BR": "Sportradar", "BG": "Genius Sports", "Rball": "Rball", "LBC": ""}
        channel = channel_map.get(lbc_channel, lbc_channel)

        rows.append({
            "Category": "Dropped by " + channel if channel else "Dropped",
            "Update": "No Source",
            "Day": day_str,
            "Date": date_str,
            "Time": time_str,
            "Channel": channel,
            "Stream": "",
            "Region": str(row.get("region", "")),
            "Sport": str(row.get("sport", "")),
            "League": str(row.get("league", "")),
            "Info": str(row.get("info", "")),
            "Match": str(row.get("event_name", "")),
            "Bookie": "",
            "Bookie Group": "",
        })

    return pd.DataFrame(rows, columns=_PROVIDER_COLUMNS)


def generate_report(result: CheckResult, config: PipelineConfig) -> str:
    os.makedirs(config.output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"LBC_Output_{timestamp}.xlsx"
    filepath = os.path.join(config.output_dir, filename)

    duplicates_sheet = _prepare_base_sheet(result.duplicates_removed)
    blacklisted_sheet = _prepare_base_sheet(result.blacklisted_removed)
    time_changes_sheet = _prepare_time_change_sheet(result.time_changes)
    channel_changes_sheet = _prepare_base_sheet(result.channel_changes)
    additions_sheet = _prepare_additions_sheet(result.additions)
    no_source_sheet = _prepare_no_source_sheet(result.no_source)

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        time_changes_sheet.to_excel(writer, sheet_name="Time Changes", index=False)
        channel_changes_sheet.to_excel(writer, sheet_name="Channel Changes", index=False)
        additions_sheet.to_excel(writer, sheet_name="Fresh Additions", index=False)
        no_source_sheet.to_excel(writer, sheet_name="No Source", index=False)
        duplicates_sheet.to_excel(writer, sheet_name="Duplicates Removed", index=False)
        blacklisted_sheet.to_excel(writer, sheet_name="Blacklisted Removed", index=False)

    logger.info("Report generated: %s", filepath)
    return filepath
