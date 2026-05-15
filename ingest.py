"""Universal provider file parser.

All provider files (BG, BR, Rball) and the LBC reference share the same
column structure:
  Category, Update, Day, Date, Time, Ticker Channel, Stream, Region,
  Sport, League, Info, Matches, Bookie, Bookie Group

File format is auto-detected: CSV, XLS, XLSX are all supported.
"""

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger("pipeline.ingest")

# Core columns we care about (Region may have trailing space in some files)
CORE_COLUMNS = ["Date", "Time", "Sport", "League", "Matches"]


def _read_any_format(filepath: str) -> pd.DataFrame:
    """Read a file as CSV, XLSX, or XLS based on extension."""
    ext = Path(filepath).suffix.lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(filepath, dtype=str, keep_default_na=False)
        elif ext == ".xlsx":
            df = pd.read_excel(filepath, engine="openpyxl", dtype=str)
            df = df.fillna("")
        elif ext == ".xls":
            df = pd.read_excel(filepath, engine="xlrd", dtype=str)
            df = df.fillna("")
        else:
            try:
                df = pd.read_csv(filepath, dtype=str, keep_default_na=False)
            except Exception:
                df = pd.read_excel(filepath, dtype=str)
                df = df.fillna("")
    except Exception as exc:
        logger.error("Failed to open file '%s': %s", filepath, exc)
        return pd.DataFrame()

    # Normalize column names: strip whitespace
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _detect_provider(df: pd.DataFrame) -> str:
    """Detect provider from Ticker Channel or Channel column."""
    for col_name in ["Ticker Channel", "Channel"]:
        if col_name in df.columns:
            channels = df[col_name].dropna().astype(str).str.strip().str.lower().unique()
            for ch in channels:
                if "genius" in ch:
                    return "BG"
                if "sportradar" in ch:
                    return "BR"
                if "rball" in ch:
                    return "Rball"
    return "Unknown"


def ingest_provider(filepath: str, expected_provider: str = "") -> pd.DataFrame:
    """Parse any provider file (BG, BR, Rball, or LBC) and return a DataFrame.

    All files share the same column structure. The provider is identified
    from the 'Ticker Channel' column.
    """
    df = _read_any_format(filepath)
    if df.empty:
        return df

    # Log what we found
    provider = _detect_provider(df)
    logger.info("Ingested '%s': %d rows, detected provider=%s", filepath, len(df), provider)

    return df


# Backward-compatible aliases
def ingest_br(filepath: str) -> pd.DataFrame:
    return ingest_provider(filepath, "BR")

def ingest_bg(filepath: str) -> pd.DataFrame:
    return ingest_provider(filepath, "BG")

def ingest_rball(filepath: str) -> pd.DataFrame:
    return ingest_provider(filepath, "Rball")
