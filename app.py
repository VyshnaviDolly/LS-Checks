"""Flask web application for the Live Scheduling ETL Pipeline."""

import logging
import os
import traceback
import uuid

import pandas as pd
from flask import Flask, render_template, request, send_from_directory, redirect, url_for

from config import CheckType, PipelineConfig, CHECK_USES_REFERENCE
from pipeline.ingest import ingest_br, ingest_bg, ingest_rball
from pipeline.normalize import normalize_all
from pipeline.check_engine import run_check
from pipeline.output import generate_report
from pipeline.blacklist import load_blacklist

app = Flask(__name__)
app.secret_key = "live-scheduling-etl-pipeline-secret"

UPLOAD_DIR = "uploads"
OUTPUT_DIR = "output"
BLACKLIST_DIR = "blacklist_locked"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(BLACKLIST_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("app")

_CHECK_TYPE_MAP = {
    "current_day":                CheckType.CURRENT_DAY,
    "upcoming_day":               CheckType.UPCOMING_DAY,
    "current_day_double_check":   CheckType.CURRENT_DAY_DOUBLE,
    "upcoming_day_double_check":  CheckType.UPCOMING_DAY_DOUBLE,
}

# Sports that use Rball provider
_RBALL_SPORTS = {"Basketball", "Ice Hockey"}

_SPORT_LIST = ["Basketball", "Volleyball", "Handball", "Ice Hockey", "Baseball"]


def _get_locked_blacklist_path() -> str | None:
    for f in os.listdir(BLACKLIST_DIR):
        fp = os.path.join(BLACKLIST_DIR, f)
        if os.path.isfile(fp):
            return fp
    return None


def _get_locked_blacklist_name() -> str | None:
    path = _get_locked_blacklist_path()
    return os.path.basename(path) if path else None


def _save_upload(field_name: str, suffix: str = "") -> str:
    """Save an uploaded file with a unique name to avoid collisions."""
    f = request.files.get(field_name)
    if f is None or f.filename == "":
        raise ValueError(f"Required file '{field_name}' was not uploaded.")
    ext = os.path.splitext(f.filename)[1]
    safe_name = f"{field_name}_{uuid.uuid4().hex}{ext}"
    path = os.path.join(UPLOAD_DIR, safe_name)
    f.save(path)
    return path


def _save_upload_optional(field_name: str) -> str | None:
    """Save an optional uploaded file. Returns None if not provided."""
    f = request.files.get(field_name)
    if f is None or f.filename == "":
        return None
    ext = os.path.splitext(f.filename)[1]
    safe_name = f"{field_name}_{uuid.uuid4().hex}{ext}"
    path = os.path.join(UPLOAD_DIR, safe_name)
    f.save(path)
    return path


def _cleanup_uploads(*paths: str | None) -> None:
    """Delete temporary upload files after pipeline run."""
    for path in paths:
        if path and os.path.isfile(path):
            try:
                os.remove(path)
            except Exception:
                pass


@app.route("/", methods=["GET"])
def index():
    return render_template(
        "index.html",
        blacklist_locked=_get_locked_blacklist_name(),
        sports=_SPORT_LIST,
    )


@app.route("/lock-blacklist", methods=["POST"])
def lock_blacklist():
    f = request.files.get("blacklist_file")
    if f and f.filename:
        ext = os.path.splitext(f.filename)[1]
        path = os.path.join(BLACKLIST_DIR, f.filename)
        f.save(path)
        try:
            bl = load_blacklist(path)
            logger.info("Blacklist locked with sport tabs: %s", list(bl.keys()))
        except Exception as exc:
            os.remove(path)
            return render_template(
                "index.html",
                blacklist_locked=None,
                sports=_SPORT_LIST,
                blacklist_error=f"Invalid blacklist file: {exc}",
            )
    return redirect(url_for("index"))


@app.route("/unlock-blacklist", methods=["POST"])
def unlock_blacklist():
    path = _get_locked_blacklist_path()
    if path and os.path.isfile(path):
        os.remove(path)
    return redirect(url_for("index"))


@app.route("/run", methods=["POST"])
def run_pipeline():
    uploaded_paths: list[str] = []  # track for cleanup

    try:
        # ── Validate check type ──────────────────────────────────────────────
        check_type_val = request.form.get("check_type", "")
        check_type = _CHECK_TYPE_MAP.get(check_type_val)
        if check_type is None:
            return render_template(
                "results.html",
                error=f"Invalid check type: '{check_type_val}'",
                summary=None, result=None, report_filename=None,
            )

        # ── Validate sport ───────────────────────────────────────────────────
        selected_sport = request.form.get("sport", "")
        if selected_sport not in _SPORT_LIST:
            return render_template(
                "results.html",
                error=f"Invalid sport: '{selected_sport}'",
                summary=None, result=None, report_filename=None,
            )

        # ── Date range ───────────────────────────────────────────────────────
        start_date_str = request.form.get("target_date", "")
        end_date_str = request.form.get("end_date", "")
        if not start_date_str or not end_date_str:
            return render_template(
                "results.html",
                error="Start date and end date are required.",
                summary=None, result=None, report_filename=None,
            )

        from datetime import date as date_cls, datetime as dt_cls, timedelta, timezone
        start_date = date_cls.fromisoformat(start_date_str)
        end_date = date_cls.fromisoformat(end_date_str)
        if end_date < start_date:
            return render_template(
                "results.html",
                error="End date cannot be before start date.",
                summary=None, result=None, report_filename=None,
            )

        # ── Provider files ───────────────────────────────────────────────────
        bg_path = _save_upload("bg_file")
        uploaded_paths.append(bg_path)

        br_path = _save_upload("br_file")
        uploaded_paths.append(br_path)

        # Rball — required only for Basketball and Ice Hockey
        rball_path = None
        if selected_sport in _RBALL_SPORTS:
            rball_path = _save_upload_optional("rball_file")
            if rball_path:
                uploaded_paths.append(rball_path)

        # ── LBC Reference ────────────────────────────────────────────────────
        requires_lbc = CHECK_USES_REFERENCE.get(check_type, False)
        reference_path = None
        if requires_lbc:
            reference_path = _save_upload("lbc_file")
            uploaded_paths.append(reference_path)
        else:
            reference_path = _save_upload_optional("lbc_file")
            if reference_path:
                uploaded_paths.append(reference_path)

        # ── Blacklist ────────────────────────────────────────────────────────
        blacklist_path = _get_locked_blacklist_path()
        if not blacklist_path:
            blacklist_path = _save_upload("blacklist_file")
            uploaded_paths.append(blacklist_path)

        # ── Ingest ───────────────────────────────────────────────────────────
        br_df = ingest_br(br_path)
        bg_df = ingest_bg(bg_path)
        provider_dfs = {"BR": br_df, "BG": bg_df}

        if rball_path:
            rball_df = ingest_rball(rball_path)
            provider_dfs["Rball"] = rball_df

        # ── Normalize ────────────────────────────────────────────────────────
        all_events = normalize_all(provider_dfs)

        # ── Filter to selected sport ─────────────────────────────────────────
        if not all_events.empty:
            all_events = all_events[
                all_events["sport"].str.strip().str.lower() == selected_sport.strip().lower()
            ].reset_index(drop=True)

        # ── Filter to date range (UTC+2) ─────────────────────────────────────
        UTC_PLUS_2 = timezone(timedelta(hours=2))
        range_start = dt_cls(start_date.year, start_date.month, start_date.day,
                             0, 0, tzinfo=UTC_PLUS_2)
        range_end = dt_cls(end_date.year, end_date.month, end_date.day,
                           23, 59, 59, tzinfo=UTC_PLUS_2)

        def _in_date_range(dt_val):
            ts = pd.Timestamp(dt_val)
            if ts is pd.NaT:
                return False
            if ts.tzinfo is not None:
                ts = ts.tz_convert(UTC_PLUS_2)
            else:
                ts = ts.tz_localize(UTC_PLUS_2)
            return range_start <= ts <= range_end

        if not all_events.empty:
            all_events = all_events[
                all_events["start_datetime"].apply(_in_date_range)
            ].reset_index(drop=True)

        # ── Validate each provider has data ──────────────────────────────────
        missing_providers = []
        for prov_name in provider_dfs.keys():
            prov_events = (
                all_events[all_events["source"] == prov_name]
                if not all_events.empty
                else pd.DataFrame()
            )
            if prov_events.empty:
                missing_providers.append(prov_name)

        if missing_providers:
            missing_str = ", ".join(missing_providers)
            return render_template(
                "results.html",
                error=(
                    f"No {selected_sport} events found for dates "
                    f"{start_date_str} to {end_date_str} in provider(s): {missing_str}. "
                    f"Please check that the uploaded files contain data for these dates."
                ),
                summary=None, result=None, report_filename=None,
            )

        # ── Load blacklist (sport-specific tab only) ──────────────────────────
        blacklist = None
        if blacklist_path:
            full_bl = load_blacklist(blacklist_path)
            blacklist = {
                k: v for k, v in full_bl.items()
                if k.strip().lower() == selected_sport.strip().lower()
            }

        # ── Load & normalize LBC reference ────────────────────────────────────
        reference = None
        if reference_path:
            try:
                from pipeline.normalize import normalize_reference
                ref_raw = ingest_br(reference_path)
                reference = normalize_reference(ref_raw)
                if reference is not None and not reference.empty:
                    reference = reference[
                        reference["sport"].str.strip().str.lower() == selected_sport.strip().lower()
                    ].reset_index(drop=True)
                    if not reference.empty:
                        reference = reference[
                            reference["start_datetime"].apply(_in_date_range)
                        ].reset_index(drop=True)
            except Exception as exc:
                logger.warning("Could not load reference file: %s", exc)

        # ── Build config ──────────────────────────────────────────────────────
        config = PipelineConfig(
            check_type=check_type,
            main_reference_path=reference_path,
            blacklist_path=blacklist_path,
            output_dir=OUTPUT_DIR,
            upload_dir=UPLOAD_DIR,
            date_override=start_date_str,
            end_date_override=end_date_str,
        )

        # ── Run pipeline ──────────────────────────────────────────────────────
        result = run_check(
            check_type=check_type,
            provider_events=all_events,
            reference=reference,
            blacklist=blacklist,
            config=config,
        )

        report_path = generate_report(result, config)
        report_filename = os.path.basename(report_path)

        check_labels = {
            "current_day":               "Current Day",
            "upcoming_day":              "Upcoming Day",
            "current_day_double_check":  "Current Day Double",
            "upcoming_day_double_check": "Upcoming Day Double",
        }

        return render_template(
            "results.html",
            error=None,
            summary=result.summary,
            result=result,
            report_filename=report_filename,
            check_type_label=(
                f"{check_labels.get(check_type_val, '')} — "
                f"{selected_sport} ({start_date_str} to {end_date_str})"
            ),
        )

    except ValueError as ve:
        return render_template(
            "results.html",
            error=str(ve),
            summary=None, result=None, report_filename=None,
        )
    except Exception:
        tb = traceback.format_exc()
        logger.error("Pipeline error:\n%s", tb)
        return render_template(
            "results.html",
            error=f"Pipeline error:\n{tb}",
            summary=None, result=None, report_filename=None,
        )
    finally:
        # Clean up temporary upload files
        _cleanup_uploads(*uploaded_paths)


@app.route("/download/<filename>")
def download(filename):
    return send_from_directory(OUTPUT_DIR, filename, as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
