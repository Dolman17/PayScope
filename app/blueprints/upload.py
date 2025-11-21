# app/blueprints/upload.py
from __future__ import annotations

import os
import io
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import pandas as pd
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from werkzeug.utils import secure_filename

from extensions import db
from models import JobRecord
from .utils import (
    commit_or_rollback,
    normalize_uk_postcode,
    bulk_geocode_postcodes,
    geocode_postcode_cached,
    snap_to_nearest_postcode,  # NEW: infer postcode from lat/lon
)

bp = Blueprint("upload", __name__)

# --- Helpers -----------------------------------------------------------------

UK_POSTCODE_RE = re.compile(r"^[A-Z]{1,2}\d[0-9A-Z]?\s*\d[A-Z]{2}$", re.I)

REQUIRED_COLS = {"company_id", "company_name", "sector", "postcode", "job_role", "pay_rate"}

ALLOWED_EXTS = {".csv", ".xlsx", ".xls"}  # keep CSV + Excel


def _as_decimal(v):
    if v is None or (isinstance(v, str) and v.strip() == ""):
        return None
    try:
        return Decimal(str(v))
    except (InvalidOperation, ValueError, TypeError):
        return "INVALID_DECIMAL"


def _as_float(v):
    if v is None or (isinstance(v, str) and str(v).strip() == ""):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _load_dataframe(filepath: str, ext: str) -> pd.DataFrame:
    if ext == ".csv":
        # Handle UTF-8 with BOM if present
        with open(filepath, "rb") as f:
            raw = f.read()
        text = raw.decode("utf-8-sig", errors="replace")
        buf = io.StringIO(text)
        df = pd.read_csv(buf)
    else:
        df = pd.read_excel(filepath)
    # normalise column names
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def _validate_row(row: dict, rownum: int):
    """
    Returns dict: { rownum, data, errors[], warnings[], postcode_norm, lat, lon }

    - Critical errors block commit.
    - Warnings are informational (e.g., odd postcode shape).
    - NEW: If postcode is missing but latitude & longitude are provided,
      we allow the row and infer postcode from coords later.
    """
    errors, warnings = [], []

    def need(field: str) -> str:
        v = str(row.get(field, "") or "").strip()
        if not v:
            errors.append(f"{field} is required")
        return v

    # Required text fields
    company_id = need("company_id")
    company_name = need("company_name")
    sector = need("sector")
    job_role = need("job_role")

    # Pay rate validation
    pay_rate_val = _as_decimal(row.get("pay_rate"))
    if pay_rate_val == "INVALID_DECIMAL":
        errors.append("pay_rate must be a number (e.g., 11.44)")
    elif pay_rate_val is None:
        errors.append("pay_rate is required")
    else:
        if pay_rate_val < 0:
            errors.append("pay_rate must be >= 0")

    # Optional coordinates (can be used to infer postcode)
    lat_from_row = _as_float(row.get("latitude"))
    lon_from_row = _as_float(row.get("longitude"))

    # Postcode normalisation + light validation
    postcode_raw = str(row.get("postcode", "") or "")
    postcode = normalize_uk_postcode(postcode_raw)

    if not postcode:
        # No postcode string
        if lat_from_row is None or lon_from_row is None:
            # No coords either -> still a hard error
            errors.append("postcode is required (or provide latitude and longitude to infer it)")
        else:
            # We'll infer postcode later from coords
            warnings.append("postcode missing; will be inferred from latitude/longitude")
    else:
        # Postcode present; warn on odd formats
        if not UK_POSTCODE_RE.match(postcode):
            warnings.append("postcode format looks unusual")

    # Optional: county
    _ = str(row.get("county", "") or "").strip()

    return {
        "rownum": rownum,
        "data": row,
        "errors": errors,
        "warnings": warnings,
        "postcode_norm": postcode,
        "lat": lat_from_row,
        "lon": lon_from_row,
    }


def _geocode_postcodes_for_df(df: pd.DataFrame, skip_geocode: bool) -> dict[str, tuple[float | None, float | None]]:
    if skip_geocode or "postcode" not in df.columns:
        return {}
    postcodes = df["postcode"].astype(str).fillna("").map(normalize_uk_postcode).tolist()
    # Bulk first
    pc_to_latlon = bulk_geocode_postcodes(postcodes)
    # Fallbacks filled ad-hoc later during commit
    return pc_to_latlon


# --- Route -------------------------------------------------------------------

@bp.route("/upload", methods=["GET", "POST"])
@login_required
def upload():
    from flask import current_app as app

    if request.method == "GET":
        return render_template("upload.html")

    # POST
    file = request.files.get("file")
    if not file or not file.filename:
        flash("No file selected.", "error")
        return redirect(request.url)

    filename = secure_filename(file.filename)
    ext = os.path.splitext(filename.lower())[1]
    if ext not in (app.config.get("ALLOWED_EXTENSIONS") or ALLOWED_EXTS):
        flash("Only Excel/CSV files are supported.", "error")
        return redirect(request.url)

    # Save to disk
    filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
    file.save(filepath)

    try:
        df = _load_dataframe(filepath, ext)

        # Check required columns (still require a postcode column, but values may be blank)
        missing = [c for c in REQUIRED_COLS if c not in df.columns]
        if missing:
            flash(f"Missing columns: {', '.join(missing)}", "error")
            return redirect(request.url)

        # Prepare preview validation
        skip_geocode = bool(request.form.get("skip_geocode"))
        action = (request.form.get("action") or "preview").lower()

        # Validate all rows
        preview = []
        critical_count = 0
        # Start row numbers at 2 to reflect header row = 1
        for i, row in enumerate(df.to_dict(orient="records"), start=2):
            result = _validate_row(row, i)
            if result["errors"]:
                critical_count += 1
            preview.append(result)

        # If user asked to preview OR there are critical errors, show preview
        if action == "preview" or critical_count > 0:
            return render_template(
                "upload_preview.html",
                preview=preview,
                columns=list(df.columns),
                critical_count=critical_count,
                skip_geocoding=skip_geocode,
                source_filename=filename,
            )

        # No critical errors -> proceed to commit
        # Optional bulk geocode map
        pc_to_latlon = _geocode_postcodes_for_df(df, skip_geocode)

        added = 0
        now_utc = datetime.now(timezone.utc)

        for row_result in preview:
            row = row_result["data"]
            postcode = row_result["postcode_norm"]
            lat = row_result.get("lat")
            lon = row_result.get("lon")

            # Geocode / snapping logic
            if skip_geocode:
                # Respect "skip_geocode": don't call postcodes.io at all.
                # Use any lat/lon in the file as-is; postcode stays as-normalised
                pass
            else:
                if postcode:
                    # Normal case: postcode present -> use bulk result, then single lookup fallback
                    lat, lon = pc_to_latlon.get(postcode, (None, None))
                    if lat is None or lon is None:
                        lat, lon = geocode_postcode_cached(postcode)
                else:
                    # No postcode but we do have coordinates -> infer nearest postcode
                    if lat is not None and lon is not None:
                        inferred_pc, snapped_lat, snapped_lon = snap_to_nearest_postcode(lat, lon)
                        if inferred_pc:
                            postcode = inferred_pc
                        lat, lon = snapped_lat, snapped_lon
                    else:
                        # Shouldn't happen due to validation, but be safe
                        lat, lon = (None, None)

            rec = JobRecord(
                company_id=str(row.get("company_id", "") or ""),
                company_name=str(row.get("company_name", "") or ""),
                sector=str(row.get("sector", "") or ""),
                postcode=postcode,
                job_role=str(row.get("job_role", "") or ""),
                # Decimal -> float for your existing model
                pay_rate=float(Decimal(str(row.get("pay_rate")))) if str(row.get("pay_rate")).strip() != "" else 0.0,
                county=str(row.get("county", "") or ""),
                latitude=lat,
                longitude=lon,
                imported_month=str(now_utc.month),
                imported_year=str(now_utc.year),
            )
            db.session.add(rec)
            added += 1

        try:
            commit_or_rollback()
            flash(f"Upload complete: {added} records imported.", "success")
        except Exception as e:
            # Log server-side; user-friendly flash
            print(f"DB commit error during upload: {e}")
            flash("Failed to save uploaded records.", "error")

    except Exception as e:
        flash("Error processing file.", "error")
        print(f"🚫 Upload failed ➡ {e}")

    finally:
        try:
            os.remove(filepath)
        except Exception:
            pass

    # After POST, stay on /upload
    return redirect(url_for("upload.upload"))
