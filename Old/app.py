from __future__ import annotations

import os
import re
import time
import json
from datetime import datetime, timezone
from functools import lru_cache
from io import BytesIO
from typing import Any, Mapping, Tuple

from openai import OpenAI
from dotenv import load_dotenv
from flask_login import LoginManager, login_required, login_user, logout_user, current_user, UserMixin
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    jsonify,
    send_file,
    abort,
)
from sqlalchemy import func, desc, or_, cast, String
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename
import click
import pandas as pd
import requests

# Shared db + migrate
from extensions import db, migrate

# Load environment variables from .env
load_dotenv()

# ---------------------- APP SETUP ----------------------

app = Flask(__name__)
app.config.from_pyfile("config.py")

# Sensible defaults if not set in config.py
app.config.setdefault("UPLOAD_FOLDER", os.path.join(app.root_path, "uploads"))
app.config.setdefault("MAX_CONTENT_LENGTH", 20 * 1024 * 1024)  # 20 MB
app.config.setdefault("ALLOWED_EXTENSIONS", {".xlsx", ".xls", ".csv"})

# Ensure upload folder exists
if app.config.get("UPLOAD_FOLDER"):
    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

# Bind db + migrate
db.init_app(app)
migrate.init_app(app, db)

from models import AIAnalysisLog, JobRecord, User, AIAnalysisLog  # must come after db.init_app

# Setup login manager
login_manager = LoginManager()
login_manager.login_view = "login"
login_manager.init_app(app)

# ---------------------- MODELS ----------------------
from models import AIAnalysisLog  # must come after db.init_app


# ---------------------- MODELS ----------------------

@login_manager.user_loader
def load_user(user_id: str):
    try:
        return db.session.get(User, int(user_id))
    except Exception:
        return None

# ---------------------- HELPERS ----------------------

def _ttl_cache(seconds: int = 120):
    """Tiny TTL cache decorator for frequently-used helpers (e.g., dropdown options)."""
    def deco(fn):
        store = {"t": 0.0, "val": None}
        def wrapper(*a, **k):
            now = time.time()
            if now - store["t"] > seconds:
                store["val"] = fn(*a, **k)
                store["t"] = now
            return store["val"]
        return wrapper
    return deco

@_ttl_cache(seconds=120)
def get_filter_options():
    """Fetch distinct values for dropdowns efficiently."""
    def col_distinct(col):
        return [
            v[0]
            for v in db.session.query(col)
            .filter(col.isnot(None))
            .distinct()
            .order_by(col)
            .all()
        ]
    return {
        "sectors": col_distinct(JobRecord.sector),
        "roles": col_distinct(JobRecord.job_role),
        "counties": col_distinct(JobRecord.county),
        "months": col_distinct(JobRecord.imported_month),
        "years": col_distinct(JobRecord.imported_year),
    }

def build_filters_from_request(mapping: Mapping[str, Any]):
    """Return a list of SQLAlchemy filter expressions based on provided mapping."""
    filters = []
    if mapping.get("sector"):
        filters.append(JobRecord.sector == mapping["sector"])
    if mapping.get("job_role"):
        filters.append(JobRecord.job_role == mapping["job_role"])
    if mapping.get("county"):
        filters.append(JobRecord.county == mapping["county"])
    if mapping.get("month"):
        filters.append(JobRecord.imported_month == mapping["month"])
    if mapping.get("year"):
        filters.append(JobRecord.imported_year == mapping["year"])
    return filters

def commit_or_rollback():
    try:
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        raise e

def logo_url_for(company_id: str) -> str:
    """Return URL for company logo, falling back to placeholder; uses FS-safe check."""
    fs_path = os.path.join(app.root_path, "static", "logos", f"{company_id}.png")
    if os.path.exists(fs_path):
        return url_for("static", filename=f"logos/{company_id}.png")
    return url_for("static", filename="logos/placeholder.png")

# ---------------------- UK GEOCODING ----------------------

# UK-specific fast geocoding (postcodes.io), with Nominatim fallback

POSTCODES_IO_BULK_URL = "https://api.postcodes.io/postcodes"
POSTCODES_IO_SINGLE_URL = "https://api.postcodes.io/postcodes/{pc}"

def normalize_uk_postcode(pc: str) -> str:
    """Uppercase, strip non-alphanumerics, insert space before last 3 chars."""
    s = re.sub(r"[^A-Za-z0-9]", "", (pc or "")).upper()
    if len(s) < 5:
        return s
    return s[:-3] + " " + s[-3:]

def bulk_geocode_postcodes(postcodes: list[str]) -> dict[str, tuple[float | None, float | None]]:
    """Return {postcode: (lat, lon)} using postcodes.io in chunks of 100."""
    results: dict[str, tuple[float | None, float | None]] = {}
    cleaned = [normalize_uk_postcode(p) for p in postcodes if p]
    unique = sorted(set(cleaned))
    for i in range(0, len(unique), 100):
        chunk = unique[i:i + 100]
        try:
            resp = requests.post(POSTCODES_IO_BULK_URL, json={"postcodes": chunk}, timeout=20)
            resp.raise_for_status()
            data = resp.json() or {}
            for item in data.get("result", []):
                query = item.get("query")
                res = item.get("result")
                if res:
                    results[query] = (res.get("latitude"), res.get("longitude"))
                else:
                    results[query] = (None, None)
        except Exception as e:
            print(f"Bulk geocode error for chunk {i}-{i+len(chunk)}: {e}")
            for q in chunk:
                results.setdefault(q, (None, None))
    return results

@lru_cache(maxsize=5000)
def geocode_postcode_cached(postcode: str) -> Tuple[float | None, float | None]:
    return geocode_postcode(postcode)

def geocode_postcode(postcode: str) -> Tuple[float | None, float | None]:
    """Try postcodes.io (fast) then Nominatim (fallback) for a *single* postcode."""
    pc = normalize_uk_postcode(postcode)
    if not pc:
        return (None, None)

    # 1) postcodes.io single
    try:
        r = requests.get(POSTCODES_IO_SINGLE_URL.format(pc=pc), timeout=10)
        if r.status_code == 200:
            d = (r.json() or {}).get("result")
            if d:
                return (float(d["latitude"]), float(d["longitude"]))
    except Exception as e:
        print(f"postcodes.io error for {pc}: {e}")

    # 2) Fallback to Nominatim
    response = None
    try:
        for attempt in range(3):
            try:
                response = requests.get(
                    "https://nominatim.openstreetmap.org/search",
                    params={"format": "json", "q": pc},
                    headers={
                        "User-Agent": "PayRateMapUploader (contact: you@example.com)",
                        "Accept-Language": "en-GB",
                    },
                    timeout=15,
                )
                response.raise_for_status()
                data = response.json()
                if data:
                    return (float(data[0]["lat"]), float(data[0]["lon"]))
                break
            except requests.HTTPError:
                if response is not None and response.status_code in (429, 502, 503, 504):
                    time.sleep(1.0 * (attempt + 1))
                    continue
                raise
    except Exception as e:
        print(f"Geocoding error for {pc}: {e}")

    return (None, None)

# ---------------------- ADMIN: USERS ----------------------

@app.route("/admin/users", methods=["GET", "POST"])
@login_required
def manage_users():
    if not current_user.is_authenticated or not current_user.is_superuser():
        flash("Access denied – superuser only.", "error")
        return redirect(url_for("home"))

    if request.method == "POST":
        action = request.form.get("action")

        if action == "add":
            username = (request.form.get("username") or "").strip()
            password = (request.form.get("password") or "").strip()
            admin_level = int(request.form.get("admin_level", 0))

            if not username or not password:
                flash("Username and password are required.", "error")
            elif db.session.query(User).filter_by(username=username).first():
                flash("Username already exists.", "error")
            else:
                new_user = User(
                    username=username,
                    password=generate_password_hash(password),
                    admin_level=admin_level,
                )
                db.session.add(new_user)
                try:
                    commit_or_rollback()
                    flash(f"User '{username}' added.", "success")
                except Exception:
                    flash("Failed to add user.", "error")

        elif action == "delete":
            user_id = request.form.get("user_id")
            if not user_id:
                flash("User ID missing.", "error")
            elif int(user_id) == current_user.id:
                flash("You cannot delete your own account.", "error")
            else:
                user = db.session.get(User, int(user_id))
                if user:
                    db.session.delete(user)
                    try:
                        commit_or_rollback()
                        flash("User deleted.", "info")
                    except Exception:
                        flash("Failed to delete user.", "error")
                else:
                    flash("User not found.", "error")

        elif action == "update":
            user_id = request.form.get("user_id")
            admin_level = request.form.get("admin_level")
            if not (user_id and admin_level is not None):
                flash("Missing user ID or admin level.", "error")
            else:
                user = db.session.get(User, int(user_id))
                if user:
                    user.admin_level = int(admin_level)
                    try:
                        commit_or_rollback()
                        flash("User updated.", "success")
                    except Exception:
                        flash("Failed to update user.", "error")
                else:
                    flash("User not found.", "error")

    users = User.query.all()
    return render_template("manage_users.html", users=users)

# ---------------------- ROUTES ----------------------

@app.route("/")
@login_required
def home():
    return render_template("index.html", now=lambda: datetime.now(timezone.utc))

@app.route("/records")
@login_required
def records():
    page = request.args.get("page", 1, type=int)
    edit_id = request.args.get("edit_id", type=int)

    filters_map = {
        "sector": request.args.get("sector"),
        "job_role": request.args.get("job_role"),
        "county": request.args.get("county"),
        "month": request.args.get("month"),
        "year": request.args.get("year"),
    }

    filters = build_filters_from_request(filters_map)
    base_q = JobRecord.query.filter(*filters)

    pagination = base_q.paginate(page=page, per_page=25, error_out=False)
    all_records = pagination.items

    options = get_filter_options()
    selected_record = db.session.get(JobRecord, edit_id) if edit_id else None

    return render_template(
        "records.html",
        records=all_records,
        pagination=pagination,
        filters=filters_map,
        options=options,
        filter_query=request.query_string.decode(),
        selected_record=selected_record,
    )

@app.route("/edit/<int:record_id>", methods=["GET", "POST"])
@login_required
def edit_record(record_id: int):
    record = JobRecord.query.get_or_404(record_id)

    if request.method == "POST":
        record.company_id = request.form.get("company_id", record.company_id)
        record.company_name = request.form.get("company_name", record.company_name)
        record.sector = request.form.get("sector", record.sector)
        record.job_role = request.form.get("job_role", record.job_role)
        record.postcode = request.form.get("postcode", record.postcode)
        record.county = request.form.get("county", record.county)
        pay_rate = request.form.get("pay_rate", None)
        if pay_rate is not None and pay_rate != "":
            try:
                record.pay_rate = float(pay_rate)
            except ValueError:
                flash("Invalid pay rate.", "error")
                return redirect(request.referrer or url_for("records"))
        try:
            commit_or_rollback()
            flash(f"Record {record_id} updated.", "success")
        except Exception:
            flash("Failed to update record.", "error")
        return redirect(request.referrer or url_for("records"))

    return jsonify(
        {
            "id": record.id,
            "company_id": record.company_id,
            "company_name": record.company_name,
            "sector": record.sector,
            "job_role": record.job_role,
            "postcode": record.postcode,
            "county": record.county,
            "pay_rate": record.pay_rate,
        }
    )

@app.route("/delete/<int:record_id>", methods=["POST"])
@login_required
def delete_record(record_id: int):
    record = JobRecord.query.get_or_404(record_id)
    db.session.delete(record)
    try:
        commit_or_rollback()
        flash(f"Record {record_id} deleted.", "success")
    except Exception:
        flash("Failed to delete record.", "error")
    return redirect(request.referrer or url_for("records"))

@app.route("/export")
@login_required
def export_records():
    """Export current filtered records to Excel (default) or CSV via ?format=csv."""
    export_format = (request.args.get("format") or "xlsx").lower()

    filters_map = {
        "sector": request.args.get("sector"),
        "job_role": request.args.get("job_role"),
        "county": request.args.get("county"),
        "month": request.args.get("month"),
        "year": request.args.get("year"),
    }
    filters = build_filters_from_request(filters_map)
    rows = (
        db.session.query(JobRecord)
        .filter(*filters)
        .order_by(JobRecord.imported_year.desc(), JobRecord.imported_month.desc())
        .all()
    )

    data = [
        {
            "company_id": r.company_id,
            "company_name": r.company_name,
            "sector": r.sector,
            "job_role": r.job_role,
            "postcode": r.postcode,
            "county": r.county,
            "pay_rate": r.pay_rate,
            "imported_month": r.imported_month,
            "imported_year": r.imported_year,
            "latitude": r.latitude,
            "longitude": r.longitude,
        }
        for r in rows
    ]

    df = pd.DataFrame(data)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    if export_format == "csv":
        buf = BytesIO()
        df.to_csv(buf, index=False)
        buf.seek(0)
        return send_file(
            buf,
            as_attachment=True,
            download_name=f"pay-rate-export-{stamp}.csv",
            mimetype="text/csv",
        )

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="records")
    buf.seek(0)
    return send_file(
        buf,
        as_attachment=True,
        download_name=f"pay-rate-export-{stamp}.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

@app.route("/map")
@login_required
def map_sector_select():
    sectors = [
        s[0]
        for s in db.session.query(JobRecord.sector)
        .filter(JobRecord.sector.isnot(None))
        .distinct()
        .order_by(JobRecord.sector)
        .all()
    ]
    return render_template("map_select.html", sectors=sectors)

@app.route("/map/<sector>")
@login_required
def sector_map(sector: str):
    job_role = request.args.get("job_role")
    min_pay = request.args.get("min_pay", type=float)
    max_pay = request.args.get("max_pay", type=float)

    query = JobRecord.query.filter(JobRecord.sector == sector)
    if job_role:
        query = query.filter(JobRecord.job_role == job_role)
    if min_pay is not None:
        query = query.filter(JobRecord.pay_rate >= min_pay)
    if max_pay is not None:
        query = query.filter(JobRecord.pay_rate <= max_pay)

    records = query.all()

    for record in records:
        record.logo_url = logo_url_for(record.company_id or "placeholder")

    job_roles = [
        r[0]
        for r in db.session.query(JobRecord.job_role)
        .filter(JobRecord.job_role.isnot(None))
        .distinct()
        .order_by(JobRecord.job_role)
        .all()
    ]

    return render_template(
        "map.html",
        sector=sector,
        records=records,
        job_roles=job_roles,
        filters={
            "job_role": job_role or "",
            "min_pay": min_pay or "",
            "max_pay": max_pay or "",
        },
    )

@app.route("/admin/backfill-counties")
@login_required
def backfill_counties():
    if not current_user.is_superuser():
        flash("Access denied – superuser only.", "error")
        return redirect(url_for("home"))

    limit = request.args.get("limit", type=int)

    from geopy.geocoders import Nominatim
    from geopy.extra.rate_limiter import RateLimiter

    geolocator = Nominatim(user_agent="pay-rate-map")
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=2)

    updated = 0
    skipped = 0

    q = JobRecord.query.filter((JobRecord.county == None) | (JobRecord.county == ""))  # noqa: E711
    if limit:
        q = q.limit(limit)
    missing = q.all()

    for record in missing:
        if record.latitude and record.longitude:
            try:
                location = reverse((record.latitude, record.longitude), exactly_one=True)
                if location and "county" in location.raw.get("address", {}):
                    record.county = location.raw["address"]["county"]
                    updated += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"❌ Error reverse geocoding ID {record.id}: {e}")
                skipped += 1

    try:
        commit_or_rollback()
        flash(
            f"✅ County backfill complete. Updated: {updated}, Skipped: {skipped}",
            "success",
        )
    except Exception:
        flash("Failed to save backfill results.", "error")
    return redirect(url_for("upload"))

@app.context_processor
def inject_now():
    return {"current_year": datetime.now(timezone.utc).year}

@app.route("/dashboard")
@login_required
def dashboard():
    selected_sector = request.args.get("sector")
    selected_county = request.args.get("county")
    selected_role = request.args.get("role")

    filters = []
    if selected_sector:
        filters.append(JobRecord.sector == selected_sector)
    if selected_county:
        filters.append(JobRecord.county == selected_county)
    if selected_role:
        filters.append(JobRecord.job_role == selected_role)

    base_q = JobRecord.query.filter(*filters)
    filtered_records = base_q.all()

    total_records = len(filtered_records)
    total_companies = len({r.company_id for r in filtered_records})
    pays = [r.pay_rate for r in filtered_records if r.pay_rate is not None]
    avg_pay = round(sum(pays) / len(pays), 2) if pays else 0

    by_sector = (
        db.session.query(JobRecord.sector, func.count(), func.avg(JobRecord.pay_rate))
        .filter(*filters)
        .group_by(JobRecord.sector)
        .all()
    )
    by_county = (
        db.session.query(JobRecord.county, func.count())
        .filter(*filters)
        .group_by(JobRecord.county)
        .all()
    )

    opts = get_filter_options()

    return render_template(
        "dashboard.html",
        total_records=total_records,
        total_companies=total_companies,
        avg_pay=avg_pay,
        by_sector=by_sector,
        by_county=by_county,
        available_sectors=opts["sectors"],
        available_counties=opts["counties"],
        available_roles=opts["roles"],
    )

@app.route("/insights")
@login_required
def insights():
    sector = request.args.get("sector")
    job_role = request.args.get("job_role")
    county = request.args.get("county")
    month = request.args.get("month")
    year = request.args.get("year")

    filters = build_filters_from_request(
        {"sector": sector, "job_role": job_role, "county": county, "month": month, "year": year}
    )
    base_q = JobRecord.query.filter(*filters)
    records = base_q.all()

    serialized_records = [
        {
            "company_id": r.company_id,
            "company_name": r.company_name,
            "sector": r.sector,
            "job_role": r.job_role,
            "postcode": r.postcode,
            "county": r.county,
            "pay_rate": r.pay_rate,
            "month": r.imported_month,
            "year": r.imported_year,
        }
        for r in records
    ]

    options = get_filter_options()

    return render_template(
        "insights.html",
        records=serialized_records,
        filters={
            "sector": sector or "",
            "job_role": job_role or "",
            "county": county or "",
            "month": month or "",
            "year": year or "",
        },
        options=options,
    )

@app.route("/company/<company_id>")
@login_required
def company_profile(company_id: str):
    jobs = JobRecord.query.filter_by(company_id=company_id).all()
    if not jobs:
        flash("No records found for this company.", "warning")
        return redirect(url_for("records"))

    company_name = jobs[0].company_name
    sector = jobs[0].sector
    logo_url = logo_url_for(company_id)

    pays = [j.pay_rate for j in jobs if j.pay_rate is not None]
    average_pay = round(sum(pays) / len(pays), 2) if pays else 0

    from collections import defaultdict

    county_pay = defaultdict(list)
    for j in jobs:
        if j.county and j.pay_rate is not None:
            county_pay[j.county].append(j.pay_rate)

    county_avg = {k: round(sum(v) / len(v), 2) for k, v in county_pay.items()}
    counties = list(county_avg.keys())

    peer_companies = []
    if counties:
        peer_jobs = JobRecord.query.filter(
            JobRecord.company_id != company_id,
            JobRecord.county.in_(counties),
            JobRecord.sector == sector,
        ).all()

        peer_data = {}
        for j in peer_jobs:
            if j.company_id not in peer_data:
                peer_data[j.company_id] = {
                    "company_name": j.company_name,
                    "jobs": [],
                    "logo": logo_url_for(j.company_id),
                }
            if j.pay_rate is not None:
                peer_data[j.company_id]["jobs"].append(j.pay_rate)

        for cid, data in peer_data.items():
            if data["jobs"]:
                avg = round(sum(data["jobs"]) / len(data["jobs"]), 2)
                peer_companies.append(
                    {
                        "company_id": cid,
                        "company_name": data["company_name"],
                        "logo_url": data["logo"],
                        "average_pay": avg,
                    }
                )
    else:
        flash("This company has no valid county data. Showing job list only.", "warning")

    return render_template(
        "company_profile.html",
        company_name=company_name,
        logo_url=logo_url,
        jobs=jobs,
        average_pay=average_pay,
        county_avg=county_avg,
        peer_companies=peer_companies,
    )
@app.route("/ai-logs", methods=["GET"])
@login_required
def ai_logs():
    # Only admins
    if getattr(current_user, "admin_level", 0) not in (1, 2):
        abort(403)

    # Query params
    q = (request.args.get("q") or "").strip()
    page = max(int(request.args.get("page", 1) or 1), 1)
    per_page = min(max(int(request.args.get("per_page", 20) or 20), 5), 100)

    # Base query
    query = AIAnalysisLog.query.order_by(desc(AIAnalysisLog.created_at))

    # Optional search
    if q:
        like = f"%{q}%"
        query = (
            query.join(User, AIAnalysisLog.user, isouter=True)
                 .filter(
                     or_(
                         AIAnalysisLog.filters.ilike(like),
                         AIAnalysisLog.output_html.ilike(like),
                         cast(AIAnalysisLog.record_count, String).ilike(like),
                         User.username.ilike(like),
                     )
                 )
        )

    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    logs = pagination.items

    return render_template(
        "ai_logs.html",
        logs=logs,
        pagination=pagination,
        q=q,
        per_page=per_page,
    )



@app.route("/ai-logs/<int:log_id>", methods=["GET"])
@login_required
def ai_logs_get(log_id):
    if getattr(current_user, "admin_level", 0) not in (1, 2):
        abort(403)
    row = AIAnalysisLog.query.get_or_404(log_id)
    return jsonify({
        "id": row.id,
        "user_id": row.user_id,
        "created_at": row.created_at.isoformat(),
        "record_count": row.record_count,
        "filters": row.filters,
        "output_html": row.output_html,
    })

# ---------------------- UPLOAD + IMPORT ----------------------

@app.route("/upload", methods=["GET", "POST"])
@login_required
def upload():
    if request.method == "POST":
        file = request.files.get("file")
        if not file or not file.filename:
            flash("No file selected.", "error")
            return redirect(request.url)

        filename = secure_filename(file.filename)
        ext = os.path.splitext(filename.lower())[1]
        allowed = app.config.get("ALLOWED_EXTENSIONS", {".xlsx", ".xls"})
        if ext not in allowed:
            flash("Only Excel/CSV files are supported.", "error")
            return redirect(request.url)

        filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(filepath)

        try:
            if ext == ".csv":
                df = pd.read_csv(filepath)
            else:
                df = pd.read_excel(filepath)

            df.columns = [str(c).strip().lower() for c in df.columns]

            required = {"company_id", "company_name", "sector", "postcode", "job_role", "pay_rate"}
            missing = [c for c in required if c not in df.columns]
            if missing:
                flash(f"Missing columns: {', '.join(missing)}", "error")
                return redirect(request.url)

            skip_geocode = bool(request.form.get("skip_geocode"))

            # Bulk geocode up-front (unless skipping)
            pc_to_latlon: dict[str, tuple[float | None, float | None]] = {}
            if not skip_geocode and "postcode" in df.columns:
                postcodes = df["postcode"].astype(str).fillna("").map(normalize_uk_postcode).tolist()
                pc_to_latlon = bulk_geocode_postcodes(postcodes)

            added = 0
            now_utc = datetime.now(timezone.utc)

            for _, row in df.iterrows():
                rowd = row.to_dict()
                postcode_raw = str(rowd.get("postcode", "") or "")
                postcode = normalize_uk_postcode(postcode_raw)

                if skip_geocode or not postcode:
                    lat, lon = (None, None)
                else:
                    lat, lon = pc_to_latlon.get(postcode, (None, None))
                    # Optional: try single-lookup fallback if bulk returned None
                    if lat is None or lon is None:
                        lat, lon = geocode_postcode_cached(postcode)

                rec = JobRecord(
                    company_id=str(rowd.get("company_id", "") or ""),
                    company_name=str(rowd.get("company_name", "") or ""),
                    sector=str(rowd.get("sector", "") or ""),
                    postcode=postcode,
                    job_role=str(rowd.get("job_role", "") or ""),
                    pay_rate=float(rowd.get("pay_rate") or 0.0),
                    county=str(rowd.get("county", "") or ""),
                    latitude=lat,
                    longitude=lon,
                    imported_month=str(now_utc.month),
                    imported_year=str(now_utc.year),
                )
                db.session.add(rec)
                added += 1

            try:
                commit_or_rollback()
                flash(f"{added} records successfully uploaded.", "success")
            except Exception as e:
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

        return redirect(url_for("upload"))

    return render_template("upload.html")

@app.route("/insights/ai-analyze", methods=["POST"])
@login_required
def ai_analyze_insights():
    payload = request.get_json(force=True, silent=True) or {}
    filters = payload.get("filters", {})
    recs = payload.get("records", [])[:5000]  # safety cap

    system_prompt = (
        "You are a data analyst specialising in UK social care workforce pay. "
        "Be concise and specific. Provide insights in HTML (<h4>, <ul>, <li>)."
    )

    user_prompt = f"""
    Current filters: {json.dumps(filters, ensure_ascii=False)}

    Records sample (first {min(len(recs), 200)} of {len(recs)}):
    {json.dumps(recs[:200], ensure_ascii=False)}

    Please:
    - Summarise pay levels (mean, median, range).
    - Highlight differences by sector, role, and county.
    - Compare against UK RLW (£12.00) and London RLW (£13.15).
    - Point out outliers and concentrations.
    - Provide 3–5 actionable insights in under 200 words.
    """

    try:
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
        )

        html = resp.choices[0].message.content.strip()

        # 🔹 Log usage
        log = AIAnalysisLog(
            user_id=current_user.id,
            filters=json.dumps(filters, ensure_ascii=False),
            record_count=len(recs),
            output_html=html,
        )
        db.session.add(log)
        db.session.commit()

        return jsonify({"ok": True, "html": html})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


# ---------------------- LOGIN ----------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        remember = "remember" in request.form

        user = User.query.filter_by(username=username).first()
        if user and check_password_hash(user.password, password):
            login_user(user, remember=remember)
            flash("Logged in successfully.", "success")
            next_page = request.args.get("next")
            return redirect(next_page) if next_page else redirect(url_for("home"))
        flash("Invalid username or password.", "error")

    return render_template("login.html")

@app.route("/logout")
def logout():
    logout_user()
    flash("Logged out.", "info")
    return redirect(url_for("login"))

# ---------------------- CLI COMMANDS ----------------------

def _read_dataframe_from_path(path: str):
    ext = os.path.splitext(path.lower())[1]
    if ext == ".csv":
        df = pd.read_csv(path)
    elif ext in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    else:
        raise ValueError("Unsupported file type; use .csv, .xlsx, or .xls")
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df, ext

def _ingest_df(df, *, month: int | None, year: int | None, skip_geocode: bool = False) -> int:
    required = {"company_id", "company_name", "sector", "postcode", "job_role", "pay_rate"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {', '.join(missing)}")

    now_utc = datetime.now(timezone.utc)
    imonth = str(month or now_utc.month)
    iyear = str(year or now_utc.year)

    pc_to_latlon: dict[str, tuple[float | None, float | None]] = {}
    if not skip_geocode and "postcode" in df.columns:
        postcodes = df["postcode"].astype(str).fillna("").map(normalize_uk_postcode).tolist()
        pc_to_latlon = bulk_geocode_postcodes(postcodes)

    added = 0
    for _, row in df.iterrows():
        rowd = row.to_dict()
        postcode_raw = str(rowd.get("postcode", "") or "")
        postcode = normalize_uk_postcode(postcode_raw)
        if skip_geocode or not postcode:
            lat, lon = (None, None)
        else:
            lat, lon = pc_to_latlon.get(postcode, (None, None))
            if lat is None or lon is None:
                lat, lon = geocode_postcode_cached(postcode)

        rec = JobRecord(
            company_id=str(rowd.get("company_id", "") or ""),
            company_name=str(rowd.get("company_name", "") or ""),
            sector=str(rowd.get("sector", "") or ""),
            postcode=postcode,
            job_role=str(rowd.get("job_role", "") or ""),
            pay_rate=float(rowd.get("pay_rate") or 0.0),
            county=str(rowd.get("county", "") or ""),
            latitude=lat,
            longitude=lon,
            imported_month=imonth,
            imported_year=iyear,
        )
        db.session.add(rec)
        added += 1

    commit_or_rollback()
    return added

@app.cli.command("purge-records")
def purge_records():
    """Delete ALL JobRecord rows (keeps users)."""
    count = db.session.query(JobRecord).delete(synchronize_session=False)
    db.session.commit()
    # For SQLite: normal INTEGER PRIMARY KEY resets to 1 when empty; no sqlite_sequence needed.
    click.echo(f"Purged {count} JobRecord rows.")

@app.cli.command("import-data")
@click.argument("path", type=click.Path(exists=True, dir_okay=False))
@click.option("--purge", is_flag=True, help="Delete all existing JobRecord rows first.")
@click.option("--skip-geocode", is_flag=True, help="Do not geocode postcodes (faster).")
@click.option("--month", type=int, default=None, help="Override imported month (1-12).")
@click.option("--year", type=int, default=None, help="Override imported year, e.g. 2025.")
def import_data(path, purge, skip_geocode, month, year):
    """Import Excel/CSV file into JobRecord."""
    if purge:
        ctx = click.get_current_context()
        ctx.invoke(purge_records)
    df, _ = _read_dataframe_from_path(path)
    added = _ingest_df(df, month=month, year=year, skip_geocode=skip_geocode)
    click.echo(f"Imported {added} records from {os.path.basename(path)} (skip_geocode={skip_geocode}).")

@app.cli.command("geocode-missing")
@click.option("--limit", type=int, default=None, help="Max rows to process this run.")
def geocode_missing(limit):
    """Bulk-geocode JobRecords missing lat/lon using postcodes.io."""
    q = JobRecord.query.filter(
        (JobRecord.latitude == None) | (JobRecord.longitude == None)  # noqa: E711
    )
    if limit:
        q = q.limit(limit)
    rows = q.all()
    pcs = [normalize_uk_postcode(r.postcode or "") for r in rows if r.postcode]
    mapping = bulk_geocode_postcodes(pcs)
    updated = 0
    for r in rows:
        pc = normalize_uk_postcode(r.postcode or "")
        latlon = mapping.get(pc)
        if latlon and latlon[0] is not None:
            r.latitude, r.longitude = latlon
            updated += 1
    commit_or_rollback()
    click.echo(f"Geocoded {updated} of {len(rows)} records.")

# ---------------------- MAIN ----------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
