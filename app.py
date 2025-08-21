from __future__ import annotations

import os
from datetime import datetime
from functools import lru_cache

import pandas as pd
import requests
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
    jsonify,
)
from flask_login import (
    LoginManager,
    login_required,
    login_user,
    logout_user,
    current_user,
    UserMixin,
)
from flask_migrate import Migrate
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename

# ---------------------- APP & DB SETUP ----------------------

app = Flask(__name__)
app.config.from_pyfile("config.py")

# Ensure upload folder exists if configured
if app.config.get("UPLOAD_FOLDER"):
    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

db = SQLAlchemy(app)
migrate = Migrate(app, db)

login_manager = LoginManager()
login_manager.login_view = "login"
login_manager.init_app(app)


# ---------------------- MODELS ----------------------

class JobRecord(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.String(50), index=True)
    company_name = db.Column(db.String(100))
    sector = db.Column(db.String(50), index=True)
    job_role = db.Column(db.String(100), index=True)
    postcode = db.Column(db.String(20))
    county = db.Column(db.String(50), index=True)
    pay_rate = db.Column(db.Float)
    imported_month = db.Column(db.String(20), index=True)
    imported_year = db.Column(db.String(10), index=True)
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)


class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(100), unique=True, nullable=False)
    password = db.Column(db.String(200), nullable=False)
    admin_level = db.Column(db.Integer, default=0)

    def is_admin(self) -> bool:
        return self.admin_level in [1, 2]

    def is_superuser(self) -> bool:
        return self.admin_level == 1


@login_manager.user_loader
def load_user(user_id: str):
    # Works with SQLAlchemy 1.x/2.x
    try:
        return db.session.get(User, int(user_id))
    except Exception:
        return None


# ---------------------- HELPERS ----------------------

def get_filter_options():
    """Fetch distinct values for dropdowns efficiently (no full-table scans)."""
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


def build_filters_from_request(mapping: dict[str, any]):
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
                except Exception as e:
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
    # Pass the function so Jinja can call it if desired
    return render_template("index.html", now=datetime.now)


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

    pagination = base_q.paginate(page=page, per_page=25)
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
        return redirect(url_for("map_sector_select"))

    # GET -> JSON for inline editor
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
    return "🚧 Export feature coming soon!"


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

    # Attach logo URLs safely
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

    from geopy.geocoders import Nominatim
    from geopy.extra.rate_limiter import RateLimiter

    geolocator = Nominatim(user_agent="pay-rate-map")
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=2)

    updated = 0
    skipped = 0

    missing = (
        JobRecord.query.filter((JobRecord.county == None) | (JobRecord.county == ""))
        .all()
    )
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
    return {"current_year": datetime.now().year}


@app.route("/dashboard")
@login_required
def dashboard():
    # Filters from query string
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

    # Totals and averages from filtered records
    total_records = len(filtered_records)
    total_companies = len({r.company_id for r in filtered_records})
    pays = [r.pay_rate for r in filtered_records if r.pay_rate is not None]
    avg_pay = round(sum(pays) / len(pays), 2) if pays else 0

    # Group by sector / county using the same filters
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

    # Dropdown options
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

    # Base query with filters
    filters = build_filters_from_request(
        {
            "sector": sector,
            "job_role": job_role,
            "county": county,
            "month": month,
            "year": year,
        }
    )
    base_q = JobRecord.query.filter(*filters)
    records = base_q.all()

    # Convert records to serializable format for JS
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
        # Peer companies in same county + same sector (excluding this company)
        peer_jobs = JobRecord.query.filter(
            JobRecord.company_id != company_id,
            JobRecord.county.in_(counties),
            JobRecord.sector == sector,
        ).all()

        # Group by peer company
        peer_data = {}
        for j in peer_jobs:
            if j.company_id not in peer_data:
                peer_data[j.company_id] = {
                    "company_name": j.company_name,
                    "jobs": [],
                    "logo": f"/static/logos/{j.company_id}.png",
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
        flash(
            "This company has no valid county data. Showing job list only.", "warning"
        )

    return render_template(
        "company_profile.html",
        company_name=company_name,
        logo_url=logo_url,
        jobs=jobs,
        average_pay=average_pay,
        county_avg=county_avg,
        peer_companies=peer_companies,
    )


# ---------------------- UPLOAD + GEOCODE ----------------------

@lru_cache(maxsize=5000)
def geocode_postcode_cached(postcode: str):
    return geocode_postcode(postcode)


def geocode_postcode(postcode: str) -> tuple[float | None, float | None]:
    postcode = (postcode or "").strip()
    if not postcode:
        return (None, None)
    try:
        response = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"format": "json", "q": postcode},
            headers={"User-Agent": "PayRateMapUploader"},
            timeout=15,
        )
        response.raise_for_status()
        data = response.json()
        if data:
            return (float(data[0]["lat"]), float(data[0]["lon"]))
    except Exception as e:
        print(f"Geocoding error for {postcode}: {e}")
    return (None, None)


@app.route("/upload", methods=["GET", "POST"])
@login_required
def upload():
    if request.method == "POST":
        file = request.files.get("file")
        if not file or not file.filename:
            flash("No file selected.", "error")
            return redirect(request.url)

        filename = secure_filename(file.filename)
        if not filename.lower().endswith((".xlsx", ".xls")):
            flash("Only Excel files are supported.", "error")
            return redirect(request.url)

        filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(filepath)

        try:
            df = pd.read_excel(filepath)

            # Normalize column names to lower-case once
            df.columns = [str(c).strip().lower() for c in df.columns]

            required = {
                "company_id",
                "company_name",
                "sector",
                "postcode",
                "job_role",
                "pay_rate",
            }
            missing = [c for c in required if c not in df.columns]
            if missing:
                flash(f"Missing columns: {', '.join(missing)}", "error")
                return redirect(request.url)

            added = 0
            for _, row in df.iterrows():
                # Row dict with safe defaults
                row_dict = row.to_dict()

                postcode = str(row_dict.get("postcode", "") or "").strip()
                lat, lon = geocode_postcode_cached(postcode.upper())

                # Store month/year as strings to match your current model types
                rec = JobRecord(
                    company_id=str(row_dict.get("company_id", "") or ""),
                    company_name=str(row_dict.get("company_name", "") or ""),
                    sector=str(row_dict.get("sector", "") or ""),
                    postcode=postcode,
                    job_role=str(row_dict.get("job_role", "") or ""),
                    pay_rate=float(row_dict.get("pay_rate") or 0.0),
                    county=str(row_dict.get("county", "") or ""),
                    latitude=lat,
                    longitude=lon,
                    imported_month=str(datetime.now().month),
                    imported_year=str(datetime.now().year),
                )
                db.session.add(rec)
                added += 1

            try:
                commit_or_rollback()
                flash(f"{added} records successfully uploaded.", "success")
            except Exception as e:
                flash("Failed to save uploaded records.", "error")

        except Exception as e:
            flash("Error processing file.", "error")
            print(f"🚫 Upload failed ➡ {e}")

        return redirect(url_for("upload"))

    return render_template("upload.html")


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


# ---------------------- MAIN ----------------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
