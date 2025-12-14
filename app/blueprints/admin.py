# app/blueprints/admin.py
from __future__ import annotations

import os
import json
from datetime import datetime, date
from functools import wraps

from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    abort,
    jsonify,
    current_app,
)
from flask_login import login_required, current_user
from sqlalchemy import desc, or_, cast, String, text, inspect, func
from sqlalchemy.orm import joinedload

from extensions import db
from models import (
    User,
    AIAnalysisLog,
    JobRecord,
    JobPosting,
    Company,
    CronRunLog,
    OnsEarnings,
    JobRoleMapping,
    ensure_default_organisation,
)
from .utils import (
    commit_or_rollback,
    normalize_uk_postcode,
    bulk_geocode_postcodes,
    geocode_postcode_cached,
    snap_to_nearest_postcode,
)

from cron_runner import run_job_role_canonicaliser
from app.scrapers.adzuna import AdzunaScraper
from app.importers.job_importer import import_posting_to_record
from ons_importer import import_ons_earnings_to_db, import_latest_ons_earnings_for_cron
from app.blueprints.coverage import get_weekly_coverage_snapshot


from . import pay_compare  # relative import of the module you just edited

# Blueprint MUST be defined before any @bp.route decorator
bp = Blueprint("admin", __name__)


def superuser_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Only allow admin_level 1
        if not current_user.is_authenticated or current_user.admin_level != 1:
            abort(403)
        return f(*args, **kwargs)

    return decorated_function


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------
def _require_superuser():
    if not current_user.is_authenticated or not current_user.is_superuser():
        flash("Access denied – superuser only.", "error")
        return False
    return True


def _safe_json_loads(value):
    """
    Best-effort JSON loader for CronRunLog.run_stats / other text fields.
    Returns dict on success, else None.
    """
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    try:
        s = value.decode("utf-8") if isinstance(value, (bytes, bytearray)) else str(value)
        s = s.strip()
        if not s:
            return None
        return json.loads(s)
    except Exception:
        return None


from datetime import date, timedelta
from sqlalchemy import func
from models import JobSummaryDaily


def _get_coverage_window(start_date: date, end_date_exclusive: date):
    """
    Shared coverage query for a window:
      start_date <= date < end_date_exclusive
    """
    # -----------------------------
    # Sector coverage
    # -----------------------------
    sector_rows = (
        db.session.query(
            JobSummaryDaily.sector.label("sector"),
            func.sum(JobSummaryDaily.adverts_count).label("adverts"),
            func.avg(JobSummaryDaily.median_pay_rate).label("median_pay"),
            func.count(func.distinct(JobSummaryDaily.date)).label("days_seen"),
        )
        .filter(JobSummaryDaily.date >= start_date)
        .filter(JobSummaryDaily.date < end_date_exclusive)
        .group_by(JobSummaryDaily.sector)
        .order_by(func.sum(JobSummaryDaily.adverts_count).desc())
        .all()
    )

    # -----------------------------
    # Location coverage
    # -----------------------------
    location_rows = (
        db.session.query(
            JobSummaryDaily.county.label("county"),
            func.sum(JobSummaryDaily.adverts_count).label("adverts"),
            func.count(func.distinct(JobSummaryDaily.sector)).label("sector_count"),
            func.count(func.distinct(JobSummaryDaily.date)).label("days_seen"),
        )
        .filter(JobSummaryDaily.date >= start_date)
        .filter(JobSummaryDaily.date < end_date_exclusive)
        .group_by(JobSummaryDaily.county)
        .order_by(func.sum(JobSummaryDaily.adverts_count).desc())
        .all()
    )

    return sector_rows, location_rows


def get_weekly_coverage(days: int = 7):
    start_date = date.today() - timedelta(days=days)
    end_excl = date.today() + timedelta(days=1)

    sector_rows, location_rows = _get_coverage_window(start_date, end_excl)

    weak_sectors = [r for r in sector_rows if r.days_seen < 2]
    weak_locations = [r for r in location_rows if r.days_seen < 2]

    summary = {
        "total_sectors": len(sector_rows),
        "total_locations": len(location_rows),
        "weak_sectors": len(weak_sectors),
        "weak_locations": len(weak_locations),
        "coverage_days": days,
    }

    return {
        "summary": summary,
        "sectors": sector_rows,
        "locations": location_rows,
        "weak_sectors": weak_sectors,
        "weak_locations": weak_locations,
    }


def get_weekly_coverage_diff():
    """
    Proper week-over-week diff:
      - this_week: last 7 days
      - last_week: the 7 days before that (non-overlapping)
    """
    today = date.today()

    this_start = today - timedelta(days=7)
    this_end_excl = today + timedelta(days=1)

    prev_start = today - timedelta(days=14)
    prev_end_excl = today - timedelta(days=7)

    this_sector_rows, this_location_rows = _get_coverage_window(this_start, this_end_excl)
    prev_sector_rows, prev_location_rows = _get_coverage_window(prev_start, prev_end_excl)

    def index_by(rows, key):
        return {getattr(r, key): r for r in rows}

    this_sectors = index_by(this_sector_rows, "sector")
    last_sectors = index_by(prev_sector_rows, "sector")

    sector_diff = []
    for sector, row in this_sectors.items():
        prev = last_sectors.get(sector)
        delta = (row.adverts or 0) - (prev.adverts or 0 if prev else 0)
        sector_diff.append({
            "sector": sector,
            "this_week": int(row.adverts or 0),
            "last_week": int(prev.adverts or 0) if prev else 0,
            "delta": delta,
        })

    this_locations = index_by(this_location_rows, "county")
    last_locations = index_by(prev_location_rows, "county")

    location_diff = []
    for county, row in this_locations.items():
        prev = last_locations.get(county)
        delta = (row.adverts or 0) - (prev.adverts or 0 if prev else 0)
        location_diff.append({
            "county": county,
            "this_week": int(row.adverts or 0),
            "last_week": int(prev.adverts or 0) if prev else 0,
            "delta": delta,
        })

    return {
        "sector_diff": sorted(sector_diff, key=lambda x: x["delta"]),
        "location_diff": sorted(location_diff, key=lambda x: x["delta"]),
    }


def upsert_job_record(record, search_role=None, search_location=None) -> JobPosting:
    """
    Insert or update a JobPosting based on (source_site, external_id) or URL.
    Mirrors the logic from app/scrapers/run.py, and stores search role/location.
    """
    query = JobPosting.query.filter(JobPosting.source_site == record.source_site)

    if record.external_id:
        query = query.filter(JobPosting.external_id == record.external_id)
    elif record.url:
        query = query.filter(JobPosting.url == record.url)

    job = query.first()

    if job is None:
        job = JobPosting(source_site=record.source_site)
        db.session.add(job)

    job.title = record.title
    job.company_name = record.company_name
    job.location_text = record.location_text
    job.postcode = record.postcode
    job.min_rate = record.min_rate
    job.max_rate = record.max_rate
    job.rate_type = record.rate_type
    job.contract_type = record.contract_type
    job.external_id = record.external_id
    job.url = record.url
    job.posted_date = record.posted_date
    job.scraped_at = datetime.utcnow()
    job.is_active = True

    if search_role is not None:
        job.search_role = search_role
    if search_location is not None:
        job.search_location = search_location

    if record.raw_json is not None:
        try:
            job.raw_json = json.dumps(record.raw_json)
        except TypeError:
            job.raw_json = json.dumps({"repr": repr(record.raw_json)})

    return job


# -------------------------------------------------------------------
# JOB SCRAPER PAGE (manual scrape)
# -------------------------------------------------------------------
@bp.route("/jobs/scrape", methods=["GET", "POST"])
@login_required
def admin_job_scrape():
    """
    Admin page to manually trigger job scrapes with custom role + location parameters.
    """
    if not _require_superuser():
        return redirect(url_for("home"))

    role = request.form.get("role") or ""
    location = request.form.get("location") or ""
    message = None
    records = []
    processed = 0

    if request.method == "POST":
        scraper = AdzunaScraper(what=role, where=location)
        try:
            records = scraper.scrape()
        except Exception as exc:  # noqa: BLE001
            message = f"Error during scrape: {exc}"
            return render_template(
                "admin/jobs_scrape.html",
                role=role,
                location=location,
                message=message,
                processed=processed,
                records=[],
            )

        for rec in records:
            upsert_job_record(rec, search_role=role, search_location=location)
            processed += 1

        db.session.commit()
        message = f"Scrape complete — {processed} records inserted/updated."

    return render_template(
        "admin/jobs_scrape.html",
        role=role,
        location=location,
        message=message,
        processed=processed,
        records=records,
    )


# -------------------------------------------------------------------
# USER MANAGEMENT
# -------------------------------------------------------------------
@bp.route("/users", methods=["GET", "POST"])
@login_required
def manage_users():
    if not _require_superuser():
        return redirect(url_for("home"))

    if request.method == "POST":
        action = request.form.get("action")

        # ----------------------------------------------------
        # ADD USER
        # ----------------------------------------------------
        if action == "add":
            from werkzeug.security import generate_password_hash

            username = (request.form.get("username") or "").strip()
            password = (request.form.get("password") or "").strip()
            admin_level = int(request.form.get("admin_level", 0))

            if not username or not password:
                flash("Username and password are required.", "error")
            elif db.session.query(User).filter_by(username=username).first():
                flash("Username already exists.", "error")
            else:
                # Attach new user to the same organisation as the current user,
                # or fall back to the default org.
                org = getattr(current_user, "organisation", None)
                if org is None:
                    org = ensure_default_organisation()

                # Derive org_role from admin_level:
                #  - superuser (1)  -> owner
                #  - admin (2)      -> admin
                #  - normal (0)     -> member
                if admin_level == 1:
                    org_role = "owner"
                elif admin_level == 2:
                    org_role = "admin"
                else:
                    org_role = "member"

                new_user = User(
                    username=username,
                    password=generate_password_hash(password),
                    admin_level=admin_level,
                    organisation_id=org.id,
                    org_role=org_role,
                )
                db.session.add(new_user)
                try:
                    commit_or_rollback()
                    flash(
                        f"User '{username}' added "
                        f"(org={org.slug}, role={org_role}, admin_level={admin_level}).",
                        "success",
                    )
                except Exception:
                    flash("Failed to add user.", "error")

        # ----------------------------------------------------
        # DELETE USER
        # ----------------------------------------------------
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

        # ----------------------------------------------------
        # UPDATE USER (admin_level only for now)
        # ----------------------------------------------------
        elif action == "update":
            user_id = request.form.get("user_id")
            admin_level = request.form.get("admin_level")
            if not (user_id and admin_level is not None):
                flash("Missing user ID or admin level.", "error")
            else:
                user = db.session.get(User, int(user_id))
                if user:
                    user.admin_level = int(admin_level)

                    # Optionally keep org_role roughly in sync:
                    if user.admin_level == 1:
                        user.org_role = "owner"
                    elif user.admin_level == 2:
                        user.org_role = "admin"
                    else:
                        if user.org_role != "owner":
                            user.org_role = "member"

                    try:
                        commit_or_rollback()
                        flash("User updated.", "success")
                    except Exception:
                        flash("Failed to update user.", "error")
                else:
                    flash("User not found.", "error")

    
    users = User.query.options(joinedload(User.organisation)).all()
    return render_template("manage_users.html", users=users)


@bp.route("/seed-default-org")
@login_required
def seed_default_org():
    # Only admins/superusers
    if getattr(current_user, "admin_level", 0) not in (1, 2):
        abort(403)

    org = ensure_default_organisation()

    users = User.query.filter(User.organisation_id.is_(None)).all()
    for u in users:
        u.organisation_id = org.id
        if not u.org_role:
            u.org_role = "owner"

    db.session.commit()

    flash(
        f"Seeded default organisation '{org.name}'. "
        f"Attached {len(users)} users.",
        "success",
    )
    return redirect(url_for("records.records"))


# -------------------------------------------------------------------
# BACKFILL COUNTIES
# -------------------------------------------------------------------
@bp.route("/backfill-counties")
@login_required
def backfill_counties():
    if not _require_superuser():
        return redirect(url_for("home"))

    from geopy.geocoders import Nominatim
    from geopy.extra.rate_limiter import RateLimiter

    geolocator = Nominatim(user_agent="pay-rate-map")
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=2)

    updated = 0
    skipped = 0

    q = JobRecord.query.filter(
        (JobRecord.county == None) | (JobRecord.county == "")  # noqa: E711
    )
    limit = request.args.get("limit", type=int)
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
            except Exception as e:  # noqa: BLE001
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
    return redirect(url_for("upload.upload"))


# -------------------------------------------------------------------
# ONS IMPORT – BUTTON 1 (legacy route)
# -------------------------------------------------------------------
@bp.route("/admin/ons-import", methods=["POST"])
@login_required
def run_ons_import():
    if getattr(current_user, "admin_level", 0) != 1:
        abort(403)

    from models import OnsEarnings  # local import to avoid cycles

    db_uri = current_app.config.get("SQLALCHEMY_DATABASE_URI")
    print("🔎 ONS IMPORT using DB URI:", db_uri)

    year = date.today().year - 1

    before_count = (
        db.session.query(func.count())
        .filter(OnsEarnings.year == year)
        .scalar()
    )
    print(f"🔢 Before import: OnsEarnings rows for {year} = {before_count}")

    result = import_ons_earnings_to_db(
        year,
        trigger="admin_button",
        triggered_by=getattr(current_user, "username", None),
        use_app_context=True,
    )

    after_count = (
        db.session.query(func.count())
        .filter(OnsEarnings.year == year)
        .scalar()
    )
    print(f"🔢 After import: OnsEarnings rows for {year} = {after_count}")

    if result.get("error"):
        flash(f"ONS import FAILED for {year}: {result['error']}", "error")
    else:
        fetched = result.get("fetched", 0)
        created = result.get("created", 0)
        updated = result.get("updated", 0)
        flash(
            f"Imported ONS ASHE for {year}: "
            f"fetched {fetched}, created {created}, updated {updated}. "
            f"(Before: {before_count}, after: {after_count})",
            "success",
        )

    return redirect(url_for("admin.admin_tools"))


@bp.route("/debug/pay-explorer-json")
@login_required
@superuser_required
def debug_pay_explorer_json():
    data = pay_compare.get_pay_explorer_data(
        start_date_str=None,
        end_date_str=None,
        sector=None,
        job_role_group=None,
        group_by="county",
    )
    return jsonify(data)


# -------------------------------------------------------------------
# ONS IMPORT – BUTTON 2 (new helper-based route)
# -------------------------------------------------------------------
@bp.route("/ons/import", methods=["POST"])
@login_required
@superuser_required
def run_ons_import_manual():
    who = (
        getattr(current_user, "email", None)
        or getattr(current_user, "username", None)
        or "admin"
    )

    try:
        result = import_latest_ons_earnings_for_cron(
            trigger="manual",
            triggered_by=who,
            use_app_context=True,
        )
        year = result.get("year")
        created = result.get("created")
        updated = result.get("updated")
        msg = f"ONS ASHE import for {year} completed: created={created}, updated={updated}"
        flash(msg, "success")
    except Exception as e:  # noqa: BLE001
        flash(f"ONS ASHE import failed: {e}", "danger")

    return redirect(url_for("admin.admin_tools"))


# -------------------------------------------------------------------
# Simple ONS inspection helper
# -------------------------------------------------------------------
@bp.route("/inspect/ons")
@superuser_required
def inspect_ons():
    from sqlalchemy import func
    from models import OnsEarnings, db

    grouped = (
        db.session.query(
            OnsEarnings.year.label("year"),
            OnsEarnings.measure_code.label("measure_code"),
            func.count().label("count"),
        )
        .group_by(OnsEarnings.year, OnsEarnings.measure_code)
        .order_by(OnsEarnings.year.asc(), OnsEarnings.measure_code.asc())
        .all()
    )

    summary = [
        {
            "year": int(row.year) if row.year is not None else None,
            "measure_code": row.measure_code,
            "count": int(row.count),
        }
        for row in grouped
    ]

    latest_year = db.session.query(func.max(OnsEarnings.year)).scalar()

    latest_rows: list[dict] = []
    if latest_year is not None:
        samples = (
            OnsEarnings.query
            .filter(OnsEarnings.year == latest_year)
            .order_by(OnsEarnings.geography_name.asc())
            .limit(10)
            .all()
        )
        latest_rows = [
            {
                "year": int(r.year) if r.year is not None else None,
                "geography_code": r.geography_code,
                "geography_name": r.geography_name,
                "measure_code": r.measure_code,
                "value": float(r.value) if r.value is not None else None,
            }
            for r in samples
        ]

    return jsonify(
        {
            "summary": summary,
            "latest_year": int(latest_year) if latest_year is not None else None,
            "latest_rows": latest_rows,
        }
    )


# -------------------------------------------------------------------
# Pay Explorer / ONS mapping debug endpoint
# -------------------------------------------------------------------
@bp.route("/debug/pay-explorer-mapping")
@login_required
@superuser_required
def debug_pay_explorer_mapping():
    days = request.args.get("days", default=30, type=int)
    rows, ons_year = pay_compare.build_pay_explorer_debug_snapshot(days=days)
    return jsonify(
        {
            "ons_year": ons_year,
            "rows": rows,
        }
    )


# -------------------------------------------------------------------
# COMPANIES ADMIN
# -------------------------------------------------------------------
@bp.route("/companies", methods=["GET", "POST"])
@login_required
def admin_companies():
    if not _require_superuser():
        return redirect(url_for("home"))

    from app.blueprints.utils import _slugify  # reuse same slug logic for mapping

    if request.method == "POST":
        action = request.form.get("action")

        if action == "merge":
            target_slug = (request.form.get("target_company_id") or "").strip()
            source_raw = (request.form.get("source_company_ids") or "").strip()
            source_slugs = [
                s.strip()
                for s in source_raw.replace("\n", ",").split(",")
                if s.strip()
            ]

            source_slugs = [s for s in source_slugs if s != target_slug]
            source_slugs = list(dict.fromkeys(source_slugs))

            if not target_slug or not source_slugs:
                flash("Please provide a target company ID and at least one source ID.", "error")
            else:
                total_moved = 0
                for s in source_slugs:
                    moved = (
                        JobRecord.query
                        .filter(JobRecord.company_id == s)
                        .update({JobRecord.company_id: target_slug})
                    )
                    total_moved += moved
                try:
                    commit_or_rollback()
                    flash(
                        f"Merged {len(source_slugs)} company IDs into '{target_slug}' "
                        f"({total_moved} job records updated).",
                        "success",
                    )
                except Exception as e:  # noqa: BLE001
                    print("Merge companies error:", e)
                    flash("Failed to merge companies.", "error")

            return redirect(url_for("admin.admin_companies"))

        elif action == "update_company":
            company_db_id = request.form.get("company_db_id", type=int)
            if not company_db_id:
                flash("Missing company ID.", "error")
                return redirect(url_for("admin.admin_companies"))

            company = Company.query.get(company_db_id)
            if not company:
                flash("Company not found.", "error")
                return redirect(url_for("admin.admin_companies"))

            company.name = (request.form.get("name") or "").strip()
            company.canonical_name = (request.form.get("canonical_name") or "").strip()
            company.sector = (request.form.get("sector") or "").strip() or None

            try:
                commit_or_rollback()
                flash("Company updated.", "success")
            except Exception as e:  # noqa: BLE001
                print("Update company error:", e)
                flash("Failed to update company.", "error")

            return redirect(url_for("admin.admin_companies"))

        elif action == "upload_logo":
            slug = (request.form.get("company_id") or "").strip()
            logo_file = request.files.get("logo_file")

            if not slug or not logo_file or not logo_file.filename:
                flash("Missing company ID or logo file.", "error")
                return redirect(url_for("admin.admin_companies"))

            logos_folder = os.path.join(current_app.root_path, "static", "logos")
            os.makedirs(logos_folder, exist_ok=True)
            filename = f"{slug}.png"
            path = os.path.join(logos_folder, filename)
            try:
                logo_file.save(path)
                flash(f"Logo uploaded for '{slug}'.", "success")
            except Exception as e:  # noqa: BLE001
                print("Logo upload error:", e)
                flash("Failed to upload logo.", "error")

            return redirect(url_for("admin.admin_companies"))

    company_groups = (
        db.session.query(
            JobRecord.company_id,
            func.count(JobRecord.id).label("job_count"),
            func.min(JobRecord.company_name).label("sample_name"),
        )
        .group_by(JobRecord.company_id)
        .order_by(func.count(JobRecord.id).desc())
        .all()
    )

    all_companies = Company.query.order_by(Company.canonical_name, Company.name).all()

    slug_to_company = {}
    for c in all_companies:
        slug = _slugify(c.canonical_name or c.name or "")
        slug_to_company.setdefault(slug, c)

    rows = []
    for row in company_groups:
        slug = (row.company_id or "").strip() or "unknown"
        rows.append(
            {
                "company_id": slug,
                "job_count": row.job_count,
                "sample_name": row.sample_name,
                "company": slug_to_company.get(slug),
            }
        )

    return render_template(
        "admin/companies.html",
        rows=rows,
        companies=all_companies,
    )


@bp.route("/companies/regenerate-ids")
@login_required
def regenerate_company_ids():
    if not _require_superuser():
        return redirect(url_for("home"))

    from app.blueprints.utils import _clean_company_name, _slugify

    companies = Company.query.all()
    jobs = JobRecord.query.all()

    canonical_to_slug: dict[str, str] = {}
    for c in companies:
        raw_canon = (c.canonical_name or "").strip()
        if not raw_canon:
            raw_canon = _clean_company_name(c.name or "")
        cleaned_canon = _clean_company_name(raw_canon)
        if not cleaned_canon:
            continue
        slug = _slugify(cleaned_canon)
        canonical_to_slug[cleaned_canon] = slug

    updated = 0
    skipped = 0

    for job in jobs:
        raw_name = (job.company_name or "").strip()
        if not raw_name:
            skipped += 1
            continue

        if job.company_id and job.company_id.strip():
            skipped += 1
            continue

        cleaned = _clean_company_name(raw_name)
        if not cleaned:
            skipped += 1
            continue

        target_slug = canonical_to_slug.get(cleaned) or _slugify(cleaned)
        if job.company_id == target_slug:
            skipped += 1
            continue

        job.company_id = target_slug
        updated += 1

    try:
        commit_or_rollback()
        flash(
            f"Regenerated company IDs from canonical names — updated {updated}, skipped {skipped}.",
            "success",
        )
    except Exception as e:  # noqa: BLE001
        print("Regenerate company IDs error:", e)
        flash("Failed to regenerate company IDs.", "error")

    return redirect(url_for("admin.admin_companies"))


# -------------------------------------------------------------------
# REGEOCODE JOBS
# -------------------------------------------------------------------
@bp.route("/regeocode-jobs")
@login_required
def regeocode_jobs():
    if not _require_superuser():
        return redirect(url_for("home"))

    geocode_postcode_cached.cache_clear()

    limit = request.args.get("limit", type=int)
    query = JobRecord.query
    if limit:
        query = query.limit(limit)
    jobs = query.all()

    updated = 0
    cleared = 0

    for job in jobs:
        lat = job.latitude
        lon = job.longitude

        if job.postcode:
            new_lat, new_lon = geocode_postcode_cached(job.postcode)
            if new_lat is not None and new_lon is not None:
                job.latitude = new_lat
                job.longitude = new_lon
                updated += 1
                continue

        if lat is not None and lon is not None:
            inferred_pc, snapped_lat, snapped_lon = snap_to_nearest_postcode(lat, lon)
            if inferred_pc and snapped_lat is not None and snapped_lon is not None:
                if not job.postcode:
                    job.postcode = inferred_pc
                job.latitude = snapped_lat
                job.longitude = snapped_lon
                updated += 1
                continue

        job.latitude = None
        job.longitude = None
        cleared += 1

    try:
        commit_or_rollback()
        flash(
            f"✅ Re-geocoding complete. Updated: {updated}, "
            f"cleared invalid locations: {cleared}",
            "success",
        )
    except Exception:
        flash("Failed to save re-geocoding results.", "error")

    return redirect(url_for("upload.upload"))


# -------------------------------------------------------------------
# AI LOGS
# -------------------------------------------------------------------
@bp.route("/ai-logs", methods=["GET"])
@login_required
def ai_logs():
    if getattr(current_user, "admin_level", 0) not in (1, 2):
        abort(403)

    q = (request.args.get("q") or "").strip()
    page = max(int(request.args.get("page", 1) or 1), 1)
    per_page = min(max(int(request.args.get("per_page", 20) or 20), 5), 100)

    query = AIAnalysisLog.query.order_by(desc(AIAnalysisLog.created_at))

    if q:
        like = f"%{q}%"
        query = query.join(User, AIAnalysisLog.user, isouter=True).filter(
            or_(
                AIAnalysisLog.filters.ilike(like),
                AIAnalysisLog.output_html.ilike(like),
                cast(AIAnalysisLog.record_count, String).ilike(like),
                User.username.ilike(like),
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


@bp.route("/ai-logs/<int:log_id>", methods=["GET"])
@login_required
def ai_logs_get(log_id: int):
    if getattr(current_user, "admin_level", 0) not in (1, 2):
        abort(403)
    row = AIAnalysisLog.query.get_or_404(log_id)
    return jsonify(
        {
            "id": row.id,
            "user_id": row.user_id,
            "created_at": row.created_at.isoformat(),
            "record_count": row.record_count,
            "filters": row.filters,
            "output_html": row.output_html,
        }
    )


@bp.route("/cron/job-role-canonicaliser/run-now", methods=["POST"])
@login_required
@superuser_required
def run_job_role_canonicaliser_now():
    # Match the cron_runner signature you shared (trigger + limit).
    result = run_job_role_canonicaliser(trigger="admin", limit=100)

    updated = (
        result.get("updated")
        or result.get("rows_updated")
        or result.get("records_updated")
        or 0
    )
    examined = (
        result.get("examined")
        or result.get("scanned")
        or result.get("rows_scanned")
        or 0
    )

    flash(
        f"Job role canonicaliser updated {updated} rows "
        f"(examined {examined}).",
        "success",
    )
    return redirect(url_for("admin.cron_runs"))


# -------------------------------------------------------------------
# DIAGNOSE POSTCODES
# -------------------------------------------------------------------
@bp.route("/diagnose-postcodes")
@login_required
def diagnose_postcodes():
    if not _require_superuser():
        return redirect(url_for("home"))

    from .utils import normalize_uk_postcode, geocode_postcode, inside_uk

    results = []
    seen = set()

    for job in JobRecord.query.all():
        raw = (job.postcode or "").strip()
        if raw in seen:
            continue
        seen.add(raw)

        normalized = normalize_uk_postcode(raw)
        lat, lon = geocode_postcode(raw)

        results.append(
            {
                "raw": raw,
                "normalized": normalized,
                "lat": lat,
                "lon": lon,
                "valid": lat is not None and lon is not None and inside_uk(lat, lon),
            }
        )

    return jsonify(results)


# -------------------------------------------------------------------
# CLEAR JOB RECORDS
# -------------------------------------------------------------------
@bp.route("/clear-job-records")
@login_required
def clear_job_records():
    if not _require_superuser():
        return redirect(url_for("home"))

    try:
        deleted = JobRecord.query.delete()
        commit_or_rollback()
        flash(f"✅ Deleted {deleted} job records.", "success")
    except Exception as e:  # noqa: BLE001
        print(f"Error clearing job records: {e}")
        flash("Failed to delete job records.", "error")

    return redirect(url_for("upload.upload"))


# -------------------------------------------------------------------
# JOB POSTINGS LIST
# -------------------------------------------------------------------
@bp.route("/jobs")
@login_required
def admin_jobs():
    if not _require_superuser():
        return redirect(url_for("home"))

    page = request.args.get("page", 1, type=int)
    per_page = 50

    query = JobPosting.query.order_by(JobPosting.scraped_at.desc())

    source = request.args.get("source", type=str, default="").strip()
    company = request.args.get("company", type=str, default="").strip()
    title = request.args.get("title", type=str, default="").strip()
    active_only = request.args.get("active", "1")

    if source:
        query = query.filter(JobPosting.source_site == source)
    if company:
        query = query.filter(JobPosting.company_name.ilike(f"%{company}%"))
    if title:
        query = query.filter(JobPosting.title.ilike(f"%{title}%"))
    if active_only == "1":
        query = query.filter(JobPosting.is_active.is_(True))

    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    jobs = pagination.items

    sources = (
        db.session.query(JobPosting.source_site)
        .distinct()
        .order_by(JobPosting.source_site)
        .all()
    )
    sources = [row[0] for row in sources if row[0]]

    return render_template(
        "admin/jobs.html",
        jobs=jobs,
        pagination=pagination,
        sources=sources,
        selected_source=source,
        company_filter=company,
        title_filter=title,
        active_only=active_only,
    )


# -------------------------------------------------------------------
# IMPORT SINGLE JOB POSTING → JobRecord
# -------------------------------------------------------------------
@bp.route("/jobs/import/<int:posting_id>", methods=["POST"])
@login_required
def admin_import_job(posting_id):
    if not _require_superuser():
        return redirect(url_for("home"))

    posting = JobPosting.query.get_or_404(posting_id)

    if getattr(posting, "imported", False):
        flash("This job has already been imported.", "warning")
        return redirect(url_for("admin.admin_jobs"))

    import_posting_to_record(posting)
    db.session.commit()

    flash("Job imported successfully into system records.", "success")
    return redirect(url_for("admin.admin_jobs"))


# -------------------------------------------------------------------
# IMPORT ALL ACTIVE JOB POSTINGS → JobRecord
# -------------------------------------------------------------------
@bp.route("/admin/jobs/import-all", methods=["POST"])
@login_required
def admin_import_all_jobs():
    if not _require_superuser():
        return redirect(url_for("home"))

    query = JobPosting.query

    search = request.form.get("search") or request.args.get("search")
    company = request.form.get("company") or request.args.get("company")
    source = request.form.get("source") or request.args.get("source")
    active_only = request.form.get("active_only") or request.args.get("active_only")

    if search:
        like = f"%{search}%"
        query = query.filter(JobPosting.title.ilike(like))
    if company:
        like = f"%{company}%"
        query = query.filter(JobPosting.company_name.ilike(like))
    if source:
        query = query.filter(JobPosting.source_site == source)
    if active_only in ("1", "true", "True", True):
        query = query.filter(JobPosting.is_active.is_(True))

    postings = query.all()

    imported_count = 0
    skipped_already_imported = 0

    for posting in postings:
        if getattr(posting, "imported", False):
            skipped_already_imported += 1
            continue

        import_posting_to_record(posting, enable_snap_to_postcode=False)
        imported_count += 1

    db.session.commit()

    msg_parts = [f"Imported {imported_count} job(s)."]
    if skipped_already_imported:
        msg_parts.append(f"Skipped {skipped_already_imported} already-imported job(s).")

    flash(" ".join(msg_parts), "success")

    return redirect(url_for("admin.admin_jobs"))


@bp.route("/admin/jobs/import/<int:posting_id>", methods=["POST"])
@login_required
def import_job(posting_id):
    posting = JobPosting.query.get_or_404(posting_id)

    from app.job_importer import import_posting_to_record as legacy_import_posting_to_record

    record = legacy_import_posting_to_record(posting)
    db.session.commit()

    flash("Job imported successfully.", "success")
    return redirect(url_for("admin.jobs_page"))


# -------------------------------------------------------------------
# ADMIN TOOLS PAGE
# -------------------------------------------------------------------
@bp.route("/tools")
@login_required
@superuser_required
def admin_tools():
    # Coverage Health tile (last 7 days)
    cov = get_weekly_coverage(days=7)

    weak_sectors = int(cov["summary"].get("weak_sectors", 0) or 0)
    weak_locations = int(cov["summary"].get("weak_locations", 0) or 0)
    weak_total = weak_sectors + weak_locations

    # Simple status rules:
    # - green: no weak sectors/locations
    # - amber: small number of weak items
    # - red: many weak items
    if weak_total == 0:
        status = "green"
    elif weak_total <= 3:
        status = "amber"
    else:
        status = "red"

    coverage_tile = {
        "status": status,
        "weak_sectors": weak_sectors,
        "weak_locations": weak_locations,
        "weak_total": weak_total,
        "window_days": 7,
    }

    return render_template("admin/admin_tools.html", coverage_tile=coverage_tile)



# -------------------------------------------------------------------
# DB HEALTH
# -------------------------------------------------------------------
@bp.route("/db-health", methods=["GET"])
@login_required
def db_health():
    if not _require_superuser():
        return redirect(url_for("home"))

    uri = current_app.config.get("SQLALCHEMY_DATABASE_URI", "") or ""
    masked_uri = uri

    if "@" in uri:
        try:
            _, suffix = uri.split("@", 1)
            masked_uri = f"***@{suffix}"
        except ValueError:
            masked_uri = uri

    ping_ok = False
    ping_error = None
    tables = []
    tables_error = None
    backend = None

    try:
        db.session.execute(text("SELECT 1"))
        ping_ok = True
    except Exception as e:  # noqa: BLE001
        ping_error = repr(e)

    if ping_ok:
        try:
            inspector = inspect(db.engine)
            tables = sorted(inspector.get_table_names())
            backend = db.engine.name
        except Exception as e:  # noqa: BLE001
            tables_error = repr(e)

    return render_template(
        "admin/db_health.html",
        db_uri_masked=masked_uri,
        ping_ok=ping_ok,
        ping_error=ping_error,
        backend=backend,
        tables=tables,
        tables_error=tables_error,
    )


# -------------------------------------------------------------------
# BACKFILL COMPANY IDS
# -------------------------------------------------------------------
@bp.route("/backfill-company-ids")
@login_required
def backfill_company_ids():
    if not _require_superuser():
        return redirect(url_for("home"))

    from app.blueprints.utils import get_or_create_company_id

    jobs = JobRecord.query.all()
    updated = 0
    skipped = 0

    for job in jobs:
        raw_name = (job.company_name or "").strip()

        if not raw_name:
            skipped += 1
            continue

        if job.company_id and job.company_id.strip():
            skipped += 1
            continue

        new_id = get_or_create_company_id(raw_name)
        job.company_id = new_id
        updated += 1

    try:
        commit_or_rollback()
        flash(f"Backfill complete — updated {updated}, skipped {skipped}.", "success")
    except Exception as e:  # noqa: BLE001
        flash("Failed to backfill company IDs.", "error")
        print("Backfill error:", e)

    return redirect(url_for("admin.admin_companies"))


@bp.route("/utils/create-job-role-mapping-table")
@login_required
def create_job_role_mapping_table():
    if not _require_superuser():
        return redirect(url_for("home"))

    JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    flash("JobRoleMapping table has been created (or already existed).", "success")
    return redirect(url_for("dashboard.admin_job_roles"))


# -------------------------------------------------------------------
# CRON RUN HISTORY + RUN NOW
# -------------------------------------------------------------------
@bp.route("/cron-runs")
@login_required
def cron_runs():
    """
    Show history of cron runs (from cron_run_logs table).
    Also attaches r.stats parsed from CronRunLog.run_stats so templates can render it.
    """
    if not _require_superuser():
        return redirect(url_for("home"))

    page = request.args.get("page", 1, type=int)
    per_page = min(max(request.args.get("per_page", 25, type=int), 5), 100)

    query = CronRunLog.query.order_by(CronRunLog.started_at.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    runs = pagination.items

    # Attach parsed stats (safe; never breaks page)
    for r in runs:
        setattr(r, "stats", _safe_json_loads(getattr(r, "run_stats", None)) or {})

    return render_template(
        "admin/cron_runs.html",
        runs=runs,
        pagination=pagination,
        per_page=per_page,
    )


@bp.route("/coverage")
@login_required
@superuser_required
def admin_coverage():
    days = request.args.get("days", default=7, type=int)
    days = max(1, min(days, 28))

    coverage = get_weekly_coverage(days=days)
    diff = get_weekly_coverage_diff()

    # Build lookup dicts so templates can do .get()
    sector_delta_map = {d["sector"]: d.get("delta", 0) for d in diff.get("sector_diff", [])}
    location_delta_map = {d["county"]: d.get("delta", 0) for d in diff.get("location_diff", [])}

    return render_template(
        "admin/coverage.html",
        days=days,
        summary=coverage["summary"],
        sectors=coverage["sectors"],
        locations=coverage["locations"],
        weak_sectors=coverage["weak_sectors"],
        weak_locations=coverage["weak_locations"],
        sector_diff=diff["sector_diff"],              # list (table-ready)
        location_diff=diff["location_diff"],          # list (table-ready)
        sector_delta_map=sector_delta_map,            # dict (lookup-ready)
        location_delta_map=location_delta_map,        # dict (lookup-ready)
    )


@bp.route("/coverage/export")
@login_required
@superuser_required
def admin_coverage_export():
    from io import StringIO
    import csv

    data = get_weekly_coverage(days=7)

    output = StringIO()
    writer = csv.writer(output)

    writer.writerow(["Type", "Name", "Adverts", "Days Seen", "Extra"])

    for s in data["sectors"]:
        writer.writerow([
            "Sector",
            s.sector,
            int(s.adverts or 0),
            s.days_seen,
            f"Median £{round(s.median_pay, 2)}" if s.median_pay else "",
        ])

    for l in data["locations"]:
        writer.writerow([
            "Location",
            l.county,
            int(l.adverts or 0),
            l.days_seen,
            f"Sectors {l.sector_count}",
        ])

    output.seek(0)
    return current_app.response_class(
        output,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=payscope_coverage.csv"},
    )


@bp.route("/coverage/heatmap")
@login_required
@superuser_required
def admin_coverage_heatmap():
    start = date.today() - timedelta(days=7)

    rows = (
        db.session.query(
            JobSummaryDaily.sector,
            JobSummaryDaily.county,
            func.sum(JobSummaryDaily.adverts_count).label("adverts"),
        )
        .filter(JobSummaryDaily.date >= start)
        .group_by(JobSummaryDaily.sector, JobSummaryDaily.county)
        .all()
    )

    return render_template(
        "admin/coverage_heatmap.html",
        rows=rows,
    )


@bp.route("/cron-runs/run-now", methods=["POST"])
@login_required
def cron_run_now():
    if not _require_superuser():
        return redirect(url_for("home"))

    # Keep compatibility with the cron runner entrypoint if present
    from cron_runner import run_scheduled_jobs

    started_at = datetime.utcnow()
    try:
        result = run_scheduled_jobs()
        status = "success"
        message = result or "OK"
    except Exception as e:  # noqa: BLE001
        status = "error"
        message = repr(e)
    finished_at = datetime.utcnow()

    log_row = CronRunLog(
        job_name="manual_run_all",
        status=status,
        message=message,
        started_at=started_at,
        finished_at=finished_at,
        # run_stats intentionally omitted here because run_scheduled_jobs() may
        # return different shapes; cron_runner itself writes rich run_stats.
    )
    db.session.add(log_row)
    commit_or_rollback()

    flash(f"Cron jobs executed with status: {status}", "info")
    return redirect(url_for("admin.cron_runs"))
