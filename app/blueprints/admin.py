# app/blueprints/admin.py
from __future__ import annotations

import os
from datetime import datetime

from datetime import date  # NEW

from flask import Blueprint, render_template, redirect, url_for, flash, request  # you may already have these
from flask_login import login_required, current_user
from werkzeug.exceptions import abort  # if not already imported

from ons_importer import import_ons_earnings_to_db  # NEW


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
from functools import wraps
from flask import abort
from flask_login import current_user

from extensions import db
from models import (
    User,
    AIAnalysisLog,
    JobRecord,
    JobPosting,
    Company,
    CronRunLog,   # <-- NEW: cron log model
)
from .utils import (
    commit_or_rollback,
    normalize_uk_postcode,
    bulk_geocode_postcodes,
    geocode_postcode_cached,
    snap_to_nearest_postcode,
)

from flask import redirect, url_for, flash
from flask_login import login_required, current_user
from cron_runner import run_job_role_canonicaliser


from app.scrapers.adzuna import AdzunaScraper
from app.importers.job_importer import import_posting_to_record

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
        import json

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
        except Exception as exc:
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
    return redirect(url_for("upload.upload"))

@bp.route("/admin/ons-import", methods=["POST"])
@login_required
def run_ons_import():
    if getattr(current_user, "admin_level", 0) != 1:
        abort(403)

    # ASHE is published for previous year, so import that
    year = date.today().year - 1

    result = import_ons_earnings_to_db(
        year,
        trigger="admin_button",
        triggered_by=getattr(current_user, "username", None),
        use_app_context=True,
    )

    if result.get("error"):
        flash(f"ONS import FAILED for {year}: {result['error']}", "error")
    else:
        fetched = result.get("fetched", 0)
        created = result.get("created", 0)
        updated = result.get("updated", 0)
        flash(
            f"Imported ONS ASHE for {year}: "
            f"fetched {fetched}, created {created}, updated {updated}.",
            "success",
        )

    return redirect(url_for("admin.admin_tools"))



# -------------------------------------------------------------------
# COMPANIES ADMIN
# -------------------------------------------------------------------
@bp.route("/companies", methods=["GET", "POST"])
@login_required
def admin_companies():
    """
    Admin view for:
      - Viewing grouped companies (by JobRecord.company_id)
      - Merging multiple company_ids into a target
      - Editing Company name/canonical_name/sector
      - Uploading logos for a given company_id
    """
    if not _require_superuser():
        return redirect(url_for("home"))

    from app.blueprints.utils import _slugify  # reuse same slug logic for mapping

    if request.method == "POST":
        action = request.form.get("action")

        # ----- MERGE company_ids -----
        if action == "merge":
            target_slug = (request.form.get("target_company_id") or "").strip()
            source_raw = (request.form.get("source_company_ids") or "").strip()
            source_slugs = [
                s.strip()
                for s in source_raw.replace("\n", ",").split(",")
                if s.strip()
            ]

            # Remove self-merge and duplicates
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
                except Exception as e:
                    print("Merge companies error:", e)
                    flash("Failed to merge companies.", "error")

            return redirect(url_for("admin.admin_companies"))

        # ----- UPDATE a Company row -----
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
            except Exception as e:
                print("Update company error:", e)
                flash("Failed to update company.", "error")

            return redirect(url_for("admin.admin_companies"))

        # ----- UPLOAD logo for company_id -----
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
            except Exception as e:
                print("Logo upload error:", e)
                flash("Failed to upload logo.", "error")

            return redirect(url_for("admin.admin_companies"))

    # ----- GET: build view model -----
    # Aggregate JobRecords by company_id
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

    # Build mapping: slug (as used in JobRecord.company_id) -> Company row (best guess)
    slug_to_company = {}
    for c in all_companies:
        slug = _slugify(c.canonical_name or c.name or "")
        # First one wins; if multiple map to same slug, we keep the first for now
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
    """
    Rebuild JobRecord.company_id values from the current Company canonical names.

    Use this AFTER:
      - You have cleaned up Company.name / Company.canonical_name via the UI
      - You want JobRecords to be aligned to those canonical values
    """
    if not _require_superuser():
        return redirect(url_for("home"))

    from app.blueprints.utils import _clean_company_name, _slugify

    companies = Company.query.all()
    jobs = JobRecord.query.all()

    # Build map: canonical_name -> slug
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

        cleaned = _clean_company_name(raw_name)
        if not cleaned:
            skipped += 1
            continue

        target_slug = canonical_to_slug.get(cleaned)
        if not target_slug:
            # Fallback: derive slug directly from cleaned name
            target_slug = _slugify(cleaned)

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
    except Exception as e:
        print("Regenerate company IDs error:", e)
        flash("Failed to regenerate company IDs.", "error")

    return redirect(url_for("admin.admin_companies"))


# -------------------------------------------------------------------
# REGEOCODE JOBS
# -------------------------------------------------------------------
@bp.route("/regeocode-jobs")
@login_required
def regeocode_jobs():
    """
    Re-geocode JobRecord.postcode values using the UK-only geocoder.
    Also, if postcode-based lookup fails but we already have coordinates,
    snap to the nearest postcode based on latitude/longitude.
    """
    if not _require_superuser():
        return redirect(url_for("home"))

    # Clear postcode cache so we get fresh results
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

        # 1) Try standard postcode geocode if we have a postcode
        if job.postcode:
            new_lat, new_lon = geocode_postcode_cached(job.postcode)
            if new_lat is not None and new_lon is not None:
                job.latitude = new_lat
                job.longitude = new_lon
                updated += 1
                continue

        # 2) If postcode geocoding failed (or no postcode) but we already
        #    have coordinates, snap them to the nearest postcode
        if lat is not None and lon is not None:
            inferred_pc, snapped_lat, snapped_lon = snap_to_nearest_postcode(lat, lon)
            if inferred_pc and snapped_lat is not None and snapped_lon is not None:
                if not job.postcode:
                    job.postcode = inferred_pc
                job.latitude = snapped_lat
                job.longitude = snapped_lon
                updated += 1
                continue

        # 3) If we get here, we couldn't geocode this record at all
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
    result = run_job_role_canonicaliser(
        trigger="admin",
        triggered_by=current_user.username,
        max_roles=100,
    )
    flash(
        f"Job role canonicaliser updated {result.get('updated', 0)} rows "
        f"(examined {result.get('examined', 0)}).",
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
    """
    Delete ALL JobRecord rows from the database.
    Keeps users, logs, etc. intact.
    """
    if not _require_superuser():
        return redirect(url_for("home"))

    try:
        deleted = JobRecord.query.delete()
        commit_or_rollback()
        flash(f"✅ Deleted {deleted} job records.", "success")
    except Exception as e:
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

    source = request.args.get("source", type=str)
    company = request.args.get("company", type=str)
    active_only = request.args.get("active", "1")

    if source:
        query = query.filter(JobPosting.source_site == source)

    if company:
        query = query.filter(JobPosting.company_name.ilike(f"%{company}%"))

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
    sources = [row[0] for row in sources]

    return render_template(
        "admin/jobs.html",
        jobs=jobs,
        pagination=pagination,
        sources=sources,
        selected_source=source or "",
        company_filter=company or "",
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
@bp.route("/jobs/import-all", methods=["POST"])
@login_required
def admin_import_all_jobs():
    if not _require_superuser():
        return redirect(url_for("home"))

    postings = JobPosting.query.filter_by(is_active=True).all()
    count = 0

    for posting in postings:
        if getattr(posting, "imported", False):
            continue
        import_posting_to_record(posting)
        count += 1

    db.session.commit()
    flash(f"{count} job postings imported successfully.", "success")
    return redirect(url_for("admin.admin_jobs"))


@bp.route("/admin/jobs/import/<int:posting_id>", methods=["POST"])
@login_required
def import_job(posting_id):
    posting = JobPosting.query.get_or_404(posting_id)

    from app.job_importer import import_posting_to_record

    record = import_posting_to_record(posting)
    db.session.commit()

    flash("Job imported successfully.", "success")
    return redirect(url_for("admin.jobs_page"))


@bp.route("/tools")
@login_required
@superuser_required
def admin_tools():
    return render_template("admin/admin_tools.html")




# -------------------------------------------------------------------
# DB HEALTH
# -------------------------------------------------------------------
@bp.route("/db-health", methods=["GET"])
@login_required
def db_health():
    """
    Simple DB health + table list page.

    - Superuser only
    - Pings the DB with SELECT 1
    - Lists all tables in the current database
    """
    if not _require_superuser():
        return redirect(url_for("home"))

    uri = current_app.config.get("SQLALCHEMY_DATABASE_URI", "") or ""
    masked_uri = uri

    # Mask credentials so we can see where we're connected
    if "@" in uri:
        # e.g. postgresql+pg8000://user:pass@host:port/db -> ***@host:port/db
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

    # 1) Basic ping
    try:
        db.session.execute(text("SELECT 1"))
        ping_ok = True
    except Exception as e:
        ping_error = repr(e)

    # 2) List tables if ping succeeded
    if ping_ok:
        try:
            inspector = inspect(db.engine)
            tables = sorted(inspector.get_table_names())
            backend = db.engine.name  # e.g. "postgresql"
        except Exception as e:
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
    """
    Backfill all JobRecord.company_id values using canonical Company table.
    Uses the same logic as importer (normalised name).
    Safe to run multiple times.
    """
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

        # Already has a company_id? Leave it alone.
        if job.company_id and job.company_id.strip():
            skipped += 1
            continue

        new_id = get_or_create_company_id(raw_name)
        job.company_id = new_id
        updated += 1

    try:
        commit_or_rollback()
        flash(f"Backfill complete — updated {updated}, skipped {skipped}.", "success")
    except Exception as e:
        flash("Failed to backfill company IDs.", "error")
        print("Backfill error:", e)

    return redirect(url_for("admin.admin_companies"))


# -------------------------------------------------------------------
# CRON RUN HISTORY + RUN NOW
# -------------------------------------------------------------------
@bp.route("/cron-runs")
@login_required
def cron_runs():
    """
    Show history of cron runs (from cron_run_logs table).
    """
    if not _require_superuser():
        return redirect(url_for("home"))

    page = request.args.get("page", 1, type=int)
    per_page = min(max(request.args.get("per_page", 25, type=int), 5), 100)

    query = CronRunLog.query.order_by(CronRunLog.started_at.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    runs = pagination.items

    return render_template(
        "admin/cron_runs.html",
        runs=runs,
        pagination=pagination,
        per_page=per_page,
    )


@bp.route("/cron-runs/run-now", methods=["POST"])
@login_required
def cron_run_now():
    if not _require_superuser():
        return redirect(url_for("home"))

    # Import from the top-level cron_runner.py
    from cron_runner import run_scheduled_jobs

    # Run, log, etc.
    started_at = datetime.utcnow()
    try:
        result = run_scheduled_jobs()
        status = "success"
        message = result or "OK"
    except Exception as e:
        status = "error"
        message = repr(e)
    finished_at = datetime.utcnow()

    from models import CronRunLog  # wherever we defined it

    log_row = CronRunLog(
        job_name="manual_run_all",
        status=status,
        message=message,
        started_at=started_at,
        finished_at=finished_at,
    )
    db.session.add(log_row)
    commit_or_rollback()

    flash(f"Cron jobs executed with status: {status}", "info")
    return redirect(url_for("admin.cron_runs"))

