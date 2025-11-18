# app/blueprints/admin.py
from __future__ import annotations

from flask import Blueprint, render_template, request, redirect, url_for, flash, abort, jsonify
from flask_login import login_required, current_user
from sqlalchemy import desc, or_, cast, String
from datetime import datetime
from extensions import db
from app.scrapers.adzuna import AdzunaScraper


from models import User, AIAnalysisLog, JobRecord, JobPosting
from .utils import (
    commit_or_rollback,
    geocode_postcode,
    geocode_postcode_cached,
    inside_uk,
)

bp = Blueprint("admin", __name__)


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

# -----------------------------------------
# JOB SCRAPER PAGE (manual scrape)
# -----------------------------------------
@bp.route("/jobs/scrape", methods=["GET", "POST"])
@login_required
def admin_job_scrape():
    """
    Admin page to manually trigger job scrapes with custom role + location parameters.
    """
    role = request.form.get("role") or ""
    location = request.form.get("location") or ""
    message = None
    records = []
    processed = 0

    if request.method == "POST":
        # Scraper instance using user-selected parameters
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
            )

        # Save to database
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
    )


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


@bp.route("/regeocode-jobs")
@login_required
def regeocode_jobs():
    """
    Re-geocode JobRecord.postcode values using the UK-only geocoder.

    - Re-runs geocoding for records that:
        * have missing latitude/longitude, OR
        * have coordinates outside the UK bounding box.
    - If a postcode still cannot be resolved inside the UK, lat/lon are cleared.
    """
    if not _require_superuser():
        return redirect(url_for("home"))

    # Clear cached geocoding results so we fetch fresh coordinates
    geocode_postcode_cached.cache_clear()

    limit = request.args.get("limit", type=int)
    query = JobRecord.query
    if limit:
        query = query.limit(limit)
    jobs = query.all()

    updated = 0
    cleared = 0

    for job in jobs:
        if not job.postcode:
            continue

        lat = job.latitude
        lon = job.longitude

        needs_update = (
            lat is None
            or lon is None
            or not inside_uk(float(lat), float(lon))
        )

        if not needs_update:
            continue

        new_lat, new_lon = geocode_postcode(job.postcode)

        if (
            new_lat is not None
            and new_lon is not None
            and inside_uk(float(new_lat), float(new_lon))
        ):
            job.latitude = new_lat
            job.longitude = new_lon
            updated += 1
        else:
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

    # Back to upload page (same as backfill_counties)
    return redirect(url_for("upload.upload"))


@bp.route("/ai-logs", methods=["GET"])
@login_required
def ai_logs():
    # Only admins
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

        results.append({
            "raw": raw,
            "normalized": normalized,
            "lat": lat,
            "lon": lon,
            "valid": lat is not None and lon is not None and inside_uk(lat, lon)
        })

    return jsonify(results)

@bp.route("/clear-job-records")
@login_required
def clear_job_records():
    """
    Delete ALL JobRecord rows from the database.
    Keeps users, logs, etc. intact.
    """
    if not _require_superuser():
        return redirect(url_for("home"))

    # Delete all rows in the JobRecord table
    try:
        deleted = JobRecord.query.delete()
        commit_or_rollback()
        flash(f"✅ Deleted {deleted} job records.", "success")
    except Exception as e:
        print(f"Error clearing job records: {e}")
        flash("Failed to delete job records.", "error")

    # Send you back to the upload page ready for a clean import
    return redirect(url_for("upload.upload"))

@bp.route("/jobs")
@login_required  # swap for your own admin-only decorator if you have one
def admin_jobs():
    page = request.args.get("page", 1, type=int)
    per_page = 50

    # Base query: newest first
    query = JobPosting.query.order_by(JobPosting.scraped_at.desc())

    # Filters
    source = request.args.get("source", type=str)
    company = request.args.get("company", type=str)
    active_only = request.args.get("active", "1")  # default: show only active

    if source:
        query = query.filter(JobPosting.source_site == source)

    if company:
        query = query.filter(JobPosting.company_name.ilike(f"%{company}%"))

    if active_only == "1":
        query = query.filter(JobPosting.is_active.is_(True))

    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    jobs = pagination.items

    # Distinct list of sources for the filter dropdown
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
