# app/blueprints/admin.py
from __future__ import annotations

import os
import json
import re
from datetime import datetime, date, timedelta
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
    WaitlistSignup,
    AccessRequest,
    WeeklyMarketChange,
    JobSummaryDaily,
    WeeklyInsight
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

from . import pay_compare  # relative import

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

# Optional OpenAI client (fail-safe)
try:
    from openai import OpenAI
    _openai_client = OpenAI()
except Exception:
    _openai_client = None



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


def _monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())


def _pick_col(model, options: list[str]):
    cols = model.__table__.columns
    for name in options:
        if name in cols:
            return cols[name]
    return None


def _safe_str(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


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


def get_weekly_source_coverage(days: int = 7):
    """
    Per-source coverage for the coverage.html 'Source Coverage' table.
    Uses the same window logic as get_weekly_coverage().
    """
    start_date = date.today() - timedelta(days=days)
    end_excl = date.today() + timedelta(days=1)

    # If your JobSummaryDaily table doesn't have source_site for any reason,
    # fail gracefully.
    if not hasattr(JobSummaryDaily, "source_site"):
        return []

    rows = (
        db.session.query(
            JobSummaryDaily.source_site.label("source_site"),
            func.sum(JobSummaryDaily.adverts_count).label("adverts"),
            func.avg(JobSummaryDaily.median_pay_rate).label("median_pay"),
            func.count(func.distinct(JobSummaryDaily.date)).label("days_seen"),
            func.count(func.distinct(JobSummaryDaily.sector)).label("sector_count"),
            func.count(func.distinct(JobSummaryDaily.county)).label("location_count"),
        )
        .filter(JobSummaryDaily.date >= start_date)
        .filter(JobSummaryDaily.date < end_excl)
        .group_by(JobSummaryDaily.source_site)
        .order_by(func.sum(JobSummaryDaily.adverts_count).desc())
        .all()
    )

    return rows


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

def _ai_enabled() -> bool:
    return _openai_client is not None and bool(os.getenv("OPENAI_API_KEY"))

def _clamp_text(s: str | None, max_len: int = 1200) -> str | None:
    if not s:
        return s
    s = str(s).strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."

def _format_change_summary(item) -> str:
    # item is WeeklyMarketChange
    metric = (item.metric_type or "").strip().lower()
    direction = (item.direction or "").strip().lower()
    role = item.job_role or "Unknown role"
    loc = item.location or "Unknown location"

    dv = item.delta_value
    dp = item.delta_percent
    prev = item.value_previous
    cur = item.value_current

    parts = [f"{role} — {loc}", f"metric={metric}", f"direction={direction}"]
    if prev is not None and cur is not None:
        parts.append(f"prev={prev}")
        parts.append(f"cur={cur}")
    if dv is not None:
        parts.append(f"delta_value={dv}")
    if dp is not None:
        parts.append(f"delta_percent={dp}")

    if item.sector:
        parts.append(f"sector={item.sector}")
    if item.confidence_level is not None:
        parts.append(f"confidence_level={item.confidence_level}")

    return " | ".join(parts)

def _ai_generate_weekly_overview(week_start: date, week_end: date, featured_items: list) -> dict:
    """
    Returns: {"headline": str, "overview": str, "model": str}
    """
    if not _ai_enabled():
        raise RuntimeError("AI not configured (missing OpenAI client or OPENAI_API_KEY).")

    model = os.getenv("PAYSOPE_AI_MODEL", "gpt-4o-mini")  # typo-safe env name doesn’t matter; override below
    model = os.getenv("PAYSCOPE_AI_MODEL", model)

    items_text = "\n".join([f"- { _format_change_summary(it) }" for it in featured_items])

    system = (
        "You write short, grounded UK labour-market briefings.\n"
        "Rules:\n"
        "- Do not invent causes. If you suggest drivers, frame as possibilities.\n"
        "- Be concise and practical.\n"
        "- If changes look extreme, note data-thin risk.\n"
        "- Use plain English.\n"
    )

    user = (
        f"Create a weekly briefing for PayScope.\n"
        f"Week: {week_start.isoformat()} to {week_end.isoformat()}.\n\n"
        "Featured changes (structured facts):\n"
        f"{items_text}\n\n"
        "Output JSON with keys:\n"
        "headline: 1 sentence\n"
        "overview: 3–5 bullet points (use • bullets)\n"
        "watchouts: 1–2 bullets if needed (use • bullets), else empty string\n"
    )

    resp = _openai_client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.4,
        max_tokens=450,
    )

    text = (resp.choices[0].message.content or "").strip()

    # Best-effort JSON extraction (model sometimes wraps)
    m = re.search(r"\{.*\}", text, flags=re.S)
    json_blob = m.group(0) if m else None

    try:
        data = json.loads(json_blob or text)
    except Exception:
        # fallback: store raw in overview
        data = {
            "headline": f"Weekly market changes for {week_start.strftime('%d %b %Y')}",
            "overview": text,
            "watchouts": "",
        }

    headline = _clamp_text(data.get("headline"), 200)
    overview = _clamp_text(data.get("overview"), 2000)
    watchouts = _clamp_text(data.get("watchouts"), 1200)

    combined = overview or ""
    if watchouts:
        combined = (combined + "\n\nWatch-outs:\n" + watchouts).strip()

    return {"headline": headline or "", "overview": combined, "model": model}

def _ai_generate_item_narrative(item) -> dict:
    """
    Returns: {"narrative": str, "driver_tags": str|None, "model": str}
    """
    if not _ai_enabled():
        raise RuntimeError("AI not configured (missing OpenAI client or OPENAI_API_KEY).")

    model = os.getenv("PAYSCOPE_AI_MODEL", "gpt-4o-mini")

    system = (
        "You write 1–2 sentence narratives for a weekly labour-market update card.\n"
        "Rules:\n"
        "- Use ONLY the provided numbers.\n"
        "- No made-up causes. If you suggest drivers, frame as 'could' / 'may'.\n"
        "- Mention caution if change is extreme or volumes may be small.\n"
        "- Keep it business-briefing tone.\n"
    )

    facts = _format_change_summary(item)

    user = (
        "Write a 1–2 sentence narrative for this featured change.\n"
        "Also output 1 short 'driver_tags' string (comma-separated) from: "
        "seasonality, data_thin, local_spike, sector_shift, wage_pressure, hiring_freeze, unclear.\n\n"
        f"Facts:\n{facts}\n\n"
        "Output JSON with keys: narrative, driver_tags"
    )

    resp = _openai_client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.4,
        max_tokens=220,
    )

    text = (resp.choices[0].message.content or "").strip()
    m = re.search(r"\{.*\}", text, flags=re.S)
    json_blob = m.group(0) if m else None

    try:
        data = json.loads(json_blob or text)
    except Exception:
        data = {"narrative": text, "driver_tags": "unclear"}

    narrative = _clamp_text(data.get("narrative"), 800)
    driver_tags = _clamp_text(data.get("driver_tags"), 200)

    return {"narrative": narrative or "", "driver_tags": driver_tags, "model": model}





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
# JOB POSTINGS (ingested adverts) — list + import
# -------------------------------------------------------------------
@bp.route("/jobs", methods=["GET"])
@login_required
def admin_jobs():
    """
    Admin page to view/filter ingested JobPosting rows and import them into JobRecord.
    Renders: templates/admin/jobs.html
    Endpoint: admin.admin_jobs
    """
    if not _require_superuser():
        return redirect(url_for("home"))

    # --- Filters (match jobs.html expectations) ---
    selected_source = (request.args.get("source") or "").strip()
    company_filter = (request.args.get("company") or "").strip()
    title_filter = (request.args.get("title") or "").strip()
    active_only = (request.args.get("active") or "").strip()  # "1" or ""

    page = request.args.get("page", default=1, type=int)
    per_page = 25

    # Distinct sources for dropdown
    sources = [
        r[0] for r in
        db.session.query(JobPosting.source_site)
        .filter(JobPosting.source_site.isnot(None))
        .distinct()
        .order_by(JobPosting.source_site.asc())
        .all()
    ]

    q = JobPosting.query

    if selected_source:
        q = q.filter(JobPosting.source_site == selected_source)

    if company_filter:
        q = q.filter(JobPosting.company_name.ilike(f"%{company_filter}%"))

    if title_filter:
        q = q.filter(JobPosting.title.ilike(f"%{title_filter}%"))

    if active_only == "1":
        q = q.filter(JobPosting.is_active.is_(True))

    # Order newest first (scraped_at preferred; fallback to id)
    if hasattr(JobPosting, "scraped_at"):
        q = q.order_by(JobPosting.scraped_at.desc())
    else:
        q = q.order_by(JobPosting.id.desc())

    pagination = q.paginate(page=page, per_page=per_page, error_out=False)
    jobs = pagination.items

    return render_template(
        "admin/jobs.html",
        jobs=jobs,
        pagination=pagination,
        sources=sources,
        selected_source=selected_source,
        company_filter=company_filter,
        title_filter=title_filter,
        active_only=active_only,
    )


@bp.route("/jobs/<int:posting_id>/import", methods=["POST"])
@login_required
@superuser_required
def admin_import_job(posting_id: int):
    """
    Import a single JobPosting into JobRecord.
    Endpoint: admin.admin_import_job
    """
    posting = db.session.get(JobPosting, posting_id)
    if not posting:
        flash("Job posting not found.", "error")
        return redirect(url_for("admin.admin_jobs"))

    try:
        import_posting_to_record(posting)  # already imported in this module :contentReference[oaicite:2]{index=2}

        # Mark imported if the column exists (template expects job.imported)
        if hasattr(posting, "imported"):
            posting.imported = True

        commit_or_rollback()
        flash("Imported job posting.", "success")
    except Exception as e:  # noqa: BLE001
        db.session.rollback()
        flash(f"Failed to import posting: {e}", "error")

    return redirect(url_for("admin.admin_jobs"))


@bp.route("/jobs/import-all", methods=["POST"])
@login_required
@superuser_required
def admin_import_all_jobs():
    """
    Import all active JobPosting rows that haven't been imported yet (if 'imported' exists),
    otherwise imports all active rows.
    Endpoint: admin.admin_import_all_jobs
    """
    # Base query: active only
    q = JobPosting.query.filter(JobPosting.is_active.is_(True))

    # If 'imported' flag exists, only bring in those not yet imported
    if hasattr(JobPosting, "imported"):
        q = q.filter(or_(JobPosting.imported.is_(False), JobPosting.imported.is_(None)))

    postings = q.all()

    imported_count = 0
    failed_count = 0

    for p in postings:
        try:
            import_posting_to_record(p)
            if hasattr(p, "imported"):
                p.imported = True
            imported_count += 1
        except Exception as e:  # noqa: BLE001
            failed_count += 1
            # keep going; store error in logs
            print(f"Import failed for posting_id={getattr(p, 'id', None)}: {e}")

    try:
        commit_or_rollback()
        if failed_count:
            flash(
                f"Import complete: {imported_count} imported, {failed_count} failed (see logs).",
                "warning",
            )
        else:
            flash(f"Import complete: {imported_count} imported.", "success")
    except Exception:
        db.session.rollback()
        flash("Import ran but failed saving changes.", "error")

    return redirect(url_for("admin.admin_jobs"))









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
                org = getattr(current_user, "organisation", None)
                if org is None:
                    org = ensure_default_organisation()

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

    q = JobRecord.query.filter((JobRecord.county == None) | (JobRecord.county == ""))  # noqa: E711
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
        flash(f"✅ County backfill complete. Updated: {updated}, Skipped: {skipped}", "success")
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
# LEADS (Waitlist + Access Requests)
# -------------------------------------------------------------------
@bp.route("/leads", methods=["GET"])
@login_required
def admin_leads():
    if not _require_superuser():
        return redirect(url_for("home"))

    q = (request.args.get("q") or "").strip()
    tab = (request.args.get("tab") or "waitlist").strip().lower()
    tab = tab if tab in ("waitlist", "access") else "waitlist"

    waitlist_query = WaitlistSignup.query.order_by(WaitlistSignup.created_at.desc())
    access_query = AccessRequest.query.order_by(AccessRequest.created_at.desc())

    if q:
        like = f"%{q}%"
        waitlist_query = waitlist_query.filter(
            or_(
                WaitlistSignup.email.ilike(like),
                WaitlistSignup.notes.ilike(like),
                WaitlistSignup.source.ilike(like),
            )
        )
        access_query = access_query.filter(
            or_(
                AccessRequest.email.ilike(like),
                AccessRequest.notes.ilike(like),
                AccessRequest.source.ilike(like),
                AccessRequest.status.ilike(like),
            )
        )

    waitlist = waitlist_query.limit(500).all()
    access_requests = access_query.limit(500).all()

    new_access_count = AccessRequest.query.filter(AccessRequest.status == "new").count()

    return render_template(
        "admin/leads.html",
        q=q,
        tab=tab,
        waitlist=waitlist,
        access_requests=access_requests,
        new_access_count=new_access_count,
    )


@bp.route("/leads/access/<int:request_id>/status", methods=["POST"])
@login_required
def admin_leads_update_access_status(request_id: int):
    if not _require_superuser():
        return redirect(url_for("home"))

    new_status = (request.form.get("status") or "").strip().lower()
    if new_status not in ("new", "triaged", "approved", "rejected"):
        flash("Invalid status.", "error")
        return redirect(url_for("admin.admin_leads", tab="access"))

    row = db.session.get(AccessRequest, request_id)
    if not row:
        flash("Access request not found.", "error")
        return redirect(url_for("admin.admin_leads", tab="access"))

    row.status = new_status
    try:
        commit_or_rollback()
        flash("Status updated.", "success")
    except Exception:
        flash("Failed to update status.", "error")

    return redirect(url_for("admin.admin_leads", tab="access"))


@bp.route("/leads/export.csv", methods=["GET"])
@login_required
def admin_leads_export_csv():
    if not _require_superuser():
        return redirect(url_for("home"))

    export_type = (request.args.get("type") or "waitlist").strip().lower()
    export_type = export_type if export_type in ("waitlist", "access") else "waitlist"

    filename_stamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    if export_type == "waitlist":
        rows = WaitlistSignup.query.order_by(WaitlistSignup.created_at.desc()).all()
        filename = f"payscope_waitlist_{filename_stamp}.csv"
        header = "id,email,notes,source,created_at\n"

        def gen():
            yield header
            for r in rows:
                notes = (r.notes or "").replace('"', '""')
                source = (r.source or "").replace('"', '""')
                yield f'{r.id},"{r.email}","{notes}","{source}","{r.created_at.isoformat()}"\n'

    else:
        rows = AccessRequest.query.order_by(AccessRequest.created_at.desc()).all()
        filename = f"payscope_access_requests_{filename_stamp}.csv"
        header = "id,email,notes,source,status,created_at\n"

        def gen():
            yield header
            for r in rows:
                email = (r.email or "").replace('"', '""')
                notes = (r.notes or "").replace('"', '""')
                source = (r.source or "").replace('"', '""')
                status = (r.status or "").replace('"', '""')
                yield f'{r.id},"{email}","{notes}","{source}","{status}","{r.created_at.isoformat()}"\n'

    return current_app.response_class(
        gen(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
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


@bp.route("/inspect/ons")
@superuser_required
def inspect_ons():
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


@bp.route("/debug/pay-explorer-mapping")
@login_required
@superuser_required
def debug_pay_explorer_mapping():
    days = request.args.get("days", default=30, type=int)
    rows, ons_year = pay_compare.build_pay_explorer_debug_snapshot(days=days)
    return jsonify({"ons_year": ons_year, "rows": rows})


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
            source_slugs = [s.strip() for s in source_raw.replace("\n", ",").split(",") if s.strip()]

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

    return render_template("admin/companies.html", rows=rows, companies=all_companies)


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
        flash(f"✅ Re-geocoding complete. Updated: {updated}, cleared invalid locations: {cleared}", "success")
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

    return render_template("ai_logs.html", logs=logs, pagination=pagination, q=q, per_page=per_page)


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
    result = run_job_role_canonicaliser(trigger="admin", limit=100)

    updated = result.get("updated") or result.get("rows_updated") or result.get("records_updated") or 0
    examined = result.get("examined") or result.get("scanned") or result.get("rows_scanned") or 0

    flash(f"Job role canonicaliser updated {updated} rows (examined {examined}).", "success")
    return redirect(url_for("admin.cron_runs"))


# -------------------------------------------------------------------
# ADMIN TOOLS PAGE
# -------------------------------------------------------------------
@bp.route("/tools")
@login_required
@superuser_required
def admin_tools():
    cov = get_weekly_coverage(days=7)

    weak_sectors = int(cov["summary"].get("weak_sectors", 0) or 0)
    weak_locations = int(cov["summary"].get("weak_locations", 0) or 0)
    weak_total = weak_sectors + weak_locations

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
# CRON RUN HISTORY + RUN NOW
# -------------------------------------------------------------------
@bp.route("/cron-runs")
@login_required
def cron_runs():
    if not _require_superuser():
        return redirect(url_for("home"))

    page = request.args.get("page", 1, type=int)
    per_page = min(max(request.args.get("per_page", 25, type=int), 5), 100)

    query = CronRunLog.query.order_by(CronRunLog.started_at.desc())
    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    runs = pagination.items

    for r in runs:
        setattr(r, "stats", _safe_json_loads(getattr(r, "run_stats", None)) or {})

    return render_template("admin/cron_runs.html", runs=runs, pagination=pagination, per_page=per_page)


@bp.route("/coverage")
@login_required
@superuser_required
def admin_coverage():
    days = request.args.get("days", default=7, type=int)
    days = max(1, min(days, 28))

    coverage = get_weekly_coverage(days=days)
    diff = get_weekly_coverage_diff()
    sources = get_weekly_source_coverage(days=days)

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
        sector_diff=diff["sector_diff"],
        location_diff=diff["location_diff"],
        sector_delta_map=sector_delta_map,
        location_delta_map=location_delta_map,
        sources=sources,
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

    return render_template("admin/coverage_heatmap.html", rows=rows)
# -------------------------------------------------------------------
# Legacy weekly admin route (kept intact)
# -------------------------------------------------------------------
@bp.route("/weekly", methods=["GET", "POST"])
@login_required
def admin_weekly_changes():
    if not _require_superuser():
        return redirect(url_for("home"))

    today = date.today()
    week_start = _monday_of(today)
    week_end = week_start + timedelta(days=6)

    qs = request.args.get("week_start")
    if qs:
        try:
            week_start = date.fromisoformat(qs)
            week_end = week_start + timedelta(days=6)
        except Exception:
            pass

    if request.method == "POST":
        action = request.form.get("action")

        if action == "add_manual":
            metric_type = (request.form.get("metric_type") or "pay").strip()
            job_role = (request.form.get("job_role") or "").strip() or None
            sector = (request.form.get("sector") or "").strip() or None
            location = (request.form.get("location") or "").strip() or None
            direction = (request.form.get("direction") or "").strip() or None
            headline = (request.form.get("headline") or "").strip()
            interpretation = (request.form.get("interpretation") or "").strip() or None
            confidence = request.form.get("confidence_level")
            confidence_level = int(confidence) if confidence and confidence.isdigit() else None

            def _to_float(v):
                v = (v or "").strip()
                if not v:
                    return None
                try:
                    return float(v)
                except Exception:
                    return None

            value_previous = _to_float(request.form.get("value_previous"))
            value_current = _to_float(request.form.get("value_current"))
            delta_value = _to_float(request.form.get("delta_value"))
            delta_percent = _to_float(request.form.get("delta_percent"))

            if not headline:
                flash("Headline is required.", "error")
            else:
                row = WeeklyMarketChange(
                    week_start=week_start,
                    week_end=week_end,
                    metric_type=metric_type,
                    job_role=job_role,
                    sector=sector,
                    location=location,
                    value_previous=value_previous,
                    value_current=value_current,
                    delta_value=delta_value,
                    delta_percent=delta_percent,
                    direction=direction,
                    headline=headline,
                    interpretation=interpretation,
                    confidence_level=confidence_level,
                    is_featured=True,
                    is_published=False,
                )
                db.session.add(row)
                commit_or_rollback()
                flash("Added weekly insight.", "success")

        elif action == "toggle_featured":
            row_id = request.form.get("id")
            row = db.session.get(WeeklyMarketChange, int(row_id)) if row_id else None
            if row:
                row.is_featured = not bool(row.is_featured)
                commit_or_rollback()
                flash("Updated featured status.", "success")

        elif action == "publish_week":
            items = (
                db.session.query(WeeklyMarketChange)
                .filter(WeeklyMarketChange.week_start == week_start)
                .filter(WeeklyMarketChange.is_featured.is_(True))
                .all()
            )
            for it in items:
                it.is_published = True
            commit_or_rollback()
            flash("Published featured weekly insights.", "success")

        return redirect(url_for("admin.admin_weekly_changes", week_start=week_start.isoformat()))

    items = (
        db.session.query(WeeklyMarketChange)
        .filter(WeeklyMarketChange.week_start == week_start)
        .order_by(desc(WeeklyMarketChange.is_featured), desc(WeeklyMarketChange.created_at))
        .all()
    )

    featured = [x for x in items if x.is_featured]

    return render_template(
        "admin/weekly_changes.html",
        week_start=week_start,
        week_end=week_end,
        items=items,
        featured=featured,
    )


# -------------------------------------------------------------------
# New Weekly Market Changes admin (newsroom-style)
# -------------------------------------------------------------------
@bp.route("/weekly-market-changes", methods=["GET", "POST"])
@login_required
def weekly_market_changes_admin():
    if not _require_superuser():
        return redirect(url_for("home"))

    today = date.today()
    week_start = _monday_of(today)
    week_end = week_start + timedelta(days=6)

    qs = (request.args.get("week_start") or "").strip()
    if qs:
        try:
            week_start = date.fromisoformat(qs)
            week_end = week_start + timedelta(days=6)
        except Exception:
            pass

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()

        def _to_float(v):
            v = (v or "").strip()
            if not v:
                return None
            try:
                return float(v)
            except Exception:
                return None

        if action == "add":
            metric_type = (request.form.get("metric_type") or "pay").strip()
            direction = (request.form.get("direction") or "").strip() or None

            headline = (request.form.get("headline") or "").strip()
            interpretation = (request.form.get("interpretation") or "").strip() or None

            job_role = (request.form.get("job_role") or "").strip() or None
            sector = (request.form.get("sector") or "").strip() or None
            location = (request.form.get("location") or "").strip() or None

            value_previous = _to_float(request.form.get("value_previous"))
            value_current = _to_float(request.form.get("value_current"))
            delta_value = _to_float(request.form.get("delta_value"))
            delta_percent = _to_float(request.form.get("delta_percent"))

            conf = (request.form.get("confidence_level") or "").strip()
            confidence_level = int(conf) if conf.isdigit() else None

            is_featured = (request.form.get("is_featured") == "1")
            is_published = (request.form.get("is_published") == "1")

            if not headline:
                flash("Headline is required.", "error")
            else:
                row = WeeklyMarketChange(
                    week_start=week_start,
                    week_end=week_end,
                    metric_type=metric_type,
                    job_role=job_role,
                    sector=sector,
                    location=location,
                    value_previous=value_previous,
                    value_current=value_current,
                    delta_value=delta_value,
                    delta_percent=delta_percent,
                    direction=direction,
                    headline=headline,
                    interpretation=interpretation,
                    confidence_level=confidence_level,
                    is_featured=is_featured,
                    is_published=is_published,
                )
                db.session.add(row)
                commit_or_rollback()
                flash("Weekly item added.", "success")

        elif action == "toggle_featured":
            row_id = request.form.get("id")
            row = db.session.get(WeeklyMarketChange, int(row_id)) if row_id else None
            if row:
                row.is_featured = not bool(row.is_featured)
                commit_or_rollback()
                flash("Updated featured status.", "success")

        elif action == "toggle_published":
            row_id = request.form.get("id")
            row = db.session.get(WeeklyMarketChange, int(row_id)) if row_id else None
            if row:
                row.is_published = not bool(row.is_published)
                commit_or_rollback()
                flash("Updated published status.", "success")

        elif action == "publish_week":
            items = (
                db.session.query(WeeklyMarketChange)
                .filter(WeeklyMarketChange.week_start == week_start)
                .filter(WeeklyMarketChange.is_featured.is_(True))
                .all()
            )
            for it in items:
                it.is_published = True
            commit_or_rollback()
            flash("Published all featured items for the week.", "success")


        elif action == "generate_ai":
            # Generate per-item narratives + top weekly brief for featured items
            featured_items = (
                db.session.query(WeeklyMarketChange)
                .filter(WeeklyMarketChange.week_start == week_start)
                .filter(WeeklyMarketChange.is_featured.is_(True))
                .order_by(desc(WeeklyMarketChange.created_at))
                .all()
            )

            if not featured_items:
                flash("No featured items to generate narratives for.", "error")
                return redirect(url_for("admin.weekly_market_changes_admin", week_start=week_start.isoformat()))

            if not _ai_enabled():
                flash("AI not configured. Set OPENAI_API_KEY (and optionally PAYSCOPE_AI_MODEL).", "error")
                return redirect(url_for("admin.weekly_market_changes_admin", week_start=week_start.isoformat()))

            force = (request.form.get("force") == "1")

            # Per-item narratives
            generated_count = 0
            item_failures = 0

            for it in featured_items:
                # Skip if already generated (unless force)
                if (not force) and getattr(it, "ai_narrative", None):
                    continue

                try:
                    out = _ai_generate_item_narrative(it)

                    # Write safely (in case migrations aren't applied yet)
                    if hasattr(it, "ai_narrative"):
                        it.ai_narrative = (out.get("narrative") or "").strip() or None
                    if hasattr(it, "ai_driver_tags"):
                        it.ai_driver_tags = (out.get("driver_tags") or "").strip() or None
                    if hasattr(it, "ai_model"):
                        it.ai_model = (out.get("model") or "").strip() or None

                    # Timestamp column name (matches your model: ai_updated_at)
                    if hasattr(it, "ai_updated_at"):
                        it.ai_updated_at = datetime.utcnow()

                    generated_count += 1

                except Exception as e:  # noqa: BLE001
                    item_failures += 1
                    print("AI item narrative error:", e)

            # Weekly overview (WeeklyInsight)
            weekly_failed = False
            try:
                week_out = _ai_generate_weekly_overview(week_start, week_end, featured_items)

                weekly = (
                    db.session.query(WeeklyInsight)
                    .filter(WeeklyInsight.week_start == week_start)
                    .first()
                )
                if weekly is None:
                    weekly = WeeklyInsight(week_start=week_start, week_end=week_end)
                    db.session.add(weekly)

                weekly.week_end = week_end
                weekly.headline = (week_out.get("headline") or "").strip()
                weekly.overview = (week_out.get("overview") or "").strip()
                weekly.ai_model = (week_out.get("model") or "").strip() or None
                weekly.ai_generated_at = datetime.utcnow()

            except Exception as e:  # noqa: BLE001
                weekly_failed = True
                print("AI weekly overview error:", e)

            try:
                commit_or_rollback()

                if weekly_failed and generated_count > 0:
                    flash(
                        f"Generated {generated_count} item narrative(s), but weekly overview failed. Check logs.",
                        "warning",
                    )
                elif weekly_failed and generated_count == 0:
                    flash("AI generation failed (no narratives saved). Check logs.", "error")
                else:
                    msg = f"AI narratives generated for {generated_count} featured item(s)."
                    if item_failures:
                        msg += f" ({item_failures} failed — see logs.)"
                    flash(msg, "success")

            except Exception as e:  # noqa: BLE001
                print("Failed saving AI narratives:", e)
                flash("Failed saving AI narratives.", "error")

    



        elif action == "generate_candidates":
            # Column auto-detection
            c_day = _pick_col(JobSummaryDaily, ["day", "date", "summary_date", "run_date"])
            c_role = _pick_col(JobSummaryDaily, ["job_role_group", "job_role", "role", "canonical_role"])
            c_loc = _pick_col(JobSummaryDaily, ["location", "region", "county", "area"])
            c_sector = _pick_col(JobSummaryDaily, ["sector", "sector_group"])

            # Prefer avg/median pay if present (your table uses median_pay_rate)
            c_pay = _pick_col(JobSummaryDaily, ["avg_hourly", "avg_hourly_pay", "hourly_avg", "avg_rate", "median_pay_rate"])
            # Prefer adverts_count if present (your table uses adverts_count)
            c_cnt = _pick_col(JobSummaryDaily, ["posting_count", "count", "total", "n_postings", "job_count", "adverts_count"])

            missing = []
            if c_day is None:
                missing.append("date/day")
            if c_role is None:
                missing.append("role")
            if c_loc is None:
                missing.append("location/region/county")
            if c_pay is None and c_cnt is None:
                missing.append("median_pay_rate OR adverts_count")
            if missing:
                flash("Cannot generate candidates — missing JobSummaryDaily columns: " + ", ".join(missing), "error")
                return redirect(url_for("admin.weekly_market_changes_admin", week_start=week_start.isoformat()))

            prev_start = week_start - timedelta(days=7)
            prev_end = week_start - timedelta(days=1)

            group_cols = [c_role, c_loc]
            if c_sector is not None:
                group_cols.append(c_sector)

            q_cur = (
                db.session.query(
                    c_role.label("role"),
                    c_loc.label("loc"),
                    (c_sector.label("sector") if c_sector is not None else func.null().label("sector")),
                    (func.avg(c_pay).label("pay") if c_pay is not None else func.null().label("pay")),
                    (func.sum(c_cnt).label("cnt") if c_cnt is not None else func.null().label("cnt")),
                )
                .filter(c_day >= week_start, c_day <= week_end)
                .group_by(*group_cols)
            )

            q_prev = (
                db.session.query(
                    c_role.label("role"),
                    c_loc.label("loc"),
                    (c_sector.label("sector") if c_sector is not None else func.null().label("sector")),
                    (func.avg(c_pay).label("pay") if c_pay is not None else func.null().label("pay")),
                    (func.sum(c_cnt).label("cnt") if c_cnt is not None else func.null().label("cnt")),
                )
                .filter(c_day >= prev_start, c_day <= prev_end)
                .group_by(*group_cols)
            )

            cur_rows = {(r.role, r.loc, r.sector): r for r in q_cur.all()}
            prev_rows = {(r.role, r.loc, r.sector): r for r in q_prev.all()}

            candidates = []
            keys = set(cur_rows.keys()) | set(prev_rows.keys())

            for key in keys:
                role, loc, sector = key
                cur = cur_rows.get(key)
                prev = prev_rows.get(key)

                cur_pay = float(cur.pay) if cur and cur.pay is not None else None
                prev_pay = float(prev.pay) if prev and prev.pay is not None else None

                cur_cnt = float(cur.cnt) if cur and cur.cnt is not None else None
                prev_cnt = float(prev.cnt) if prev and prev.cnt is not None else None

                if cur_pay is not None and prev_pay is not None and prev_pay > 0:
                    dv = cur_pay - prev_pay
                    dp = (dv / prev_pay) * 100.0
                    candidates.append({"metric_type": "pay", "role": role, "loc": loc, "sector": sector, "prev": prev_pay, "cur": cur_pay, "dv": dv, "dp": dp})

                if cur_cnt is not None and prev_cnt is not None and prev_cnt > 0:
                    dv = cur_cnt - prev_cnt
                    dp = (dv / prev_cnt) * 100.0
                    candidates.append({"metric_type": "volume", "role": role, "loc": loc, "sector": sector, "prev": prev_cnt, "cur": cur_cnt, "dv": dv, "dp": dp})

            def top_n(metric, n):
                items = [c for c in candidates if c["metric_type"] == metric]
                items.sort(key=lambda x: abs(x["dv"]), reverse=True)
                return items[:n]

            top = top_n("pay", 25) + top_n("volume", 25)

            inserted = 0
            for c in top:
                role = _safe_str(c["role"])
                loc = _safe_str(c["loc"])
                sector = _safe_str(c.get("sector"))

                dv = float(c["dv"])
                dp = float(c["dp"])
                direction = "up" if dv > 0 else "down" if dv < 0 else "flat"

                if c["metric_type"] == "pay":
                    headline = f"{role} — {loc}: {dv:+.2f} ({dp:+.1f}%)"
                else:
                    headline = f"{role} — {loc}: volume {dv:+.0f} ({dp:+.1f}%)"

                exists = (
                    db.session.query(WeeklyMarketChange.id)
                    .filter(WeeklyMarketChange.week_start == week_start)
                    .filter(WeeklyMarketChange.metric_type == c["metric_type"])
                    .filter(WeeklyMarketChange.job_role == role)
                    .filter(WeeklyMarketChange.location == loc)
                    .filter(WeeklyMarketChange.headline == headline)
                    .first()
                )
                if exists:
                    continue

                row = WeeklyMarketChange(
                    week_start=week_start,
                    week_end=week_end,
                    metric_type=c["metric_type"],
                    job_role=role,
                    location=loc,
                    sector=sector,
                    value_previous=c["prev"],
                    value_current=c["cur"],
                    delta_value=dv,
                    delta_percent=dp,
                    direction=direction,
                    headline=headline,
                    interpretation=None,
                    confidence_level=None,
                    is_featured=False,
                    is_published=False,
                )
                db.session.add(row)
                inserted += 1

            commit_or_rollback()
            flash(f"Generated {inserted} candidate items (draft).", "success")

        elif action == "delete":
            row_id = request.form.get("id")
            row = db.session.get(WeeklyMarketChange, int(row_id)) if row_id else None
            if row:
                db.session.delete(row)
                commit_or_rollback()
                flash("Deleted item.", "success")

        return redirect(url_for("admin.weekly_market_changes_admin", week_start=week_start.isoformat()))

    items = (
        db.session.query(WeeklyMarketChange)
        .filter(WeeklyMarketChange.week_start == week_start)
        .order_by(desc(WeeklyMarketChange.is_featured), desc(WeeklyMarketChange.created_at))
        .all()
    )

    featured = [x for x in items if x.is_featured]
    published_featured = [x for x in featured if x.is_published]

    public_url = None
    if published_featured:
        public_url = url_for("insights.weekly_insight", week_start_iso=week_start.isoformat())

    prev_week = (week_start - timedelta(days=7)).isoformat()
    next_week = (week_start + timedelta(days=7)).isoformat()

    return render_template(
        "admin/weekly_market_changes.html",
        week_start=week_start,
        week_end=week_end,
        items=items,
        featured=featured,
        public_url=public_url,
        prev_week=prev_week,
        next_week=next_week,
    )


@bp.route("/cron-runs/run-now", methods=["POST"])
@login_required
def cron_run_now():
    if not _require_superuser():
        return redirect(url_for("home"))

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
    )
    db.session.add(log_row)
    commit_or_rollback()

    flash(f"Cron jobs executed with status: {status}", "info")
    return redirect(url_for("admin.cron_runs"))
