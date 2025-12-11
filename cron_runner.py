# cron_runner.py
from __future__ import annotations

import json
from datetime import datetime, date

from sqlalchemy import func
from sqlalchemy.exc import IntegrityError

from app import create_app
from app.scrapers.adzuna import AdzunaScraper
from app.importers.job_importer import import_posting_to_record, classify_sector
from extensions import db
from models import JobPosting, CronRunLog, JobRecord, JobRoleMapping
from job_summaries import build_daily_job_summaries  # Daily summaries
from ons_importer import import_ons_earnings_to_db   # ONS → DB upsert


def _truncate(value, max_len: int | None):
    """Safely truncate strings for fixed-length VARCHAR columns."""
    if value is None or max_len is None:
        return value
    s = str(value)
    if len(s) <= max_len:
        return s
    return s[:max_len]


# Optional OpenAI client for AI canonicalisation
try:
    from openai import OpenAI
    _openai_client = OpenAI()
except Exception:
    _openai_client = None


# -------------------------------------------------------------------
# Day-of-week scrape configuration
# -------------------------------------------------------------------
# 0 = Monday, 6 = Sunday
DAY_CONFIG = {
    0: {  # Monday – Social Care & Nursing
        "label": "Social Care & Nursing",
        "roles": [
            "support worker",
            "care assistant",
            "senior care assistant",
            "healthcare assistant",
            "nurse",
            "registered nurse",
            "team leader",
            "deputy manager",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Birmingham",
            "Manchester",
            "Leeds",
            "Glasgow",
        ],
    },
    1: {  # Tuesday – IT & Tech
        "label": "IT & Technology",
        "roles": [
            "software developer",
            "software engineer",
            "it support",
            "data analyst",
            "business analyst",
            "devops engineer",
        ],
        "locations": [
            "London",
            "Manchester",
            "Birmingham",
            "Leeds",
            "Bristol",
        ],
    },
    2: {  # Wednesday – Finance & Accounting
        "label": "Finance & Accounting",
        "roles": [
            "accountant",
            "finance manager",
            "financial analyst",
            "bookkeeper",
            "payroll clerk",
        ],
        "locations": [
            "London",
            "Manchester",
            "Leeds",
            "Edinburgh",
        ],
    },
    3: {  # Thursday – HR, Admin & Operations
        "label": "HR, Admin & Operations",
        "roles": [
            "hr advisor",
            "hr manager",
            "recruitment consultant",
            "office manager",
            "administrator",
            "operations manager",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Birmingham",
            "Manchester",
        ],
    },
    4: {  # Friday – Mixed Support Roles
        "label": "Support & Customer",
        "roles": [
            "customer service advisor",
            "call centre advisor",
            "receptionist",
            "support officer",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Birmingham",
            "Leeds",
        ],
    },
    5: {  # Saturday – Light Social Care refresh
        "label": "Weekend Social Care",
        "roles": [
            "support worker",
            "care assistant",
            "senior care assistant",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Manchester",
        ],
    },
    6: {  # Sunday – Light Nursing / Care
        "label": "Weekend Nursing & Care",
        "roles": [
            "nurse",
            "registered nurse",
            "support worker",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Birmingham",
        ],
    },
}


# -------------------------------------------------------------------
# Gap-filling config: target coverage + mappings
# -------------------------------------------------------------------
SECTOR_TARGET_MIN = 1500       # desired minimum records per sector before we de-prioritise it
COUNTY_TARGET_MIN = 800        # desired minimum records per county

MAX_GAP_SECTOR_JOBS = 6        # max extra sector-focused scrapes per run
MAX_GAP_COUNTY_JOBS = 6        # max extra county-focused scrapes per run

# Map *canonical sector names* in JobRecord.sector → representative Adzuna keywords
SECTOR_KEYWORDS = {
    "Education & Training": [
        "teacher",
        "lecturer",
        "trainer",
        "tutor",
    ],
    "Legal": [
        "solicitor",
        "paralegal",
        "legal assistant",
    ],
    "Sales & Marketing": [
        "sales executive",
        "sales manager",
        "marketing manager",
        "business development",
    ],
    "Customer Service": [
        "customer service advisor",
        "call centre advisor",
        "contact centre",
    ],
    "Support Worker": [
        "support worker",
        "care support worker",
    ],
    "HR & Recruitment": [
        "hr advisor",
        "hr officer",
        "recruitment consultant",
    ],
    "Finance & Accounting": [
        "accountant",
        "finance analyst",
        "bookkeeper",
    ],
    "IT & Digital": [
        "web developer",
        "frontend developer",
        "backend developer",
        "python developer",
    ],
    # extend / tweak as your sector naming settles
}

# Map DB counties/pseudo-counties to Adzuna "where" values
COUNTY_LOCATIONS = {
    "Staffordshire": "Staffordshire",
    "Cheshire": "Cheshire",
    "Norfolk": "Norfolk",
    "Lancashire": "Lancashire",
    "Herefordshire": "Herefordshire",
    "Shropshire": "Shropshire",
    "Worcestershire": "Worcestershire",
    "West Midlands": "West Midlands",
    "Greater Manchester": "Greater Manchester",
    "Bristol": "Bristol",
    "Glasgow": "Glasgow",
    "Edinburgh": "Edinburgh",
    "London": "London",
    "Leeds": "Leeds",
    "Birmingham": "Birmingham",
    # add more from your Records by County table as needed
}


def get_underrepresented_sectors(limit: int = 20) -> list[str]:
    """
    Return sector names that are below SECTOR_TARGET_MIN and that we know
    how to query (present in SECTOR_KEYWORDS), ordered from lowest coverage.
    """
    rows = (
        db.session.query(JobRecord.sector, func.count(JobRecord.id))
        .filter(JobRecord.sector.isnot(None))
        .group_by(JobRecord.sector)
        .order_by(func.count(JobRecord.id).asc())
        .limit(limit)
        .all()
    )

    result: list[str] = []
    for sector, count in rows:
        if not sector:
            continue
        if sector in SECTOR_KEYWORDS and (count or 0) < SECTOR_TARGET_MIN:
            result.append(sector)
    return result


def get_underrepresented_counties(limit: int = 50) -> list[str]:
    """
    Return county names that are below COUNTY_TARGET_MIN and present
    in COUNTY_LOCATIONS, ordered from lowest coverage.
    """
    rows = (
        db.session.query(JobRecord.county, func.count(JobRecord.id))
        .filter(JobRecord.county.isnot(None))
        .group_by(JobRecord.county)
        .order_by(func.count(JobRecord.id).asc())
        .limit(limit)
        .all()
    )

    result: list[str] = []
    for county, count in rows:
        if not county:
            continue
        if county in COUNTY_LOCATIONS and (count or 0) < COUNTY_TARGET_MIN:
            result.append(county)
    return result


def build_gap_fill_pairs() -> list[tuple[str, str]]:
    """
    Build a small list of (role_query, where_location) pairs that target
    under-represented sectors and counties.

    These are appended to the day's fixed role/location grid so each cron run
    nudges the dataset toward a better spread.
    """
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()

    # --- Sector-focused gap fill ------------------------------------------------
    for sector in get_underrepresented_sectors():
        keywords = SECTOR_KEYWORDS.get(sector) or []
        for kw in keywords:
            pair = (kw, "United Kingdom")
            if pair in seen:
                continue
            pairs.append(pair)
            seen.add(pair)
            if len([p for p in pairs if p[0]]) >= MAX_GAP_SECTOR_JOBS:
                break
        if len([p for p in pairs if p[0]]) >= MAX_GAP_SECTOR_JOBS:
            break

    # --- County-focused gap fill ------------------------------------------------
    for county in get_underrepresented_counties():
        where = COUNTY_LOCATIONS[county]
        # Use an empty 'what' to pull a cross-section of roles in that county.
        pair = ("", where)
        if pair in seen:
            continue
        pairs.append(pair)
        seen.add(pair)
        if len([p for p in pairs if p[0] == ""]) >= MAX_GAP_COUNTY_JOBS:
            break

    return pairs


# -------------------------------------------------------------------
# Core scrape logic
# -------------------------------------------------------------------
def _find_existing_posting(
    source_site: str,
    external_id: str | None,
    url: str | None,
):
    """
    Look up an existing JobPosting for dedup:
    - Prefer (source_site, external_id)
    - Fall back to (source_site, url) if no external_id
    """
    q = None
    if external_id:
        q = JobPosting.query.filter_by(source_site=source_site, external_id=external_id)
    elif url:
        q = JobPosting.query.filter_by(source_site=source_site, url=url)

    return q.first() if q is not None else None


def _run_for_config(
    label: str,
    roles: list[str],
    locations: list[str],
    extra_pairs: list[tuple[str, str]] | None = None,
) -> dict:
    """
    Run Adzuna scrapes for the given roles/locations.

    - Builds the cartesian product of (roles × locations)
    - Optionally appends gap-filling (role, where) pairs
    - Dedupes pairs
    - Returns a dict with counts and any error messages.
    """
    rows_scraped = 0              # total items returned from Adzuna
    records_created = 0           # JobRecord rows created
    postings_created = 0          # new JobPosting rows
    postings_updated = 0          # existing JobPosting rows updated

    errors: list[str] = []

    # Build base grid
    pairs: set[tuple[str, str]] = set()
    for role in roles:
        for loc in locations:
            pairs.add((role, loc))

    # Add gap-fill pairs (sector/county driven)
    if extra_pairs:
        for r, l in extra_pairs:
            pairs.add((r, l))

    for role, loc in sorted(pairs):
        try:
            scraper = AdzunaScraper(
                what=role,
                where=loc,
                max_pages=2,
                results_per_page=40,
            )
            results = scraper.scrape()

            for rec in results:
                rows_scraped += 1

                existing = _find_existing_posting(
                    source_site=rec.source_site,
                    external_id=rec.external_id,
                    url=rec.url,
                )

                now = datetime.utcnow()

                # Sector is derived from the live title + search role
                sector_value = classify_sector(rec.title, role)

                # Safely truncate string fields to match DB column sizes
                title = _truncate(rec.title, 255)
                company_name = _truncate(rec.company_name, 255)
                location_text = _truncate(rec.location_text, 255)
                postcode = _truncate(rec.postcode, 20)
                sector_val = _truncate(sector_value, 100)
                rate_type = _truncate(rec.rate_type, 50)
                contract_type = _truncate(rec.contract_type, 50)
                source_site = _truncate(rec.source_site, 100)
                external_id = _truncate(rec.external_id, 255)
                search_role_val = _truncate(role, 255)
                search_location_val = _truncate(loc, 255)

                if existing:
                    # Update existing posting with fresh data
                    posting = existing
                    posting.title = title
                    posting.company_name = company_name
                    posting.location_text = location_text
                    posting.postcode = postcode
                    posting.sector = sector_val
                    posting.min_rate = rec.min_rate
                    posting.max_rate = rec.max_rate
                    posting.rate_type = rate_type
                    posting.contract_type = contract_type
                    posting.url = rec.url
                    posting.posted_date = rec.posted_date
                    posting.raw_json = json.dumps(rec.raw_json or {})
                    posting.search_role = search_role_val
                    posting.search_location = search_location_val
                    posting.scraped_at = now  # treat as "last seen"
                    posting.is_active = True
                    postings_updated += 1
                else:
                    # Create new posting
                    posting = JobPosting(
                        title=title,
                        company_name=company_name,
                        location_text=location_text,
                        postcode=postcode,
                        sector=sector_val,
                        min_rate=rec.min_rate,
                        max_rate=rec.max_rate,
                        rate_type=rate_type,
                        contract_type=contract_type,
                        source_site=source_site,
                        external_id=external_id,
                        url=rec.url,
                        posted_date=rec.posted_date,
                        raw_json=json.dumps(rec.raw_json or {}),
                        search_role=search_role_val,
                        search_location=search_location_val,
                    )
                    db.session.add(posting)
                    postings_created += 1

                # Always import into JobRecord for time-series history
                job_record = import_posting_to_record(posting)
                db.session.add(job_record)
                records_created += 1

            db.session.commit()
        except Exception as e:  # noqa: BLE001
            db.session.rollback()
            msg = f"{label}: error scraping '{role}' @ '{loc}': {e!r}"
            print("⚠", msg)
            errors.append(msg)

    return {
        "rows_scraped": rows_scraped,
        "records_created": records_created,
        "postings_created": postings_created,
        "postings_updated": postings_updated,
        "errors": errors,
    }


# -------------------------------------------------------------------
# Daily JobSummary builder
# -------------------------------------------------------------------
def _run_daily_summaries_for_date(
    target_date: date,
    trigger: str = "scheduled",
    triggered_by: str | None = None,
) -> dict:
    """
    Build JobSummaryDaily rows for a given date and log it via CronRunLog.

    Assumes we are already inside app.app_context().
    """
    now = datetime.utcnow()
    safe_label = target_date.isoformat()[:20]

    log = CronRunLog(
        job_name="job_summary_daily_builder",
        started_at=now,
        status="running",
        trigger=trigger,
        triggered_by=triggered_by,
        day_label=safe_label,
    )
    db.session.add(log)
    db.session.commit()

    result: dict = {
        "log_id": log.id,
        "rows_created": 0,
        "error": None,
        "date": target_date.isoformat(),
    }

    try:
        rows_created = build_daily_job_summaries(target_date)
        log.finished_at = datetime.utcnow()
        log.status = "success"
        log.records_created = rows_created
        log.message = f"Built {rows_created} JobSummaryDaily rows for {target_date.isoformat()}."
        db.session.commit()

        result["rows_created"] = rows_created
        return result

    except Exception as e:  # noqa: BLE001
        db.session.rollback()
        log.finished_at = datetime.utcnow()
        log.status = "error"
        log.message = f"{e!r}"
        db.session.commit()

        result["error"] = repr(e)
        return result


# -------------------------------------------------------------------
# AI-driven job-role canonicaliser (via JobRoleMapping)
# -------------------------------------------------------------------
def _canonicalise_job_roles_with_ai(
    trigger: str = "scheduled",
    triggered_by: str | None = None,
    day_label: str | None = None,
    max_roles: int = 500,
    chunk_size: int = 25,
) -> dict:
    """
    Canonicalise job titles in small AI batches.

    NEW BEHAVIOUR:
    - Works at the level of JobRecord.job_role (distinct raw values).
    - Only processes roles that do NOT yet have a JobRoleMapping row.
    - For each raw role, asks OpenAI for a canonical group name.
    - Writes/updates JobRoleMapping(raw_value, canonical_role).
    - Applies that canonical role back to JobRecord:
        - JobRecord.job_role       = canonical_role
        - JobRecord.job_role_group = canonical_role
    - Logs progress via CronRunLog.

    Assumes it is called inside app.app_context().
    """

    now = datetime.utcnow()
    safe_day_label = (day_label or "Job roles")[:20]

    log = CronRunLog(
        job_name="job_role_canonicaliser",
        started_at=now,
        status="running",
        trigger=trigger,
        triggered_by=triggered_by,
        day_label=safe_day_label,
    )
    db.session.add(log)
    db.session.commit()

    summary: dict = {
        "log_id": log.id,
        "updated": 0,
        "examined": 0,
        "skipped": 0,
        "error": None,
    }

    try:
        if _openai_client is None:
            msg = "OpenAI client not available; skipping canonicaliser."
            log.status = "error"
            log.message = msg
            db.session.commit()
            summary["error"] = msg
            return summary

        # ------------------------------------------------------------------
        # 1) Fetch distinct raw roles that do NOT yet have a mapping
        # ------------------------------------------------------------------
        roles_query = (
            db.session.query(
                JobRecord.job_role.label("raw_value"),
                func.count(JobRecord.id).label("count"),
            )
            .outerjoin(
                JobRoleMapping,
                JobRoleMapping.raw_value == JobRecord.job_role,
            )
            .filter(JobRecord.job_role.isnot(None))
            .filter(JobRoleMapping.id.is_(None))  # no mapping yet
            .group_by(JobRecord.job_role)
            .order_by(func.count(JobRecord.id).desc())
            .limit(max_roles)
        )

        rows = roles_query.all()
        summary["examined"] = len(rows)

        if not rows:
            log.status = "success"
            log.message = "No job roles needing canonicalisation."
            db.session.commit()
            return summary

        # Helper to chunk the work
        def chunked(seq, n):
            for i in range(0, len(seq), n):
                yield seq[i : i + n]

        system_msg = (
            "You are normalising job titles for an analytics tool.\n"
            "You will be given a list of raw job titles from multiple sectors "
            "(social care, IT, finance, admin, etc.). Your job is to assign a "
            "short, human-readable canonical group name for each raw title.\n\n"
            "Rules:\n"
            "- Similar titles must share the SAME canonical group label.\n"
            "- Keep labels concise (ideally 2–4 words, max ~40 characters).\n"
            "- Do NOT include contract details (full-time, part-time, nights).\n"
            "- Do NOT include seniority modifiers in the label unless it changes the job level "
            "(e.g. 'Senior Support Worker' vs 'Support Worker' can be separate groups).\n"
            "- It is OK to reuse common groups such as 'Support Worker', "
            "'Care Assistant', 'Registered Nurse', 'Administrator', etc.\n\n"
            "Response format:\n"
            "Return ONLY valid JSON, no commentary, no markdown fences. The JSON should look like:\n"
            "{\n"
            '  \"mappings\": {\n'
            '    \"raw title 1\": \"Canonical Group 1\",\n'
            '    \"raw title 2\": \"Canonical Group 2\"\n'
            "  }\n"
            "}\n"
        )

        total_updated = 0
        total_skipped = 0

        # ------------------------------------------------------------------
        # 2) Process in chunks to keep prompts and responses small
        # ------------------------------------------------------------------
        for chunk_index, chunk_rows in enumerate(chunked(rows, chunk_size), start=1):
            # chunk_rows: list of (raw_value, count)
            role_items: list[str] = []
            for raw_value, count in chunk_rows:
                raw_title = (raw_value or "").strip()
                if not raw_title:
                    total_skipped += 1
                    continue
                role_items.append(raw_title)

            if not role_items:
                continue

            list_block = "\n".join(f"- {title}" for title in role_items)
            user_msg = (
                "Here is the list of raw job titles you need to normalise:\n\n"
                f"{list_block}\n\n"
                "Remember: respond ONLY with JSON as described "
                "in the system message, covering ALL of the titles above."
            )

            # Call OpenAI with a hard timeout so we don't hang the worker
            try:
                resp = _openai_client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {"role": "system", "content": system_msg},
                        {"role": "user", "content": user_msg},
                    ],
                    temperature=0.1,
                    timeout=20,  # seconds – keep under gunicorn worker timeout
                    max_tokens=1500,
                )
                content = resp.choices[0].message.content.strip()
            except Exception as e:  # noqa: BLE001
                msg = (
                    f"OpenAI call failed during job-role canonicaliser "
                    f"(chunk {chunk_index}): {e!r}"
                )
                log.status = "error"
                log.message = msg
                db.session.commit()
                summary["error"] = msg
                summary["updated"] = total_updated
                summary["skipped"] = total_skipped
                return summary

            # ---- strip ```json / ``` fences defensively ----
            cleaned = content.strip()
            if cleaned.startswith("```"):
                first_newline = cleaned.find("\n")
                if first_newline != -1:
                    cleaned = cleaned[first_newline + 1 :]
                else:
                    cleaned = cleaned.lstrip("`")
                if "```" in cleaned:
                    cleaned = cleaned.rsplit("```", 1)[0].strip()

            try:
                data = json.loads(cleaned)
                if isinstance(data, dict) and "mappings" in data:
                    mapping = data.get("mappings") or {}
                else:
                    mapping = data if isinstance(data, dict) else {}
            except Exception as e:  # noqa: BLE001
                msg = (
                    f"Failed to parse AI JSON for chunk {chunk_index}: {e!r} "
                    f"| content={cleaned[:500]}"
                )
                log.status = "error"
                log.message = msg
                db.session.commit()
                summary["error"] = msg
                summary["updated"] = total_updated
                summary["skipped"] = total_skipped
                return summary

            # ------------------------------------------------------------------
            # 3) Apply mapping back to JobRoleMapping + JobRecord for this chunk
            # ------------------------------------------------------------------
            for raw_value, count in chunk_rows:
                raw_title = (raw_value or "").strip()
                if not raw_title:
                    continue

                canonical = (mapping.get(raw_title) or "").strip()
                if not canonical:
                    total_skipped += 1
                    continue

                # Upsert JobRoleMapping (race-safe)
                try:
                    m = JobRoleMapping.query.filter_by(raw_value=raw_title).first()
                    if m is None:
                        m = JobRoleMapping(
                            raw_value=raw_title,
                            canonical_role=canonical,
                        )
                        db.session.add(m)
                        db.session.flush()  # force uniqueness check here
                    else:
                        m.canonical_role = canonical
                except IntegrityError:
                    # Another process inserted this mapping between SELECT and INSERT
                    db.session.rollback()
                    m = JobRoleMapping.query.filter_by(raw_value=raw_title).first()
                    if not m:
                        # If it's still not there, bubble up the real error
                        raise

                # Apply to all JobRecord rows with this raw_title
                q = JobRecord.query.filter(JobRecord.job_role == raw_title)
                count_updated = q.update(
                    {
                        "job_role": canonical,
                        "job_role_group": canonical,
                    },
                    synchronize_session=False,
                )
                total_updated += count_updated

        # If we get here, everything completed
        db.session.commit()
        log.finished_at = datetime.utcnow()
        log.status = "success"
        log.message = (
            f"Canonicalised job roles. updated={total_updated}, "
            f"examined={summary['examined']}, skipped={total_skipped}"
        )
        db.session.commit()

        summary["updated"] = total_updated
        summary["skipped"] = total_skipped
        return summary

    except Exception as e:  # noqa: BLE001
        db.session.rollback()
        log.finished_at = datetime.utcnow()
        log.status = "error"
        log.message = f"{e!r}"
        db.session.commit()
        summary["error"] = repr(e)
        return summary


# -------------------------------------------------------------------
# Public API used by:
#   - Railway cron (python cron_runner.py)
#   - Admin "Run Scrape Now" button
# -------------------------------------------------------------------
def run_scheduled_jobs(
    trigger: str = "scheduled",
    triggered_by: str | None = None,
) -> dict:
    """
    Entry point used by both:
      - Railway cron task
      - /admin/cron-runs/run-now

    Creates an app context, runs the correct day's config,
    logs a CronRunLog row, and returns a summary dict.
    """
    app = create_app()

    with app.app_context():
        now = datetime.utcnow()
        weekday = date.today().weekday()
        day_cfg = DAY_CONFIG.get(weekday) or DAY_CONFIG[0]
        label = day_cfg["label"]
        roles = day_cfg["roles"]
        locations = day_cfg["locations"]

        print(
            f"🔔 Cron: starting scheduled Adzuna scrape for "
            f"'{label}' ({date.today().isoformat()})"
        )

        # Make sure label fits VARCHAR(20)
        safe_label = str(label)[:20] if label is not None else None

        # Create CronRunLog row for the scrape
        log = CronRunLog(
            job_name="adzuna_daily_scrape",
            started_at=now,
            status="running",
            trigger=trigger,
            triggered_by=triggered_by,
            day_label=safe_label,
        )
        db.session.add(log)
        db.session.commit()

        try:
            # Build adaptive gap-fill queries based on current dataset coverage
            gap_pairs = build_gap_fill_pairs()
            if gap_pairs:
                print(
                    f"🎯 Gap-fill enabled: {len(gap_pairs)} extra "
                    f"(role, location) pairs this run."
                )

            result = _run_for_config(label, roles, locations, extra_pairs=gap_pairs)

            log.finished_at = datetime.utcnow()
            log.rows_scraped = result["rows_scraped"]
            log.records_created = result["records_created"]

            if result["errors"]:
                log.status = "partial"
                log.message = "\n".join(result["errors"])
            else:
                log.status = "success"
                log.message = None

            db.session.commit()

            print(
                f"✅ Cron complete: {result['rows_scraped']} postings, "
                f"{result['records_created']} JobRecords, "
                f"{result['postings_created']} new, "
                f"{result['postings_updated']} updated, "
                f"errors={len(result['errors'])}"
            )

            result_with_log = dict(result)
            result_with_log["log_id"] = log.id
            result_with_log["day_label"] = safe_label

            # Weekly job-role canonicaliser (e.g. run on Sunday = 6)
            if weekday == 6:
                try:
                    print("🧹 Weekly job-role canonicaliser: starting…")
                    canon_result = _canonicalise_job_roles_with_ai(
                        trigger=trigger,
                        triggered_by=triggered_by,
                        day_label=safe_label,
                        max_roles=200,
                    )
                    print(
                        "🧹 Canonicaliser done: "
                        f"updated={canon_result.get('updated')}, "
                        f"examined={canon_result.get('examined')}, "
                        f"error={canon_result.get('error')}"
                    )
                    result_with_log["canonicaliser_log_id"] = canon_result.get("log_id")
                    result_with_log["canonicaliser_updated"] = canon_result.get(
                        "updated", 0
                    )
                except Exception as e:  # noqa: BLE001
                    # Don't break the main cron if canonicaliser fails
                    print("⚠ job_role_canonicaliser failed:", e)

            # Daily JobSummaryDaily for today's data
            try:
                target_date = date.today()
                print(
                    f"📊 Building daily job summaries "
                    f"for {target_date.isoformat()}…"
                )
                summary_result = _run_daily_summaries_for_date(
                    target_date=target_date,
                    trigger=trigger,
                    triggered_by=triggered_by,
                )
                print(
                    "📊 Daily summaries done: "
                    f"created={summary_result.get('rows_created')}, "
                    f"error={summary_result.get('error')}"
                )
                result_with_log["summary_log_id"] = summary_result.get("log_id")
                result_with_log["summary_rows_created"] = summary_result.get(
                    "rows_created", 0
                )
            except Exception as e:  # noqa: BLE001
                print("⚠ job_summary_daily_builder failed:", e)

            # NEW: ONS ASHE earnings import / upsert (annual, but idempotent)
            try:
                ashe_year = date.today().year - 1
                print(f"📈 Importing ONS ASHE earnings for {ashe_year}…")
                ons_result = import_ons_earnings_to_db(
                    ashe_year,
                    trigger=trigger,
                    triggered_by=triggered_by,
                    use_app_context=True,  # already inside app.app_context()
                )
                print(
                    "📈 ONS import done: "
                    f"fetched={ons_result.get('fetched')}, "
                    f"created={ons_result.get('created')}, "
                    f"updated={ons_result.get('updated')}, "
                    f"error={ons_result.get('error')}"
                )
                result_with_log["ons_log_id"] = ons_result.get("log_id")
                result_with_log["ons_fetched"] = ons_result.get("fetched", 0)
                result_with_log["ons_created"] = ons_result.get("created", 0)
                result_with_log["ons_updated"] = ons_result.get("updated", 0)
            except Exception as e:  # noqa: BLE001
                print("⚠ ONS ASHE import failed:", e)

            return result_with_log

        except Exception as e:  # noqa: BLE001
            log.finished_at = datetime.utcnow()
            log.status = "error"
            log.message = f"{e!r}"
            db.session.commit()
            print("💥 Cron failed:", e)
            raise


# -------------------------------------------------------------------
# One-off canonicaliser entry point (manual/admin/CLI)
# -------------------------------------------------------------------
def run_job_role_canonicaliser(
    trigger: str = "manual",
    triggered_by: str | None = None,
    max_roles: int = 5000,
) -> dict:
    """
    One-off entry point to canonicalise job roles on demand.

    Can be called from:
      - Admin UI button
      - CLI: python -c "from cron_runner import run_job_role_canonicaliser; run_job_role_canonicaliser()"
    """
    app = create_app()
    with app.app_context():
        print("🧹 One-off job-role canonicaliser: starting…")
        result = _canonicalise_job_roles_with_ai(
            trigger=trigger,
            triggered_by=triggered_by,
            day_label="Manual run",
            max_roles=max_roles,
        )
        print(
            "🧹 Canonicaliser complete: "
            f"updated={result.get('updated')}, "
            f"examined={result.get('examined')}, "
            f"error={result.get('error')}"
        )
        return result


# -------------------------------------------------------------------
# CLI entrypoint for Railway schedule
# -------------------------------------------------------------------
if __name__ == "__main__":
    # When Railway runs: python cron_runner.py
    run_scheduled_jobs(trigger="railway")

