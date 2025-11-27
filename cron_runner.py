# cron_runner.py
from __future__ import annotations

import json
from datetime import datetime, date

from app import create_app
from app.scrapers.adzuna import AdzunaScraper
from app.importers.job_importer import import_posting_to_record, classify_sector
from extensions import db
from models import JobPosting, CronRunLog, JobRecord

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
# Core scrape logic
# -------------------------------------------------------------------
def _find_existing_posting(source_site: str, external_id: str | None, url: str | None):
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


def _run_for_config(label: str, roles: list[str], locations: list[str]) -> dict:
    """
    Run Adzuna scrapes for the given roles/locations.
    Returns a dict with counts and any error messages.
    """
    rows_scraped = 0              # total items returned from Adzuna
    records_created = 0           # JobRecord rows created
    postings_created = 0          # new JobPosting rows
    postings_updated = 0          # existing JobPosting rows updated

    errors: list[str] = []

    for role in roles:
        for loc in locations:
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

                    if existing:
                        # Update existing posting with fresh data
                        posting = existing
                        posting.title = rec.title
                        posting.company_name = rec.company_name
                        posting.location_text = rec.location_text
                        posting.postcode = rec.postcode
                        posting.sector = sector_value
                        posting.min_rate = rec.min_rate
                        posting.max_rate = rec.max_rate
                        posting.rate_type = rec.rate_type
                        posting.contract_type = rec.contract_type
                        posting.url = rec.url
                        posting.posted_date = rec.posted_date
                        posting.raw_json = json.dumps(rec.raw_json or {})
                        posting.search_role = role
                        posting.search_location = loc
                        posting.scraped_at = now  # treat as "last seen"
                        posting.is_active = True
                        postings_updated += 1
                    else:
                        # Create new posting
                        posting = JobPosting(
                            title=rec.title,
                            company_name=rec.company_name,
                            location_text=rec.location_text,
                            postcode=rec.postcode,
                            sector=sector_value,
                            min_rate=rec.min_rate,
                            max_rate=rec.max_rate,
                            rate_type=rec.rate_type,
                            contract_type=rec.contract_type,
                            source_site=rec.source_site,
                            external_id=rec.external_id,
                            url=rec.url,
                            posted_date=rec.posted_date,
                            raw_json=json.dumps(rec.raw_json or {}),
                            search_role=role,
                            search_location=loc,
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
# AI-driven job-role canonicaliser (chunked)
# -------------------------------------------------------------------
def _canonicalise_job_roles_with_ai(
    trigger: str = "scheduled",
    triggered_by: str | None = None,
    day_label: str | None = None,
    max_roles: int = 200,
    chunk_size: int = 25,
) -> dict:
    """
    Canonicalise job titles in small AI batches.

    - Fetch up to `max_roles` distinct (sector, job_role) needing grouping.
    - Split into chunks of `chunk_size` items.
    - For each chunk, call OpenAI with a compact JSON payload.
    - Apply canonical labels to JobRecord.job_role_group.
    - Log everything in CronRunLog.

    Assumes it is called inside app.app_context().
    """

    now = datetime.utcnow()
    safe_day_label = (day_label or "Weekly Roles")[:20]

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

        # 1) Fetch distinct roles that haven't been grouped yet
        rows = (
            db.session.query(JobRecord.sector, JobRecord.job_role)
            .filter(JobRecord.job_role.isnot(None))
            .filter(
                (JobRecord.job_role_group.is_(None)) |
                (JobRecord.job_role_group == "")
            )
            .distinct()
            .order_by(JobRecord.sector, JobRecord.job_role)
            .limit(max_roles)
            .all()
        )

        summary["examined"] = len(rows)

        if not rows:
            log.status = "success"
            log.message = "No job roles needing canonicalisation."
            db.session.commit()
            return summary

        # Helper to chunk the work
        def chunked(seq, n):
            for i in range(0, len(seq), n):
                yield seq[i: i + n]

        system_msg = (
            "You are cleaning job titles for an analytics tool. "
            "For each input item (sector + raw title), you assign a short, "
            "human-readable canonical group name. Similar roles should share "
            "the same group label. Keep labels concise (max ~40 characters). "
            "Respond ONLY with valid JSON, no commentary or markdown fences."
        )

        total_updated = 0
        total_skipped = 0

        # Process in chunks to keep prompts and responses small
        for chunk_index, chunk_rows in enumerate(chunked(rows, chunk_size), start=1):
            # Build JSON payload for this chunk
            role_items = []
            for sector, raw in chunk_rows:
                raw_title = (raw or "").strip()
                if not raw_title:
                    total_skipped += 1
                    continue
                role_items.append({
                    "sector": sector or "Unknown",
                    "title": raw_title,
                })

            if not role_items:
                continue

            payload = json.dumps(role_items, ensure_ascii=False)

            user_msg = (
                "You are given a JSON array of job roles. "
                "Each item has keys: 'sector' and 'title'.\n\n"
                "Return a JSON object where:\n"
                '- each key is exactly \"sector|||title\" using the given values, and\n'
                "- each value is your canonical group label (short, human-readable).\n\n"
                "Input JSON:\n"
                f"{payload}\n\n"
                "Output JSON only, no explanation, no markdown fences."
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
            except Exception as e:
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

            # ---- NEW: strip ```json / ``` fences defensively ----
            cleaned = content.strip()
            if cleaned.startswith("```"):
                # Drop first fence line (``` or ```json)
                first_newline = cleaned.find("\n")
                if first_newline != -1:
                    cleaned = cleaned[first_newline + 1 :]
                else:
                    cleaned = cleaned.lstrip("`")
                # Drop trailing ``` if present
                if "```" in cleaned:
                    cleaned = cleaned.rsplit("```", 1)[0].strip()

            try:
                mapping = json.loads(cleaned)
            except Exception as e:
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

            # Apply mapping back to JobRecord for this chunk
            for sector, raw in chunk_rows:
                raw_title = (raw or "").strip()
                if not raw_title:
                    continue
                key = f"{sector or 'Unknown'}|||{raw_title}"
                canonical = (mapping.get(key) or "").strip()
                if not canonical:
                    total_skipped += 1
                    continue

                q = JobRecord.query.filter(
                    JobRecord.job_role == raw_title,
                    JobRecord.sector == sector,
                )
                count = q.update(
                    {"job_role_group": canonical},
                    synchronize_session=False,
                )
                total_updated += count

            # Commit after each chunk so progress is saved
            db.session.commit()

        # All chunks processed (or gracefully aborted earlier)
        log.finished_at = datetime.utcnow()
        log.status = "success"
        log.records_created = total_updated  # reuse field for "rows updated"
        log.message = (
            f"Canonicalised job roles. Updated rows={total_updated}, "
            f"examined={summary['examined']}, skipped={total_skipped}."
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
def run_scheduled_jobs(trigger: str = "scheduled", triggered_by: str | None = None) -> dict:
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

        print(f"🔔 Cron: starting scheduled Adzuna scrape for '{label}' ({date.today().isoformat()})")

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
            result = _run_for_config(label, roles, locations)

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

            # Include log_id in case the caller wants to link back
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
                        f"🧹 Canonicaliser done: updated={canon_result.get('updated')}, "
                        f"examined={canon_result.get('examined')}, "
                        f"error={canon_result.get('error')}"
                    )
                    result_with_log["canonicaliser_log_id"] = canon_result.get("log_id")
                    result_with_log["canonicaliser_updated"] = canon_result.get("updated", 0)
                except Exception as e:  # noqa: BLE001
                    # Don't break the main cron if canonicaliser fails
                    print("⚠ job_role_canonicaliser failed:", e)

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
    max_roles: int = 500,
) -> dict:
    """
    One-off entry point to canonicalise job roles on demand.

    Can be called from:
      - Admin UI button
      - CLI: `python -c "from cron_runner import run_job_role_canonicaliser; run_job_role_canonicaliser()"`.
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
            f"🧹 Canonicaliser complete: updated={result.get('updated')}, "
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
