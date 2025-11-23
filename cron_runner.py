# cron_runner.py
from __future__ import annotations

import json
from datetime import datetime, date

from app import create_app
from app.scrapers.adzuna import AdzunaScraper
from app.importers.job_importer import import_posting_to_record
from extensions import db
from models import JobPosting, CronRunLog


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
def _run_for_config(label: str, roles: list[str], locations: list[str]) -> dict:
    """
    Run Adzuna scrapes for the given roles/locations.
    Returns a dict with counts and any error messages.
    """
    rows_scraped = 0
    records_created = 0
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
                    posting = JobPosting(
                        title=rec.title,
                        company_name=rec.company_name,
                        location_text=rec.location_text,
                        postcode=rec.postcode,
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
                    rows_scraped += 1

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
        "errors": errors,
    }


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

        # Create CronRunLog row
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
                f"errors={len(result['errors'])}"
            )
            # Include log_id in case the caller wants to link back
            result_with_log = dict(result)
            result_with_log["log_id"] = log.id
            result_with_log["day_label"] = safe_label
            return result_with_log

        except Exception as e:  # noqa: BLE001
            log.finished_at = datetime.utcnow()
            log.status = "error"
            log.message = f"{e!r}"
            db.session.commit()
            print("💥 Cron failed:", e)
            raise


# -------------------------------------------------------------------
# CLI entrypoint for Railway schedule
# -------------------------------------------------------------------
if __name__ == "__main__":
    # When Railway runs: python cron_runner.py
    run_scheduled_jobs(trigger="railway")
