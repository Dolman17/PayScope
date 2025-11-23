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
    scraped_postings = 0
    created_records = 0
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
                    # Store a JobPosting row
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
                    scraped_postings += 1

                    # Convert to JobRecord
                    job_record = import_posting_to_record(posting)
                    db.session.add(job_record)
                    created_records += 1

                db.session.commit()
            except Exception as e:  # noqa: BLE001
                db.session.rollback()
                msg = f"{label}: error scraping '{role}' @ '{loc}': {e!r}"
                print("⚠", msg)
                errors.append(msg)

    return {
        "scraped_postings": scraped_postings,
        "created_records": created_records,
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
        weekday = now.weekday()
        day_cfg = DAY_CONFIG.get(weekday) or DAY_CONFIG[0]
        label = day_cfg["label"]
        roles = day_cfg["roles"]
        locations = day_cfg["locations"]

        print(f"🔔 Cron: starting scheduled Adzuna scrape for '{label}' ({date.today().isoformat()})")

        log = CronRunLog(
            started_at=now,
            trigger=trigger,
            triggered_by=triggered_by,
            status="running",
        )
        # Set day_label *after* construction to avoid kwargs issues
        log.day_label = label

        db.session.add(log)
        db.session.commit()
 

        try:
            result = _run_for_config(label, roles, locations)
            log.finished_at = datetime.utcnow()
            log.status = "success" if not result["errors"] else "partial"
            log.scraped_postings = result["scraped_postings"]
            log.created_records = result["created_records"]
            log.error_messages = "\n".join(result["errors"]) if result["errors"] else None
            db.session.commit()

            print(
                f"✅ Cron complete: {result['scraped_postings']} postings, "
                f"{result['created_records']} JobRecords, "
                f"errors={len(result['errors'])}"
            )
            return result

        except Exception as e:  # noqa: BLE001
            log.finished_at = datetime.utcnow()
            log.status = "error"
            log.error_messages = f"{e!r}"
            db.session.commit()
            print("💥 Cron failed:", e)
            raise


# -------------------------------------------------------------------
# CLI entrypoint for Railway schedule
# -------------------------------------------------------------------
if __name__ == "__main__":
    # When Railway runs: python cron_runner.py
    run_scheduled_jobs(trigger="railway")
