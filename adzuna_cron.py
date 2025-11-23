# adzuna_cron.py
from __future__ import annotations

import os
from datetime import datetime

from app import create_app
from extensions import db
from app.scrapers.adzuna import AdzunaScraper
from app.blueprints.admin import upsert_job_record


# Edit these lists to control what you scrape
SEARCH_ROLES = [
    "support worker",
    "senior support worker",
    "care assistant",
    "senior care assistant",
    "team leader",
    "registered manager",
]

SEARCH_LOCATIONS = [
    "United Kingdom",      # broad
    "London",
    "Birmingham",
    "Manchester",
    "Newcastle upon Tyne",
    "Leeds",
]


MAX_PAGES_PER_QUERY = 2  # Adzuna pages per role/location combo


def run_all_searches():
    app = create_app()
    with app.app_context():
        total_jobs = 0
        total_records = 0

        print(f"[{datetime.utcnow().isoformat()}] Starting Adzuna cron scrape")

        for role in SEARCH_ROLES:
            for loc in SEARCH_LOCATIONS:
                print(f"  → Scraping role={role!r}, location={loc!r}")
                try:
                    scraper = AdzunaScraper(
                        what=role,
                        where=loc,
                        max_pages=MAX_PAGES_PER_QUERY,
                    )
                    records = scraper.scrape()
                except Exception as exc:  # noqa: BLE001
                    print(f"    ✖ Error scraping {role!r} / {loc!r}: {exc}")
                    continue

                # Upsert into JobPostings
                inserted_for_combo = 0
                for rec in records:
                    upsert_job_record(rec, search_role=role, search_location=loc)
                    inserted_for_combo += 1
                    total_records += 1

                db.session.commit()
                total_jobs += 1
                print(f"    ✓ {inserted_for_combo} records processed for {role!r} / {loc!r}")

        print(
            f"[{datetime.utcnow().isoformat()}] Finished Adzuna cron scrape "
            f"— queries run: {total_jobs}, records processed: {total_records}"
        )


if __name__ == "__main__":
    run_all_searches()
