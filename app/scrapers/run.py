# app/scrapers/run.py
from __future__ import annotations

import json
from datetime import datetime

from app import create_app
from extensions import db
from models import JobPosting

from .adzuna import AdzunaScraper
# from .indeed import IndeedScraper  # optional – keep disabled for now


app = create_app()


def upsert_job_record(record, search_role=None, search_location=None) -> JobPosting:
    """
    Insert or update a JobPosting based on (source_site, external_id) or URL.
    Also stores the search role/location used for this scrape, if provided.
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

    # NEW: record which search produced this posting
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



def run_all() -> None:
    scrapers = [
        AdzunaScraper(),
    ]

    with app.app_context():
        total = 0

        for scraper in scrapers:
            print(f"Running scraper: {scraper.source_site}")
            try:
                records = scraper.scrape()
            except Exception as exc:
                print(f"Error running {scraper.source_site}: {exc}")
                continue

            search_role = getattr(scraper, "what", None)
            search_location = getattr(scraper, "where", None)

            for rec in records:
                upsert_job_record(rec, search_role=search_role, search_location=search_location)
                total += 1

        db.session.commit()
        print(f"Scraping complete. {total} records processed.")



if __name__ == "__main__":
    run_all()
