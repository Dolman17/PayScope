from app import create_app
from app.scrapers.adzuna import AdzunaScraper
from app.job_importer import import_posting_to_record
from extensions import db
from models import JobPosting
import sys

def run_scrape():
    app = create_app()
    with app.app_context():
        print("🔍 Running scheduled Adzuna scrape...")

        # EXAMPLE: multiple roles + multiple locations
        roles = [
            "support worker",
            "care assistant",
            "senior care assistant",
            "nurse",
            "team leader",
        ]

        locations = [
            "United Kingdom",
            "London",
            "Birmingham",
            "Manchester",
            "Leeds",
        ]

        scraped = 0
        created = 0

        for role in roles:
            for loc in locations:
                try:
                    scraper = AdzunaScraper(
                        what=role,
                        where=loc,
                        max_pages=2,
                        results_per_page=40
                    )
                    results = scraper.scrape()

                    for rec in results:
                        # Save JobPosting first (if needed)
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
                            raw_json=str(rec.raw_json),
                            search_role=role,
                            search_location=loc,
                        )
                        db.session.add(posting)
                        scraped += 1

                        # Convert Posting → JobRecord
                        job_record = import_posting_to_record(posting)
                        db.session.add(job_record)
                        created += 1

                    db.session.commit()

                except Exception as e:
                    print(f"⚠ Error during scrape for {role} @ {loc}: {e}")

        print(f"✔ Finished. Scraped {scraped} postings, created {created} JobRecords")

if __name__ == "__main__":
    run_scrape()
