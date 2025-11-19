from app import db
from datetime import datetime
from models import JobPosting, JobRecord
import re

def _derive_pay_rate(posting: JobPosting):
    """
    Convert scraped salary data to a single hourly pay_rate.
    - If hourly → midpoint or single value
    - If annual → convert using 52 weeks × 37.5 hours
    """
    if posting.rate_type == "hourly":
        if posting.min_rate and posting.max_rate:
            return float((posting.min_rate + posting.max_rate) / 2)
        if posting.min_rate:
            return float(posting.min_rate)
        if posting.max_rate:
            return float(posting.max_rate)
        return None

    if posting.rate_type == "annual":
        annual = None
        if posting.min_rate and posting.max_rate:
            annual = float((posting.min_rate + posting.max_rate) / 2)
        elif posting.min_rate:
            annual = float(posting.min_rate)
        elif posting.max_rate:
            annual = float(posting.max_rate)

        if annual:
            return round(annual / 52 / 37.5, 2)

    return None


def _extract_county(location_text: str) -> str | None:
    """
    Extract county from location_text using simple comma/dash splitting.
    """
    if not location_text:
        return None

    parts = re.split(r"[,\-]", location_text)
    parts = [p.strip() for p in parts if p.strip()]

    if len(parts) >= 2:
        return parts[-1]

    return None


def import_posting_to_record(posting: JobPosting) -> JobRecord:
    """
    Create a new JobRecord from a JobPosting.
    Maps all real JobRecord fields.
    """

    pay_rate = _derive_pay_rate(posting)
    county = _extract_county(posting.location_text)

    imported_month = posting.scraped_at.strftime("%B")
    imported_year = posting.scraped_at.strftime("%Y")

    record = JobRecord(
        company_id=None,
        company_name=posting.company_name,
        sector=posting.search_role,      # ← Leave or replace if you prefer smarter mapping
        job_role=posting.title,
        postcode=posting.postcode,
        county=county,
        pay_rate=pay_rate,
        imported_month=imported_month,
        imported_year=imported_year,
        latitude=None,
        longitude=None,
        imported_from_posting_id=posting.id,
        imported_at=datetime.utcnow(),
        external_url=posting.url,
    )

    db.session.add(record)
    posting.imported = True
    return record
