# app/importers/job_importer.py
from __future__ import annotations

from datetime import datetime

from extensions import db
from models import JobRecord, JobPosting
from app.blueprints.utils import (
    get_or_create_company_id,
    normalize_uk_postcode,
    geocode_postcode_cached,
)

# Match DB column limits
MAX_JOB_ROLE_LEN = 100  # job_record.job_role is VARCHAR(100)
MAX_SECTOR_LEN = 100    # job_record.sector is VARCHAR(100)
MAX_COUNTY_LEN = 100    # job_record.county is VARCHAR(100)
MAX_URL_LEN = 500       # be defensive; adjust if your model uses a different length


def _truncate(value: str | None, max_len: int) -> str | None:
    """
    Safely truncate a string to max_len characters, preserving None.

    This prevents "value too long for type character varying(N)" errors when
    importing from external sources with wild job titles / sectors / counties.
    """
    if value is None:
        return None
    value = str(value)
    if len(value) <= max_len:
        return value
    return value[:max_len]


def import_posting_to_record(
    posting: JobPosting,
    enable_snap_to_postcode: bool = True,
) -> JobRecord:
    """
    Import a JobPosting row into the JobRecord table.

    - Creates/looks up company_id from JobPosting.company_name
    - Normalises and geocodes postcode
    - Optionally snaps to postcode (if you add that logic)
    - **Truncates fields to DB limits** to avoid varchar overflow.
    """

    # --- Company ID + name ---
    company_name = (posting.company_name or "").strip()
    company_id = None
    if company_name:
        company_id = get_or_create_company_id(company_name)

    # --- Pay + imported month/year ---
    pay_rate = posting.min_rate or posting.max_rate
    imported_at = datetime.utcnow()
    imported_month = imported_at.strftime("%B")
    imported_year = imported_at.strftime("%Y")

    # --- Postcode normalisation / geocoding ---
    raw_postcode = (posting.postcode or "").strip()
    norm_pc = normalize_uk_postcode(raw_postcode) if raw_postcode else ""

    latitude = None
    longitude = None

    if norm_pc:
        lat, lon = geocode_postcode_cached(norm_pc)
        if lat is not None and lon is not None:
            latitude, longitude = lat, lon

    # You can add snap-to-postcode logic here if you want to infer from coords
    # when postcode is missing/bad. For bulk imports we typically set
    # enable_snap_to_postcode=False to avoid hammering APIs.
    if enable_snap_to_postcode:
        # Placeholder for any future snap logic; currently no-op.
        pass

    # --- Job role + group ---
    raw_job_role = (posting.title or "").strip()
    job_role = _truncate(raw_job_role, MAX_JOB_ROLE_LEN)

    raw_group = getattr(posting, "job_role_group", None)
    job_role_group = (raw_group or "").strip() or None

    # --- Sector ---
    raw_sector = getattr(posting, "sector", None)
    sector = _truncate((raw_sector or "").strip() or None, MAX_SECTOR_LEN)

    # --- County ---
    raw_county = getattr(posting, "county", None)
    county = _truncate((raw_county or "").strip() or None, MAX_COUNTY_LEN)

    # --- External URL (defensive) ---
    external_url = getattr(posting, "url", None)
    external_url = _truncate(external_url, MAX_URL_LEN) if external_url else None

    record = JobRecord(
        company_id=company_id,
        company_name=company_name or None,
        sector=sector,
        job_role=job_role,
        job_role_group=job_role_group,
        postcode=norm_pc or None,
        county=county,
        pay_rate=pay_rate,
        imported_month=imported_month,
        imported_year=imported_year,
        latitude=latitude,
        longitude=longitude,
        created_at=imported_at,
        imported_from_posting_id=posting.id,
        imported_at=imported_at,
        external_url=external_url,
        logo_filename=None,
    )

    # Mark the posting as imported if the column exists
    if hasattr(posting, "imported"):
        posting.imported = True

    db.session.add(record)
    return record
