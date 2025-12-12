# app/importers/job_importer.py
from __future__ import annotations

from datetime import datetime

from extensions import db
from models import JobRecord, JobPosting, SectorMapping
from app.blueprints.utils import (
    get_or_create_company_id,
    normalize_uk_postcode,
    geocode_postcode_cached,
)

# Match DB column limits in your models.py
MAX_COMPANY_NAME_LEN = 100     # JobRecord.company_name
MAX_SECTOR_LEN = 50            # JobRecord.sector
MAX_COUNTY_LEN = 50            # JobRecord.county
MAX_JOB_ROLE_LEN = 100         # JobRecord.job_role
MAX_JOB_ROLE_GROUP_LEN = 120   # JobRecord.job_role_group
MAX_POSTCODE_LEN = 20          # JobRecord.postcode


def _truncate(value: str | None, max_len: int) -> str | None:
    """Safely truncate a string to max_len characters, preserving None."""
    if value is None:
        return None
    s = str(value)
    if len(s) <= max_len:
        return s
    return s[:max_len]


def classify_sector(
    job_title: str | None = None,
    company_name: str | None = None,
    **kwargs,
) -> str | None:
    """
    Very simple heuristic sector classifier.

    Kept primarily for backwards compatibility with code that imports
    `classify_sector` (e.g. cron_runner).

    Returns a short sector label (e.g. "Nursing", "Social Care", "IT & Technology")
    or None if nothing matches.
    """
    # Accept common kwarg variants from older calls
    if job_title is None:
        job_title = kwargs.get("job_role") or kwargs.get("title") or kwargs.get("role")

    if not job_title:
        return None

    title = job_title.lower()

    # Light-touch keyword rules (tweak freely later)
    rules: list[tuple[str, list[str]]] = [
        ("Nursing", ["nurse", "rgn", "rmn", "rnl", "registered nurse"]),
        ("Social Care", ["support worker", "care assistant", "care worker", "carer", "senior carer", "hca", "healthcare assistant"]),
        ("HR / People", ["hr", "human resources", "recruitment", "talent acquisition", "people advisor"]),
        ("Finance & Accounting", ["accountant", "finance", "bookkeeper", "payroll", "financial analyst"]),
        ("IT & Technology", ["software", "developer", "engineer", "devops", "data analyst", "data engineer", "it support", "security", "cloud"]),
        ("Admin & Office", ["administrator", "admin", "office manager", "receptionist", "coordinator", "secretary"]),
        ("Customer Service", ["customer service", "call centre", "contact centre", "customer advisor"]),
        ("Sales & Marketing", ["sales", "business development", "marketing", "seo", "account manager"]),
        ("Education & Training", ["teacher", "lecturer", "trainer", "tutor", "instructor"]),
        ("Legal", ["solicitor", "paralegal", "legal assistant", "legal secretary"]),
        ("Retail", ["retail", "store", "shop", "sales assistant", "cashier", "merchandiser"]),
        ("Hospitality", ["hotel", "chef", "cook", "waiter", "waitress", "bar", "restaurant"]),
        ("Construction & Trades", ["electrician", "plumber", "carpenter", "site manager", "bricklayer", "labourer"]),
        ("Logistics & Driving", ["driver", "delivery", "warehouse", "forklift", "logistics", "hgv"]),
        ("Cleaning & Domestic", ["cleaner", "domestic", "housekeeper", "porter"]),
    ]

    for sector_name, keywords in rules:
        for kw in keywords:
            if kw in title:
                return sector_name

    return None


def normalise_sector(value: str | None) -> str:
    """
    Map messy/raw sector strings to canonical sector labels via SectorMapping.
    If no mapping exists, return "Other" (stable bucket).
    """
    if not value:
        return "Other"

    key = value.strip()
    if not key:
        return "Other"

    # stored as case-insensitive via uppercase key
    m = SectorMapping.query.filter_by(raw_value=key.upper()).first()
    if m and m.canonical_sector:
        return m.canonical_sector.strip() or "Other"

    return "Other"


def import_posting_to_record(
    posting: JobPosting,
    enable_snap_to_postcode: bool = True,
) -> JobRecord:
    """
    Import a JobPosting into JobRecord.

    Key behaviour:
    - Company ID is derived from company_name
    - Postcode is normalised & geocoded (cached)
    - JobRecord.sector is ALWAYS canonical via SectorMapping
    - Strings are truncated to match JobRecord column sizes
    """

    # --- Company ---
    company_name_raw = (posting.company_name or "").strip()
    company_name = _truncate(company_name_raw or None, MAX_COMPANY_NAME_LEN)

    company_id = None
    if company_name:
        company_id = get_or_create_company_id(company_name)

    # --- Pay + timestamps ---
    pay_rate = posting.min_rate or posting.max_rate
    imported_at = datetime.utcnow()
    imported_month = imported_at.strftime("%B")
    imported_year = imported_at.strftime("%Y")

    # --- Postcode normalisation / geocoding ---
    raw_postcode = (posting.postcode or "").strip()
    norm_pc = normalize_uk_postcode(raw_postcode) if raw_postcode else ""
    norm_pc = _truncate(norm_pc or None, MAX_POSTCODE_LEN)

    latitude = None
    longitude = None
    if norm_pc:
        lat, lon = geocode_postcode_cached(norm_pc)
        if lat is not None and lon is not None:
            latitude, longitude = lat, lon

    if enable_snap_to_postcode:
        # placeholder: no-op for now
        pass

    # --- Job role + group ---
    raw_job_role = (posting.title or "").strip()
    job_role = _truncate(raw_job_role or None, MAX_JOB_ROLE_LEN)

    raw_group = getattr(posting, "job_role_group", None)
    job_role_group = (raw_group or "").strip() or None
    job_role_group = _truncate(job_role_group, MAX_JOB_ROLE_GROUP_LEN)

    # --- Sector (canonical) ---
    raw_sector = getattr(posting, "sector", None)
    raw_sector = (raw_sector or "").strip() or None
    if not raw_sector:
        raw_sector = classify_sector(job_title=raw_job_role, company_name=company_name_raw)

    canonical_sector = normalise_sector(raw_sector)
    canonical_sector = _truncate(canonical_sector, MAX_SECTOR_LEN)

    # --- County ---
    raw_county = getattr(posting, "county", None)
    raw_county = (raw_county or "").strip() or None
    county = _truncate(raw_county, MAX_COUNTY_LEN)

    # --- External URL (JobRecord.external_url is Text, no need to truncate) ---
    external_url = getattr(posting, "url", None)

    record = JobRecord(
        company_id=company_id,
        company_name=company_name,
        sector=canonical_sector,
        job_role=job_role,
        job_role_group=job_role_group or job_role or None,
        postcode=norm_pc,
        county=county,
        pay_rate=float(pay_rate) if pay_rate is not None else None,
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

    # Mark posting imported if the column exists
    if hasattr(posting, "imported"):
        posting.imported = True

    db.session.add(record)
    return record
