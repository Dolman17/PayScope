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

# Match DB column limits (models.py)
MAX_COMPANY_NAME_LEN = 100
MAX_SECTOR_LEN = 50
MAX_COUNTY_LEN = 50
MAX_JOB_ROLE_LEN = 100
MAX_JOB_ROLE_GROUP_LEN = 120


def _truncate(value: str | None, max_len: int) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if len(s) <= max_len:
        return s
    return s[:max_len]


def _normalise_sector(raw: str | None) -> str:
    """
    Normalise raw sector text into your canonical sectors using SectorMapping.

    Rules:
    - If mapped -> canonical_sector
    - Else -> 'Other'
    """
    if not raw:
        return "Other"
    key = raw.strip()
    if not key:
        return "Other"

    m = SectorMapping.query.filter_by(raw_value=key.upper()).first()
    if m and m.canonical_sector:
        canon = (m.canonical_sector or "").strip()
        return canon or "Other"

    return "Other"


def classify_sector(job_title: str | None = None, company_name: str | None = None, **kwargs) -> str | None:
    """
    Lightweight fallback classifier when no sector is supplied.
    This is *only* used if posting.sector is missing.
    """
    if job_title is None:
        job_title = kwargs.get("job_role") or kwargs.get("title") or kwargs.get("role")

    if not job_title:
        return None

    title = job_title.lower()

    rules: list[tuple[str, list[str]]] = [
        ("Nursing", ["nurse", "rgn", "rmn", "rnl"]),
        ("Social Care", ["support worker", "care assistant", "care worker", "carer", "hca", "health care assistant"]),
        ("Admin & Office", ["admin", "administrator", "receptionist", "coordinator", "assistant"]),
        ("Customer Service", ["customer service", "call centre", "contact centre", "advisor"]),
        ("HR / People", ["hr ", "human resources", "talent", "recruitment"]),
        ("Finance & Accounting", ["accountant", "finance", "payroll", "bookkeeper"]),
        ("IT & Technology", ["developer", "engineer", "devops", "data analyst", "software", "it support"]),
        ("Legal", ["solicitor", "paralegal", "legal"]),
        ("Education & Training", ["trainer", "tutor", "lecturer", "teacher"]),
        ("Sales & Marketing", ["sales", "business development", "marketing"]),
        ("Domestic", ["domestic", "cleaner", "housekeeper", "kitchen", "chef", "cook"]),
        ("Operations and Logistics", ["warehouse", "logistics", "driver", "supply chain", "operations"]),
        ("Leadership & Management", ["director", "head of", "manager", "lead", "chief"]),
    ]

    for sector_name, keywords in rules:
        for kw in keywords:
            if kw in title:
                return sector_name

    return None


def import_posting_to_record(
    posting: JobPosting,
    enable_snap_to_postcode: bool = True,
) -> JobRecord:
    """
    Import a JobPosting row into JobRecord.

    This is the SINGLE place where we enforce:
    - Canonical sector mapping (SectorMapping)
    - Truncation to DB limits
    - Optional fallback sector classification if posting.sector is missing
    """

    # --- Company ID + name ---
    company_name_raw = (posting.company_name or "").strip()
    company_name = _truncate(company_name_raw, MAX_COMPANY_NAME_LEN)

    company_id = None
    if company_name:
        company_id = get_or_create_company_id(company_name)

    # --- Pay ---
    pay_rate = posting.min_rate or posting.max_rate

    # --- Dates ---
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

    if enable_snap_to_postcode:
        # placeholder for future snap logic
        pass

    # --- Job role + group ---
    raw_job_role = (posting.title or "").strip()
    job_role = _truncate(raw_job_role, MAX_JOB_ROLE_LEN)

    raw_group = getattr(posting, "job_role_group", None)
    job_role_group = _truncate((raw_group or "").strip() or None, MAX_JOB_ROLE_GROUP_LEN)

    # --- Sector ---
    raw_sector = getattr(posting, "sector", None)
    sector_value = (raw_sector or "").strip() or None

    # If the scraper didn't provide a sector, try to infer one from title
    if not sector_value:
        sector_value = classify_sector(job_title=raw_job_role, company_name=company_name_raw)

    # Now enforce canonical mapping (always)
    sector = _normalise_sector(sector_value)
    sector = _truncate(sector, MAX_SECTOR_LEN) or "Other"

    # --- County ---
    raw_county = getattr(posting, "county", None)
    county = _truncate((raw_county or "").strip() or None, MAX_COUNTY_LEN)

    record = JobRecord(
        company_id=company_id,
        company_name=company_name,
        sector=sector,
        job_role=job_role,
        job_role_group=job_role_group,
        postcode=norm_pc or None,
        county=county,
        pay_rate=float(pay_rate) if pay_rate is not None else None,
        imported_month=imported_month,
        imported_year=imported_year,
        latitude=latitude,
        longitude=longitude,
        created_at=imported_at,
        imported_from_posting_id=posting.id,
        imported_at=imported_at,
        external_url=getattr(posting, "url", None),
        logo_filename=None,
    )

    # Mark posting imported
    if hasattr(posting, "imported"):
        posting.imported = True

    db.session.add(record)
    return record
