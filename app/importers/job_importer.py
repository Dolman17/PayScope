# app/importers/job_importer.py
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict

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


# -------------------------------------------------------------------
# Location helpers (derive from raw_json)
# -------------------------------------------------------------------
_COUNTRY_TOKENS = {
    "uk",
    "united kingdom",
    "great britain",
    "england",
    "scotland",
    "wales",
    "northern ireland",
}


def _safe_load_raw_json(raw: Any) -> Dict[str, Any]:
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return data
        except Exception:
            return {}
    return {}


def _derive_location_from_raw_json(posting: JobPosting) -> Dict[str, Any]:
    """
    Try to derive postcode / county / lat / lon from the scraper payload.

    - Adzuna: uses location.area + latitude/longitude
    - Reed: uses locationName + latitude/longitude
    """
    data = _safe_load_raw_json(getattr(posting, "raw_json", None))
    if not data:
        return {}

    source = (getattr(posting, "source_site", "") or "").lower()
    out: Dict[str, Any] = {}

    # Generic postcode key if present anywhere
    pc = data.get("postcode") or data.get("post_code")
    if isinstance(pc, str) and pc.strip():
        out["postcode"] = pc.strip()

    if source == "adzuna":
        loc = data.get("location") or {}
        area = loc.get("area") or []
        if isinstance(area, list):
            parts = [str(p).strip() for p in area if p]
            # Strip out country-level tokens
            non_country = [p for p in parts if p.lower() not in _COUNTRY_TOKENS]
            # Heuristic: last = city, second-last = county/region
            if len(non_country) >= 2:
                out["county"] = non_country[-2]
        # lat/lon
        lat = data.get("latitude") or (loc.get("lat") if isinstance(loc, dict) else None)
        lon = data.get("longitude") or (loc.get("lon") if isinstance(loc, dict) else None)
        try:
            if lat is not None and lon is not None:
                out["latitude"] = float(lat)
                out["longitude"] = float(lon)
        except Exception:
            pass

    elif source == "reed":
        loc_name = (data.get("locationName") or data.get("location") or "").strip()
        if loc_name:
            parts = [p.strip() for p in loc_name.split(",") if p.strip()]
            # e.g. "Birmingham, West Midlands" -> county = "West Midlands"
            if len(parts) >= 2:
                out["county"] = parts[-1]
        lat = data.get("latitude")
        lon = data.get("longitude")
        try:
            if lat is not None and lon is not None:
                out["latitude"] = float(lat)
                out["longitude"] = float(lon)
        except Exception:
            pass

    return out


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
    - Location derivation from scrape payload (county / lat / lon / postcode)
    """

    # --- Company ID + name ---
    company_name_raw = (posting.company_name or "").strip()
    company_name = _truncate(company_name_raw, MAX_COMPANY_NAME_LEN)

    company_id = None
    if company_name:
        company_id = get_or_create_company_id(company_name)

    # --- Pay ---
    pay_rate = posting.min_rate or posting.max_rate

    # Additive safety: if posting fields are missing but raw_json has derived hourly values, use them
    if pay_rate is None:
        data = _safe_load_raw_json(getattr(posting, "raw_json", None))
        if isinstance(data, dict):
            # Prefer sane computed values if present
            if data.get("_hourly_is_sane") is True:
                pay_rate = data.get("_hourly_min") or data.get("_hourly_max")

    # --- Dates ---
    imported_at = datetime.utcnow()
    imported_month = imported_at.strftime("%B")
    imported_year = imported_at.strftime("%Y")

    # --- Location from raw_json (Adzuna/Reed specific hints) ---
    loc_info = _derive_location_from_raw_json(posting)

    # --- Postcode normalisation / geocoding ---
    raw_postcode = (posting.postcode or loc_info.get("postcode") or "").strip()
    norm_pc = normalize_uk_postcode(raw_postcode) if raw_postcode else ""

    # Prefer coords from the scrape payload if present
    latitude = loc_info.get("latitude")
    longitude = loc_info.get("longitude")

    # If we still don't have coords but we do have a normalised postcode, geocode it
    if norm_pc and (latitude is None or longitude is None):
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
    raw_county = getattr(posting, "county", None) or loc_info.get("county")
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
