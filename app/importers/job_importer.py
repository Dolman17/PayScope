# app/importers/job_importer.py
from app import db
from datetime import datetime
from models import JobPosting, JobRecord
import re
import json

from app.blueprints.utils import (
    normalize_uk_postcode,
    snap_to_nearest_postcode,
    get_or_create_company_id,
)


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


def _coords_from_raw_json(raw_json: str | None) -> tuple[float | None, float | None]:
    """
    Try to extract latitude/longitude from the job posting raw JSON.

    For Adzuna, we store:
      - "_latitude", "_longitude" in raw_json
      - plus original "latitude"/"longitude" if present
    """
    if not raw_json:
        return (None, None)

    try:
        data = json.loads(raw_json)
    except Exception as e:
        print(f"⚠️ Could not parse posting.raw_json for coords: {e}")
        return (None, None)

    lat = data.get("_latitude") or data.get("latitude") or data.get("lat")
    lon = data.get("_longitude") or data.get("longitude") or data.get("lon")

    try:
        lat = float(lat) if lat is not None else None
    except (TypeError, ValueError):
        lat = None

    try:
        lon = float(lon) if lon is not None else None
    except (TypeError, ValueError):
        lon = None

    return (lat, lon)


def import_posting_to_record(posting: JobPosting) -> JobRecord:
    """
    Create a new JobRecord from a JobPosting.
    Maps all real JobRecord fields.

    - Derives hourly pay rate
    - Extracts county from location text
    - Normalises postcode
    - Pulls lat/lon from raw_json where available
    - If postcode missing but coords exist, snaps to nearest postcode
    - Assigns a stable company_id based on grouped company name
    """

    pay_rate = _derive_pay_rate(posting)
    county = _extract_county(posting.location_text)

    imported_month = posting.scraped_at.strftime("%B")
    imported_year = posting.scraped_at.strftime("%Y")

    # Postcode and coordinates
    raw_pc = posting.postcode or ""
    postcode = normalize_uk_postcode(raw_pc)

    lat, lon = _coords_from_raw_json(posting.raw_json)

    if (not postcode) and (lat is not None and lon is not None):
        inferred_pc, snapped_lat, snapped_lon = snap_to_nearest_postcode(lat, lon)
        if inferred_pc:
            postcode = inferred_pc
            lat = snapped_lat
            lon = snapped_lon

    # Stable grouped company_id from company_name
    company_id = get_or_create_company_id(posting.company_name)

    record = JobRecord(
        company_id=company_id,
        company_name=posting.company_name,
        sector=posting.search_role,      # still using search_role as sector for now
        job_role=posting.title,
        postcode=postcode,
        county=county,
        pay_rate=pay_rate,
        imported_month=imported_month,
        imported_year=imported_year,
        latitude=lat,
        longitude=lon,
        imported_from_posting_id=posting.id,
        imported_at=datetime.utcnow(),
        external_url=posting.url,
    )

    db.session.add(record)
    posting.imported = True
    return record
