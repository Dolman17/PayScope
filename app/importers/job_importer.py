# app/importers/job_importer.py
from datetime import datetime
import re
import json

from app import db
from models import JobPosting, JobRecord

from app.blueprints.utils import (
    normalize_uk_postcode,
    snap_to_nearest_postcode,
    get_or_create_company_id,
)


# -------------------------------------------------------------------
# Sector classification
# -------------------------------------------------------------------
def classify_sector(job_title: str | None, search_role: str | None = None) -> str:
    """
    Map a free-text job title (and optional search_role) into a high-level sector.

    This is intentionally simple, keyword-based logic so it's:
      - transparent
      - easy to tweak later
    """
    parts = []
    if job_title:
        parts.append(job_title)
    if search_role and search_role.lower() not in (job_title or "").lower():
        parts.append(search_role)

    text = " ".join(parts).lower()

    # Social care first (very important precedence)
    if any(k in text for k in [
        "support worker",
        "care assistant",
        "senior care assistant",
        "senior carer",
        "care worker",
        "care worker",
        "caregiver",
        "carer",
        "domiciliary",
        "home care",
        "live-in carer",
    ]):
        return "Social Care"

    # Nursing
    if any(k in text for k in [
        "nurse",
        "rgn",
        "rmn",
        "rldn",
        "staff nurse",
        "registered nurse",
    ]):
        return "Nursing"

    # HR / People
    if "human resources" in text or "hr " in f"{text} " or " hr" in f" {text}":
        return "HR / People"
    if any(k in text for k in ["recruitment", "talent acquisition", "resourcing"]):
        return "HR / People"

    # Finance / Accounting
    if any(k in text for k in [
        "accountant",
        "accounts assistant",
        "finance assistant",
        "finance manager",
        "financial controller",
        "payroll",
        "bookkeeper",
        "book-keeper",
        "credit control",
    ]):
        return "Finance & Accounting"

    # Admin & Office
    if any(k in text for k in [
        "administrator",
        "admin assistant",
        "office admin",
        "office assistant",
        "office manager",
        "receptionist",
        "secretary",
        "personal assistant",
        "pa ",
    ]):
        return "Admin & Office"

    # Operations / Service Mgmt
    if any(k in text for k in [
        "operations manager",
        "operations director",
        "service manager",
        "registered manager",
        "branch manager",
        "area manager",
    ]):
        return "Operations & Management"

    # IT & Tech
    if any(k in text for k in [
        "developer",
        "engineer",
        "software",
        "devops",
        "data analyst",
        "data engineer",
        "business analyst",
        "it support",
        "1st line support",
        "2nd line support",
        "helpdesk",
        "service desk",
    ]):
        return "IT & Technology"

    # Customer Service / Contact centre
    if any(k in text for k in [
        "customer service",
        "call centre",
        "call center",
        "contact centre",
        "call handler",
    ]):
        return "Customer Service"

    # Generic leadership (only if nothing else hit)
    if any(k in text for k in [
        "manager",
        "team leader",
        "head of",
        "director",
        "lead ",
    ]):
        return "Leadership & Management"

    return "Other"


# -------------------------------------------------------------------
# Helper functions for import
# -------------------------------------------------------------------
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
    """Extract county from location_text using simple comma/dash splitting."""
    if not location_text:
        return None

    parts = re.split(r"[,\-]", location_text)
    parts = [p.strip() for p in parts if p.strip()]

    if len(parts) >= 2:
        return parts[-1]

    return None


def _coords_from_raw_json(raw_json: str | None) -> tuple[float | None, float | None]:
    """
    Extract latitude/longitude from posting.raw_json.
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
    Safely import a JobPosting into a JobRecord.
    Handles null dates, postcode normalisation, coords, company mapping.
    """

    # Pay rate
    pay_rate = _derive_pay_rate(posting)

    # County
    county = _extract_county(posting.location_text)

    # FIXED: scraped_at may be None (cron creates JobPosting before commit)
    scraped = posting.scraped_at or datetime.utcnow()
    imported_month = scraped.strftime("%B")
    imported_year = scraped.strftime("%Y")

    # Postcode + coordinates
    postcode_raw = posting.postcode or ""
    postcode = normalize_uk_postcode(postcode_raw)

    lat, lon = _coords_from_raw_json(posting.raw_json)

    if (not postcode) and (lat is not None and lon is not None):
        inferred_pc, snapped_lat, snapped_lon = snap_to_nearest_postcode(lat, lon)
        if inferred_pc:
            postcode = inferred_pc
            lat = snapped_lat
            lon = snapped_lon

    # Company
    company_id = get_or_create_company_id(posting.company_name)

    # Sector: prefer the normalised sector on JobPosting,
    # fall back to search_role for older data / safety.
    sector_value = posting.sector or posting.search_role

    # Build JobRecord
    record = JobRecord(
        company_id=company_id,
        company_name=posting.company_name,
        sector=sector_value,
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
