from __future__ import annotations

from datetime import datetime, timedelta

from app import create_app
from app.scrapers.adzuna import AdzunaScraper
from app.job_importer import import_posting_to_record
from app.blueprints.admin import upsert_job_record  # reuse existing logic
from app.blueprints.utils import geocode_postcode_cached, snap_to_nearest_postcode

from extensions import db
from models import JobPosting, JobRecord, Company

# -------------------------------------------------------------------
# BASE CONFIG
# -------------------------------------------------------------------

# Default “social care” roles / locations (used as base + weekend fallback)
DEFAULT_ROLES = [
    "support worker",
    "care assistant",
    "senior care assistant",
    "team leader",
    "registered manager",
]

DEFAULT_LOCATIONS = [
    "United Kingdom",
    "London",
    "Birmingham",
    "Manchester",
    "Leeds",
]

MAX_JOB_AGE_DAYS = 45  # how long until a posting is marked inactive (NOT deleted)


# -------------------------------------------------------------------
# BASE CONFIG
# -------------------------------------------------------------------

# Core locations we care about (can be reused across days)
BASE_LOCATIONS = [
    "United Kingdom",
    "London",
    "Birmingham",
    "Manchester",
    "Leeds",
    "Glasgow",
    "Bristol",
]

# Default “health & social care” roles (weekend fallback)
DEFAULT_ROLES = [
    "support worker",
    "care assistant",
    "senior care assistant",
    "health care assistant",
    "support worker social care",
    "registered manager",
]

DEFAULT_LOCATIONS = BASE_LOCATIONS

MAX_JOB_AGE_DAYS = 45  # how long until a posting is marked inactive (NOT deleted)


# -------------------------------------------------------------------
# DAY-OF-WEEK CONFIG
# datetime.utcnow().weekday(): 0=Mon, 1=Tue, ..., 6=Sun
# Each day focuses on a major Adzuna vertical.
# -------------------------------------------------------------------

DAY_CONFIG = {
    0: {  # Monday – Health & Social Care
        "label": "Health & Social Care",
        "roles": [
            "support worker",
            "care assistant",
            "senior care assistant",
            "healthcare assistant",
            "mental health support worker",
            "registered manager",
            "domiciliary care worker",
        ],
        "locations": BASE_LOCATIONS,
    },
    1: {  # Tuesday – IT & Tech
        "label": "IT & Technology",
        "roles": [
            "software developer",
            "software engineer",
            "it support",
            "service desk analyst",
            "data analyst",
            "devops engineer",
            "systems administrator",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Manchester",
            "Leeds",
            "Birmingham",
            "Cambridge",
        ],
    },
    2: {  # Wednesday – Finance & Accounting
        "label": "Finance & Accounting",
        "roles": [
            "accounts assistant",
            "assistant accountant",
            "management accountant",
            "finance manager",
            "payroll clerk",
            "credit controller",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Birmingham",
            "Leeds",
            "Manchester",
            "Edinburgh",
        ],
    },
    3: {  # Thursday – Engineering & Construction
        "label": "Engineering & Construction",
        "roles": [
            "mechanical engineer",
            "electrical engineer",
            "maintenance engineer",
            "civil engineer",
            "project engineer",
            "site manager",
            "quantity surveyor",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Birmingham",
            "Manchester",
            "Leeds",
            "Newcastle upon Tyne",
        ],
    },
    4: {  # Friday – Office / HR / Admin
        "label": "Office, HR & Admin",
        "roles": [
            "hr advisor",
            "hr officer",
            "hr manager",
            "talent acquisition",
            "recruitment consultant",
            "office administrator",
            "personal assistant",
            "receptionist",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Manchester",
            "Birmingham",
            "Leeds",
            "Nottingham",
        ],
    },
    5: {  # Saturday – Logistics, Warehouse & Driving
        "label": "Logistics, Warehouse & Driving",
        "roles": [
            "warehouse operative",
            "forklift driver",
            "delivery driver",
            "hgv driver",
            "courier",
            "logistics coordinator",
        ],
        "locations": [
            "United Kingdom",
            "Birmingham",
            "Manchester",
            "Leeds",
            "Liverpool",
            "Milton Keynes",
        ],
    },
    6: {  # Sunday – Retail, Sales & Hospitality
        "label": "Retail, Sales & Hospitality",
        "roles": [
            "retail assistant",
            "store manager",
            "sales executive",
            "business development manager",
            "customer service advisor",
            "chef",
            "kitchen assistant",
            "bar staff",
        ],
        "locations": [
            "United Kingdom",
            "London",
            "Manchester",
            "Birmingham",
            "Leeds",
            "Glasgow",
        ],
    },
}

# -------------------------------------------------------------------
# Company normalisation / grouping
# -------------------------------------------------------------------

def normalise_company_name(raw: str | None) -> str | None:
    """Very simple canonicalisation for grouping similar names."""
    if not raw:
        return None
    s = raw.strip().lower()
    for suffix in (" ltd", " limited", " group", " care services", " care", " services"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    s = s.replace("&", "and")
    return " ".join(s.split()) or None


def get_or_create_company(name: str | None) -> Company | None:
    if not name:
        return None

    canonical = normalise_company_name(name)
    if not canonical:
        return None

    existing = Company.query.filter_by(canonical_name=canonical).first()
    if existing:
        # If the stored display name is empty, fill it
        if not existing.name:
            existing.name = name
        return existing

    c = Company(name=name, canonical_name=canonical)
    db.session.add(c)
    db.session.commit()
    return c


# -------------------------------------------------------------------
# Geocoding helpers (postcode + snap-to-nearest)
# -------------------------------------------------------------------

def geocode_and_snap(record: JobRecord) -> None:
    """
    Ensure JobRecord has lat/lon if possible, and snap postcode to the nearest
    known postcode when we have coordinates.

    NOTE: This never deletes or overwrites history, it just fills in blanks.
    """
    # First try postcode → lat/lon
    if record.postcode:
        lat, lon = geocode_postcode_cached(record.postcode)
        if lat is not None and lon is not None:
            record.latitude = lat
            record.longitude = lon
            return

    # If we somehow already have coords (future-proof), snap them to nearest postcode
    if record.latitude is not None and record.longitude is not None:
        pc, lat2, lon2 = snap_to_nearest_postcode(record.latitude, record.longitude)
        if pc and lat2 is not None and lon2 is not None:
            record.postcode = pc
            record.latitude = lat2
            record.longitude = lon2


# -------------------------------------------------------------------
# Duplicate cleanup (JobRecord only, not JobPosting)
# -------------------------------------------------------------------

def cleanup_duplicate_job_records() -> int:
    """
    Remove exact duplicate JobRecords:

      (company_name, job_role, postcode, pay_rate, imported_month, imported_year)

    This only dedupes JobRecord; JobPosting (the advert history) is untouched.
    """
    seen: set[tuple] = set()
    removed = 0

    # newest first so we keep the latest copy and drop earlier ones
    records = JobRecord.query.order_by(JobRecord.created_at.desc()).all()

    for r in records:
        key = (
            r.company_name,
            r.job_role,
            r.postcode,
            r.pay_rate,
            r.imported_month,
            r.imported_year,
        )
        if key in seen:
            db.session.delete(r)
            removed += 1
        else:
            seen.add(key)

    if removed:
        db.session.commit()
    return removed


# -------------------------------------------------------------------
# Expired posting handling (NO deletes)
# -------------------------------------------------------------------

def deactivate_old_postings() -> int:
    """
    Mark JobPosting.is_active = False when older than MAX_JOB_AGE_DAYS.

    This keeps rows in the DB for historic analysis; they just stop being 'current'.
    """
    cutoff_date = datetime.utcnow().date() - timedelta(days=MAX_JOB_AGE_DAYS)

    to_deactivate: list[JobPosting] = []

    for p in JobPosting.query.filter(JobPosting.is_active.is_(True)).all():
        basis = p.posted_date or (p.scraped_at.date() if p.scraped_at else None)
        if basis and basis < cutoff_date:
            to_deactivate.append(p)

    for p in to_deactivate:
        p.is_active = False

    if to_deactivate:
        db.session.commit()

    return len(to_deactivate)


# -------------------------------------------------------------------
# Main scheduled scrape (day-aware)
# -------------------------------------------------------------------

def run_scrape() -> None:
    app = create_app()
    with app.app_context():
        weekday = datetime.utcnow().weekday()  # 0 = Monday
        day_cfg = DAY_CONFIG.get(weekday)

        if day_cfg:
            roles = day_cfg["roles"]
            locations = day_cfg.get("locations", DEFAULT_LOCATIONS)
            label = day_cfg.get("label", f"weekday-{weekday}")
            print(f"📅 Day-based scrape: {label} (weekday={weekday})")
        else:
            roles = DEFAULT_ROLES
            locations = DEFAULT_LOCATIONS
            label = "Default (Social Care)"
            print(f"📅 No day-specific config for weekday={weekday}, using default roles.")

        print(f"   Roles: {roles}")
        print(f"   Locations: {locations}")

        total_scraped_postings = 0
        total_created_records = 0

        for role in roles:
            for loc in locations:
                print(f"→ Scraping role={role!r}, location={loc!r}")
                try:
                    scraper = AdzunaScraper(
                        what=role,
                        where=loc,
                        max_pages=2,
                        results_per_page=40,
                    )
                    results = scraper.scrape()

                    for rec in results:
                        # Upsert JobPosting so we don't create duplicates for same external_id/url
                        posting = upsert_job_record(
                            rec,
                            search_role=role,
                            search_location=loc,
                        )
                        total_scraped_postings += 1

                        # Only import to JobRecord once per posting
                        if not getattr(posting, "imported", False):
                            job_record = import_posting_to_record(posting)

                            # Auto company grouping
                            company = get_or_create_company(job_record.company_name)
                            if company is not None:
                                job_record.company_id = str(company.id)

                            # Geocode / snap to postcode
                            geocode_and_snap(job_record)

                            total_created_records += 1

                    db.session.commit()

                except Exception as e:  # noqa: BLE001
                    print(f"⚠ Error during scrape for {role} @ {loc}: {e}")

        # Post-scrape housekeeping
        dup_removed = cleanup_duplicate_job_records()
        deactivated = deactivate_old_postings()

        print(
            "✔ Finished scheduled Adzuna scrape.\n"
            f"   Theme: {label}\n"
            f"   JobPostings scraped/upserted: {total_scraped_postings}\n"
            f"   JobRecords created: {total_created_records}\n"
            f"   Duplicate JobRecords removed: {dup_removed}\n"
            f"   Old JobPostings deactivated (not deleted): {deactivated}"
        )


if __name__ == "__main__":
    run_scrape()
