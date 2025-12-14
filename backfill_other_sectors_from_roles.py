# backfill_other_sectors_from_roles.py
from __future__ import annotations

from app import create_app
from extensions import db
from models import JobRecord, SectorMapping

BATCH_SIZE = 1000

# Canonical sector labels (keep these stable)
SECTOR_OTHER = "Other"
SECTOR_SOCIAL_CARE = "Social Care"
SECTOR_NURSING = "Nursing"
SECTOR_HR = "HR / People"
SECTOR_IT = "IT & Technology"
SECTOR_FINANCE = "Finance & Accounting"
SECTOR_ADMIN = "Admin & Office"
SECTOR_CUSTOMER = "Customer Service"
SECTOR_SALES = "Sales & Marketing"
SECTOR_EDU = "Education & Training"
SECTOR_LEGAL = "Legal"
SECTOR_DOMESTIC = "Domestic"
SECTOR_LEADERSHIP = "Leadership & Management"
SECTOR_OPS_LOGISTICS = "Operations & Logistics"


def _norm(s: str | None) -> str:
    return (s or "").strip().lower()


def _map_via_sector_mappings(raw: str | None) -> str | None:
    if not raw:
        return None
    key = raw.strip()
    if not key:
        return None
    m = SectorMapping.query.filter_by(raw_value=key.upper()).first()
    if m and m.canonical_sector:
        v = (m.canonical_sector or "").strip()
        return v or None
    return None


def _infer_from_role(role_text: str) -> str | None:
    """
    Second-pass inference for anything still sitting in 'Other'/blank.
    Target the stubborn leftovers: ops/logistics, IT/product, legal, HR variants, social worker, etc.
    """
    t = role_text.lower()

    # --- Social care (social worker variants) ---
    if "social worker" in t:
        return SECTOR_SOCIAL_CARE

    # --- Operations & Logistics ---
    ops_words = [
        "warehouse", "operative", "driver", "7.5t", "c1", "hgv", "courier", "delivery",
        "logistics", "transport", "fleet", "stores", "stock", "picker", "picking",
        "pack", "packing", "dispatch", "forklift", "flt",
        "front of house", "front-of-house", "foh",
        "brand ambassador", "events", "venue", "steward",
        "facilities", "estates", "caretaker", "grounds", "gardener",
    ]
    if any(w in t for w in ops_words):
        return SECTOR_OPS_LOGISTICS

    # --- IT & Product ---
    it_words = [
        ".net", "dotnet", "developer", "software", "engineer", "devops",
        "infrastructure", "sysadmin", "cloud", "data engineer",
        "product owner", "product analyst", "business analyst",
    ]
    if any(w in t for w in it_words):
        return SECTOR_IT

    # --- Legal ---
    legal_words = [
        "solicitor", "litigation", "in-house counsel", "counsel",
        "intellectual property", "ip ", "paralegal", "legal",
    ]
    if any(w in t for w in legal_words):
        return SECTOR_LEGAL

    # --- HR / People (reward variants + HR) ---
    hr_words = [
        "hr ", "human resources", "people partner", "people advisor",
        "talent", "rewards", "total rewards", "compensation", "benefits", "payroll",
    ]
    if any(w in t for w in hr_words):
        return SECTOR_HR

    # --- Finance ---
    fin_words = ["finance", "accountant", "accounting", "financial", "fp&a", "bank reconciliation"]
    if any(w in t for w in fin_words):
        return SECTOR_FINANCE

    # --- Admin ---
    admin_words = ["administrator", "admin ", "receptionist", "personal assistant", "executive assistant", "data entry"]
    if any(w in t for w in admin_words):
        return SECTOR_ADMIN

    # --- Customer service ---
    cust_words = ["customer service", "call centre", "contact centre"]
    if any(w in t for w in cust_words):
        return SECTOR_CUSTOMER

    # --- Sales & Marketing ---
    sales_words = ["sales", "business development", "account director", "account handler", "marketing"]
    if any(w in t for w in sales_words):
        return SECTOR_SALES

    # --- Education ---
    edu_words = ["trainer", "tutor", "lecturer", "teacher", "education", "learning"]
    if any(w in t for w in edu_words):
        return SECTOR_EDU

    return None


def run():
    app = create_app()
    with app.app_context():
        total_other = (
            JobRecord.query
            .filter((JobRecord.sector == SECTOR_OTHER) | (JobRecord.sector.is_(None)) | (db.func.trim(JobRecord.sector) == ""))
            .count()
        )
        total_all = JobRecord.query.count()
        print(f"Backfilling sector for {total_other} JobRecord rows currently in '{SECTOR_OTHER}'/blank/NULL...")
        print(f"Total JobRecord rows: {total_all}")

        updated = 0
        offset = 0

        while True:
            rows = (
                JobRecord.query
                .filter((JobRecord.sector == SECTOR_OTHER) | (JobRecord.sector.is_(None)) | (db.func.trim(JobRecord.sector) == ""))
                .order_by(JobRecord.id)
                .offset(offset)
                .limit(BATCH_SIZE)
                .all()
            )
            if not rows:
                break

            for r in rows:
                # 1) Try SectorMapping based on current sector (usually "Other") -> will be None, but harmless
                mapped = _map_via_sector_mappings(r.sector)

                # 2) Infer from job role group/name
                role_text = (r.job_role_group or r.job_role or "").strip()
                inferred = _infer_from_role(role_text) if role_text else None

                canonical = mapped or inferred
                if canonical and r.sector != canonical:
                    r.sector = canonical
                    updated += 1

            db.session.commit()
            offset += BATCH_SIZE
            print(f"Processed {min(offset, total_other)}/{total_other} | updated={updated}")

        print(f"✅ Done. Updated {updated} rows from '{SECTOR_OTHER}'/blank/NULL.")


if __name__ == "__main__":
    run()
