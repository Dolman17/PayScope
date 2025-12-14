# seed_sector_mappings.py
from __future__ import annotations

import os
from datetime import datetime

from app import create_app
from extensions import db
from models import SectorMapping

# Canonical sectors you actually want
CANON = {
    "Social Care": [
        "SOCIAL CARE", "CARE", "SUPPORT", "SUPPORT WORKER", "SENIOR CARE",
        "CARE ASSISTANT", "CARE WORKER", "CARER", "HEALTHCARE ASSISTANT",
        "HCA", "RESIDENTIAL", "SUPPORTED LIVING",
    ],
    "Nursing": [
        "NURSING", "NURSING & CLINICAL", "NURSE", "REGISTERED NURSE", "RGN", "RMN", "RNLD",
        "CLINICAL",
    ],
    "IT & Technology": [
        "IT & TECHNOLOGY", "IT & DIGITAL", "IT DEVELOPER", "PYTHON", "WEB DEVELOPER", "IT ENGINEER",
        "SOFTWARE", "DEVELOPER", "ENGINEER", "DATA",
    ],
    "Admin & Office": [
        "ADMIN & OFFICE", "OFFICE / ADMIN", "OFFICE", "ADMIN", "ADMINISTRATOR", "RECEPTIONIST",
        "COORDINATOR",
    ],
    "HR / People": [
        "HR / PEOPLE", "HR & RECRUITMENT", "HR", "RECRUITMENT", "PEOPLE",
    ],
    "Finance & Accounting": [
        "FINANCE & ACCOUNTING", "FINANCE", "ACCOUNTING", "ACCOUNTANT", "BOOKKEEPER", "PAYROLL",
    ],
    "Customer Service": [
        "CUSTOMER SERVICE", "CUSTOMER", "CALL CENTRE", "CONTACT CENTRE",
    ],
    "Sales & Marketing": [
        "SALES & MARKETING", "SALES", "MARKETING", "BUSINESS DEVELOPMENT",
    ],
    "Education & Training": [
        "EDUCATION & TRAINING", "EDUCATION", "TRAINER", "TRAINING", "TEACHER", "TUTOR",
    ],
    "Legal": [
        "LEGAL", "SOLICITOR", "PARALEGAL",
    ],
    "Retail": [
        "RETAIL", "STORE", "SHOP", "SALES ASSISTANT (RETAIL)", "RETAIL ASSISTANT",
    ],
    "Leadership & Management": [
        "LEADERSHIP & MANAGEMENT", "LEADERSHIP & OPERATIONS", "LEADERSHIP",
        "MANAGEMENT", "OPERATIONS & MANAGEMENT", "OPERATIONS",
        "MANAGER", "DIRECTOR", "HEAD OF", "OPERATIONS MANAGER",
    ],
    "Other": [
        "OTHER", "UNKNOWN", "BLUE RIBBON",
    ],
}

def _upsert(raw_value: str, canonical_sector: str) -> bool:
    """
    Insert if missing; update if canonical changed.
    Returns True if inserted/updated.
    """
    raw_key = raw_value.strip().upper()
    if not raw_key:
        return False

    row = SectorMapping.query.filter_by(raw_value=raw_key).first()
    if row:
        if row.canonical_sector != canonical_sector:
            row.canonical_sector = canonical_sector
            row.updated_at = datetime.utcnow()
            db.session.add(row)
            return True
        return False

    row = SectorMapping(
        raw_value=raw_key,
        canonical_sector=canonical_sector,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )
    db.session.add(row)
    return True

def run():
    app = create_app()
    with app.app_context():
        # Prove which DB we're seeding
        try:
            db_url = app.config.get("SQLALCHEMY_DATABASE_URI") or os.getenv("SQLALCHEMY_DATABASE_URI")
        except Exception:
            db_url = os.getenv("SQLALCHEMY_DATABASE_URI")
        safe_url = (db_url or "").split("@")[-1] if db_url else "UNKNOWN"
        print(f"[seed_sector_mappings] DB = {safe_url}")

        # Ensure table exists
        exists = db.session.execute(db.text("SELECT to_regclass('public.sector_mappings')")).scalar()
        print(f"[seed_sector_mappings] sector_mappings table = {exists}")
        if not exists:
            raise RuntimeError("sector_mappings table does not exist in this DB. Run migrations / db.create_all first.")

        before = SectorMapping.query.count()
        print(f"[seed_sector_mappings] Before count = {before}")

        changed = 0
        for canonical, raw_list in CANON.items():
            for raw in raw_list:
                if _upsert(raw, canonical):
                    changed += 1

        db.session.commit()

        after = SectorMapping.query.count()
        print(f"[seed_sector_mappings] Changed (insert/update) = {changed}")
        print(f"[seed_sector_mappings] After count = {after}")

if __name__ == "__main__":
    run()
