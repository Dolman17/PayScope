# seed_sector_mappings.py (or put in an admin/seed route you already have)

from extensions import db
from models import SectorMapping

CANONICAL_SECTORS = [
    "Social Care",
    "Nursing & Clinical",
    "IT & Technology",
    "Finance & Accounting",
    "HR & Recruitment",
    "Admin & Office",
    "Operations & Management",
    "Customer Service",
    "Retail",
    "Sales & Marketing",
    "Education & Training",
    "Legal",
    "Other / Unknown",
]

ALIASES = {
    # IT
    "IT & DIGITAL": "IT & Technology",
    "PYTHON": "IT & Technology",
    "IT DEVELOPER": "IT & Technology",
    "WEB DEVELOPER": "IT & Technology",
    "IT ENGINEER": "IT & Technology",

    # Admin
    "OFFICE / ADMIN": "Admin & Office",
    "ADMIN & OFFICE": "Admin & Office",

    # Nursing
    "NURSING": "Nursing & Clinical",
    "NURSING & CLINICAL": "Nursing & Clinical",

    # HR
    "HR / PEOPLE": "HR & Recruitment",

    # Ops/Leadership
    "LEADERSHIP & MANAGEMENT": "Operations & Management",
    "LEADERSHIP & OPERATIONS": "Operations & Management",
    "MANAGEMENT": "Operations & Management",
    "OPERATIONS & MANAGEMENT": "Operations & Management",

    # Training
    "TRAINER": "Education & Training",
    "EDUCATION & TRAINING": "Education & Training",

    # Care fragments
    "CARE": "Social Care",
    "SENIOR CARE": "Social Care",
    "SUPPORT WORKER": "Social Care",

    # Misc
    "UNKNOWN": "Other / Unknown",
    "OTHER": "Other / Unknown",
    "BLUE RIBBON": "Social Care",
}

def seed_sector_mappings():
    for raw, canon in ALIASES.items():
        raw_clean = raw.strip()
        exists = SectorMapping.query.filter_by(raw_value=raw_clean).first()
        if not exists:
            db.session.add(SectorMapping(raw_value=raw_clean, canonical_sector=canon))
    db.session.commit()
