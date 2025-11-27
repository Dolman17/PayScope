# backfill_sectors.py
from __future__ import annotations

from typing import Optional

from app import create_app
from extensions import db
from models import JobPosting, JobRecord


# ---------- Sector classifier ----------

def classify_sector(title: Optional[str]) -> str:
    """
    Map a raw job title into a high-level sector bucket.
    This DOES NOT touch the original title in the DB; it only returns a label.
    """
    if not title:
        return "Other"

    t = title.lower()

    # Social care / residential / support
    care_keywords = [
        "support worker",
        "care assistant",
        "care worker",
        "healthcare assistant",
        "health care assistant",
        "senior care assistant",
        "senior support worker",
        "residential support",
        "children's residential",
        "childrens residential",
        "learning disability",
        "learning disabilities",
        "supported living",
        "care home",
        "domiciliary",
        "home care",
    ]
    if any(k in t for k in care_keywords):
        return "Social Care"

    # Nursing / clinical
    nursing_keywords = [
        "nurse",
        "rgn",
        "rscn",
        "rmn",
        "rgn/rmn",
        "clinical lead",
        "registered nurse",
        "staff nurse",
    ]
    if any(k in t for k in nursing_keywords):
        return "Nursing & Clinical"

    # HR / Recruitment / People
    hr_keywords = [
        "hr ",
        " human resources",
        "people partner",
        "talent acquisition",
        "recruitment consultant",
        "recruitment manager",
        "resourcing",
    ]
    if any(k in t for k in hr_keywords):
        return "HR & Recruitment"

    # Finance / accounting / payroll
    fin_keywords = [
        "accountant",
        "accounts assistant",
        "finance assistant",
        "finance manager",
        "financial analyst",
        "bookkeeper",
        "payroll",
        "credit control",
    ]
    if any(k in t for k in fin_keywords):
        return "Finance & Accounting"

    # IT / digital / data
    it_keywords = [
        "software developer",
        "software engineer",
        "developer",
        "engineer",
        "data analyst",
        "business analyst",
        "bi analyst",
        "it support",
        "service desk",
        "devops",
        "systems administrator",
        "infrastructure",
    ]
    if any(k in t for k in it_keywords):
        return "IT & Digital"

    # Admin / office / PA
    admin_keywords = [
        "administrator",
        "receptionist",
        "office manager",
        "office administrator",
        "secretary",
        "pa to",
        "personal assistant",
        "team admin",
    ]
    if any(k in t for k in admin_keywords):
        return "Admin & Office"

    # Operations / leadership / management (generic)
    ops_keywords = [
        "operations manager",
        "operations director",
        "service manager",
        "registered manager",
        "home manager",
        "deputy manager",
        "team leader",
        "coordinator",
        "co-ordinator",
        "supervisor",
        "head of",
        "director",
        "managing director",
        "regional manager",
    ]
    if any(k in t for k in ops_keywords):
        return "Leadership & Operations"

    # Customer service / contact centre
    cs_keywords = [
        "customer service",
        "call centre",
        "contact centre",
        "customer advisor",
        "customer adviser",
    ]
    if any(k in t for k in cs_keywords):
        return "Customer Service"

    # Sales / marketing
    sales_keywords = [
        "sales ",
        "sales executive",
        "business development",
        "bdm",
        "account manager",
        "marketing",
        "brand manager",
    ]
    if any(k in t for k in sales_keywords):
        return "Sales & Marketing"

    # Education / training
    edu_keywords = [
        "teacher",
        "teaching assistant",
        "learning support assistant",
        "trainer",
        "training officer",
        "lecturer",
        "tutor",
    ]
    if any(k in t for k in edu_keywords):
        return "Education & Training"

    # Legal
    legal_keywords = [
        "solicitor",
        "paralegal",
        "legal assistant",
        "legal secretary",
    ]
    if any(k in t for k in legal_keywords):
        return "Legal"

    return "Other"


# ---------- Backfill helpers (no yield_per, single commit) ----------

def backfill_postings():
    print("=== Backfilling JobPosting.sector ===")
    postings = JobPosting.query.order_by(JobPosting.id).all()
    total = len(postings)
    print(f"[Postings] Total rows: {total}")

    # Samples
    for p in postings[:5]:
        print(f"[Postings] Sample: id={p.id}, title={p.title!r}, sector={p.sector!r}")

    updated = 0
    for idx, posting in enumerate(postings, start=1):
        title = posting.title or posting.search_role or ""
        new_sector = classify_sector(title)
        if posting.sector != new_sector:
            posting.sector = new_sector
            updated += 1

        if idx % 500 == 0:
            print(f"[Postings] {idx}/{total} processed... (updated so far={updated})")

    print("[Postings] Committing updates...")
    db.session.commit()
    print(f"[Postings] DONE: {total} processed, {updated} updated.")


def backfill_job_records():
    print("=== Backfilling JobRecord.sector ===")
    records = JobRecord.query.order_by(JobRecord.id).all()
    total = len(records)
    print(f"[JobRecord] Total rows: {total}")

    for r in records[:5]:
        print(f"[JobRecord] Sample: id={r.id}, job_role={r.job_role!r}, sector={r.sector!r}")

    updated = 0
    for idx, rec in enumerate(records, start=1):
        # Prefer the original job_role; fall back to existing sector text if needed
        title = rec.job_role or rec.sector or ""
        new_sector = classify_sector(title)
        if rec.sector != new_sector:
            rec.sector = new_sector
            updated += 1

        if idx % 500 == 0:
            print(f"[JobRecord] {idx}/{total} processed... (updated so far={updated})")

    print("[JobRecord] Committing updates...")
    db.session.commit()
    print(f"[JobRecord] DONE: {total} processed, {updated} updated.")


# ---------- Entry point ----------

def main():
    app = create_app()
    with app.app_context():
        print("Starting sector backfill (safe for titles; only sector field is updated)...")
        backfill_postings()
        backfill_job_records()
        print("All done.")


if __name__ == "__main__":
    main()
