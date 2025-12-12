# recover_sectors_from_postings.py
from __future__ import annotations

from app import create_app
from extensions import db
from models import JobRecord, JobPosting, SectorMapping

# Import classify_sector from your importer (where you keep the heuristic)
from app.importers.job_importer import classify_sector

BATCH_SIZE = 1000


def _norm_key(s: str | None) -> str:
    return (s or "").strip()


def _is_otherish(s: str | None) -> bool:
    v = _norm_key(s).lower()
    return (not v) or v in {"other", "other / unknown", "unknown", "n/a"}


def load_sector_map() -> dict[str, str]:
    """
    Returns dict of RAW_VALUE_UPPER -> canonical_sector
    """
    rows = SectorMapping.query.all()
    m: dict[str, str] = {}
    for r in rows:
        raw = _norm_key(r.raw_value).upper()
        canon = _norm_key(r.canonical_sector)
        if raw and canon:
            m[raw] = canon
    print(f"Loaded {len(m)} sector mappings")
    return m


def canonicalise(raw_sector: str | None, sector_map: dict[str, str]) -> str | None:
    """
    Apply canonical sector mapping if known; otherwise return the raw sector as-is.
    (IMPORTANT: do NOT collapse unknowns to Other here.)
    """
    s = _norm_key(raw_sector)
    if not s:
        return None

    hit = sector_map.get(s.upper())
    return hit if hit else s


def run():
    app = create_app()

    with app.app_context():
        sector_map = load_sector_map()

        total = JobRecord.query.count()
        print(f"Recovering sectors for {total} JobRecord rows...")

        updated = 0
        processed = 0
        offset = 0

        while True:
            rows = (
                JobRecord.query
                .order_by(JobRecord.id)
                .offset(offset)
                .limit(BATCH_SIZE)
                .all()
            )
            if not rows:
                break

            # Collect posting ids we need
            posting_ids = [r.imported_from_posting_id for r in rows if r.imported_from_posting_id]
            postings_by_id = {}
            if posting_ids:
                postings = (
                    JobPosting.query
                    .filter(JobPosting.id.in_(posting_ids))
                    .all()
                )
                postings_by_id = {p.id: p for p in postings}

            for r in rows:
                processed += 1

                current = _norm_key(r.sector)

                # 1) Try JobPosting.sector
                posting_sector = None
                if r.imported_from_posting_id:
                    p = postings_by_id.get(r.imported_from_posting_id)
                    if p and p.sector:
                        posting_sector = _norm_key(p.sector)

                candidate = None

                # Prefer posting sector if it isn't empty/Other
                if posting_sector and not _is_otherish(posting_sector):
                    candidate = posting_sector

                # 2) If still nothing useful, infer from job_role/title
                if not candidate:
                    inferred = classify_sector(job_title=r.job_role, company_name=r.company_name)
                    if inferred and not _is_otherish(inferred):
                        candidate = inferred

                # 3) If current sector is meaningful (not Other), keep it
                if not candidate and current and not _is_otherish(current):
                    candidate = current

                # 4) If we still have nothing, leave it as-is (don’t keep hammering “Other”)
                if not candidate:
                    continue

                # Apply canonical mapping if available; otherwise keep candidate
                final_sector = canonicalise(candidate, sector_map)

                if final_sector and r.sector != final_sector:
                    r.sector = final_sector
                    updated += 1

            db.session.commit()
            offset += BATCH_SIZE

            print(f"Processed {min(offset, total)}/{total} | updated={updated}")

        print(f"✅ Done. Updated {updated} JobRecord rows.")


if __name__ == "__main__":
    run()
