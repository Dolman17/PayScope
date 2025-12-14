# normalise_historic_sectors_safe.py
from __future__ import annotations

from app import create_app
from extensions import db
from models import JobRecord, SectorMapping

BATCH_SIZE = 2000


def normalise_sector(raw: str | None) -> str | None:
    """
    Only normalise if we have an explicit mapping.
    Otherwise return the original value unchanged.
    """
    if raw is None:
        return None

    key = raw.strip()
    if not key:
        return None

    m = SectorMapping.query.filter_by(raw_value=key.upper()).first()
    if m and m.canonical_sector:
        canon = (m.canonical_sector or "").strip()
        return canon or raw

    # IMPORTANT: don't overwrite unknowns
    return raw


def run():
    app = create_app()
    with app.app_context():
        total = JobRecord.query.count()
        print(f"Normalising sectors for {total} JobRecord rows…")

        offset = 0
        updated = 0

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

            batch_updates = 0
            for r in rows:
                canon = normalise_sector(r.sector)
                if canon != r.sector:
                    r.sector = canon
                    updated += 1
                    batch_updates += 1

            db.session.commit()
            offset += BATCH_SIZE
            print(f"Processed {min(offset, total)}/{total} — updated this batch: {batch_updates} (total updated: {updated})")

        print(f"✅ Sector normalisation complete. Updated {updated} rows.")


if __name__ == "__main__":
    run()
