# normalise_historic_sectors.py
from __future__ import annotations

from app import create_app
from extensions import db
from models import JobRecord, SectorMapping

BATCH_SIZE = 5000


def run():
    app = create_app()
    with app.app_context():
        # Load mappings ONCE (your old script re-queried per row = slow)
        mappings = {
            (m.raw_value or "").strip().upper(): (m.canonical_sector or "").strip()
            for m in SectorMapping.query.all()
            if m.raw_value and m.canonical_sector
        }
        print(f"Loaded {len(mappings)} sector mappings.")

        total = db.session.query(db.func.count(JobRecord.id)).scalar() or 0
        print(f"Normalising sectors for {total} JobRecord rows…")

        updated = 0
        last_id = 0

        while True:
            rows = (
                JobRecord.query
                .filter(JobRecord.id > last_id)
                .order_by(JobRecord.id.asc())
                .limit(BATCH_SIZE)
                .all()
            )
            if not rows:
                break

            for r in rows:
                raw = (r.sector or "").strip()
                key = raw.upper()

                # Only change when we have a mapping
                canonical = mappings.get(key)
                if canonical and r.sector != canonical:
                    r.sector = canonical
                    updated += 1

                last_id = r.id

            db.session.commit()
            print(f"Processed up to id={last_id} / updated={updated}")

        print(f"✅ Sector normalisation complete. Updated {updated} rows.")


if __name__ == "__main__":
    run()
