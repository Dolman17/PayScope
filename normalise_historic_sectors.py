# normalise_historic_sectors.py
from __future__ import annotations

from app import create_app
from extensions import db
from models import JobRecord, SectorMapping

BATCH_SIZE = 2000  # 1000–5000 is usually fine


def _clean_key(raw: str | None) -> str:
    if not raw:
        return ""
    return raw.strip().upper()


def run():
    app = create_app()
    with app.app_context():
        total = db.session.query(db.func.count(JobRecord.id)).scalar() or 0
        print(f"Normalising sectors for {total} JobRecord rows…")

        # Load all mappings once
        mapping_rows = SectorMapping.query.all()
        mappings = {
            (m.raw_value or "").strip().upper(): (m.canonical_sector or "").strip()
            for m in mapping_rows
            if m.raw_value
        }

        print(f"Loaded {len(mappings)} sector mappings.")

        updated = 0
        processed = 0
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
                last_id = r.id

                key = _clean_key(r.sector)
                canonical = mappings.get(key) if key else None
                canonical = canonical if canonical else "Other"

                if (r.sector or "").strip() != canonical:
                    r.sector = canonical
                    updated += 1

            db.session.commit()
            processed += len(rows)
            print(f"Processed {processed}/{total} (updated {updated})")

        print(f"✅ Sector normalisation complete. Updated {updated} rows.")


if __name__ == "__main__":
    run()
