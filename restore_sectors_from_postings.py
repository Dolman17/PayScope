# restore_sectors_from_postings.py
from __future__ import annotations

from app import create_app
from extensions import db
from models import JobRecord, JobPosting

BATCH = 5000

def run():
    app = create_app()
    with app.app_context():
        total = JobRecord.query.count()
        print(f"Restoring JobRecord.sector from JobPosting.sector for {total} rows...")

        # How many JobRecords have a linked posting?
        linked = (
            db.session.query(db.func.count(JobRecord.id))
            .filter(JobRecord.imported_from_posting_id.isnot(None))
            .scalar()
        )
        print(f"Linked to postings: {linked}")

        updated = 0
        offset = 0

        while True:
            # Pull just ids to keep memory down
            ids = (
                db.session.query(JobRecord.id)
                .order_by(JobRecord.id)
                .offset(offset)
                .limit(BATCH)
                .all()
            )
            if not ids:
                break
            id_list = [x[0] for x in ids]

            # Update sectors where we can
            # Set JobRecord.sector = JobPosting.sector when posting has a sector value
            res = db.session.execute(
                db.text("""
                    UPDATE job_record jr
                    SET sector = jp.sector
                    FROM job_postings jp
                    WHERE jr.id = ANY(:ids)
                      AND jr.imported_from_posting_id = jp.id
                      AND jp.sector IS NOT NULL
                      AND jp.sector <> ''
                """),
                {"ids": id_list},
            )
            # rowcount can be -1 depending on driver; we still commit and count separately
            db.session.commit()

            offset += BATCH
            print(f"Processed {min(offset, total)}/{total}")

        # Count how many are no longer 'Other'
        not_other = (
            db.session.query(db.func.count(JobRecord.id))
            .filter(JobRecord.sector.isnot(None))
            .filter(JobRecord.sector != "Other")
            .scalar()
        )
        print(f"✅ Restore pass complete. Records with sector != 'Other': {not_other}")

if __name__ == "__main__":
    run()
