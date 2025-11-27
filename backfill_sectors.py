# backfill_sectors.py
from app import create_app
from extensions import db
from models import JobPosting, JobRecord
from app.importers.job_importer import classify_sector

BATCH_COMMIT = 500  # commit every N updated rows


def backfill_postings():
    postings = JobPosting.query.all()
    total = len(postings)
    print(f"[Postings] Total rows: {total}")
    if total == 0:
        return

    updated = 0
    batch_updates = 0

    for i, posting in enumerate(postings, start=1):
        if not posting.sector:
            posting.sector = classify_sector(posting.title, posting.search_role)
            updated += 1
            batch_updates += 1

        if i <= 5:
            print(f"[Postings] Sample {i}: id={posting.id}, title={posting.title[:60]!r}")

        if i % 1000 == 0:
            print(f"[Postings] {i}/{total} processed...")

        # Commit every BATCH_COMMIT updated rows
        if batch_updates >= BATCH_COMMIT:
            print(f"[Postings] Committing batch (total updated so far={updated})...")
            db.session.commit()
            batch_updates = 0

    # Final commit for any remaining updates
    if batch_updates > 0:
        print(f"[Postings] Final commit for remaining {batch_updates} updates...")
        db.session.commit()

    print(f"[Postings] DONE: {total} processed, {updated} updated.")


def backfill_records():
    records = JobRecord.query.all()
    total = len(records)
    print(f"[JobRecord] Total rows: {total}")
    if total == 0:
        return

    updated = 0
    batch_updates = 0

    for i, record in enumerate(records, start=1):
        # If sector is missing or still equal to the raw job_role, recompute
        if not record.sector or record.sector == record.job_role:
            record.sector = classify_sector(record.job_role, None)
            updated += 1
            batch_updates += 1

        if i <= 5:
            print(f"[JobRecord] Sample {i}: id={record.id}, job_role={record.job_role[:60]!r}")

        if i % 1000 == 0:
            print(f"[JobRecord] {i}/{total} processed...")

        if batch_updates >= BATCH_COMMIT:
            print(f"[JobRecord] Committing batch (total updated so far={updated})...")
            db.session.commit()
            batch_updates = 0

    if batch_updates > 0:
        print(f"[JobRecord] Final commit for remaining {batch_updates} updates...")
        db.session.commit()

    print(f"[JobRecord] DONE: {total} processed, {updated} updated.")


def main():
    app = create_app()
    with app.app_context():
        print("Starting sector backfill...")

        try:
            backfill_postings()
            backfill_records()
            print("All done.")
        except Exception as exc:
            db.session.rollback()
            print("💥 Backfill failed, transaction rolled back.")
            print(repr(exc))


if __name__ == "__main__":
    main()
