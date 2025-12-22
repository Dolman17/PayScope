# historic_location_backfill.py
from __future__ import annotations

import os
from typing import Any, Dict

from sqlalchemy import func

from app import create_app
from extensions import db
from models import JobRecord, JobPosting, CronRunLog
from app.importers.job_importer import _derive_location_from_raw_json  # re-use helper
from app.blueprints.utils import normalize_uk_postcode, geocode_postcode_cached


BATCH_LIMIT = int(os.getenv("HIST_LOC_BACKFILL_LIMIT", "2000"))


def _utcnow():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).replace(tzinfo=None)


def run_historic_location_backfill(trigger: str = "manual") -> Dict[str, Any]:
    """
    One-off / occasional job to enrich historic JobRecord rows that have *no* location:

    - Only picks rows where:
        postcode IS NULL
        AND latitude IS NULL
        AND longitude IS NULL
        AND county IS NULL/blank
    - Uses JobPosting.raw_json via _derive_location_from_raw_json() to fill:
        postcode, county, latitude, longitude
    """
    app = create_app()
    with app.app_context():
        limit = BATCH_LIMIT

        log = CronRunLog(
            job_name="historic_location_backfill",
            started_at=_utcnow(),
            status="running",
            trigger=trigger,
        )
        db.session.add(log)
        db.session.commit()

        stats: Dict[str, Any] = {
            "limit": limit,
            "processed": 0,
            "updated": 0,
            "no_posting": 0,
            "no_location_from_posting": 0,
            "errors": [],
        }

        try:
            # Only jobs with *no* location at all
            needing = (
                JobRecord.query
                .filter(
                    (JobRecord.postcode.is_(None)) &
                    (JobRecord.latitude.is_(None)) &
                    (JobRecord.longitude.is_(None)) &
                    (
                        (JobRecord.county.is_(None)) |
                        (func.trim(JobRecord.county) == "")
                    )
                )
                .order_by(JobRecord.id.asc())
                .limit(limit)
                .all()
            )

            for idx, job in enumerate(needing, start=1):
                stats["processed"] += 1

                if not job.imported_from_posting_id:
                    stats["no_posting"] += 1
                    continue

                posting = JobPosting.query.get(job.imported_from_posting_id)
                if not posting:
                    stats["no_posting"] += 1
                    continue

                try:
                    loc = _derive_location_from_raw_json(posting) or {}
                except Exception as e:
                    stats["errors"].append(f"derive_loc job_id={job.id}: {e!r}")
                    continue

                if not loc:
                    stats["no_location_from_posting"] += 1
                    continue

                changed = False

                # Postcode → normalise & maybe geocode
                raw_pc = (loc.get("postcode") or "").strip()
                norm_pc = normalize_uk_postcode(raw_pc) if raw_pc else None

                lat = loc.get("latitude")
                lon = loc.get("longitude")

                if norm_pc:
                    job.postcode = norm_pc
                    changed = True

                    # If we still don't have coords from payload, try geocoding
                    if lat is None or lon is None:
                        try:
                            g_lat, g_lon = geocode_postcode_cached(norm_pc)
                            if g_lat is not None and g_lon is not None:
                                lat, lon = g_lat, g_lon
                        except Exception as e:
                            stats["errors"].append(f"geocode_pc job_id={job.id}: {e!r}")

                if lat is not None and lon is not None:
                    try:
                        job.latitude = float(lat)
                        job.longitude = float(lon)
                        changed = True
                    except Exception as e:
                        stats["errors"].append(f"set_coords job_id={job.id}: {e!r}")

                # County from payload
                raw_county = (loc.get("county") or "").strip()
                if raw_county:
                    job.county = raw_county[:50]
                    changed = True

                if changed:
                    stats["updated"] += 1

                if idx % 100 == 0:
                    db.session.flush()

            db.session.commit()

            msg = (
                f"Historic location backfill processed={stats['processed']}, "
                f"updated={stats['updated']}, "
                f"no_posting={stats['no_posting']}, "
                f"no_loc_from_posting={stats['no_location_from_posting']}"
            )
            log.finished_at = _utcnow()
            log.status = "success"
            log.message = msg
            log.run_stats = msg
            db.session.commit()

            return {"ok": True, "message": msg, "stats": stats}

        except Exception as e:
            db.session.rollback()
            log.finished_at = _utcnow()
            log.status = "error"
            log.message = str(e)
            db.session.commit()
            stats["errors"].append(repr(e))
            return {"ok": False, "error": str(e), "stats": stats}


if __name__ == "__main__":
    result = run_historic_location_backfill(trigger="manual-cli")
    print(result)
