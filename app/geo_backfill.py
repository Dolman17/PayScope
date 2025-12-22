# app/services/geo_backfill.py

from __future__ import annotations

from typing import Optional

from extensions import db
from models import JobRecord
from sqlalchemy import or_

from app.blueprints.utils import (
    geocode_postcode_cached,
    snap_to_nearest_postcode,
    normalize_uk_postcode,
    commit_or_rollback,
)


def regeocode_missing_jobs(limit: Optional[int] = None) -> dict:
    """
    Geocode JobRecord rows that *have* a postcode but are missing lat/lon.

    Returns:
        {
            "processed": int,
            "updated": int,
            "skipped": int,
        }
    """
    q = JobRecord.query.filter(
        JobRecord.postcode.isnot(None),
        JobRecord.postcode != "",
        or_(JobRecord.latitude.is_(None), JobRecord.longitude.is_(None)),
    )

    if limit:
        q = q.limit(limit)

    jobs = q.all()

    processed = 0
    updated = 0
    skipped = 0

    # Clear postcode cache so we don't serve stale coordinates
    geocode_postcode_cached.cache_clear()

    for job in jobs:
        processed += 1

        pc = (job.postcode or "").strip()
        if not pc:
            skipped += 1
            continue

        pc_norm = normalize_uk_postcode(pc)
        lat, lon = geocode_postcode_cached(pc_norm)

        if lat is None or lon is None:
            skipped += 1
            continue

        job.postcode = pc_norm
        job.latitude = lat
        job.longitude = lon
        updated += 1

    try:
        commit_or_rollback()
    except Exception:
        # Rollback already handled in commit_or_rollback; just surface as skipped
        pass

    return {
        "processed": processed,
        "updated": updated,
        "skipped": skipped,
    }


def infer_postcode_from_coords(limit: Optional[int] = None) -> dict:
    """
    For JobRecord rows with lat/lon but no postcode, try to infer postcode
    using snap_to_nearest_postcode().
    """
    q = JobRecord.query.filter(
        or_(JobRecord.postcode.is_(None), JobRecord.postcode == ""),
        JobRecord.latitude.isnot(None),
        JobRecord.longitude.isnot(None),
    )

    if limit:
        q = q.limit(limit)

    jobs = q.all()

    processed = 0
    updated = 0
    skipped = 0

    for job in jobs:
        processed += 1

        lat = job.latitude
        lon = job.longitude
        if lat is None or lon is None:
            skipped += 1
            continue

        inferred_pc, snapped_lat, snapped_lon = snap_to_nearest_postcode(lat, lon)

        if inferred_pc and snapped_lat is not None and snapped_lon is not None:
            job.postcode = normalize_uk_postcode(inferred_pc)
            job.latitude = snapped_lat
            job.longitude = snapped_lon
            updated += 1
        else:
            skipped += 1

    try:
        commit_or_rollback()
    except Exception:
        pass

    return {
        "processed": processed,
        "updated": updated,
        "skipped": skipped,
    }


def backfill_missing_counties(limit: Optional[int] = None) -> dict:
    """
    Fill JobRecord.county where missing, using reverse geocoding on lat/lon.

    NOTE: This uses Nominatim and should be rate-limited; keep 'limit' sensible
    for nightly runs to avoid hammering the API.
    """
    from geopy.geocoders import Nominatim
    from geopy.extra.rate_limiter import RateLimiter

    geolocator = Nominatim(user_agent="payscope-geo-backfill")
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=2)

    q = JobRecord.query.filter(
        or_(JobRecord.county.is_(None), JobRecord.county == "")
    ).filter(
        JobRecord.latitude.isnot(None),
        JobRecord.longitude.isnot(None),
    )

    if limit:
        q = q.limit(limit)

    records = q.all()

    processed = 0
    updated = 0
    skipped = 0

    for record in records:
        processed += 1

        try:
            location = reverse((record.latitude, record.longitude), exactly_one=True)
        except Exception:
            skipped += 1
            continue

        if not location:
            skipped += 1
            continue

        address = location.raw.get("address", {})
        county = address.get("county") or address.get("state_district")

        if county:
            record.county = county
            updated += 1
        else:
            skipped += 1

    try:
        commit_or_rollback()
    except Exception:
        pass

    return {
        "processed": processed,
        "updated": updated,
        "skipped": skipped,
    }


def run_nightly_geo_backfill(
    limit_geocode: int = 1000,
    limit_infer_postcode: int = 1000,
    limit_counties: int = 500,
) -> dict:
    """
    Composite job to run once per night via cron.

    Steps:
    1) Geocode missing lat/lon for rows that already have postcodes.
    2) Infer missing postcodes from lat/lon where possible.
    3) Backfill missing counties using reverse geocoding.

    Returns a combined stats dict suitable for CronRunLog.run_stats.
    """
    stats_geocode = regeocode_missing_jobs(limit=limit_geocode)
    stats_infer = infer_postcode_from_coords(limit=limit_infer_postcode)
    stats_counties = backfill_missing_counties(limit=limit_counties)

    return {
        "job": "nightly_geo_backfill",
        "geocode": stats_geocode,
        "infer_postcode": stats_infer,
        "counties": stats_counties,
    }
