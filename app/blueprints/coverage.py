from __future__ import annotations

from datetime import date, timedelta
from sqlalchemy import func

from extensions import db
from models import JobSummaryDaily, JobPosting


def _has_col(model, name: str) -> bool:
    """Defensive check: avoid crashing if schema differs between envs."""
    return hasattr(model, name)


def get_weekly_coverage_snapshot(days: int = 7) -> dict:
    """
    Existing behaviour: sector coverage = number of distinct days seen per sector.
    Keeps output shape unchanged.
    """
    start = date.today() - timedelta(days=days)

    rows = (
        db.session.query(
            JobSummaryDaily.sector,
            func.count(func.distinct(JobSummaryDaily.date)).label("days_seen"),
        )
        .filter(JobSummaryDaily.date >= start)
        .group_by(JobSummaryDaily.sector)
        .all()
    )

    coverage = []
    weak_sectors = []

    for r in rows:
        entry = {
            "sector": r.sector,
            "days_seen": int(r.days_seen or 0),
            "ok": (r.days_seen or 0) >= 2,
        }
        coverage.append(entry)

        if not entry["ok"]:
            weak_sectors.append(entry["sector"])

    return {
        "window_days": days,
        "sectors": sorted(coverage, key=lambda x: x["sector"] or ""),
        "weak_sectors": weak_sectors,
        "weak_sectors_count": len(weak_sectors),
        "status": (
            "green" if len(weak_sectors) == 0
            else "amber" if len(weak_sectors) <= 3
            else "red"
        ),
    }


def get_weekly_source_coverage(days: int = 7) -> list[dict]:
    """
    Source coverage for coverage.html.

    IMPORTANT: Your JobSummaryDaily model currently has no source_site, so this
    is derived from JobPosting (which does have source_site). This means Reed
    shows up immediately without changing summaries.

    Output rows (per source_site):
      - source_site
      - adverts (count of postings in window)
      - days_seen (distinct posted_date; fallback to scraped_at date)
      - median_pay (proxy via avg(midpoint(min,max)) when possible)
      - sector_count (distinct JobPosting.sector if available; else 0)
      - location_count (distinct outward code from postcode if available; else 0)
    """
    start_date = date.today() - timedelta(days=days)
    end_excl = date.today() + timedelta(days=1)

    has_sector = _has_col(JobPosting, "sector")
    has_postcode = _has_col(JobPosting, "postcode")

    # Use posted_date if present, else scraped_at date (cast to date)
    day_expr = func.coalesce(
        JobPosting.posted_date,
        func.date(JobPosting.scraped_at),
    )

    # Pay proxy: midpoint if both present, else whichever exists, else NULL
    pay_expr = func.avg(
        func.coalesce(
            (JobPosting.min_rate + JobPosting.max_rate) / 2.0,
            JobPosting.min_rate,
            JobPosting.max_rate,
        )
    ).label("median_pay")

    if has_sector:
        sector_count_expr = func.count(func.distinct(JobPosting.sector)).label("sector_count")
    else:
        sector_count_expr = func.literal(0).label("sector_count")

    if has_postcode:
        outward_expr = func.upper(func.split_part(func.trim(JobPosting.postcode), " ", 1))
        location_count_expr = func.count(func.distinct(outward_expr)).label("location_count")
    else:
        location_count_expr = func.literal(0).label("location_count")

    rows = (
        db.session.query(
            JobPosting.source_site.label("source_site"),
            func.count(JobPosting.id).label("adverts"),
            func.count(func.distinct(day_expr)).label("days_seen"),
            pay_expr,
            sector_count_expr,
            location_count_expr,
        )
        .filter(day_expr >= start_date)
        .filter(day_expr < end_excl)
        .group_by(JobPosting.source_site)
        .order_by(func.count(JobPosting.id).desc())
        .all()
    )

    out: list[dict] = []
    for r in rows:
        out.append(
            {
                "source_site": getattr(r, "source_site", None),
                "adverts": int(getattr(r, "adverts", 0) or 0),
                "days_seen": int(getattr(r, "days_seen", 0) or 0),
                "median_pay": (
                    float(getattr(r, "median_pay", 0))
                    if getattr(r, "median_pay", None) is not None
                    else None
                ),
                "sector_count": int(getattr(r, "sector_count", 0) or 0),
                "location_count": int(getattr(r, "location_count", 0) or 0),
            }
        )

    # Sort: most adverts first, then source name
    out.sort(key=lambda x: (-(x.get("adverts") or 0), (x.get("source_site") or "")))
    return out
