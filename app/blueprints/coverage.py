from datetime import date, timedelta
from sqlalchemy import func

from extensions import db
from models import JobSummaryDaily


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
    NEW: Source coverage for coverage.html.

    Expected output rows (per source_site):
      - source_site
      - adverts (sum)
      - days_seen (distinct days)
      - median_pay (avg of daily medians as a safe proxy)
      - sector_count (distinct sectors)
      - location_count (distinct counties) if county exists else 0
    """
    # If source_site doesn't exist on JobSummaryDaily, we can't compute this.
    if not _has_col(JobSummaryDaily, "source_site"):
        return []

    start = date.today() - timedelta(days=days)

    # Optional fields (not all envs may have them)
    has_adverts = _has_col(JobSummaryDaily, "adverts")
    has_median = _has_col(JobSummaryDaily, "median_pay")
    has_county = _has_col(JobSummaryDaily, "county")
    has_sector = _has_col(JobSummaryDaily, "sector")

    adverts_expr = func.sum(JobSummaryDaily.adverts).label("adverts") if has_adverts else func.count().label("adverts")
    median_expr = func.avg(JobSummaryDaily.median_pay).label("median_pay") if has_median else func.null().label("median_pay")

    sector_count_expr = (
        func.count(func.distinct(JobSummaryDaily.sector)).label("sector_count")
        if has_sector
        else func.literal(0).label("sector_count")
    )

    location_count_expr = (
        func.count(func.distinct(JobSummaryDaily.county)).label("location_count")
        if has_county
        else func.literal(0).label("location_count")
    )

    rows = (
        db.session.query(
            JobSummaryDaily.source_site.label("source_site"),
            adverts_expr,
            func.count(func.distinct(JobSummaryDaily.date)).label("days_seen"),
            median_expr,
            sector_count_expr,
            location_count_expr,
        )
        .filter(JobSummaryDaily.date >= start)
        .group_by(JobSummaryDaily.source_site)
        .all()
    )

    out: list[dict] = []
    for r in rows:
        out.append(
            {
                "source_site": getattr(r, "source_site", None),
                "adverts": int(getattr(r, "adverts", 0) or 0),
                "days_seen": int(getattr(r, "days_seen", 0) or 0),
                "median_pay": (float(getattr(r, "median_pay", 0)) if getattr(r, "median_pay", None) is not None else None),
                "sector_count": int(getattr(r, "sector_count", 0) or 0),
                "location_count": int(getattr(r, "location_count", 0) or 0),
            }
        )

    # Sort: most adverts first, then name
    out.sort(key=lambda x: (-(x.get("adverts") or 0), (x.get("source_site") or "")))
    return out
