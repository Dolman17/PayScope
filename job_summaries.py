# job_summaries.py
#
# Utilities to build daily summary rows in JobSummaryDaily
# from the existing JobRecord table.
#
# This does NOT change your import flow; it just reads whatever
# is already in job_record and writes aggregated stats per day.

from __future__ import annotations

from datetime import date, datetime, timedelta
import statistics
from typing import Dict, Iterable, List, Tuple

from sqlalchemy import func

from extensions import db
from models import JobRecord, JobSummaryDaily


def _percentile(sorted_values: List[float], fraction: float) -> float:
    """
    Return an approximate percentile from a sorted list.
    fraction=0.25 -> 25th percentile, 0.75 -> 75th, etc.
    """
    if not sorted_values:
        return 0.0

    n = len(sorted_values)
    if n == 1:
        return float(sorted_values[0])

    # Clamp fraction between 0 and 1
    fraction = max(0.0, min(1.0, fraction))
    idx = int(round(fraction * (n - 1)))
    return float(sorted_values[idx])


def _safe_label(value: str | None, fallback: str) -> str:
    s = (value or "").strip()
    return s if s else fallback


def build_daily_job_summaries(target_date: date, delete_existing: bool = True) -> int:
    """
    Build JobSummaryDaily rows for a single calendar day,
    based on JobRecord.imported_at.

    - Groups by (county, sector, job_role_group)
    - Computes adverts_count, median, p25, p75, min, max of pay_rate
    - Returns the number of JobSummaryDaily rows created.

    This function is idempotent if delete_existing=True:
    it will delete any existing summaries for that date before inserting.
    """

    # Optionally clear existing rows for that day to avoid duplicates
    if delete_existing:
        JobSummaryDaily.query.filter(JobSummaryDaily.date == target_date).delete()
        db.session.flush()

    # Pull only rows for that calendar day (imported_at can be NULL)
    q = (
        JobRecord.query.filter(JobRecord.imported_at.isnot(None))
        .filter(func.date(JobRecord.imported_at) == target_date)
    )

    buckets: Dict[Tuple[str, str, str], List[float]] = {}  # (county, sector, job_role_group) -> [pay_rate,...]

    for rec in q:
        if rec.pay_rate is None:
            continue

        county = _safe_label(getattr(rec, "county", None), "Unknown")
        sector = _safe_label(getattr(rec, "sector", None), "Unknown")
        job_role_group = _safe_label(getattr(rec, "job_role_group", None), "") or _safe_label(
            getattr(rec, "job_role", None), "Unknown"
        )

        key = (county, sector, job_role_group)
        buckets.setdefault(key, []).append(float(rec.pay_rate))

    created = 0

    for (county, sector, job_role_group), rates in buckets.items():
        if not rates:
            continue

        rates_sorted = sorted(rates)
        n = len(rates_sorted)

        # Robust stats: if anything odd sneaks in, fail "softly" for this bucket
        try:
            median = float(statistics.median(rates_sorted))
            p25 = _percentile(rates_sorted, 0.25)
            p75 = _percentile(rates_sorted, 0.75)
            min_rate = float(rates_sorted[0])
            max_rate = float(rates_sorted[-1])
        except Exception:
            continue

        summary = JobSummaryDaily(
            date=target_date,
            county=county,
            sector=sector,
            job_role_group=job_role_group,
            adverts_count=n,
            median_pay_rate=median,
            p25_pay_rate=p25,
            p75_pay_rate=p75,
            min_pay_rate=min_rate,
            max_pay_rate=max_rate,
        )
        db.session.add(summary)
        created += 1

    db.session.commit()
    return created


def build_daily_job_summaries_range(
    start_date: date,
    end_date: date,
    delete_existing: bool = True,
) -> int:
    """
    Convenience helper to build summaries across a date range (inclusive).

    Returns total JobSummaryDaily rows created across all days.
    """
    if start_date > end_date:
        start_date, end_date = end_date, start_date

    total_created = 0
    d = start_date
    while d <= end_date:
        total_created += build_daily_job_summaries(d, delete_existing=delete_existing)
        d = d + timedelta(days=1)

    return total_created
