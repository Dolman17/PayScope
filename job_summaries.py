# job_summaries.py
#
# Utilities to build daily summary rows in JobSummaryDaily
# from the existing JobRecord table.
#
# This does NOT change your import flow; it just reads whatever
# is already in job_record and writes aggregated stats per day.

from datetime import date
import statistics

from extensions import db
from models import JobRecord, JobSummaryDaily


def _percentile(sorted_values, fraction: float) -> float:
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

    # Pull all JobRecords imported on that date
    # NOTE: imported_at can be NULL, so we guard against that.
    q = (
        JobRecord.query
        .filter(db.func.date(JobRecord.imported_at) == target_date)
    )

    buckets = {}  # (county, sector, job_role_group) -> [pay_rate, ...]

    for rec in q:
        if rec.pay_rate is None:
            continue  # skip records without a pay rate

        county = rec.county or "Unknown"
        sector = rec.sector or "Unknown"
        job_role_group = rec.job_role_group or rec.job_role or "Unknown"

        key = (county, sector, job_role_group)
        buckets.setdefault(key, []).append(float(rec.pay_rate))

    created = 0

    for (county, sector, job_role_group), rates in buckets.items():
        if not rates:
            continue

        rates_sorted = sorted(rates)
        n = len(rates_sorted)

        median = float(statistics.median(rates_sorted))
        p25 = _percentile(rates_sorted, 0.25)
        p75 = _percentile(rates_sorted, 0.75)
        min_rate = float(rates_sorted[0])
        max_rate = float(rates_sorted[-1])

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
