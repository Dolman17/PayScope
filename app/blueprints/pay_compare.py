# app/pay_compare.py

from datetime import date, timedelta
from sqlalchemy import func, desc

from models import db, JobSummaryDaily, OnsEarnings

# This should match whatever your ons_importer uses for
# "Gross hourly pay (excluding overtime), median".
# Adjust if your importer uses a different code.
ONS_MEDIAN_HOURLY_MEASURE_CODE = "20100"


def _get_latest_ons_year():
    """Return the most recent ONS year we have in OnsEarnings."""
    latest = (
        db.session.query(func.max(OnsEarnings.year))
        .scalar()
    )
    return latest


def get_latest_ons_median_for_county(
    county: str,
    measure_code: str = ONS_MEDIAN_HOURLY_MEASURE_CODE,
):
    """
    Fetch the latest ONS median hourly pay for a given county
    using OnsEarnings.

    We match on geography_name == county and the latest year.
    """
    latest_year = _get_latest_ons_year()
    if not latest_year:
        return None

    record = (
        OnsEarnings.query
        .filter(
            OnsEarnings.year == latest_year,
            OnsEarnings.geography_name == county,
            OnsEarnings.measure_code == measure_code,
        )
        .order_by(desc(OnsEarnings.year))
        .first()
    )

    return record


def get_advertised_pay(
    county: str | None = None,
    sector: str | None = None,
    job_role_group: str | None = None,
    days: int = 30,
):
    """
    Aggregate advertised pay from JobSummaryDaily over the last N days.

    Returns a dict like:
    {
        "sample_size": 123,
        "median_pay_rate": 12.75,
        "p25_pay_rate": 11.50,
        "p75_pay_rate": 13.50,
        "min_pay_rate": 10.90,
        "max_pay_rate": 14.20,
    }
    or None if no data.
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    q = JobSummaryDaily.query.filter(
        JobSummaryDaily.date >= start_date,
        JobSummaryDaily.date <= end_date,
    )

    if county:
        q = q.filter(JobSummaryDaily.county == county)
    if sector:
        q = q.filter(JobSummaryDaily.sector == sector)
    if job_role_group:
        q = q.filter(JobSummaryDaily.job_role_group == job_role_group)

    result = q.with_entities(
        func.sum(JobSummaryDaily.adverts_count).label("sample_size"),
        func.avg(JobSummaryDaily.median_pay_rate).label("median_pay_rate"),
        func.avg(JobSummaryDaily.p25_pay_rate).label("p25_pay_rate"),
        func.avg(JobSummaryDaily.p75_pay_rate).label("p75_pay_rate"),
        func.min(JobSummaryDaily.min_pay_rate).label("min_pay_rate"),
        func.max(JobSummaryDaily.max_pay_rate).label("max_pay_rate"),
    ).one_or_none()

    if not result or not result.sample_size:
        return None

    return {
        "sample_size": int(result.sample_size),
        "median_pay_rate": float(result.median_pay_rate) if result.median_pay_rate is not None else None,
        "p25_pay_rate": float(result.p25_pay_rate) if result.p25_pay_rate is not None else None,
        "p75_pay_rate": float(result.p75_pay_rate) if result.p75_pay_rate is not None else None,
        "min_pay_rate": float(result.min_pay_rate) if result.min_pay_rate is not None else None,
        "max_pay_rate": float(result.max_pay_rate) if result.max_pay_rate is not None else None,
    }


def get_pay_comparison(
    county: str | None = None,
    sector: str | None = None,
    job_role_group: str | None = None,
    days: int = 30,
):
    """
    Core engine: compares advertised pay vs ONS median (county-level),
    returns a JSON-safe dict.

    We compare:
    - Advertised median_pay_rate (from JobSummaryDaily)
    - ONS median hourly earnings (OnsEarnings.value)
    """

    advertised = get_advertised_pay(
        county=county,
        sector=sector,
        job_role_group=job_role_group,
        days=days,
    )

    ons_record = None
    if county:
        ons_record = get_latest_ons_median_for_county(county=county)

    # If nothing at all, bail with empty result
    if not advertised and not ons_record:
        return {
            "filters": {
                "county": county,
                "sector": sector,
                "job_role_group": job_role_group,
                "days": days,
            },
            "advertised": None,
            "ons": None,
            "gap": None,
        }

    # Build advertised block
    advertised_block = advertised if advertised else None

    # Build ONS block
    ons_block = None
    if ons_record:
        ons_block = {
            "year": ons_record.year,
            "geography_code": ons_record.geography_code,
            "geography_name": ons_record.geography_name,
            "measure_code": ons_record.measure_code,
            "median_hourly": ons_record.value,
            "source": "ASHE",
        }

    # Compute gap (only if both sides exist and have numbers)
    gap_block = None
    if (
        advertised_block
        and ons_block
        and advertised_block.get("median_pay_rate") is not None
        and ons_block.get("median_hourly") is not None
    ):
        adv = advertised_block["median_pay_rate"]
        ons_val = ons_block["median_hourly"]
        if ons_val:
            absolute = round(adv - ons_val, 2)
            ratio = adv / ons_val
            percent = round((adv - ons_val) / ons_val * 100, 1)
            gap_block = {
                "absolute": absolute,   # £ difference
                "percent": percent,     # % below/above ONS
                "ratio": ratio,         # advertised / ONS
            }

    return {
        "filters": {
            "county": county,
            "sector": sector,
            "job_role_group": job_role_group,
            "days": days,
        },
        "advertised": advertised_block,
        "ons": ons_block,
        "gap": gap_block,
    }
