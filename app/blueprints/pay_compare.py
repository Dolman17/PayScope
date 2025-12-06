# app/blueprints/pay_compare.py

from datetime import date, datetime, timedelta

from sqlalchemy import func

from models import db, JobSummaryDaily, OnsEarnings

# Adjust this if your ons_importer uses a different code
# for "Gross hourly pay (excluding overtime), median"
ONS_MEDIAN_HOURLY_MEASURE_CODE = "20100"


def _parse_date(value: str | None, default: date) -> date:
    if not value:
        return default
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return default


def _get_latest_ons_year() -> int | None:
    return db.session.query(func.max(OnsEarnings.year)).scalar()


def _build_ons_map(counties: set[str]) -> tuple[dict[str, float], int | None]:
    """
    Return (map of county -> median value, year).
    Only used when grouping includes county.
    """
    if not counties:
        return {}, None

    latest_year = _get_latest_ons_year()
    if not latest_year:
        return {}, None

    rows = (
        OnsEarnings.query
        .filter(
            OnsEarnings.year == latest_year,
            OnsEarnings.measure_code == ONS_MEDIAN_HOURLY_MEASURE_CODE,
            OnsEarnings.geography_name.in_(list(counties)),
        )
        .all()
    )

    ons_map: dict[str, float] = {}
    for r in rows:
        if r.value is not None:
            ons_map[r.geography_name] = float(r.value)

    if not ons_map:
        return {}, None

    return ons_map, latest_year


def get_pay_explorer_data(
    start_date_str: str | None,
    end_date_str: str | None,
    sector: str | None,
    job_role_group: str | None,
    group_by: str,
) -> dict:
    """
    Returns data in the exact shape expected by the Pay Explorer JS:

    {
      "results": [...],
      "ons_available": true/false,
      "ons_year": 2024 or null
    }
    """

    today = date.today()
    default_start = today - timedelta(days=30)

    start = _parse_date(start_date_str, default_start)
    end = _parse_date(end_date_str, today)

    # Guard against inverted ranges
    if start > end:
        start, end = end - timedelta(days=30), end

    group_by = group_by or "county"
    if group_by not in ("county", "sector", "sector_county"):
        group_by = "county"

    # Base query
    base_filters = [
        JobSummaryDaily.date >= start,
        JobSummaryDaily.date <= end,
    ]
    if sector:
        base_filters.append(JobSummaryDaily.sector == sector)
    if job_role_group:
        base_filters.append(JobSummaryDaily.job_role_group == job_role_group)

    results: list[dict] = []
    ons_available = False
    ons_year: int | None = None

    if group_by == "county":
        q = (
            db.session.query(
                JobSummaryDaily.county.label("county"),
                func.sum(JobSummaryDaily.adverts_count).label("adverts_count"),
                func.avg(JobSummaryDaily.median_pay_rate).label("median_pay_rate"),
                func.avg(JobSummaryDaily.p25_pay_rate).label("p25_pay_rate"),
                func.avg(JobSummaryDaily.p75_pay_rate).label("p75_pay_rate"),
            )
            .filter(*base_filters)
            .group_by(JobSummaryDaily.county)
        )
        rows = q.all()

        county_names = {r.county for r in rows if r.county}
        ons_map, year = _build_ons_map(county_names)
        if year is not None:
            ons_available = True
            ons_year = year

        for r in rows:
            county = r.county or "Unknown county"
            adv_med = float(r.median_pay_rate) if r.median_pay_rate is not None else None
            ons_val = ons_map.get(r.county) if r.county else None

            pay_vs_ons = None
            pay_vs_ons_pct = None
            if adv_med is not None and ons_val is not None and ons_val != 0:
                pay_vs_ons = round(adv_med - ons_val, 2)
                pay_vs_ons_pct = round((adv_med - ons_val) / ons_val * 100, 1)

            results.append(
                {
                    "county": county,
                    "sector": None,
                    "adverts_count": int(r.adverts_count or 0),
                    "median_pay_rate": adv_med,
                    "ons_median_hourly": ons_val,
                    "pay_vs_ons": pay_vs_ons,
                    "pay_vs_ons_pct": pay_vs_ons_pct,
                }
            )

    elif group_by == "sector":
        # Aggregated by sector only; no ONS (county-based) overlay here
        q = (
            db.session.query(
                JobSummaryDaily.sector.label("sector"),
                func.sum(JobSummaryDaily.adverts_count).label("adverts_count"),
                func.avg(JobSummaryDaily.median_pay_rate).label("median_pay_rate"),
                func.avg(JobSummaryDaily.p25_pay_rate).label("p25_pay_rate"),
                func.avg(JobSummaryDaily.p75_pay_rate).label("p75_pay_rate"),
            )
            .filter(*base_filters)
            .group_by(JobSummaryDaily.sector)
        )
        rows = q.all()

        for r in rows:
            adv_med = float(r.median_pay_rate) if r.median_pay_rate is not None else None

            results.append(
                {
                    "county": None,
                    "sector": r.sector or "Unknown sector",
                    "adverts_count": int(r.adverts_count or 0),
                    "median_pay_rate": adv_med,
                    "ons_median_hourly": None,
                    "pay_vs_ons": None,
                    "pay_vs_ons_pct": None,
                }
            )

        ons_available = False
        ons_year = None

    else:  # "sector_county"
        q = (
            db.session.query(
                JobSummaryDaily.sector.label("sector"),
                JobSummaryDaily.county.label("county"),
                func.sum(JobSummaryDaily.adverts_count).label("adverts_count"),
                func.avg(JobSummaryDaily.median_pay_rate).label("median_pay_rate"),
                func.avg(JobSummaryDaily.p25_pay_rate).label("p25_pay_rate"),
                func.avg(JobSummaryDaily.p75_pay_rate).label("p75_pay_rate"),
            )
            .filter(*base_filters)
            .group_by(JobSummaryDaily.sector, JobSummaryDaily.county)
        )
        rows = q.all()

        county_names = {r.county for r in rows if r.county}
        ons_map, year = _build_ons_map(county_names)
        if year is not None:
            ons_available = True
            ons_year = year

        for r in rows:
            county = r.county or "Unknown county"
            adv_med = float(r.median_pay_rate) if r.median_pay_rate is not None else None
            ons_val = ons_map.get(r.county) if r.county else None

            pay_vs_ons = None
            pay_vs_ons_pct = None
            if adv_med is not None and ons_val is not None and ons_val != 0:
                pay_vs_ons = round(adv_med - ons_val, 2)
                pay_vs_ons_pct = round((adv_med - ons_val) / ons_val * 100, 1)

            results.append(
                {
                    "county": county,
                    "sector": r.sector or "Unknown sector",
                    "adverts_count": int(r.adverts_count or 0),
                    "median_pay_rate": adv_med,
                    "ons_median_hourly": ons_val,
                    "pay_vs_ons": pay_vs_ons,
                    "pay_vs_ons_pct": pay_vs_ons_pct,
                }
            )

    return {
        "results": results,
        "ons_available": ons_available,
        "ons_year": ons_year,
    }
