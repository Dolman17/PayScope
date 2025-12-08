# app/blueprints/pay_compare.py

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import func

from models import db, JobSummaryDaily, OnsEarnings

print("=== pay_compare.py loaded (Pay Explorer with canonical ONS mapping) ===")

# ONS measure code for:
# "Gross hourly pay (excluding overtime), median"
# This must match what ons_importer writes into OnsEarnings.measure_code.
ONS_MEDIAN_HOURLY_MEASURE_CODE = "20100"


# ---------------------------------------------------------------------------
# Fuzzy → canonical mapping
# ---------------------------------------------------------------------------

# Keys: fuzzy "county" values coming from JobSummaryDaily.county (uppercased)
# Values: exact OnsEarnings.geography_name strings for ASHE 2024
#
# You can tweak these to taste, but they must match the ONS geography_name
# exactly as seen in /admin/inspect/ons.
FUZZY_TO_CANONICAL_2024: Dict[str, str] = {
    # London macro labels → representative boroughs
    "WEST LONDON": "Hounslow",
    "NORTH LONDON": "Enfield",
    "EAST LONDON": "Tower Hamlets",
    "SOUTH EAST LONDON": "Lewisham",
    "SOUTH WEST LONDON": "Wandsworth",
    "CENTRAL LONDON": "Westminster",
    "LONDON": "Westminster",

    # City-region labels → core cities
    "GREATER MANCHESTER": "Manchester",
    "WEST MIDLANDS": "Birmingham",
    "WEST YORKSHIRE": "Leeds",
    "SCOTLAND": "Glasgow City",           # check exact name in /admin/inspect/ons
    "GLASGOW CITY CENTRE": "Glasgow City",  # as above
    "SOUTH WEST ENGLAND": "Bristol, City of",  # check exact text in ONS

    # Common suburb / area rollups → nearest LA
    "RUISLIP": "Hillingdon",
    "WEST DRAYTON": "Hillingdon",
    "UXBRIDGE": "Hillingdon",
    "HAYES": "Hillingdon",
    "PINNER": "Harrow",
    "SURBITON": "Kingston upon Thames",
    "KINGSTON UPON THAMES": "Kingston upon Thames",
    "MITCHAM": "Merton",
    "RICHMOND": "Richmond upon Thames",
    "FELTHAM": "Hounslow",
    "ORPINGTON": "Bromley",

    # Manchester / Leeds satellites
    "WORSLEY": "Salford",
    "SWINTON": "Salford",
    "MORLEY": "Leeds",
    "YEADON": "Leeds",
    "MICKLEFIELD": "Leeds",
    "TYLDESLEY": "Wigan",
    "Wythenshawe".upper(): "Manchester",

    # Bristol satellites
    "ALMONDSBURY": "South Gloucestershire",
    "THORNBURY": "South Gloucestershire",
    "ALVECHURCH": "Bromsgrove",

    # Generic catch-alls (only if you *know* there is such a geography in ONS)
    # "UK": "United Kingdom",
}


def _canonicalise_county_name(name: Optional[str]) -> Optional[str]:
    """
    Normalise JobSummaryDaily.county into something that should exist
    in OnsEarnings.geography_name.

    - Trims whitespace
    - Uppercases for fuzzy lookup
    - Falls back to the original clean name if no mapping exists
    """
    if not name:
        return None
    raw = name.strip()
    upper = raw.upper()

    canonical = FUZZY_TO_CANONICAL_2024.get(upper)
    if canonical:
        # Tiny bit of debug; comment out if it gets too chatty.
        # print(f"[PAY_COMPARE] Canonicalised county '{raw}' -> '{canonical}'")
        return canonical

    return raw


def _parse_date(value: str | None, default: date) -> date:
    if not value:
        return default
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return default


def _get_latest_ons_year() -> Optional[int]:
    """
    Return the newest ONS ASHE year we have in the DB, or None if empty.
    """
    return db.session.query(func.max(OnsEarnings.year)).scalar()


def _build_ons_map(counties: Set[str]) -> Tuple[Dict[str, float], Optional[int]]:
    """
    Return (map of canonical_name -> median value, year).

    - Only uses rows for the latest available ONS year.
    - Filters to measure_code=ONS_MEDIAN_HOURLY_MEASURE_CODE.
    - Applies fuzzy→canonical mapping before hitting the DB.
    - Only used when grouping includes county.
    """
    if not counties:
        return {}, None

    latest_year = _get_latest_ons_year()
    if not latest_year:
        return {}, None

    # Canonicalise all distinct county names first
    canonical_counties: Set[str] = set()
    for name in counties:
        if not name:
            continue
        canonical = _canonicalise_county_name(name)
        if canonical:
            canonical_counties.add(canonical)

    if not canonical_counties:
        return {}, None

    rows: List[OnsEarnings] = (
        OnsEarnings.query
        .filter(
            OnsEarnings.year == latest_year,
            OnsEarnings.measure_code == ONS_MEDIAN_HOURLY_MEASURE_CODE,
            OnsEarnings.geography_name.in_(list(canonical_counties)),
        )
        .all()
    )

    ons_map: Dict[str, float] = {}
    for r in rows:
        if r.value is not None:
            # Keyed by *canonical* geography_name as stored by ONS
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

    Logic:
    - Aggregates JobSummaryDaily between start/end.
    - For any grouping that includes county, looks up ONS median
      hourly earnings for the *latest* ASHE year in OnsEarnings.
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

    # Base filters for JobSummaryDaily
    base_filters = [
        JobSummaryDaily.date >= start,
        JobSummaryDaily.date <= end,
    ]
    if sector:
        base_filters.append(JobSummaryDaily.sector == sector)
    if job_role_group:
        base_filters.append(JobSummaryDaily.job_role_group == job_role_group)

    results: List[dict] = []
    ons_available = False
    ons_year: Optional[int] = None

    # ----------------------------------------------------------
    # Group by COUNTY
    # ----------------------------------------------------------
    if group_by == "county":
        q = (
            db.session.query(
                JobSummaryDaily.county.label("county"),
                func.sum(JobSummaryDaily.adverts_count).label("adverts_count"),
                func.avg(JobSummaryDaily.median_pay_rate).label("median_pay_rate"),
                func.avg(JobSummaryDaily.p25_pay_rate).label("p25_pay_rate"),
                func.avg(JobSummaryDaily.p75_pay_rate).label("p75_pay_rate"),
                func.min(JobSummaryDaily.median_pay_rate).label("min_pay_rate"),
                func.max(JobSummaryDaily.median_pay_rate).label("max_pay_rate"),
            )
            .filter(*base_filters)
            .group_by(JobSummaryDaily.county)
        )
        rows = q.all()

        # Build ONS map keyed by canonical county name, for the latest year we have
        raw_county_names = {r.county for r in rows if r.county}
        ons_map, year = _build_ons_map(raw_county_names)
        if year is not None:
            ons_available = True
            ons_year = year

        for r in rows:
            raw_county = r.county
            county_name = raw_county or "Unknown county"
            adv_med = float(r.median_pay_rate) if r.median_pay_rate is not None else None

            canonical_key = _canonicalise_county_name(raw_county) if raw_county else None
            ons_val = ons_map.get(canonical_key) if canonical_key else None

            pay_vs_ons = None
            pay_vs_ons_pct = None
            if adv_med is not None and ons_val is not None and ons_val != 0:
                diff = adv_med - ons_val
                pay_vs_ons = round(diff, 2)
                pay_vs_ons_pct = round(diff / ons_val * 100, 1)

            results.append(
                {
                    "county": county_name,
                    "sector": None,
                    "adverts_count": int(r.adverts_count or 0),
                    "median_pay_rate": adv_med,
                    "p25_pay_rate": float(r.p25_pay_rate) if r.p25_pay_rate is not None else None,
                    "p75_pay_rate": float(r.p75_pay_rate) if r.p75_pay_rate is not None else None,
                    "min_pay_rate": float(r.min_pay_rate) if r.min_pay_rate is not None else None,
                    "max_pay_rate": float(r.max_pay_rate) if r.max_pay_rate is not None else None,
                    "ons_median_hourly": ons_val,
                    "pay_vs_ons": pay_vs_ons,
                    "pay_vs_ons_pct": pay_vs_ons_pct,
                }
            )

    # ----------------------------------------------------------
    # Group by SECTOR ONLY (no ONS overlay – no geography)
    # ----------------------------------------------------------
    elif group_by == "sector":
        q = (
            db.session.query(
                JobSummaryDaily.sector.label("sector"),
                func.sum(JobSummaryDaily.adverts_count).label("adverts_count"),
                func.avg(JobSummaryDaily.median_pay_rate).label("median_pay_rate"),
                func.avg(JobSummaryDaily.p25_pay_rate).label("p25_pay_rate"),
                func.avg(JobSummaryDaily.p75_pay_rate).label("p75_pay_rate"),
                func.min(JobSummaryDaily.median_pay_rate).label("min_pay_rate"),
                func.max(JobSummaryDaily.median_pay_rate).label("max_pay_rate"),
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
                    "p25_pay_rate": float(r.p25_pay_rate) if r.p25_pay_rate is not None else None,
                    "p75_pay_rate": float(r.p75_pay_rate) if r.p75_pay_rate is not None else None,
                    "min_pay_rate": float(r.min_pay_rate) if r.min_pay_rate is not None else None,
                    "max_pay_rate": float(r.max_pay_rate) if r.max_pay_rate is not None else None,
                    "ons_median_hourly": None,
                    "pay_vs_ons": None,
                    "pay_vs_ons_pct": None,
                }
            )

        # No ONS overlay in this mode
        ons_available = False
        ons_year = None

    # ----------------------------------------------------------
    # Group by SECTOR + COUNTY
    # ----------------------------------------------------------
    else:  # "sector_county"
        q = (
            db.session.query(
                JobSummaryDaily.sector.label("sector"),
                JobSummaryDaily.county.label("county"),
                func.sum(JobSummaryDaily.adverts_count).label("adverts_count"),
                func.avg(JobSummaryDaily.median_pay_rate).label("median_pay_rate"),
                func.avg(JobSummaryDaily.p25_pay_rate).label("p25_pay_rate"),
                func.avg(JobSummaryDaily.p75_pay_rate).label("p75_pay_rate"),
                func.min(JobSummaryDaily.median_pay_rate).label("min_pay_rate"),
                func.max(JobSummaryDaily.median_pay_rate).label("max_pay_rate"),
            )
            .filter(*base_filters)
            .group_by(JobSummaryDaily.sector, JobSummaryDaily.county)
        )
        rows = q.all()

        raw_county_names = {r.county for r in rows if r.county}
        ons_map, year = _build_ons_map(raw_county_names)
        if year is not None:
            ons_available = True
            ons_year = year

        for r in rows:
            raw_county = r.county
            county_name = raw_county or "Unknown county"
            adv_med = float(r.median_pay_rate) if r.median_pay_rate is not None else None

            canonical_key = _canonicalise_county_name(raw_county) if raw_county else None
            ons_val = ons_map.get(canonical_key) if canonical_key else None

            pay_vs_ons = None
            pay_vs_ons_pct = None
            if adv_med is not None and ons_val is not None and ons_val != 0:
                diff = adv_med - ons_val
                pay_vs_ons = round(diff, 2)
                pay_vs_ons_pct = round(diff / ons_val * 100, 1)

            results.append(
                {
                    "county": county_name,
                    "sector": r.sector or "Unknown sector",
                    "adverts_count": int(r.adverts_count or 0),
                    "median_pay_rate": adv_med,
                    "p25_pay_rate": float(r.p25_pay_rate) if r.p25_pay_rate is not None else None,
                    "p75_pay_rate": float(r.p75_pay_rate) if r.p75_pay_rate is not None else None,
                    "min_pay_rate": float(r.min_pay_rate) if r.min_pay_rate is not None else None,
                    "max_pay_rate": float(r.max_pay_rate) if r.max_pay_rate is not None else None,
                    "ons_median_hourly": ons_val,
                    "pay_vs_ons": pay_vs_ons,
                    "pay_vs_ons_pct": pay_vs_ons_pct,
                }
            )

    return {
        "results": results,
        "ons_available": ons_available,
        "ons_year": ons_year,
        # Optional: echo params for debugging – frontend ignores this
        "params": {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "sector": sector,
            "job_role_group": job_role_group,
            "group_by": group_by,
        },
    }
