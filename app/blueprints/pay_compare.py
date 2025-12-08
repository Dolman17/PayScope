# app/blueprints/pay_compare.py

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

from sqlalchemy import func

from models import db, JobSummaryDaily, OnsEarnings

print("=== pay_compare.py loaded (Pay Explorer) ===")

# ONS measure code for:
# "Gross hourly pay (excluding overtime), median"
# This must match what ons_importer writes into OnsEarnings.measure_code.
ONS_MEDIAN_HOURLY_MEASURE_CODE = "20100"


# ------------------------------------------------------------------
# Explicit area → ONS local authority mapping
# ------------------------------------------------------------------
# Keys are UPPERCASED JobSummaryDaily.county labels.
# Values must be valid OnsEarnings.geography_name values for the ONS year.
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
    "SCOTLAND": "Glasgow City",              # check exact spelling in /admin/inspect/ons
    "GLASGOW CITY CENTRE": "Glasgow City",

    # Regional rollups
    "SOUTH WEST ENGLAND": "Bristol, City of",

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
    "WYTHENSHAWE": "Manchester",

    # Bristol satellites
    "ALMONDSBURY": "South Gloucestershire",
    "THORNBURY": "South Gloucestershire",

    # Midlands satellite
    "ALVECHURCH": "Bromsgrove",
}


def _parse_date(value: str | None, default: date) -> date:
    if not value:
        return default
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return default


def _get_latest_ons_year() -> Optional[int]:
    """
    Return the newest ONS ASHE year we have in the DB for the
    median measure we actually use.
    """
    latest_year = (
        db.session.query(func.max(OnsEarnings.year))
        .filter(OnsEarnings.measure_code == ONS_MEDIAN_HOURLY_MEASURE_CODE)
        .scalar()
    )
    print(
        f"[PAY_COMPARE] _get_latest_ons_year (measure={ONS_MEDIAN_HOURLY_MEASURE_CODE}) -> "
        f"{latest_year}"
    )
    return latest_year


def _map_to_canonical_geography(raw: Optional[str]) -> Optional[str]:
    """
    Map a raw JobSummaryDaily.county label to a canonical ONS geography_name.

    This does NOT look in the DB – it just applies our explicit rules.
    """
    if not raw:
        return None

    s = raw.strip()
    upper = s.upper()

    mapped = FUZZY_TO_CANONICAL_2024.get(upper)
    if mapped:
        print(f"[PAY_COMPARE] MAP (dict): '{raw}' -> '{mapped}'")
        return mapped

    # Heuristic fallbacks (for safety)
    if "WEST LONDON" in upper:
        print(f"[PAY_COMPARE] MAP (heuristic): '{raw}' -> 'Hounslow'")
        return "Hounslow"
    if "NORTH LONDON" in upper:
        print(f"[PAY_COMPARE] MAP (heuristic): '{raw}' -> 'Enfield'")
        return "Enfield"
    if "EAST LONDON" in upper:
        print(f"[PAY_COMPARE] MAP (heuristic): '{raw}' -> 'Tower Hamlets'")
        return "Tower Hamlets"
    if "SOUTH EAST LONDON" in upper:
        print(f"[PAY_COMPARE] MAP (heuristic): '{raw}' -> 'Lewisham'")
        return "Lewisham"
    if "SOUTH WEST LONDON" in upper:
        print(f"[PAY_COMPARE] MAP (heuristic): '{raw}' -> 'Wandsworth'")
        return "Wandsworth"
    if "CENTRAL LONDON" in upper or upper == "LONDON":
        print(f"[PAY_COMPARE] MAP (heuristic): '{raw}' -> 'Westminster'")
        return "Westminster"
    if "SCOTLAND" in upper:
        print(f"[PAY_COMPARE] MAP (heuristic): '{raw}' -> 'Glasgow City'")
        return "Glasgow City"
    if "WEST YORKSHIRE" in upper:
        print(f"[PAY_COMPARE] MAP (heuristic): '{raw}' -> 'Leeds'")
        return "Leeds"
    if "WEST MIDLANDS" in upper:
        print(f"[PAY_COMPARE] MAP (heuristic): '{raw}' -> 'Birmingham'")
        return "Birmingham"
    if "GREATER MANCHESTER" in upper:
        print(f"[PAY_COMPARE] MAP (heuristic): '{raw}' -> 'Manchester'")
        return "Manchester"

    # Default – treat raw label as the geography name
    return s


def _build_ons_map(counties: Set[str]) -> Tuple[Dict[str, float], Optional[int]]:
    """
    Return (map of geography_name -> median value, year).

    Strategy:
    - Determine latest ONS year that has the right measure_code.
    - Build a set of all geography names we want ONS for:
        - every raw county label
        - plus any canonical mappings from FUZZY_TO_CANONICAL_2024/_map_to_canonical_geography.
    - Query OnsEarnings for all of those names.
    """
    if not counties:
        return {}, None

    latest_year = _get_latest_ons_year()
    if not latest_year:
        print("[PAY_COMPARE] No ONS earnings data found in DB.")
        return {}, None

    canonical_names: Set[str] = set()

    for c in counties:
        if not c:
            continue
        raw = c.strip()
        canonical_names.add(raw)

        mapped = _map_to_canonical_geography(raw)
        if mapped:
            canonical_names.add(mapped)

    # Remove empties just in case
    canonical_names = {n for n in canonical_names if n}

    print(
        f"[PAY_COMPARE] ONS lookup for year {latest_year}, "
        f"names={sorted(canonical_names)}"
    )

    if not canonical_names:
        return {}, None

    rows: List[OnsEarnings] = (
        OnsEarnings.query
        .filter(
            OnsEarnings.year == latest_year,
            OnsEarnings.measure_code == ONS_MEDIAN_HOURLY_MEASURE_CODE,
            OnsEarnings.geography_name.in_(list(canonical_names)),
        )
        .all()
    )

    ons_map: Dict[str, float] = {}
    for r in rows:
        if r.value is not None and r.geography_name:
            key = r.geography_name.strip()
            ons_map[key] = float(r.value)

    print(f"[PAY_COMPARE] Loaded {len(ons_map)} ONS rows for Pay Explorer")

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
      "ons_year": 2024 or null,
      "params": {...debug echo...}
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
                func.min(JobSummaryDaily.min_pay_rate).label("min_pay_rate"),
                func.max(JobSummaryDaily.max_pay_rate).label("max_pay_rate"),
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
            display_name = r.county or "Unknown"
            raw_name = r.county.strip() if r.county else None
            canonical_name = _map_to_canonical_geography(raw_name) if raw_name else None

            if canonical_name and canonical_name != raw_name:
                print(
                    f"[PAY_COMPARE] RESULT MAP: county='{raw_name}' "
                    f"uses canonical='{canonical_name}'"
                )

            adv_med = float(r.median_pay_rate) if r.median_pay_rate is not None else None

            ons_val = None
            if canonical_name:
                # Prefer canonical mapping (e.g. West London -> Hounslow)
                ons_val = ons_map.get(canonical_name)
                # If that fails, fall back to raw
                if ons_val is None and raw_name:
                    ons_val = ons_map.get(raw_name)
            elif raw_name:
                ons_val = ons_map.get(raw_name)

            if raw_name and ons_val is None:
                print(
                    f"[PAY_COMPARE] NO ONS MATCH: "
                    f"raw='{raw_name}', canonical='{canonical_name}'"
                )

            pay_vs_ons = None
            pay_vs_ons_pct = None
            if adv_med is not None and ons_val is not None and ons_val != 0:
                diff = adv_med - ons_val
                pay_vs_ons = round(diff, 2)
                pay_vs_ons_pct = round(diff / ons_val * 100, 1)

            results.append(
                {
                    "county": display_name,
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
                func.min(JobSummaryDaily.min_pay_rate).label("min_pay_rate"),
                func.max(JobSummaryDaily.max_pay_rate).label("max_pay_rate"),
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
                func.min(JobSummaryDaily.min_pay_rate).label("min_pay_rate"),
                func.max(JobSummaryDaily.max_pay_rate).label("max_pay_rate"),
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
            display_name = r.county or "Unknown"
            raw_name = r.county.strip() if r.county else None
            canonical_name = _map_to_canonical_geography(raw_name) if raw_name else None

            if canonical_name and canonical_name != raw_name:
                print(
                    f"[PAY_COMPARE] RESULT MAP (sector_county): "
                    f"county='{raw_name}' uses canonical='{canonical_name}'"
                )

            adv_med = float(r.median_pay_rate) if r.median_pay_rate is not None else None

            ons_val = None
            if canonical_name:
                ons_val = ons_map.get(canonical_name)
                if ons_val is None and raw_name:
                    ons_val = ons_map.get(raw_name)
            elif raw_name:
                ons_val = ons_map.get(raw_name)

            if raw_name and ons_val is None:
                print(
                    f"[PAY_COMPARE] NO ONS MATCH (sector_county): "
                    f"raw='{raw_name}', canonical='{canonical_name}'"
                )

            pay_vs_ons = None
            pay_vs_ons_pct = None
            if adv_med is not None and ons_val is not None and ons_val != 0:
                diff = adv_med - ons_val
                pay_vs_ons = round(diff, 2)
                pay_vs_ons_pct = round(diff / ons_val * 100, 1)

            results.append(
                {
                    "county": display_name,
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
        "params": {
            "start_date": start_date_str,
            "end_date": end_date_str,
            "sector": sector,
            "job_role_group": job_role_group,
            "group_by": group_by,
        },
    }
