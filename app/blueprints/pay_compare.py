# app/blueprints/pay_compare.py

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple

import logging
from difflib import get_close_matches

from sqlalchemy import func

from models import db, JobSummaryDaily, OnsEarnings

print("=== pay_compare.py loaded (Pay Explorer) ===")
logger = logging.getLogger(__name__)

# Try to use RapidFuzz if available for better fuzzy matching
try:
    from rapidfuzz import fuzz, process  # type: ignore

    _HAS_RAPIDFUZZ = True
    print("[PAY_COMPARE] RapidFuzz detected – using for ONS geography fuzzy matching.")
except Exception:
    _HAS_RAPIDFUZZ = False
    print("[PAY_COMPARE] RapidFuzz not available – falling back to difflib.")


# ONS measure code for:
# "Gross hourly pay (excluding overtime), median"
# This must match what ons_importer writes into OnsEarnings.measure_code.
ONS_MEDIAN_HOURLY_MEASURE_CODE = "20100"


# ------------------------------------------------------------------
# Fuzzy county → canonical ONS geography name mapping
# ------------------------------------------------------------------
# Keys are UPPERCASED JobSummaryDaily.county labels.
# Values should map to *some* valid OnsEarnings.geography_name,
# but we now also sanity-check & fuzzy-match against actual ONS names.
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

# ------------------------------------------------------------------
# Cache of available ONS geography names for the latest year
# ------------------------------------------------------------------

ONS_GEOG_CACHE_YEAR: Optional[int] = None
ONS_GEOG_NAMES: Set[str] = set()


def _ensure_ons_geog_cache(year: int) -> None:
    """
    Populate the global ONS_GEOG_NAMES with the distinct geography_name
    values we actually have in OnsEarnings for the given year.
    """
    global ONS_GEOG_CACHE_YEAR, ONS_GEOG_NAMES

    if year and ONS_GEOG_CACHE_YEAR == year and ONS_GEOG_NAMES:
        return

    rows = (
        db.session.query(OnsEarnings.geography_name)
        .filter(
            OnsEarnings.year == year,
            OnsEarnings.measure_code == ONS_MEDIAN_HOURLY_MEASURE_CODE,
        )
        .distinct()
        .all()
    )

    names: Set[str] = set()
    for (name,) in rows:
        if name:
            names.add(name.strip())

    ONS_GEOG_CACHE_YEAR = year
    ONS_GEOG_NAMES = names

    print(f"[PAY_COMPARE] Cached {len(ONS_GEOG_NAMES)} ONS geography names for year {year}")


def _fuzzy_match_geography(name: str) -> Optional[str]:
    """
    Fuzzy match an arbitrary county/area name to the closest ONS geography_name.
    Uses RapidFuzz if installed, otherwise difflib.get_close_matches.
    """
    if not name:
        return None
    if not ONS_GEOG_NAMES:
        # Nothing to match against; bail out
        return None

    candidates = list(ONS_GEOG_NAMES)

    # Try exact-ish first (case-insensitive)
    for g in candidates:
        if g.lower() == name.lower():
            return g

    if _HAS_RAPIDFUZZ:
        # token_set_ratio copes well with extra words / order changes
        match = process.extractOne(
            name,
            candidates,
            scorer=fuzz.token_set_ratio,
            score_cutoff=80,  # tweak if needed
        )
        if match:
            best_name, score, _ = match
            print(f"[PAY_COMPARE] FUZZY ONS GEO: '{name}' -> '{best_name}' (score={score})")
            return best_name
    else:
        best = get_close_matches(name, candidates, n=1, cutoff=0.8)
        if best:
            best_name = best[0]
            print(f"[PAY_COMPARE] FUZZY ONS GEO (difflib): '{name}' -> '{best_name}'")
            return best_name

    return None


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


def _canonicalise_county_name(name: Optional[str]) -> Optional[str]:
    """
    Normalise JobSummaryDaily.county into something that should exist
    in OnsEarnings.geography_name.

    Steps (in order):
    - Trim whitespace
    - Handle explicit FUZZY_TO_CANONICAL_2024 dictionary overrides
    - Handle specific London-region heuristics
    - If we have an ONS geography cache, check for direct matches
    - Otherwise fuzzy-match against ONS geography_name values
    """
    if not name:
        return None

    raw = name.strip()
    upper = raw.upper()
    candidate = raw  # default if nothing smarter kicks in

    # Explicit dictionary mapping
    dict_match = FUZZY_TO_CANONICAL_2024.get(upper)
    if dict_match:
        print(f"[PAY_COMPARE] FUZZY MAP (dict): '{raw}' -> '{dict_match}'")
        candidate = dict_match
    else:
        # Heuristic fallbacks (deal with weird spacing / punctuation)
        if "WEST LONDON" in upper:
            candidate = "Hounslow"
            print(f"[PAY_COMPARE] HEURISTIC MAP: '{raw}' -> '{candidate}'")
        elif "NORTH LONDON" in upper:
            candidate = "Enfield"
            print(f"[PAY_COMPARE] HEURISTIC MAP: '{raw}' -> '{candidate}'")
        elif "EAST LONDON" in upper:
            candidate = "Tower Hamlets"
            print(f"[PAY_COMPARE] HEURISTIC MAP: '{raw}' -> '{candidate}'")
        elif "SOUTH EAST LONDON" in upper:
            candidate = "Lewisham"
            print(f"[PAY_COMPARE] HEURISTIC MAP: '{raw}' -> '{candidate}'")
        elif "SOUTH WEST LONDON" in upper:
            candidate = "Wandsworth"
            print(f"[PAY_COMPARE] HEURISTIC MAP: '{raw}' -> '{candidate}'")
        elif "CENTRAL LONDON" in upper:
            candidate = "Westminster"
            print(f"[PAY_COMPARE] HEURISTIC MAP: '{raw}' -> '{candidate}'")
        elif upper == "LONDON":
            candidate = "Westminster"
            print(f"[PAY_COMPARE] HEURISTIC MAP: '{raw}' -> '{candidate}'")

    # If we don't yet have an ONS geography cache, we can't validate/fuzzy,
    # so return the best candidate we have.
    if not ONS_GEOG_NAMES:
        return candidate

    # If candidate is already a valid ONS geography_name, we're done.
    if candidate in ONS_GEOG_NAMES:
        return candidate

    # Try fuzzy against ONS geography names
    fuzzy = _fuzzy_match_geography(candidate)
    if fuzzy:
        print(f"[PAY_COMPARE] FINAL CANONICAL (fuzzy): '{raw}' / '{candidate}' -> '{fuzzy}'")
        return fuzzy

    # As a last resort, try fuzzy on the raw input
    raw_fuzzy = _fuzzy_match_geography(raw)
    if raw_fuzzy:
        print(f"[PAY_COMPARE] FINAL CANONICAL (raw fuzzy): '{raw}' -> '{raw_fuzzy}'")
        return raw_fuzzy

    # Default – keep the candidate name as-is (may not match any ONS row)
    return candidate


def _build_ons_map(counties: Set[str]) -> Tuple[Dict[str, float], Optional[int]]:
    """
    Return (map of canonical_name -> median value, year).

    - Only uses rows for the latest available ONS year.
    - Filters to measure_code=ONS_MEDIAN_HOURLY_MEASURE_CODE.
    - Only used when grouping includes county.
    """
    if not counties:
        return {}, None

    latest_year = _get_latest_ons_year()
    if not latest_year:
        print("[PAY_COMPARE] No ONS earnings data found in DB.")
        return {}, None

    # Populate ONS_GEOG_NAMES so _canonicalise_county_name can use it
    _ensure_ons_geog_cache(latest_year)

    # Canonicalise all county labels into valid-ish ONS geography_name values
    canonical_names: Set[str] = set()
    for c in counties:
        canon = _canonicalise_county_name(c)
        if canon:
            canonical_names.add(canon)

    print(
        f"[PAY_COMPARE] ONS lookup for year {latest_year}, "
        f"canonical_names={sorted(canonical_names)}"
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
            # Keyed by the ONS geography_name
            ons_map[r.geography_name.strip()] = float(r.value)

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
            display_name = r.county or "Unknown county"
            canonical_name = _canonicalise_county_name(r.county) if r.county else None

            if canonical_name and canonical_name != r.county:
                print(f"[PAY_COMPARE] RESULT MAP: county='{r.county}' uses canonical='{canonical_name}'")

            adv_med = float(r.median_pay_rate) if r.median_pay_rate is not None else None

            # ons_map is keyed by ONS geography_name
            ons_val = None
            if canonical_name:
                # try direct; if not found, last-ditch fuzzy vs map keys
                ons_val = ons_map.get(canonical_name)
                if ons_val is None and ons_map:
                    fuzzy_geo = _fuzzy_match_geography(canonical_name)
                    if fuzzy_geo and fuzzy_geo in ons_map:
                        print(
                            f"[PAY_COMPARE] LATE FUZZY MAP: canonical='{canonical_name}' -> '{fuzzy_geo}' "
                            f"(county='{r.county}')"
                        )
                        ons_val = ons_map.get(fuzzy_geo)

            if canonical_name and ons_val is None:
                print(f"[PAY_COMPARE] NO ONS MATCH: canonical='{canonical_name}' (county='{r.county}')")

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
            display_name = r.county or "Unknown county"
            canonical_name = _canonicalise_county_name(r.county) if r.county else None

            if canonical_name and canonical_name != r.county:
                print(
                    f"[PAY_COMPARE] RESULT MAP (sector_county): "
                    f"county='{r.county}' uses canonical='{canonical_name}'"
                )

            adv_med = float(r.median_pay_rate) if r.median_pay_rate is not None else None

            ons_val = None
            if canonical_name:
                ons_val = ons_map.get(canonical_name)
                if ons_val is None and ons_map:
                    fuzzy_geo = _fuzzy_match_geography(canonical_name)
                    if fuzzy_geo and fuzzy_geo in ons_map:
                        print(
                            f"[PAY_COMPARE] LATE FUZZY MAP (sector_county): canonical='{canonical_name}' "
                            f"-> '{fuzzy_geo}' (county='{r.county}')"
                        )
                        ons_val = ons_map.get(fuzzy_geo)

            if canonical_name and ons_val is None:
                print(
                    f"[PAY_COMPARE] NO ONS MATCH (sector_county): "
                    f"canonical='{canonical_name}' (county='{r.county}')"
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
