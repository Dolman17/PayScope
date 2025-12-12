# app/blueprints/pay_compare.py

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

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
# Sector normalisation (UI + query compatibility)
# ------------------------------------------------------------------

SECTOR_ALIASES: Dict[str, str] = {
    # IT
    "IT & Digital": "IT & Digital",
    "IT & Technology": "IT & Digital",
    "Information Technology": "IT & Digital",
    "Technology": "IT & Digital",
    "Tech": "IT & Digital",
    # HR / Ops
    "HR & Recruitment": "HR & Recruitment",
    "HR, Admin & Operations": "HR & Recruitment",
    "Human Resources": "HR & Recruitment",
    "Recruitment": "HR & Recruitment",
    # Finance
    "Finance & Accounting": "Finance & Accounting",
    "Finance": "Finance & Accounting",
    "Accounting": "Finance & Accounting",
    # Care
    "Social Care & Nursing": "Social Care & Nursing",
    "Weekend Social Care": "Social Care & Nursing",
    "Weekend Nursing & Care": "Social Care & Nursing",
    "Health & Social Care": "Social Care & Nursing",
    # Customer/Support
    "Support & Customer": "Customer Service",
    "Customer Service": "Customer Service",
    "Customer Support": "Customer Service",
}


def normalise_sector_name(value: str | None) -> str | None:
    """
    Map a sector label to a canonical form. Used for filtering so the UI
    works even if the DB still contains mixed/legacy sector labels.
    """
    if not value:
        return None
    s = value.strip()
    if not s:
        return None

    # exact
    if s in SECTOR_ALIASES:
        return SECTOR_ALIASES[s]

    # case-insensitive
    s_lower = s.lower()
    for k, v in SECTOR_ALIASES.items():
        if k.lower() == s_lower:
            return v

    return s


# ------------------------------------------------------------------
# Hint mapping: area label -> "target-ish" name for fuzzy search
# ------------------------------------------------------------------
# Keys are UPPERCASED JobSummaryDaily.county labels.
# Values are *hints* that we then fuzzy-match against actual ONS names.
FUZZY_HINTS: Dict[str, str] = {
    # London macro labels → borough-ish hints
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
    "SCOTLAND": "Glasgow City",
    "GLASGOW CITY CENTRE": "Glasgow City",
    # Regional rollups
    "SOUTH WEST ENGLAND": "Bristol",
    # Common suburb / area rollups → nearest LA hints
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
# Cached ONS index for the latest year
# ------------------------------------------------------------------

ONS_INDEX_YEAR: Optional[int] = None
ONS_GEOG_LIST: List[str] = []  # list of geography_name
ONS_VALUES: Dict[str, float] = {}  # geography_name -> median value


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


def _ensure_ons_index() -> Optional[int]:
    """
    Load all ONS geography_name + median values for the latest year
    into ONS_GEOG_LIST and ONS_VALUES.
    """
    global ONS_INDEX_YEAR, ONS_GEOG_LIST, ONS_VALUES

    if ONS_INDEX_YEAR is not None and ONS_GEOG_LIST and ONS_VALUES:
        return ONS_INDEX_YEAR

    year = _get_latest_ons_year()
    if not year:
        print("[PAY_COMPARE] No ONS earnings data found in DB.")
        return None

    rows: List[OnsEarnings] = (
        OnsEarnings.query.filter(
            OnsEarnings.year == year,
            OnsEarnings.measure_code == ONS_MEDIAN_HOURLY_MEASURE_CODE,
        ).all()
    )

    geogs: List[str] = []
    values: Dict[str, float] = {}

    for r in rows:
        if r.geography_name and r.value is not None:
            name = r.geography_name.strip()
            geogs.append(name)
            values[name] = float(r.value)

    ONS_INDEX_YEAR = year
    ONS_GEOG_LIST = sorted(set(geogs))
    ONS_VALUES = values

    print(f"[PAY_COMPARE] Built ONS index for year {year}: {len(ONS_GEOG_LIST)} geographies")
    sample = ONS_GEOG_LIST[:15]
    print("[PAY_COMPARE] Sample ONS geographies:", sample)

    return year


def _hint_for_area(raw_name: str) -> str:
    """
    Take a JobSummaryDaily.county label and turn it into a *hint* string
    to fuzzy-match against ONS geography_name.
    """
    s = raw_name.strip()
    upper = s.upper()

    hint = FUZZY_HINTS.get(upper)
    if hint:
        print(f"[PAY_COMPARE] HINT (dict): '{raw_name}' -> '{hint}'")
        return hint

    return s


def _match_to_ons_geography(raw_name: Optional[str]) -> Optional[str]:
    """
    Fuzzy-match a JobSummaryDaily.county label to an ONS geography_name:
    - Use FUZZY_HINTS as a starting point (e.g. "West London" -> "Hounslow")
    - Then fuzzy that hint against the actual ONS_GEOG_LIST
    - Fall back to raw_name if needed
    """
    if not raw_name:
        return None
    if not ONS_GEOG_LIST:
        return None

    # Step 1: derive a hint (may just be the raw name)
    hint = _hint_for_area(raw_name)

    candidates = ONS_GEOG_LIST

    # Try an exact-ish match first, case-insensitive
    for g in candidates:
        if g.lower() == hint.lower():
            print(f"[PAY_COMPARE] ONS GEO EXACT: raw='{raw_name}' -> '{g}' (via hint)")
            return g

    for g in candidates:
        if g.lower() == raw_name.strip().lower():
            print(f"[PAY_COMPARE] ONS GEO EXACT: raw='{raw_name}' -> '{g}'")
            return g

    # Step 2: fuzzy-match using RapidFuzz or difflib
    best_name: Optional[str] = None
    best_score: float = 0.0

    if _HAS_RAPIDFUZZ:
        match = process.extractOne(
            hint,
            candidates,
            scorer=fuzz.token_set_ratio,
        )
        if match:
            name, score, _ = match
            best_name, best_score = name, float(score)
    else:
        matches = get_close_matches(hint, candidates, n=1, cutoff=0.6)
        if matches:
            best_name = matches[0]
            best_score = 80.0  # arbitrary "good enough" score for difflib

    if best_name:
        print(
            f"[PAY_COMPARE] ONS GEO FUZZY: raw='{raw_name}', hint='{hint}' "
            f"-> '{best_name}' (score={best_score})"
        )
        return best_name

    # Last ditch: try fuzzy on the raw_name if hint failed completely
    if _HAS_RAPIDFUZZ:
        match = process.extractOne(
            raw_name,
            candidates,
            scorer=fuzz.token_set_ratio,
        )
        if match:
            name, score, _ = match
            print(f"[PAY_COMPARE] ONS GEO FUZZY RAW: raw='{raw_name}' -> '{name}' (score={score})")
            return name
    else:
        matches = get_close_matches(raw_name, candidates, n=1, cutoff=0.6)
        if matches:
            name = matches[0]
            print(f"[PAY_COMPARE] ONS GEO FUZZY RAW: raw='{raw_name}' -> '{name}'")
            return name

    print(f"[PAY_COMPARE] ONS GEO NO MATCH: raw='{raw_name}', hint='{hint}'")
    return None


# ------------------------------------------------------------------
# MAIN: Pay Explorer data
# ------------------------------------------------------------------
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

    # Normalise sector for compatibility with mixed DB values
    sector_norm = normalise_sector_name(sector)
    if sector_norm:
        raw_sector = (sector or "").strip()
        if raw_sector and raw_sector != sector_norm:
            base_filters.append(JobSummaryDaily.sector.in_([raw_sector, sector_norm]))
        else:
            base_filters.append(JobSummaryDaily.sector == sector_norm)

    if job_role_group:
        base_filters.append(JobSummaryDaily.job_role_group == job_role_group)

    results: List[dict] = []
    ons_available = False
    ons_year: Optional[int] = None

    # Build ONS index if we're going to need geography
    if group_by in ("county", "sector_county"):
        ons_year = _ensure_ons_index()
        if ons_year:
            ons_available = True
        else:
            ons_available = False

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

        for r in rows:
            display_name = r.county or "Unknown"
            raw_name = r.county.strip() if r.county else None

            adv_med = float(r.median_pay_rate) if r.median_pay_rate is not None else None

            ons_val = None
            if raw_name and ONS_VALUES:
                matched_geo = _match_to_ons_geography(raw_name)
                if matched_geo:
                    ons_val = ONS_VALUES.get(matched_geo)
                if matched_geo and ons_val is None:
                    print(
                        f"[PAY_COMPARE] NO ONS VALUE: raw='{raw_name}', matched_geo='{matched_geo}'"
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

        for r in rows:
            display_name = r.county or "Unknown"
            raw_name = r.county.strip() if r.county else None

            adv_med = float(r.median_pay_rate) if r.median_pay_rate is not None else None

            ons_val = None
            if raw_name and ONS_VALUES:
                matched_geo = _match_to_ons_geography(raw_name)
                if matched_geo:
                    ons_val = ONS_VALUES.get(matched_geo)
                if matched_geo and ons_val is None:
                    print(
                        f"[PAY_COMPARE] NO ONS VALUE (sector_county): raw='{raw_name}', matched_geo='{matched_geo}'"
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
            "sector": sector_norm or sector,
            "job_role_group": job_role_group,
            "group_by": group_by,
        },
    }


# ------------------------------------------------------------------
# DEBUG HELPERS – used by /admin/debug/pay-explorer-mapping
# ------------------------------------------------------------------

def _debug_match_to_ons_geography(raw_name: Optional[str]):
    """
    Same idea as _match_to_ons_geography, but returns debug metadata:
    (matched_geo, method, score).

    method is one of:
      - 'NO_INDEX'     : no ONS index loaded
      - 'EMPTY'        : raw_name was empty
      - 'EXACT_HINT'   : case-insensitive exact match on hint
      - 'EXACT_RAW'    : case-insensitive exact match on raw name
      - 'FUZZY_HINT'   : fuzzy match using hint string
      - 'FUZZY_RAW'    : fuzzy match using raw string
      - 'NONE'         : nothing suitable found
    score is a numeric similarity score where we can compute one, else None.
    """
    if not raw_name:
        return None, "EMPTY", None
    if not ONS_GEOG_LIST:
        return None, "NO_INDEX", None

    hint = _hint_for_area(raw_name)
    candidates = ONS_GEOG_LIST

    # 1) Exact-ish on hint
    for g in candidates:
        if g.lower() == hint.lower():
            return g, "EXACT_HINT", 100.0

    # 2) Exact-ish on raw
    for g in candidates:
        if g.lower() == raw_name.strip().lower():
            return g, "EXACT_RAW", 100.0

    # 3) Fuzzy on hint
    best_name = None
    best_score = None

    if _HAS_RAPIDFUZZ:
        match = process.extractOne(
            hint,
            candidates,
            scorer=fuzz.token_set_ratio,
        )
        if match:
            name, score, _ = match
            best_name, best_score = name, float(score)
    else:
        matches = get_close_matches(hint, candidates, n=1, cutoff=0.6)
        if matches:
            best_name = matches[0]
            best_score = 80.0  # arbitrary "good enough" score for difflib

    if best_name is not None:
        return best_name, "FUZZY_HINT", best_score

    # 4) Fuzzy on raw if hint failed
    if _HAS_RAPIDFUZZ:
        match = process.extractOne(
            raw_name,
            candidates,
            scorer=fuzz.token_set_ratio,
        )
        if match:
            name, score, _ = match
            return name, "FUZZY_RAW", float(score)
    else:
        matches = get_close_matches(raw_name, candidates, n=1, cutoff=0.6)
        if matches:
            name = matches[0]
            return name, "FUZZY_RAW", 80.0

    return None, "NONE", None


def build_pay_explorer_debug_snapshot(
    days: int = 30,
) -> Tuple[List[dict], Optional[int]]:
    """
    Build a debug snapshot of how Pay Explorer is mapping counties to ONS:

    Returns (rows, ons_year) where rows is a list of dicts:
      {
        "raw_county": "West London",
        "adverts": 111,
        "adv_median": 17.65,
        "matched_geo": "London Borough of Hounslow",
        "match_method": "FUZZY_HINT",
        "match_score": 92.0,
        "ons_median": 19.02,
      }
    """

    # Ensure ONS index is ready
    ons_year = _ensure_ons_index()

    today = date.today()
    start = today - timedelta(days=days)
    end = today

    base_filters = [
        JobSummaryDaily.date >= start,
        JobSummaryDaily.date <= end,
    ]

    q = (
        db.session.query(
            JobSummaryDaily.county.label("county"),
            func.sum(JobSummaryDaily.adverts_count).label("adverts_count"),
            func.avg(JobSummaryDaily.median_pay_rate).label("median_pay_rate"),
        )
        .filter(*base_filters)
        .group_by(JobSummaryDaily.county)
    )

    rows = q.all()

    debug_rows: List[dict] = []

    for r in rows:
        raw_name = r.county.strip() if r.county else None
        adv_med = float(r.median_pay_rate) if r.median_pay_rate is not None else None

        matched_geo, method, score = _debug_match_to_ons_geography(raw_name)
        ons_val = None
        if matched_geo and ONS_VALUES:
            ons_val = ONS_VALUES.get(matched_geo)

        debug_rows.append(
            {
                "raw_county": raw_name or "Unknown",
                "adverts": int(r.adverts_count or 0),
                "adv_median": adv_med,
                "matched_geo": matched_geo,
                "match_method": method,
                "match_score": score,
                "ons_median": ons_val,
            }
        )

    # Sort for readability
    debug_rows.sort(key=lambda x: x["raw_county"])

    return debug_rows, ons_year
