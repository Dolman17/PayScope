from __future__ import annotations

from datetime import date, datetime, timedelta
import math
import re
from collections import defaultdict
from statistics import mean
from typing import Tuple, List, Dict, Any

import requests
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required
from sqlalchemy import func, or_, and_

from extensions import db
from models import JobRecord, JobSummaryDaily
from app.blueprints.dashboard.helpers import _canonical_role_filter_options  # type: ignore
from .utils import geocode_postcode_cached, inside_uk

bp = Blueprint("recruiter", __name__)

# Used only for outcode lookups like WS13, B1 etc.
POSTCODES_IO_OUTCODE_URL = "https://api.postcodes.io/outcodes/{outcode}"

# Pay filtering rules for summary/recommendation calculations only.
NLW_CHANGE_DATE = date(2026, 4, 6)
NLW_PRE_2026_04_06 = 12.21
NLW_FROM_2026_04_06 = 12.71

# Any hourly rate above median * (1 + pct) is excluded from calculations.
# 0.50 = 50% above median.
HIGH_RATE_OUTLIER_PCT = 0.50


def _geocode_flexible_location(raw_location: str) -> Tuple[float | None, float | None]:
    """
    Try to geocode either a full postcode or an outcode.

    1) Delegate to geocode_postcode_cached (full postcodes + cached lookups).
    2) If that fails and the token *looks* like an outcode (WS13, B1),
       call postcodes.io /outcodes/{outcode}.

    Returns (lat, lon) or (None, None) if nothing can be resolved.
    """
    loc = (raw_location or "").strip()
    if not loc:
        return (None, None)

    # 1) Normal postcode path ("B1 1AA" etc.)
    lat, lon = geocode_postcode_cached(loc)
    if lat is not None and lon is not None:
        return lat, lon

    # 2) Outcode path – very lightweight heuristic so we don't
    # accidentally treat "West Midlands" as a postcode.
    token = loc.upper().replace(" ", "")

    # 1–4 chars is a good guard rail for things like B1, B15, WS13
    if not token or len(token) > 4:
        return (None, None)

    try:
        resp = requests.get(
            POSTCODES_IO_OUTCODE_URL.format(outcode=token),
            timeout=10,
        )
        if resp.status_code != 200:
            return (None, None)

        payload = resp.json() or {}
        result = payload.get("result") or {}

        lat_val = result.get("latitude")
        lon_val = result.get("longitude")
        if lat_val is None or lon_val is None:
            return (None, None)

        lat_f = float(lat_val)
        lon_f = float(lon_val)

        if not inside_uk(lat_f, lon_f):
            return (None, None)

        return (lat_f, lon_f)

    except Exception as exc:  # pragma: no cover – defensive logging only
        print(f"[recruiter_radar] Outcode geocode error for '{loc}': {exc}")
        return (None, None)


def _bounding_box(lat: float, lon: float, radius_miles: float) -> Dict[str, float]:
    """
    Approximate bounding box for a radius (in miles) around a lat/lon.

    Good enough for 5–25 mile searches.
    """
    radius_km = radius_miles * 1.60934

    km_per_deg_lat = 111.0
    km_per_deg_lon = 111.0 * max(math.cos(math.radians(lat)), 0.1)

    d_lat = radius_km / km_per_deg_lat
    d_lon = radius_km / km_per_deg_lon

    return {
        "min_lat": lat - d_lat,
        "max_lat": lat + d_lat,
        "min_lon": lon - d_lon,
        "max_lon": lon + d_lon,
    }


def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    True great-circle distance in miles between two lat/lon points.
    """
    earth_radius_miles = 3958.7613

    lat1_r = math.radians(lat1)
    lon1_r = math.radians(lon1)
    lat2_r = math.radians(lat2)
    lon2_r = math.radians(lon2)

    d_lat = lat2_r - lat1_r
    d_lon = lon2_r - lon1_r

    a = (
        math.sin(d_lat / 2) ** 2
        + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(d_lon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return earth_radius_miles * c


def _record_distance_miles(record: JobRecord, centre_lat: float, centre_lon: float) -> float | None:
    """
    Safe distance helper for JobRecord rows.
    """
    if record.latitude is None or record.longitude is None:
        return None

    try:
        return _haversine_miles(
            float(centre_lat),
            float(centre_lon),
            float(record.latitude),
            float(record.longitude),
        )
    except (TypeError, ValueError):
        return None


def _filter_records_to_true_radius(
    records: List[JobRecord],
    centre_lat: float,
    centre_lon: float,
    radius_miles: float,
) -> tuple[List[JobRecord], Dict[int, float], int]:
    """
    Apply a true circular radius filter after the SQL bounding-box prefilter.

    Returns:
    - filtered records
    - distance map keyed by record.id
    - count excluded for being outside the real radius
    """
    filtered: List[JobRecord] = []
    distance_map: Dict[int, float] = {}
    excluded = 0

    for record in records:
        distance = _record_distance_miles(record, centre_lat, centre_lon)
        if distance is None:
            continue

        if distance <= radius_miles:
            filtered.append(record)
            if record.id is not None:
                distance_map[int(record.id)] = distance
        else:
            excluded += 1

    return filtered, distance_map, excluded


def _median_from_rates(rates: List[float]) -> float | None:
    if not rates:
        return None

    sorted_rates = sorted(rates)
    n = len(sorted_rates)
    mid = n // 2

    if n % 2 == 1:
        return float(sorted_rates[mid])

    return float(sorted_rates[mid - 1] + sorted_rates[mid]) / 2.0


def _effective_record_date(record: JobRecord) -> date | None:
    """
    Prefer created_at for recruiter radar calculations.
    Fall back to posted_date if available.
    """
    if getattr(record, "created_at", None):
        return record.created_at.date()

    posted_date = getattr(record, "posted_date", None)
    if isinstance(posted_date, datetime):
        return posted_date.date()
    if isinstance(posted_date, date):
        return posted_date

    return None


def _nlw_floor_for_record(record: JobRecord) -> float:
    record_date = _effective_record_date(record)

    if record_date is not None and record_date < NLW_CHANGE_DATE:
        return NLW_PRE_2026_04_06

    return NLW_FROM_2026_04_06


def _build_pay_calculation_slice(records: List[JobRecord]) -> Dict[str, Any]:
    """
    Build the calculation-eligible pay slice.

    Rules:
    - Exclude missing pay_rate
    - Exclude rates below NLW for the record period
    - From the remaining rates, calculate a baseline median
    - Exclude rates above baseline_median * (1 + HIGH_RATE_OUTLIER_PCT)

    These exclusions affect only:
    - median/min/max/avg
    - recommended rate

    They do NOT affect:
    - demand counts
    - recent adverts
    - top employers
    """
    records_with_pay: List[JobRecord] = [r for r in records if r.pay_rate is not None]

    nlw_valid_records: List[JobRecord] = []
    excluded_below_nlw = 0

    for record in records_with_pay:
        try:
            rate = float(record.pay_rate)
        except (TypeError, ValueError):
            continue

        floor = _nlw_floor_for_record(record)
        if rate < floor:
            excluded_below_nlw += 1
            continue

        nlw_valid_records.append(record)

    nlw_valid_rates = [float(r.pay_rate) for r in nlw_valid_records if r.pay_rate is not None]
    baseline_median = _median_from_rates(nlw_valid_rates)

    excluded_above_high_cutoff = 0
    high_cutoff = None
    final_records: List[JobRecord] = []

    if baseline_median is not None:
        high_cutoff = baseline_median * (1.0 + HIGH_RATE_OUTLIER_PCT)

        for record in nlw_valid_records:
            try:
                rate = float(record.pay_rate)
            except (TypeError, ValueError):
                continue

            if rate > high_cutoff:
                excluded_above_high_cutoff += 1
                continue

            final_records.append(record)

    final_rates = [float(r.pay_rate) for r in final_records if r.pay_rate is not None]

    return {
        "records": final_records,
        "rates": final_rates,
        "baseline_median_before_high_cutoff": baseline_median,
        "high_rate_cutoff": high_cutoff,
        "excluded_below_nlw": excluded_below_nlw,
        "excluded_above_high_cutoff": excluded_above_high_cutoff,
        "eligible_count": len(final_rates),
        "raw_with_pay_count": len(records_with_pay),
        "nlw_valid_count": len(nlw_valid_rates),
    }


def _pay_stats_from_rates(rates: List[float]) -> Dict[str, Any]:
    if not rates:
        return {
            "count": 0,
            "min_rate": None,
            "max_rate": None,
            "avg_rate": None,
            "median_rate": None,
        }

    sorted_rates = sorted(rates)
    n = len(sorted_rates)
    mid = n // 2

    if n % 2 == 1:
        median = float(sorted_rates[mid])
    else:
        median = float(sorted_rates[mid - 1] + sorted_rates[mid]) / 2.0

    return {
        "count": len(rates),
        "min_rate": min(rates),
        "max_rate": max(rates),
        "avg_rate": sum(rates) / len(rates),
        "median_rate": median,
    }


def _role_token_groups(raw_role_input: str) -> List[List[str]]:
    """
    Convert free text into OR groups of AND tokens.

    Behaviour:
    - Split groups on / , ; | +
    - Within each group, split into word tokens
    - Each group means "all these words must appear"
    - Multiple groups mean "any of these groups may match"

    Examples:
      "Senior Support" -> [["senior", "support"]]
      "HR Advisor / HR Officer" -> [["hr", "advisor"], ["hr", "officer"]]
    """
    text = (raw_role_input or "").strip()
    if not text:
        return []

    raw_groups = re.split(r"[/,;|+]", text)
    cleaned_groups: List[List[str]] = []
    seen_groups: set[tuple[str, ...]] = set()

    for group in raw_groups:
        group = group.strip().lower()
        if not group:
            continue

        tokens = re.findall(r"[a-z0-9]+", group)
        tokens = [t for t in tokens if len(t) >= 2 or any(ch.isdigit() for ch in t)]

        if not tokens:
            continue

        deduped_tokens: List[str] = []
        seen_tokens: set[str] = set()
        for token in tokens:
            if token in seen_tokens:
                continue
            seen_tokens.add(token)
            deduped_tokens.append(token)

        key = tuple(deduped_tokens)
        if key in seen_groups:
            continue

        seen_groups.add(key)
        cleaned_groups.append(deduped_tokens)

    if not cleaned_groups:
        fallback_tokens = re.findall(r"[a-z0-9]+", text.lower())
        fallback_tokens = [t for t in fallback_tokens if len(t) >= 2 or any(ch.isdigit() for ch in t)]
        if fallback_tokens:
            cleaned_groups.append(fallback_tokens)

    return cleaned_groups


def _role_fragments(raw_role_input: str) -> List[str]:
    """
    Human-readable debug fragments rebuilt from token groups.
    """
    groups = _role_token_groups(raw_role_input)
    return [" ".join(group) for group in groups if group]


def _build_role_match_clauses(model, token_groups: List[List[str]], field_names: List[str]) -> List[Any]:
    """
    Build OR-of-AND role matching clauses.

    For each token group:
      every token must appear in at least one of the provided fields.
    Across groups:
      any group may match.
    """
    clauses: List[Any] = []

    for token_group in token_groups:
        token_clauses: List[Any] = []

        for token in token_group:
            pattern = f"%{token}%"
            field_token_clauses: List[Any] = []

            for field_name in field_names:
                column = getattr(model, field_name, None)
                if column is not None:
                    field_token_clauses.append(func.lower(column).like(pattern))

            if field_token_clauses:
                token_clauses.append(or_(*field_token_clauses))

        if token_clauses:
            clauses.append(and_(*token_clauses))

    return clauses


def _build_timeseries(raw_role_input: str, counties: List[str], lookback_days: int) -> Dict[str, Any]:
    """
    Build a daily median pay time series for the slice, aggregated across
    matching counties, and fit a straight line for a lightweight 90-day forecast.

    Role matching uses word-token logic:
    e.g. "Senior Support" matches rows where both "senior" and "support"
    appear in the role text.
    """
    today = date.today()
    start_date = today - timedelta(days=lookback_days)

    token_groups = _role_token_groups(raw_role_input)
    if not token_groups:
        return {"points": [], "forecast_3m": None}

    q = (
        db.session.query(
            JobSummaryDaily.date,
            func.avg(JobSummaryDaily.median_pay_rate).label("median_pay_rate"),
        )
        .filter(
            JobSummaryDaily.date >= start_date,
            JobSummaryDaily.date <= today,
        )
    )

    # JobSummaryDaily normally has job_role_group; include job_role too if present.
    field_names = ["job_role_group"]
    if hasattr(JobSummaryDaily, "job_role"):
        field_names.append("job_role")

    role_clauses = _build_role_match_clauses(JobSummaryDaily, token_groups, field_names)
    if role_clauses:
        q = q.filter(or_(*role_clauses))

    if counties:
        q = q.filter(JobSummaryDaily.county.in_(counties))

    q = q.group_by(JobSummaryDaily.date).order_by(JobSummaryDaily.date.asc())
    rows = q.all()

    if not rows:
        return {"points": [], "forecast_3m": None}

    points = [
        {
            "date": r.date.isoformat(),
            "median_pay_rate": float(r.median_pay_rate),
        }
        for r in rows
        if r.date is not None and r.median_pay_rate is not None
    ]

    if not points:
        return {"points": [], "forecast_3m": None}

    # Linear regression on (ordinal date, median)
    xs = [date.fromisoformat(p["date"]).toordinal() for p in points]
    ys = [float(p["median_pay_rate"]) for p in points]

    n = len(xs)
    if n < 2:
        return {
            "points": points,
            "forecast_3m": ys[0] if ys else None,
        }

    x_mean = sum(xs) / n
    y_mean = sum(ys) / n

    s_xx = sum((x - x_mean) ** 2 for x in xs)
    s_xy = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))

    slope = 0.0 if s_xx == 0 else s_xy / s_xx
    intercept = y_mean - slope * x_mean

    future_date = today + timedelta(days=90)
    future_x = future_date.toordinal()
    forecast_val = slope * future_x + intercept

    return {
        "points": points,
        "forecast_3m": forecast_val,
    }


def _build_competition_summary(records: List[JobRecord]) -> Dict[str, Any]:
    employer_map: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {"company_name": None, "rates": [], "adverts_count": 0}
    )

    for record in records:
        company_name = (record.company_name or "").strip() or "Unknown employer"
        row = employer_map[company_name]
        row["company_name"] = company_name
        row["adverts_count"] += 1
        if record.pay_rate is not None:
            row["rates"].append(float(record.pay_rate))

    companies = []
    for company_name, row in employer_map.items():
        avg_pay_rate = mean(row["rates"]) if row["rates"] else None
        companies.append(
            {
                "company_name": company_name,
                "adverts_count": row["adverts_count"],
                "avg_pay_rate": avg_pay_rate,
            }
        )

    companies.sort(
        key=lambda x: (x["adverts_count"], x["avg_pay_rate"] or 0),
        reverse=True,
    )

    return {
        "top_companies": companies[:10],
    }


def _build_commentary_text(payload: Dict[str, Any]) -> str:
    summary = payload.get("summary") or {}
    demand = payload.get("demand") or {}
    recommended = payload.get("recommended") or {}
    competition = payload.get("competition") or {}
    params = payload.get("params") or {}
    trend = payload.get("trend") or {}

    role = params.get("role") or "this role"
    location = (payload.get("location") or {}).get("input") or params.get("location") or "this area"
    radius = params.get("radius_miles") or 15

    records_count = summary.get("records_count") or 0
    median_pay = summary.get("median_pay")
    min_pay = summary.get("min_pay")
    max_pay = summary.get("max_pay")

    adverts_30 = demand.get("adverts_last_30") or 0
    adverts_90 = demand.get("adverts_last_90") or 0
    demand_level = demand.get("level") or "unknown"

    recommended_rate = recommended.get("recommended_rate")
    top_companies = competition.get("top_companies") or []
    employer_count = len(top_companies)

    if records_count == 0:
        return (
            f"No matching adverts were found for {role} within {radius} miles of {location}. "
            "Try widening the radius, increasing the lookback window, or using a broader role search."
        )

    parts: List[str] = []

    lead = f"For {role} within {radius} miles of {location}, we found {records_count} matching adverts"
    if median_pay is not None:
        lead += f" with a typical advertised rate around £{median_pay:.2f}/hr"
        if min_pay is not None and max_pay is not None:
            lead += f" (range £{min_pay:.2f}–£{max_pay:.2f})"
    lead += "."
    parts.append(lead)

    demand_text = (
        f"Demand looks {demand_level}, with {adverts_30} matching adverts in the last 30 days "
        f"and {adverts_90} in the last 90 days."
    )
    parts.append(demand_text)

    if recommended_rate is not None:
        parts.append(
            f"A sensible advertised starting point is around £{recommended_rate:.2f}/hr based on the current local market picture."
        )

    if employer_count > 0:
        top_names = ", ".join(c["company_name"] for c in top_companies[:3] if c.get("company_name"))
        if top_names:
            parts.append(f"The most visible employers in this slice include {top_names}.")

    points = trend.get("points") or []
    forecast_3m = trend.get("forecast_3m")
    if len(points) >= 2 and forecast_3m is not None and median_pay is not None:
        delta = forecast_3m - median_pay
        if abs(delta) >= 0.15:
            direction = "upward" if delta > 0 else "downward"
            parts.append(
                f"The recent trend suggests a slight {direction} pay trajectory over the next three months, though this should be treated as directional rather than certain."
            )

    return " ".join(parts)


@bp.route("/recruiter/radar")
@login_required
def recruiter_radar():
    """
    Top-level recruiter radar view.

    We just need enough context to build the role typeahead;
    everything else is driven by the /api/recruiter/radar endpoint.
    """
    role_options = _canonical_role_filter_options()
    return render_template("recruiter_radar.html", role_options=role_options)


@bp.route("/api/recruiter/radar")
@login_required
def api_recruiter_radar():
    print("LIVE RECRUITER ROUTE FILE:", __file__)
    print("LIVE RECRUITER ROUTE VERSION: token-groups-v4-true-radius-pay-filters")
    """
    One-shot recruiter radar API.

    Query params:
    - role (required): free-text role search
    - location (required): UK postcode or outcode (e.g. WS13)
    - radius_miles: 5, 15, 25 (default 15)
    - lookback_days: history window for demand/forecast (default 180)

    Matching behaviour:
    - free-text role search is token-based
    - "Senior Support" matches roles containing both words
    - matches against BOTH job_role_group and job_role

    Geo behaviour:
    - SQL bounding-box prefilter for speed
    - true circular distance filter in Python for accuracy

    Pay calculation behaviour:
    - all adverts remain visible in recent/top-employer slices
    - pay calculations exclude:
        * sub-NLW rates for the advert period
        * extreme high hourly rates above median * (1 + HIGH_RATE_OUTLIER_PCT)
    """
    raw_role = (request.args.get("role") or "").strip()
    raw_location = (request.args.get("location") or "").strip()
    radius_miles = request.args.get("radius_miles", type=float) or 15.0
    lookback_days = request.args.get("lookback_days", type=int) or 180

    if not raw_role:
        return jsonify({"error": "Job role is required."}), 400

    if not raw_location:
        return jsonify({"error": "Location is required."}), 400

    token_groups = _role_token_groups(raw_role)
    if not token_groups:
        return jsonify({"error": "Job role is required."}), 400

    centre_lat, centre_lon = _geocode_flexible_location(raw_location)
    if centre_lat is None or centre_lon is None:
        return (
            jsonify(
                {
                    "error": (
                        "Could not geocode that location. "
                        "Try a full postcode (e.g. B1 1AA) or an outcode (e.g. WS13)."
                    )
                }
            ),
            400,
        )

    bbox = _bounding_box(centre_lat, centre_lon, radius_miles)

    # Base JobRecord slice for current adverts and detail.
    today = date.today()
    start_dt = datetime.combine(today - timedelta(days=lookback_days), datetime.min.time())
    start_30 = datetime.combine(today - timedelta(days=30), datetime.min.time())
    start_90 = datetime.combine(today - timedelta(days=90), datetime.min.time())

    jr_q = (
        db.session.query(JobRecord)
        .filter(
            JobRecord.latitude.isnot(None),
            JobRecord.longitude.isnot(None),
            JobRecord.latitude >= bbox["min_lat"],
            JobRecord.latitude <= bbox["max_lat"],
            JobRecord.longitude >= bbox["min_lon"],
            JobRecord.longitude <= bbox["max_lon"],
            JobRecord.created_at >= start_dt,
        )
    )

    role_clauses = _build_role_match_clauses(
        JobRecord,
        token_groups,
        ["job_role_group", "job_role"],
    )
    if role_clauses:
        jr_q = jr_q.filter(or_(*role_clauses))

    bbox_records: List[JobRecord] = jr_q.limit(5000).all()

    records, distance_map, excluded_outside_true_radius = _filter_records_to_true_radius(
        bbox_records,
        centre_lat,
        centre_lon,
        radius_miles,
    )

    records.sort(
        key=lambda r: (
            distance_map.get(int(r.id), float("inf")) if r.id is not None else float("inf"),
            -(r.created_at.timestamp() if r.created_at else float("-inf")),
        )
    )

    # Demand/visibility slices continue to use all records in the true-radius slice.
    adverts_count = len(records)
    adverts_last_30 = sum(
        1 for r in records if r.created_at is not None and r.created_at >= start_30
    )
    adverts_last_90 = sum(
        1 for r in records if r.created_at is not None and r.created_at >= start_90
    )

    if adverts_last_30 >= 15 or adverts_last_90 >= 40:
        demand_level = "high"
    elif adverts_last_30 >= 5 or adverts_last_90 >= 15:
        demand_level = "moderate"
    elif adverts_count > 0:
        demand_level = "low"
    else:
        demand_level = "none"

    # Pay calculations use filtered eligible rates only.
    calc_slice = _build_pay_calculation_slice(records)
    calc_records: List[JobRecord] = calc_slice["records"]
    pay_stats = _pay_stats_from_rates(calc_slice["rates"])

    counties = sorted({r.county for r in records if r.county})
    timeseries = _build_timeseries(raw_role, counties, lookback_days)

    base_rate = pay_stats["avg_rate"]
    if base_rate is None:
        recommended_rate = None
        recommended_basis = (
            "No eligible pay-rate data available after applying National Living Wage and high-rate outlier filters."
        )
    else:
        if demand_level == "high":
            recommended_rate = base_rate + 1.00
            recommended_basis = "Eligible average local rate plus a £1.00/hr uplift for strong demand."
        elif demand_level == "moderate":
            recommended_rate = base_rate + 0.50
            recommended_basis = "Eligible average local rate plus a £0.50/hr uplift for moderate demand."
        else:
            recommended_rate = base_rate
            recommended_basis = "Eligible average local advertised rate in this market slice."

    recent_records = sorted(
        records,
        key=lambda r: r.created_at or datetime.min,
        reverse=True,
    )[:10]

    recent_postings = [
        {
            "company_name": r.company_name,
            "job_role": r.job_role,
            "job_role_group": r.job_role_group,
            "postcode": r.postcode,
            "county": r.county,
            "pay_rate": float(r.pay_rate) if r.pay_rate is not None else None,
            "created_at": r.created_at.isoformat() if r.created_at else None,
            "distance_miles": round(distance_map[int(r.id)], 2)
            if r.id is not None and int(r.id) in distance_map
            else None,
            "included_in_pay_calculations": bool(r in calc_records),
        }
        for r in recent_records
    ]

    competition = _build_competition_summary(records)

    distinct_roles = []
    seen_role_pairs: set[tuple[str | None, str | None]] = set()
    for record in records:
        key = (record.job_role, record.job_role_group)
        if key in seen_role_pairs:
            continue
        seen_role_pairs.add(key)
        distinct_roles.append(
            {
                "job_role": record.job_role,
                "job_role_group": record.job_role_group,
            }
        )
        if len(distinct_roles) >= 50:
            break

    payload = {
        "params": {
            "role": raw_role,
            "location": raw_location,
            "radius_miles": radius_miles,
            "lookback_days": lookback_days,
            "high_rate_outlier_pct": HIGH_RATE_OUTLIER_PCT,
        },
        "location": {
            "input": raw_location,
            "lat": centre_lat,
            "lon": centre_lon,
        },
        "summary": {
            "records_count": adverts_count,
            "min_pay": pay_stats["min_rate"],
            "max_pay": pay_stats["max_rate"],
            "avg_pay": pay_stats["avg_rate"],
            "median_pay": pay_stats["median_rate"],
            "eligible_rates_count": calc_slice["eligible_count"],
        },
        "demand": {
            "level": demand_level,
            "adverts_last_30": adverts_last_30,
            "adverts_last_90": adverts_last_90,
        },
        "recommended": {
            "recommended_rate": recommended_rate,
            "basis": recommended_basis,
        },
        "trend": {
            "points": timeseries.get("points", []),
            "forecast_3m": timeseries.get("forecast_3m"),
        },
        "competition": competition,
        "recent_postings": recent_postings,
        "debug": {
            "role_fragments": _role_fragments(raw_role),
            "role_token_groups": token_groups,
            "matched_sample_roles": distinct_roles,
            "bbox_records_count": len(bbox_records),
            "excluded_outside_true_radius": excluded_outside_true_radius,
            "true_radius_records_count": len(records),
            "max_returned_distance_miles": round(max(distance_map.values()), 2) if distance_map else None,
            "raw_with_pay_count": calc_slice["raw_with_pay_count"],
            "excluded_below_nlw": calc_slice["excluded_below_nlw"],
            "nlw_valid_count": calc_slice["nlw_valid_count"],
            "baseline_median_before_high_cutoff": calc_slice["baseline_median_before_high_cutoff"],
            "high_rate_cutoff": calc_slice["high_rate_cutoff"],
            "excluded_above_high_cutoff": calc_slice["excluded_above_high_cutoff"],
            "eligible_pay_rates_count": calc_slice["eligible_count"],
            "nlw_rules": {
                "before_2026_04_06": NLW_PRE_2026_04_06,
                "from_2026_04_06": NLW_FROM_2026_04_06,
            },
        },
    }

    return jsonify(payload)


@bp.route("/api/recruiter/radar/commentary", methods=["POST"])
@login_required
def api_recruiter_radar_commentary():
    """
    Lightweight commentary endpoint used by the Recruiter Radar UI.

    For now this is deterministic and grounded in the supplied payload,
    so the UI always gets commentary even without an external AI service.
    """
    payload = request.get_json(silent=True) or {}
    text = _build_commentary_text(payload)
    return jsonify({"text": text})