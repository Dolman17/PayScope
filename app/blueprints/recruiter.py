from __future__ import annotations

from datetime import date, datetime, timedelta
import math
import re
from typing import Tuple, List, Dict, Any

import requests
from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required
from sqlalchemy import func, or_

from extensions import db
from models import JobRecord, JobSummaryDaily
from app.blueprints.dashboard.helpers import _canonical_role_filter_options  # type: ignore
from .utils import geocode_postcode_cached, inside_uk


bp = Blueprint("recruiter", __name__)

# Used only for outcode lookups like WS13, B1 etc.
POSTCODES_IO_OUTCODE_URL = "https://api.postcodes.io/outcodes/{outcode}"


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
    #    accidentally treat "West Midlands" as a postcode.
    token = loc.upper().replace(" ", "")
    # 1–4 chars is a good guard rail for things like B1, B15, WS13
    if not token or len(token) > 4:
        return (None, None)

    try:
        resp = requests.get(POSTCODES_IO_OUTCODE_URL.format(outcode=token), timeout=10)
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


def _pay_stats_from_records(records: List[JobRecord]) -> Dict[str, Any]:
    rates = [r.pay_rate for r in records if r.pay_rate is not None]
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


def _role_fragments(raw_role_input: str) -> List[str]:
    """
    Turn the free-text role input into one or more lowercase fragments that we
    will use as case-insensitive substring matches.

    Examples:
      "HR Advisor"                  -> ["hr advisor"]
      "Support Worker / Senior SW"  -> ["support worker", "senior sw"]
    """
    text = (raw_role_input or "").strip()
    if not text:
        return []

    # Split on common separators, but keep words together.
    parts = re.split(r"[/,;|+]", text)
    seen: set[str] = set()
    fragments: List[str] = []

    for part in parts:
        frag = part.strip()
        if not frag:
            continue
        low = frag.lower()
        if low in seen:
            continue
        seen.add(low)
        fragments.append(low)

    # If we failed to produce any fragments for some reason, fall back to the
    # whole string lowercased.
    if not fragments:
        fragments.append(text.lower())

    return fragments


def _build_timeseries(raw_role_input: str, counties: List[str], lookback_days: int) -> Dict[str, Any]:
    """
    Build a simple daily median pay time series for the slice, aggregated
    across counties in the radius, and fit a straight line for a
    very lightweight 'forecast' 90 days ahead.

    We treat the role input as free text and include any JobSummaryDaily row
    whose job_role_group contains one of the role fragments (case-insensitive).
    """
    today = date.today()
    start_date = today - timedelta(days=lookback_days)

    fragments = _role_fragments(raw_role_input)
    if not fragments:
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

    # Apply role substring filters (case-insensitive) on canonical job_role_group.
    role_conditions = []
    for frag in fragments:
        pattern = f"%{frag}%"
        role_conditions.append(
            func.lower(JobSummaryDaily.job_role_group).like(pattern)
        )

    if role_conditions:
        q = q.filter(or_(*role_conditions))

    if counties:
        q = q.filter(JobSummaryDaily.county.in_(counties))

    q = q.group_by(JobSummaryDaily.date).order_by(JobSummaryDaily.date.asc())

    rows = q.all()
    if not rows:
        return {"points": [], "forecast_3m": None}

    points = [
        {"date": r.date.isoformat(), "median_pay_rate": float(r.median_pay_rate)}
        for r in rows
    ]

    # Linear regression on (ordinal date, median)
    xs = [r.date.toordinal() for r in rows]
    ys = [float(r.median_pay_rate) for r in rows]

    n = len(xs)
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n

    s_xx = sum((x - x_mean) ** 2 for x in xs)
    s_xy = sum((x - x_mean) * (y - y_mean) for x in ys)
    slope = 0.0 if s_xx == 0 else s_xy / s_xx
    intercept = y_mean - slope * x_mean

    future_date = today + timedelta(days=90)
    future_x = future_date.toordinal()
    forecast_val = slope * future_x + intercept

    return {
        "points": points,
        "forecast_3m": forecast_val,
    }


@bp.route("/recruiter/radar")
@login_required
def recruiter_radar():
    """
    Top-level recruiter radar view.

    We just need enough context to build the role typeahead; everything else
    is driven by the /api/recruiter/radar endpoint.
    """
    role_options = _canonical_role_filter_options()
    return render_template("recruiter_radar.html", role_options=role_options)


@bp.route("/api/recruiter/radar")
@login_required
def api_recruiter_radar():
    """
    One-shot 'recruiter radar' API.

    Query params:
      - role (required): free-text job role text; we include all roles where
        job_role_group or job_role contains this text (case-insensitive).
      - location (required): UK postcode or outcode (e.g. WS13)
      - radius_miles: 5, 15, 25 (default 15)
      - lookback_days: history window for demand/forecast (default 180)
    """
    raw_role = (request.args.get("role") or "").strip()
    raw_location = (request.args.get("location") or "").strip()
    radius_miles = request.args.get("radius_miles", type=float) or 15.0
    lookback_days = request.args.get("lookback_days", type=int) or 180

    if not raw_role:
        return jsonify({"error": "Job role is required."}), 400
    if not raw_location:
        return jsonify({"error": "Location is required."}), 400

    # Turn the input into one or more lowercase fragments we will use for
    # substring matching (e.g. "HR Advisor" -> ["hr advisor"]).
    role_fragments = _role_fragments(raw_role)
    if not role_fragments:
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

    # Base JobRecord slice for 'current adverts' and employer detail.
    # Use created_at as a simple 'recentness' clock.
    today = datetime.utcnow().date()
    start_dt = datetime.combine(today - timedelta(days=lookback_days), datetime.min.time())

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

    # Apply case-insensitive substring matching on BOTH job_role_group and job_role.
    role_clauses = []
    for frag in role_fragments:
        pattern = f"%{frag}%"
        role_clauses.append(
            or_(
                func.lower(JobRecord.job_role_group).like(pattern),
                func.lower(JobRecord.job_role).like(pattern),
            )
        )

    if role_clauses:
        jr_q = jr_q.filter(or_(*role_clauses))

    records: List[JobRecord] = jr_q.limit(5000).all()
    pay_stats = _pay_stats_from_records(records)

    counties = sorted({r.county for r in records if r.county})
    timeseries = _build_timeseries(raw_role, counties, lookback_days)

    # Simple demand signal: adverts per day over the window.
    adverts_count = len(records)
    adverts_per_day = adverts_count / float(lookback_days) if lookback_days > 0 else adverts_count
    if adverts_per_day >= 3:
        demand_level = "high"
    elif adverts_per_day >= 1:
        demand_level = "moderate"
    elif adverts_per_day > 0:
        demand_level = "low"
    else:
        demand_level = "none"

    # Recommended rate: start with current avg, nudge if demand is hot.
    base_rate = pay_stats["avg_rate"]
    if base_rate is None:
        recommended_rate = None
    else:
        if demand_level == "high":
            recommended_rate = base_rate + 1.0
        elif demand_level == "moderate":
            recommended_rate = base_rate + 0.5
        else:
            recommended_rate = base_rate

    # Latest adverts slice.
    recent_records = (
        jr_q.order_by(JobRecord.created_at.desc().nullslast())
        .limit(10)
        .all()
    )

    recent_roles = [
        {
            "company_name": r.company_name,
            "job_role": r.job_role,
            "job_role_group": r.job_role_group,
            "postcode": r.postcode,
            "county": r.county,
            "pay_rate": r.pay_rate,
            "created_at": r.created_at.isoformat() if r.created_at else None,
        }
        for r in recent_records
    ]

    # Distinct employers in slice.
    employer_count = (
        db.session.query(func.count(func.distinct(JobRecord.company_name)))
        .filter(jr_q.whereclause)
        .scalar()
    )

    # For debugging: what roles actually matched?
    distinct_roles = (
        db.session.query(
            JobRecord.job_role,
            JobRecord.job_role_group,
        )
        .filter(jr_q.whereclause)
        .limit(50)
        .all()
    )
    debug_roles = [
        {
            "job_role": jr.job_role,
            "job_role_group": jr.job_role_group,
        }
        for jr in distinct_roles
    ]

    response = {
        "params": {
            "role": raw_role,
            "location": raw_location,
            "radius_miles": radius_miles,
            "lookback_days": lookback_days,
        },
        "centre": {
            "lat": centre_lat,
            "lon": centre_lon,
        },
        "slice": {
            "adverts_count": adverts_count,
            "employers_count": employer_count or 0,
            "demand_level": demand_level,
        },
        "pay": {
            "min_rate": pay_stats["min_rate"],
            "max_rate": pay_stats["max_rate"],
            "avg_rate": pay_stats["avg_rate"],
            "median_rate": pay_stats["median_rate"],
            "forecast_3m": timeseries.get("forecast_3m"),
            "recommended_rate": recommended_rate,
        },
        "timeseries": timeseries,
        "recent_roles": recent_roles,
        "ai_commentary": None,
        "debug": {
            "role_fragments": role_fragments,
            "matched_sample_roles": debug_roles,
        },
    }

    return jsonify(response)
