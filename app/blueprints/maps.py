# app/blueprints/maps.py
from __future__ import annotations

import csv
import math
import os
from datetime import date, datetime, timedelta
from io import StringIO

from flask import Blueprint, render_template, request, jsonify, make_response
from flask_login import login_required
from sqlalchemy import or_, func

from extensions import db
from models import JobRecord, JobSummaryDaily, OnsEarnings
from .utils import (
    logo_url_for,
    company_has_logo,
    build_role_groups_for_sector,
    get_raw_roles_for_group,
    geocode_postcode_cached,
)
from .pay_compare import get_pay_explorer_data

bp = Blueprint("maps", __name__)

# Optional OpenAI client for AI commentary
try:
    from openai import OpenAI  # type: ignore
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore[assignment]


def _get_openai_client():
    if OpenAI is None:
        return None
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        return OpenAI()
    except Exception:
        return None


_openai_client = _get_openai_client()


def _ai_enabled() -> bool:
    return _openai_client is not None


# -------------------------------------------------------------------
# Existing map views
# -------------------------------------------------------------------
@bp.route("/map")
@login_required
def map_sector_select():
    sectors = [
        s[0]
        for s in db.session.query(JobRecord.sector)
        .filter(JobRecord.sector.isnot(None))
        .distinct()
        .order_by(JobRecord.sector)
        .all()
    ]
    return render_template("map_select.html", sectors=sectors)


@bp.route("/map/<sector>")
@login_required
def sector_map(sector: str):
    """
    Sector-specific map view.

    - Sector is taken from the URL segment.
    - Filters (job_role/min_pay/max_pay) are optional GET params.
    - job_roles in the dropdown are limited to roles that actually exist
      for this sector.
    """

    job_role = request.args.get("job_role") or ""
    min_pay = request.args.get("min_pay", type=float)
    max_pay = request.args.get("max_pay", type=float)

    # Get distinct roles for THIS sector only (grouped via helper)
    raw_roles = build_role_groups_for_sector(sector)

    # Force everything to a clean, non-empty string
    job_roles = []
    for r in raw_roles:
        s = str(r).strip()
        if s:
            job_roles.append(s)

    # Optional: debug to logs so you can see what's coming through
    print(f"[DEBUG] job_roles for sector '{sector}': {job_roles[:20]}")

    return render_template(
        "map.html",
        sector=sector,
        records=[],  # markers will be loaded via API
        job_roles=job_roles,
        filters={
            "job_role": job_role or "",
            "min_pay": min_pay or "",
            "max_pay": max_pay or "",
        },
    )


def _apply_map_filters(q, sector: str, args):
    """
    Apply sector + filters to the base JobRecord query.

    - Always constrains to sector.
    - job_role filter uses get_raw_roles_for_group so we can later group
      multiple raw titles under one UI label without changing this code.
    """
    # Always lock to sector
    q = q.filter(JobRecord.sector == sector)

    # Job role group from query string
    group_label = (args.get("job_role") or "").strip()
    if group_label:
        raw_roles = get_raw_roles_for_group(group_label, sector)
        if raw_roles:
            q = q.filter(JobRecord.job_role.in_(raw_roles))

    # Pay range
    min_pay = args.get("min_pay", type=float)
    max_pay = args.get("max_pay", type=float)
    if min_pay is not None:
        q = q.filter(JobRecord.pay_rate >= float(min_pay))
    if max_pay is not None:
        q = q.filter(JobRecord.pay_rate <= float(max_pay))

    # Optional free text for map via ?q=
    txt = (args.get("q") or "").strip()
    if txt:
        like = f"%{txt}%"
        q = q.filter(
            or_(
                JobRecord.company_name.ilike(like),
                JobRecord.job_role.ilike(like),
                JobRecord.postcode.ilike(like),
            )
        )

    return q


def _compute_bins(rates):
    """Return thresholds [t1,t2,t3,t4] for 5 bins (quintiles)."""
    rs = [float(r) for r in rates if r is not None]
    if not rs:
        return [0, 0, 0, 0]
    rs.sort()

    def pct(p):
        i = max(0, min(len(rs) - 1, int(round(p * (len(rs) - 1)))))
        return rs[i]

    return [pct(0.2), pct(0.4), pct(0.6), pct(0.8)]


@bp.route("/api/points")
@login_required
def api_points():
    """Return GeoJSON feature collection for points within bbox and filters."""
    sector = request.args.get("sector")
    bbox = (request.args.get("bbox") or "").split(",")

    if not sector:
        return jsonify({"error": "sector is required"}), 400
    if len(bbox) != 4:
        return jsonify({"error": "bbox required: minLon,minLat,maxLon,maxLat"}), 400

    try:
        min_lon, min_lat, max_lon, max_lat = map(float, bbox)
    except ValueError:
        return jsonify({"error": "bbox values must be numbers"}), 400

    q = (
        db.session.query(JobRecord)
        .filter(
            JobRecord.latitude.isnot(None),
            JobRecord.longitude.isnot(None),
            JobRecord.longitude >= min_lon,
            JobRecord.longitude <= max_lon,
            JobRecord.latitude >= min_lat,
            JobRecord.latitude <= max_lat,
        )
    )
    q = _apply_map_filters(q, sector, request.args)

    # Compute quintile thresholds on the filtered set in view
    rates = [r[0] for r in q.with_entities(JobRecord.pay_rate).all()]
    thresholds = _compute_bins(rates)

    def bin_for(rate: float) -> int:
        if rate is None:
            return 1
        r = float(rate)
        t1, t2, t3, t4 = thresholds
        if r <= t1:
            return 1
        if r <= t2:
            return 2
        if r <= t3:
            return 3
        if r <= t4:
            return 4
        return 5

    features = []
    for rec in q.limit(5000):
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [rec.longitude, rec.latitude],
                },
                "properties": {
                    "id": rec.id,
                    "company_id": rec.company_id,
                    "name": rec.company_name,
                    "role": rec.job_role,
                    "sector": rec.sector,
                    "county": rec.county,
                    "postcode": rec.postcode,
                    "rate": float(rec.pay_rate) if rec.pay_rate is not None else None,
                    "rate_bin": bin_for(rec.pay_rate),
                    "logo_url": logo_url_for(rec.company_id or "placeholder"),
                    "has_logo": company_has_logo(rec.company_id),
                    "imported_month": rec.imported_month,
                    "imported_year": rec.imported_year,
                },
            }
        )

    return jsonify(
        {
            "type": "FeatureCollection",
            "features": features,
            "thresholds": thresholds,
        }
    )


# -------------------------------------------------------------------
# Pay Explorer: view + API with ONS overlay
# -------------------------------------------------------------------
@bp.route("/pay-explorer")
@login_required
def pay_explorer():
    """
    Pay Explorer view.

    - Sectors: distinct JobSummaryDaily.sector values.
    - role_groups_by_sector: mapping used by the front-end to show
      sector-specific job_role_group options, plus an "__ALL__" key
      for when no sector is selected.
    """
    # Sector list (for dropdown) from JobSummaryDaily so it matches the summary data
    sectors = [
        s[0]
        for s in db.session.query(JobSummaryDaily.sector)
        .filter(JobSummaryDaily.sector.isnot(None))
        .distinct()
        .order_by(JobSummaryDaily.sector)
        .all()
    ]

    # Build mapping: sector -> [job_role_group...]
    role_map: dict[str, set[str]] = {}
    all_groups: set[str] = set()

    rows = (
        db.session.query(
            JobSummaryDaily.sector,
            JobSummaryDaily.job_role_group,
        )
        .filter(
            JobSummaryDaily.sector.isnot(None),
            JobSummaryDaily.job_role_group.isnot(None),
        )
        .distinct()
        .all()
    )

    for sector_val, group_val in rows:
        sec = (sector_val or "").strip()
        grp = (group_val or "").strip()
        if not sec or not grp:
            continue

        role_map.setdefault(sec, set()).add(grp)
        all_groups.add(grp)

    role_groups_by_sector = {
        sec: sorted(groups) for sec, groups in role_map.items()
    }
    # Special key when no sector is selected
    role_groups_by_sector["__ALL__"] = sorted(all_groups)

    # Default to last 30 days
    default_end = date.today()
    default_start = default_end - timedelta(days=30)

    return render_template(
        "pay_explorer.html",
        sectors=sectors,
        role_groups_by_sector=role_groups_by_sector,
        default_start=default_start,
        default_end=default_end,
    )


def _load_ons_medians_for_year(ashe_year: int) -> dict[str, float]:
    """
    Load ONS ASHE median values for a given year, keyed by geography_name (lowercased).

    - If multiple measures exist per geography, we pick a 'best' one
      with a simple priority (e.g. 20101/20100 > others).
    """
    rows = OnsEarnings.query.filter_by(year=ashe_year).all()
    index: dict[str, OnsEarnings] = {}

    def score(measure_code: str | None) -> int:
        code = (measure_code or "").strip()
        if code in ("20101", "20100", "20701"):
            return 2
        return 1

    for r in rows:
        name = (r.geography_name or "").strip()
        if not name:
            continue
        key = name.lower()
        if key not in index:
            index[key] = r
        else:
            if score(r.measure_code) > score(index[key].measure_code):
                index[key] = r

    # Flatten to { "lancashire": 13.42, ... }
    return {k: (v.value if v.value is not None else None) for k, v in index.items()}


@bp.route("/api/pay-compare")
@login_required
def api_pay_compare():
    """
    Compare advertised pay using JobSummaryDaily and overlay ONS median earnings.

    Request params:
      - sector (optional)
      - job_role_group (optional)
      - group_by: 'county' | 'sector' | 'sector_county'
      - start_date, end_date (YYYY-MM-DD)
      - format (optional): 'json' (default) | 'csv'
    """
    sector = (request.args.get("sector") or "").strip() or None
    job_role_group = (request.args.get("job_role_group") or "").strip() or None
    group_by = (request.args.get("group_by") or "county").strip() or "county"
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")
    format_ = (request.args.get("format") or "json").lower()

    data = get_pay_explorer_data(
        start_date_str=start_date,
        end_date_str=end_date,
        sector=sector,
        job_role_group=job_role_group,
        group_by=group_by,
    )

    # CSV export branch
    if format_ == "csv":
        results = data.get("results", [])
        params = data.get("params", {}) or {}
        group_by_param = params.get("group_by") or group_by or "county"
        start = params.get("start_date") or start_date or ""
        end = params.get("end_date") or end_date or ""

        output = StringIO()
        writer = csv.writer(output)

        # Header row – matches table + some extras
        writer.writerow(
            [
                "Area",
                "Sector",
                "Adverts count",
                "Advertised median £/hr",
                "ONS median £/hr",
                "Gap £/hr",
                "Gap %",
                "P25 £/hr",
                "P75 £/hr",
                "Min £/hr",
                "Max £/hr",
            ]
        )

        for row in results:
            county = row.get("county")
            sector_val = row.get("sector")

            if group_by_param == "sector":
                area = sector_val or "Unknown sector"
            elif group_by_param == "sector_county":
                area = f"{sector_val or 'Unknown sector'} – {county or 'Unknown county'}"
            else:  # county
                area = county or "Unknown county"

            writer.writerow(
                [
                    area,
                    sector_val or "",
                    row.get("adverts_count") or 0,
                    row.get("median_pay_rate") or "",
                    row.get("ons_median_hourly") or "",
                    row.get("pay_vs_ons") or "",
                    row.get("pay_vs_ons_pct") or "",
                    row.get("p25_pay_rate") or "",
                    row.get("p75_pay_rate") or "",
                    row.get("min_pay_rate") or "",
                    row.get("max_pay_rate") or "",
                ]
            )

        csv_data = output.getvalue()
        resp = make_response(csv_data)
        filename = f"pay_explorer_{group_by_param}_{start}_{end}.csv".replace(" ", "_")
        resp.headers["Content-Type"] = "text/csv"
        resp.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return resp

    # Default: JSON for the front-end
    return jsonify(data)


# -------------------------------------------------------------------
# Recruiter Radar – role + postcode radius slice
# -------------------------------------------------------------------

def _haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float | None:
    """Great-circle distance between two lat/lon points in miles."""
    try:
        lat1 = float(lat1)
        lon1 = float(lon1)
        lat2 = float(lat2)
        lon2 = float(lon2)
    except (TypeError, ValueError):
        return None

    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(
        dlambda / 2
    ) ** 2
    c = 2 * math.asin(math.sqrt(a))
    earth_radius_km = 6371.0
    distance_km = earth_radius_km * c
    return distance_km * 0.621371  # miles


def _percentile(values: list[float], p: float) -> float | None:
    """Simple percentile helper (0–1)."""
    if not values:
        return None
    values_sorted = sorted(values)
    if len(values_sorted) == 1:
        return values_sorted[0]
    if p <= 0:
        return values_sorted[0]
    if p >= 1:
        return values_sorted[-1]

    idx = p * (len(values_sorted) - 1)
    lower = int(math.floor(idx))
    upper = int(math.ceil(idx))
    if lower == upper:
        return values_sorted[lower]
    frac = idx - lower
    return values_sorted[lower] * (1 - frac) + values_sorted[upper] * frac


def _linear_regression_forecast(
    points: list[tuple[int, float]], horizon_days: int
) -> dict | None:
    """
    Very small least-squares fit for y = m x + c.

    points: [(x_index, value)...], x_index in days from start.
    """
    if len(points) < 3:
        return None

    n = len(points)
    sumx = sum(p[0] for p in points)
    sumy = sum(p[1] for p in points)
    sumxy = sum(p[0] * p[1] for p in points)
    sumx2 = sum(p[0] ** 2 for p in points)

    denom = n * sumx2 - (sumx ** 2)
    if denom == 0:
        return None

    m = (n * sumxy - sumx * sumy) / denom
    c = (sumy - m * sumx) / n

    last_x = max(p[0] for p in points)
    current = m * last_x + c
    future_x = last_x + horizon_days
    forecast_val = m * future_x + c

    trend = "flat"
    if m > 0.001:
        trend = "up"
    elif m < -0.001:
        trend = "down"

    return {
        "slope_per_day": m,
        "current": current,
        "forecast": forecast_val,
        "horizon_days": horizon_days,
        "trend": trend,
    }


@bp.route("/recruiter/radar")
@login_required
def recruiter_radar():
    """
    Main Recruiter Radar page.

    - Role is free-text with datalist suggestions from job_role_group.
    - Location is a postcode / outcode.
    - Radius options: 5, 15, 25 miles (default 15).
    """
    # Distinct role groups for suggestions
    role_rows = (
        db.session.query(JobSummaryDaily.job_role_group)
        .filter(JobSummaryDaily.job_role_group.isnot(None))
        .distinct()
        .order_by(JobSummaryDaily.job_role_group)
        .all()
    )
    role_options = [r[0] for r in role_rows if r[0]]

    selected_role = (request.args.get("role") or "").strip()
    selected_location = (request.args.get("location") or "").strip()
    radius = request.args.get("radius_miles", type=int)
    if radius not in (5, 15, 25):
        radius = 15

    lookback_days = request.args.get("lookback_days", type=int) or 180

    return render_template(
        "recruiter_radar.html",
        role_options=role_options,
        selected_role=selected_role,
        selected_location=selected_location,
        selected_radius=radius,
        lookback_days=lookback_days,
    )


@bp.route("/api/recruiter/radar")
@login_required
def api_recruiter_radar():
    """
    Data endpoint for Recruiter Radar.

    Query params:
      - role: job_role_group (required)
      - location: postcode / outcode (required)
      - radius_miles: 5, 15, 25 (default 15)
      - lookback_days: default 180
    """
    role = (request.args.get("role") or "").strip()
    location = (request.args.get("location") or "").strip()
    radius_miles = request.args.get("radius_miles", type=float) or 15.0
    lookback_days = request.args.get("lookback_days", type=int) or 180

    if not role:
        return jsonify({"error": "role is required"}), 400
    if not location:
        return jsonify({"error": "location is required"}), 400

    center_lat, center_lon = geocode_postcode_cached(location)
    if center_lat is None or center_lon is None:
        return (
            jsonify(
                {
                    "error": "Could not geocode that location. "
                    "Please use a valid UK postcode or outcode."
                }
            ),
            400,
        )

    radius_miles = float(radius_miles or 15.0)
    if radius_miles <= 0:
        radius_miles = 15.0

    today = date.today()
    from_date = today - timedelta(days=lookback_days)

    # Pull JobRecords for role + timeframe (we'll filter radius in Python)
    rec_query = (
        JobRecord.query.filter(
            JobRecord.job_role_group == role,
            JobRecord.latitude.isnot(None),
            JobRecord.longitude.isnot(None),
            JobRecord.pay_rate.isnot(None),
            JobRecord.created_at >= datetime.combine(from_date, datetime.min.time()),
        )
    )

    records = rec_query.all()

    # Radius filter
    records_in_radius: list[tuple[JobRecord, float]] = []
    counties: set[str] = set()

    for rec in records:
        dist = _haversine_miles(center_lat, center_lon, rec.latitude, rec.longitude)
        if dist is None or dist > radius_miles:
            continue
        records_in_radius.append((rec, dist))
        if rec.county:
            counties.add(rec.county)

    # If nothing in radius, return a structured empty payload
    if not records_in_radius:
        return jsonify(
            {
                "params": {
                    "role": role,
                    "location": location,
                    "radius_miles": radius_miles,
                    "lookback_days": lookback_days,
                },
                "location": {
                    "input": location,
                    "lat": center_lat,
                    "lon": center_lon,
                },
                "summary": None,
                "forecast": None,
                "demand": None,
                "competition": None,
                "recent_postings": [],
                "trend": None,
                "recommended": None,
            }
        )

    rec_only = [r for (r, _) in records_in_radius]

    # Pay distribution from JobRecord
    pay_values = [float(r.pay_rate) for r in rec_only if r.pay_rate is not None]
    pay_values_sorted = sorted(pay_values)

    avg_pay = sum(pay_values) / len(pay_values) if pay_values else None
    median_pay = _percentile(pay_values_sorted, 0.5) if pay_values_sorted else None
    p25 = _percentile(pay_values_sorted, 0.25) if pay_values_sorted else None
    p75 = _percentile(pay_values_sorted, 0.75) if pay_values_sorted else None
    min_pay = min(pay_values) if pay_values else None
    max_pay = max(pay_values) if pay_values else None

    summary = {
        "records_count": len(rec_only),
        "area_counties": sorted(counties),
        "avg_pay": avg_pay,
        "median_pay": median_pay,
        "p25_pay": p25,
        "p75_pay": p75,
        "min_pay": min_pay,
        "max_pay": max_pay,
    }

    # Timeseries + demand from JobSummaryDaily (county-based)
    trend_points: list[dict] = []
    forecast: dict | None = None
    demand: dict | None = None

    if counties:
        start_summary = today - timedelta(days=lookback_days)
        summary_rows = (
            JobSummaryDaily.query.filter(
                JobSummaryDaily.date >= start_summary,
                JobSummaryDaily.date <= today,
                JobSummaryDaily.job_role_group == role,
                JobSummaryDaily.county.in_(list(counties)),
            ).all()
        )

        by_day: dict[date, dict[str, float]] = {}
        for row in summary_rows:
            if row.median_pay_rate is None or not row.adverts_count:
                continue
            d = row.date
            bucket = by_day.setdefault(d, {"adverts": 0.0, "weighted_sum": 0.0})
            bucket["adverts"] += float(row.adverts_count or 0)
            bucket["weighted_sum"] += float(row.median_pay_rate) * float(
                row.adverts_count or 0
            )

        for d, bucket in sorted(by_day.items()):
            if bucket["adverts"] <= 0:
                continue
            median_day = bucket["weighted_sum"] / bucket["adverts"]
            trend_points.append(
                {
                    "date": d.isoformat(),
                    "median_pay_rate": median_day,
                    "adverts_count": int(bucket["adverts"]),
                }
            )

        # Regression on the daily median series (if we have enough points)
        reg_points: list[tuple[int, float]] = []
        for p in trend_points:
            d = date.fromisoformat(p["date"])
            x = (d - start_summary).days
            reg_points.append((x, float(p["median_pay_rate"])))

        forecast_info = _linear_regression_forecast(reg_points, horizon_days=90)
        if forecast_info:
            forecast = {
                "current_median": forecast_info["current"],
                "forecast_median": forecast_info["forecast"],
                "horizon_days": forecast_info["horizon_days"],
                "trend": forecast_info["trend"],
                "slope_per_month": forecast_info["slope_per_day"] * 30.0,
            }

        # Demand: adverts in last 30 / 90 days
        last_30 = today - timedelta(days=30)
        last_90 = today - timedelta(days=90)
        adverts30 = 0
        adverts90 = 0
        for d, bucket in by_day.items():
            if d >= last_30:
                adverts30 += int(bucket["adverts"])
            if d >= last_90:
                adverts90 += int(bucket["adverts"])

        demand = {
            "adverts_last_30": adverts30,
            "adverts_last_90": adverts90,
            "lookback_days": lookback_days,
        }

    # Competition: distinct companies + top companies by advert count
    company_stats: dict[tuple[str, str], dict[str, float]] = {}
    for rec in rec_only:
        key = (rec.company_id or "", rec.company_name or "Unknown employer")
        stats = company_stats.setdefault(
            key,
            {"count": 0.0, "pay_sum": 0.0, "pay_n": 0.0},
        )
        stats["count"] += 1.0
        if rec.pay_rate is not None:
            stats["pay_sum"] += float(rec.pay_rate)
            stats["pay_n"] += 1.0

    top_companies: list[dict] = []
    for (company_id, company_name), stats in sorted(
        company_stats.items(), key=lambda kv: kv[1]["count"], reverse=True
    )[:5]:
        avg_company_pay = (
            stats["pay_sum"] / stats["pay_n"] if stats["pay_n"] else None
        )
        top_companies.append(
            {
                "company_id": company_id or None,
                "company_name": company_name,
                "adverts_count": int(stats["count"]),
                "avg_pay_rate": avg_company_pay,
            }
        )

    competition = {
        "distinct_companies": len(company_stats),
        "top_companies": top_companies,
    }

    # Most recent postings (up to 10)
    def _safe_created_at(r: JobRecord):
        return getattr(r, "created_at", None) or datetime.min

    sorted_recent = sorted(rec_only, key=_safe_created_at, reverse=True)
    recent_postings: list[dict] = []
    for rec in sorted_recent[:10]:
        created = getattr(rec, "created_at", None)
        recent_postings.append(
            {
                "company_name": rec.company_name,
                "job_role": rec.job_role,
                "pay_rate": float(rec.pay_rate) if rec.pay_rate is not None else None,
                "county": rec.county,
                "postcode": rec.postcode,
                "created_at": created.isoformat() if created is not None else None,
            }
        )

    # Recommended rate: aim roughly at P75, blended with 3-month forecast if higher
    recommended_rate = None
    basis = None
    if p75 is not None:
        recommended_rate = p75
        basis = "75th percentile of advertised rates in this radius"
    elif median_pay is not None:
        recommended_rate = median_pay
        basis = "Median of advertised rates in this radius"

    if (
        forecast
        and recommended_rate is not None
        and forecast.get("forecast_median") is not None
    ):
        forecast_med = float(forecast["forecast_median"])
        if forecast_med > recommended_rate:
            recommended_rate = (recommended_rate + forecast_med) / 2.0
            basis = (basis or "Blended rate") + " blended with 3-month forecast"

    recommended = {
        "recommended_rate": recommended_rate,
        "basis": basis,
    }

    trend = {"points": trend_points} if trend_points else None

    resp = {
        "params": {
            "role": role,
            "location": location,
            "radius_miles": radius_miles,
            "lookback_days": lookback_days,
        },
        "location": {
            "input": location,
            "lat": center_lat,
            "lon": center_lon,
        },
        "summary": summary,
        "forecast": forecast,
        "demand": demand,
        "competition": competition,
        "recent_postings": recent_postings,
        "trend": trend,
        "recommended": recommended,
    }
    return jsonify(resp)


@bp.route("/api/recruiter/radar/commentary", methods=["POST"])
@login_required
def api_recruiter_radar_commentary():
    """
    AI (or fallback) commentary for a Recruiter Radar slice.

    Expects JSON payload structured like the /api/recruiter/radar response.
    Returns:
      { "text": "...", "ai_enabled": bool }
    """
    payload = request.get_json(silent=True) or {}

    summary = payload.get("summary") or {}
    forecast = payload.get("forecast") or {}
    demand = payload.get("demand") or {}
    competition = payload.get("competition") or {}
    recommended = payload.get("recommended") or {}
    params = payload.get("params") or {}

    role = params.get("role") or "this role"
    location = params.get("location") or payload.get("location", {}).get(
        "input", ""
    )
    radius_miles = params.get("radius_miles")

    def fallback_comment() -> str:
        parts: list[str] = []

        recs = summary.get("records_count")
        if recs:
            parts.append(
                f"We found about {int(recs)} adverts for {role} within this radius."
            )

        median_pay = summary.get("median_pay")
        if isinstance(median_pay, (int, float)):
            parts.append(f"Typical advertised pay is around £{median_pay:.2f} per hour.")

        forecast_med = forecast.get("forecast_median")
        trend = (forecast.get("trend") or "").lower()
        if isinstance(forecast_med, (int, float)) and trend:
            if trend == "up":
                parts.append(
                    f"Rates appear to be trending upwards towards roughly £{forecast_med:.2f} over the next few months."
                )
            elif trend == "down":
                parts.append(
                    f"Rates appear to be easing down towards roughly £{forecast_med:.2f} over the next few months."
                )
            else:
                parts.append(
                    f"Rates look fairly stable at around £{forecast_med:.2f} over the next few months."
                )

        adverts30 = demand.get("adverts_last_30")
        if adverts30:
            parts.append(
                f"There have been around {int(adverts30)} adverts in the last 30 days, suggesting steady demand."
            )

        rec_rate = recommended.get("recommended_rate")
        if isinstance(rec_rate, (int, float)):
            parts.append(
                f"A competitive starting point would be to advertise at roughly £{rec_rate:.2f} per hour."
            )

        if not parts:
            return (
                "There isn't enough data in this slice yet to give a robust commentary. "
                "Try widening the radius or extending the lookback period."
            )

        return " ".join(parts)

    if not _ai_enabled():
        return jsonify({"text": fallback_comment(), "ai_enabled": False})

    try:
        prompt = (
            "You are an HR labour market analyst helping a recruiter.\n"
            "Write a short, plain-English commentary (120–180 words) summarising the pay and demand picture "
            "for the selected role and area. Assume the reader is a busy recruiter who wants quick, practical guidance.\n\n"
            f"Role: {role}\n"
            f"Location string: {location}\n"
            f"Radius miles: {radius_miles}\n\n"
            f"Summary stats: {summary}\n\n"
            f"Forecast: {forecast}\n\n"
            f"Demand: {demand}\n\n"
            f"Competition: {competition}\n\n"
            f"Recommended: {recommended}\n\n"
            "Focus on: where the role sits versus typical local pay, whether the market is heating up or cooling down, "
            "how crowded the employer landscape is, and what pay level you'd recommend to attract applicants. "
            "Avoid bullet points; use 2–3 short paragraphs."
        )

        completion = _openai_client.chat.completions.create(
            model=os.getenv("PAYSCOPE_OPENAI_MODEL", "gpt-4o-mini"),
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an expert UK labour market analyst who writes concise, practical commentary "
                        "for HR and recruitment."
                    ),
                },
                {
                    "role": "user",
                    "content": prompt,
                },
            ],
            max_tokens=260,
            temperature=0.4,
        )
        text = completion.choices[0].message.content.strip()
        return jsonify({"text": text, "ai_enabled": True})
    except Exception:
        # If AI fails for any reason, quietly fall back to deterministic commentary
        return jsonify({"text": fallback_comment(), "ai_enabled": False})
