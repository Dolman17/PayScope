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

