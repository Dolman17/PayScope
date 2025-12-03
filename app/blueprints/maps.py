# app/blueprints/maps.py
from __future__ import annotations

from datetime import date, timedelta

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required
from sqlalchemy import or_, func
from datetime import date

from extensions import db
from models import JobRecord, JobSummaryDaily, OnsEarnings
from .utils import (
    logo_url_for,
    company_has_logo,
    build_role_groups_for_sector,
    get_raw_roles_for_group,
)

bp = Blueprint("maps", __name__)


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

    # Get distinct roles for THIS sector only
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
    # Sector list for dropdown
    sectors = [
        s[0]
        for s in db.session.query(JobRecord.sector)
        .filter(JobRecord.sector.isnot(None))
        .distinct()
        .order_by(JobRecord.sector)
        .all()
    ]

    # Default to last 30 days
    default_end = date.today()
    default_start = default_end - timedelta(days=30)

    return render_template(
        "pay_explorer.html",
        sectors=sectors,
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
    """
    # Dates
    default_end = date.today()
    default_start = default_end - timedelta(days=30)

    def parse_date(param, fallback):
        s = request.args.get(param)
        if not s:
            return fallback
        try:
            return date.fromisoformat(s)
        except ValueError:
            return fallback

    start_date = parse_date("start_date", default_start)
    end_date = parse_date("end_date", default_end)

    # Filters
    sector = (request.args.get("sector") or "").strip() or None
    job_role_group = (request.args.get("job_role_group") or "").strip() or None
    group_by = request.args.get("group_by", "county")

    # Common WHERE filters
    filters = [
        JobSummaryDaily.date >= start_date,
        JobSummaryDaily.date <= end_date,
    ]
    if sector:
        filters.append(JobSummaryDaily.sector == sector)
    if job_role_group:
        filters.append(JobSummaryDaily.job_role_group == job_role_group)

    # ------------------------------------------------------------------
    # Build SELECT + GROUP BY depending on group_by
    # ------------------------------------------------------------------
    if group_by == "sector":
        q = db.session.query(
            JobSummaryDaily.sector.label("sector"),
            func.sum(JobSummaryDaily.adverts_count).label("adverts_count"),
            func.avg(JobSummaryDaily.median_pay_rate).label("median_pay_rate"),
            func.avg(JobSummaryDaily.p25_pay_rate).label("p25_pay_rate"),
            func.avg(JobSummaryDaily.p75_pay_rate).label("p75_pay_rate"),
            func.min(JobSummaryDaily.min_pay_rate).label("min_pay_rate"),
            func.max(JobSummaryDaily.max_pay_rate).label("max_pay_rate"),
        ).filter(*filters).group_by(JobSummaryDaily.sector)

    elif group_by == "sector_county":
        q = db.session.query(
            JobSummaryDaily.sector.label("sector"),
            JobSummaryDaily.county.label("county"),
            func.sum(JobSummaryDaily.adverts_count).label("adverts_count"),
            func.avg(JobSummaryDaily.median_pay_rate).label("median_pay_rate"),
            func.avg(JobSummaryDaily.p25_pay_rate).label("p25_pay_rate"),
            func.avg(JobSummaryDaily.p75_pay_rate).label("p75_pay_rate"),
            func.min(JobSummaryDaily.min_pay_rate).label("min_pay_rate"),
            func.max(JobSummaryDaily.max_pay_rate).label("max_pay_rate"),
        ).filter(*filters).group_by(JobSummaryDaily.sector, JobSummaryDaily.county)

    else:  # default: group_by == "county"
        group_by = "county"  # normalise any weird input
        q = db.session.query(
            JobSummaryDaily.county.label("county"),
            func.sum(JobSummaryDaily.adverts_count).label("adverts_count"),
            func.avg(JobSummaryDaily.median_pay_rate).label("median_pay_rate"),
            func.avg(JobSummaryDaily.p25_pay_rate).label("p25_pay_rate"),
            func.avg(JobSummaryDaily.p75_pay_rate).label("p75_pay_rate"),
            func.min(JobSummaryDaily.min_pay_rate).label("min_pay_rate"),
            func.max(JobSummaryDaily.max_pay_rate).label("max_pay_rate"),
        ).filter(*filters).group_by(JobSummaryDaily.county)

    rows = q.all()

    # Build basic result dicts first
    results = []
    for row in rows:
        county = getattr(row, "county", None)
        sector_val = getattr(row, "sector", None)
        results.append(
            {
                "county": county,
                "sector": sector_val,
                "adverts_count": int(row.adverts_count or 0),
                "median_pay_rate": float(row.median_pay_rate) if row.median_pay_rate is not None else None,
                "p25_pay_rate": float(row.p25_pay_rate) if row.p25_pay_rate is not None else None,
                "p75_pay_rate": float(row.p75_pay_rate) if row.p75_pay_rate is not None else None,
                "min_pay_rate": float(row.min_pay_rate) if row.min_pay_rate is not None else None,
                "max_pay_rate": float(row.max_pay_rate) if row.max_pay_rate is not None else None,
            }
        )

    # ------------------------------------------------------------------
    # ONS overlay: only meaningful when we have a county dimension
    # ------------------------------------------------------------------
    ons_available = False
    ons_year = None

    if results and group_by in ("county", "sector_county"):
        counties = sorted({r["county"] for r in results if r["county"]})
        if counties:
            # OnsEarnings has year + geography_name + value
            ons_rows = (
                OnsEarnings.query.filter(
                    OnsEarnings.geography_name.in_(counties),
                )
                .order_by(OnsEarnings.geography_name, OnsEarnings.year.desc())
                .all()
            )

            ons_map = {}
            for r in ons_rows:
                name = (r.geography_name or "").strip()
                if not name:
                    continue
                if r.value is None:
                    continue
                # keep latest year per county
                if name not in ons_map or r.year > ons_map[name]["year"]:
                    ons_map[name] = {
                        "year": r.year,
                        "median_hourly": float(r.value),
                    }

            if ons_map:
                ons_available = True
                ons_year = max(v["year"] for v in ons_map.values())

                for r in results:
                    c = (r.get("county") or "").strip()
                    info = ons_map.get(c)
                    if not info:
                        r["ons_median_hourly"] = None
                        r["pay_vs_ons"] = None
                        r["pay_vs_ons_pct"] = None
                        continue

                    advertised = r.get("median_pay_rate")
                    ons_val = info["median_hourly"]
                    r["ons_median_hourly"] = ons_val
                    if advertised is None or ons_val is None:
                        r["pay_vs_ons"] = None
                        r["pay_vs_ons_pct"] = None
                    else:
                        gap = advertised - ons_val
                        r["pay_vs_ons"] = gap
                        r["pay_vs_ons_pct"] = (gap / ons_val * 100) if ons_val else None

    return jsonify(
        {
            "params": {
                "sector": sector,
                "job_role_group": job_role_group,
                "group_by": group_by,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
            },
            "results": results,
            "ons_available": ons_available,
            "ons_year": ons_year,
        }
    )


