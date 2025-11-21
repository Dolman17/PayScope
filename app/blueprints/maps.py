# app/blueprints/maps.py
from __future__ import annotations

from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required
from sqlalchemy import or_
from extensions import db
from models import JobRecord
from .utils import logo_url_for, company_has_logo   # <-- NEW IMPORT

bp = Blueprint("maps", __name__)

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
    # Keep existing query params for UI controls; data now loaded via API
    job_role = request.args.get("job_role") or ""
    min_pay = request.args.get("min_pay", type=float)
    max_pay = request.args.get("max_pay", type=float)

    job_roles = [
        r[0]
        for r in db.session.query(JobRecord.job_role)
        .filter(JobRecord.job_role.isnot(None))
        .distinct()
        .order_by(JobRecord.job_role)
        .all()
    ]

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
    q = q.filter(JobRecord.sector == sector)
    jr = (args.get("job_role") or "").strip()
    if jr:
        q = q.filter(JobRecord.job_role == jr)

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
                    "has_logo": company_has_logo(rec.company_id),   # <-- NEW
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
