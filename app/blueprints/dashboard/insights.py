from __future__ import annotations

from typing import Dict, List

from collections import Counter
import statistics as stats

from flask import render_template, request, jsonify
from flask_login import login_required
from sqlalchemy import func
from werkzeug.datastructures import MultiDict

from extensions import db
from models import JobRecord, JobRoleMapping
from app.blueprints.utils import build_filters_from_request, get_filter_options
from . import bp
from .helpers import _canonical_role_filter_options


@bp.route("/insights")
@login_required
def insights():
    """
    Insights over JobRecord with filters.

    Uses JobRoleMapping to prefer canonical roles in analytics:
    job_role = COALESCE(JobRoleMapping.canonical_role, JobRecord.job_role)
    """
    # Capture role filters separately (these are canonical labels in the UI)
    role_filter_values = request.args.getlist("job_role") or request.args.getlist("role")

    # Filters map used only for template binding / pills
    filters_map = {
        "q": request.args.get("q"),
        "sector": request.args.getlist("sector"),
        "job_role": role_filter_values,
        "county": request.args.getlist("county"),
        "month": request.args.get("month"),
        "year": request.args.get("year"),
        "rate_min": request.args.get("rate_min"),
        "rate_max": request.args.get("rate_max"),
    }

    # For the generic builder, we do NOT want to filter on raw job_role.
    # Build a MultiDict copy of request.args with job_role/role stripped out.
    raw_args = request.args.to_dict(flat=False)
    raw_args.pop("job_role", None)
    raw_args.pop("role", None)
    params = MultiDict(raw_args)

    filters, extra_search = build_filters_from_request(params)

    base_q = JobRecord.query.filter(*filters)
    if extra_search is not None:
        base_q = extra_search(base_q)

    # Join to JobRoleMapping so we can use canonical roles where available
    base_q = base_q.outerjoin(
        JobRoleMapping,
        JobRecord.job_role == JobRoleMapping.raw_value,
    )

    # Apply role filter against canonical expression, not raw job_role.
    if role_filter_values:
        canonical_expr = func.coalesce(JobRoleMapping.canonical_role, JobRecord.job_role)
        base_q = base_q.filter(canonical_expr.in_(role_filter_values))

    # Subquery with canonical job_role label
    sq = base_q.with_entities(
        JobRecord.id.label("id"),
        JobRecord.company_id.label("company_id"),
        JobRecord.company_name.label("company_name"),
        JobRecord.sector.label("sector"),
        func.coalesce(JobRoleMapping.canonical_role, JobRecord.job_role).label("job_role"),
        JobRecord.postcode.label("postcode"),
        JobRecord.county.label("county"),
        JobRecord.pay_rate.label("pay_rate"),
        JobRecord.imported_month.label("imported_month"),
        JobRecord.imported_year.label("imported_year"),
    ).subquery(name="sq_records")

    # Aggregates
    agg_row = db.session.query(
        func.count(sq.c.id),
        func.avg(sq.c.pay_rate),
        func.min(sq.c.pay_rate),
        func.max(sq.c.pay_rate),
    ).first()

    total = int(agg_row[0] or 0)
    avg_rate = float(agg_row[1]) if agg_row[1] is not None else None
    min_rate = float(agg_row[2]) if agg_row[2] is not None else None
    max_rate = float(agg_row[3]) if agg_row[3] is not None else None

    # Top counties
    top_counties_rows = (
        db.session.query(sq.c.county, func.count(sq.c.id))
        .filter(sq.c.county.isnot(None))
        .group_by(sq.c.county)
        .order_by(func.count(sq.c.id).desc())
        .limit(10)
        .all()
    )
    top_counties = [{"county": c or "—", "count": int(n or 0)} for c, n in top_counties_rows]

    # Top roles (canonical where mapping exists)
    top_roles_rows = (
        db.session.query(sq.c.job_role, func.count(sq.c.id))
        .filter(sq.c.job_role.isnot(None))
        .group_by(sq.c.job_role)
        .order_by(func.count(sq.c.id).desc())
        .limit(10)
        .all()
    )
    top_roles = [{"role": r or "—", "count": int(n or 0)} for r, n in top_roles_rows]

    # Sector breakdown (count + avg/min/max pay per sector)
    sector_rows = (
        db.session.query(
            sq.c.sector,
            func.count(sq.c.id),
            func.avg(sq.c.pay_rate),
            func.min(sq.c.pay_rate),
            func.max(sq.c.pay_rate),
        )
        .group_by(sq.c.sector)
        .order_by(func.count(sq.c.id).desc())
        .all()
    )
    sector_stats = [
        {
            "sector": s or "Unknown",
            "count": int(n or 0),
            "avg_rate": float(a or 0.0) if a is not None else 0.0,
            "min_rate": float(mn or 0.0) if mn is not None else 0.0,
            "max_rate": float(mx or 0.0) if mx is not None else 0.0,
        }
        for (s, n, a, mn, mx) in sector_rows
    ]

    # Distribution bands
    def _band_count(lower, upper, include_lower=True, include_upper=False):
        q = db.session.query(func.count(sq.c.id))
        if lower is not None:
            q = q.filter(sq.c.pay_rate >= lower if include_lower else sq.c.pay_rate > lower)
        if upper is not None:
            q = q.filter(sq.c.pay_rate <= upper if include_upper else sq.c.pay_rate < upper)
        return int(q.scalar() or 0)

    dist = [
        {"label": "< £11", "count": _band_count(None, 11, include_upper=False)},
        {"label": "£11–£12", "count": _band_count(11, 12, include_lower=True, include_upper=False)},
        {"label": "£12–£13", "count": _band_count(12, 13, include_lower=True, include_upper=False)},
        {"label": "£13–£14", "count": _band_count(13, 14, include_lower=True, include_upper=False)},
        {"label": "≥ £14", "count": _band_count(14, None, include_lower=True)},
    ]

    # Monthly trend (average pay)
    monthly_trend_rows = (
        db.session.query(
            sq.c.imported_year,
            sq.c.imported_month,
            func.avg(sq.c.pay_rate),
        )
        .group_by(sq.c.imported_year, sq.c.imported_month)
        .order_by(sq.c.imported_year, sq.c.imported_month)
        .all()
    )
    monthly_trend = [
        {"year": y, "month": m, "avg_rate": float(a or 0.0) if a is not None else 0.0}
        for (y, m, a) in monthly_trend_rows
    ]

    # Sector volatility (std dev)
    dialect = getattr(getattr(db, "engine", None), "dialect", None)
    dialect_name = getattr(dialect, "name", "") if dialect is not None else ""
    if dialect_name == "sqlite":
        sector_vol_rows = (
            db.session.query(
                sq.c.sector,
                func.count(sq.c.id),
                func.avg(sq.c.pay_rate),
                func.literal(0.0).label("stddev"),
            )
            .group_by(sq.c.sector)
            .order_by(func.count(sq.c.id).desc())
            .all()
        )
        sector_volatility = [
            {
                "sector": s or "Unknown",
                "count": int(n or 0),
                "avg_rate": float(a or 0.0) if a is not None else 0.0,
                "stddev": float(sd or 0.0),
            }
            for (s, n, a, sd) in sector_vol_rows
        ]
    else:
        sector_vol_rows = (
            db.session.query(
                sq.c.sector,
                func.count(sq.c.id),
                func.avg(sq.c.pay_rate),
                func.stddev_pop(sq.c.pay_rate),
            )
            .group_by(sq.c.sector)
            .order_by(func.stddev_pop(sq.c.pay_rate).desc().nullslast())
            .all()
        )
        sector_volatility = [
            {
                "sector": s or "Unknown",
                "count": int(n or 0),
                "avg_rate": float(a or 0.0) if a is not None else 0.0,
                "stddev": float(sd or 0.0) if sd is not None else 0.0,
            }
            for (s, n, a, sd) in sector_vol_rows
        ]

    # Sector × county heat (avg pay)
    sector_county_rows = (
        db.session.query(
            sq.c.sector,
            sq.c.county,
            func.avg(sq.c.pay_rate),
            func.count(sq.c.id),
        )
        .filter(sq.c.sector.isnot(None), sq.c.county.isnot(None))
        .group_by(sq.c.sector, sq.c.county)
        .all()
    )
    sector_county_heat = [
        {
            "sector": s or "Unknown",
            "county": c or "Unknown",
            "avg_rate": float(a or 0.0) if a is not None else 0.0,
            "count": int(n or 0),
        }
        for (s, c, a, n) in sector_county_rows
    ]

    # Top companies by pay
    top_companies_rows = (
        db.session.query(
            sq.c.company_id,
            sq.c.company_name,
            func.avg(sq.c.pay_rate),
            func.count(sq.c.id),
        )
        .filter(sq.c.company_id.isnot(None))
        .group_by(sq.c.company_id, sq.c.company_name)
        .order_by(func.avg(sq.c.pay_rate).desc())
        .limit(10)
        .all()
    )
    top_companies = [
        {
            "company_id": cid,
            "company_name": cname or "Unknown",
            "avg_rate": float(a or 0.0) if a is not None else 0.0,
            "count": int(n or 0),
        }
        for (cid, cname, a, n) in top_companies_rows
    ]

    # Role mix by sector (canonical where available)
    role_mix_rows = (
        db.session.query(
            sq.c.sector,
            sq.c.job_role,
            func.count(sq.c.id),
        )
        .filter(sq.c.sector.isnot(None), sq.c.job_role.isnot(None))
        .group_by(sq.c.sector, sq.c.job_role)
        .all()
    )
    role_mix = [
        {"sector": s or "Unknown", "role": r or "Unknown", "count": int(n or 0)}
        for (s, r, n) in role_mix_rows
    ]

    # County trends (top counties by volume)
    county_counts_rows = (
        db.session.query(
            sq.c.county,
            func.count(sq.c.id),
        )
        .filter(sq.c.county.isnot(None))
        .group_by(sq.c.county)
        .order_by(func.count(sq.c.id).desc())
        .limit(5)
        .all()
    )
    top_county_names = [c for (c, _) in county_counts_rows]

    county_trends: dict[str, List[Dict[str, object]]] = {}
    if top_county_names:
        trend_rows = (
            db.session.query(
                sq.c.county,
                sq.c.imported_year,
                sq.c.imported_month,
                func.avg(sq.c.pay_rate),
            )
            .filter(sq.c.county.in_(top_county_names))
            .group_by(sq.c.county, sq.c.imported_year, sq.c.imported_month)
            .order_by(sq.c.county, sq.c.imported_year, sq.c.imported_month)
            .all()
        )
        for (county, y, m, a) in trend_rows:
            county_trends.setdefault(county or "Unknown", []).append(
                {"year": y, "month": m, "avg_rate": float(a or 0.0) if a is not None else 0.0}
            )

    # Role × sector matrix (canonical where available)
    role_sector_rows = (
        db.session.query(
            sq.c.sector,
            sq.c.job_role,
            func.avg(sq.c.pay_rate),
            func.count(sq.c.id),
        )
        .filter(sq.c.sector.isnot(None), sq.c.job_role.isnot(None))
        .group_by(sq.c.sector, sq.c.job_role)
        .all()
    )
    role_sector_matrix = [
        {
            "sector": s or "Unknown",
            "role": r or "Unknown",
            "avg_rate": float(a or 0.0) if a is not None else 0.0,
            "count": int(n or 0),
        }
        for (s, r, a, n) in role_sector_rows
    ]

    stats_obj = {
        "total": total,
        "avg_rate": avg_rate,
        "min_rate": min_rate,
        "max_rate": max_rate,
        "top_counties": top_counties,
        "top_roles": top_roles,
        "sector_stats": sector_stats,
        "distribution": dist,
        "monthly_trend": monthly_trend,
        "sector_volatility": sector_volatility,
        "sector_county_heat": sector_county_heat,
        "top_companies": top_companies,
        "role_mix": role_mix,
        "county_trends": county_trends,
        "role_sector_matrix": role_sector_matrix,
    }

    options = get_filter_options(force=True)

    # Override Job Role filter options with canonical roles where possible
    canonical_roles = _canonical_role_filter_options()
    if canonical_roles:
        options["roles"] = canonical_roles

    # Uncategorised roles (prefer canonical if available) — counts None OR empty
    if hasattr(JobRecord, "job_role_group"):
        uncategorised_roles_count = (
            db.session.query(func.count(JobRecord.id))
            .filter(
                (JobRecord.job_role_group.is_(None))
                | (func.trim(JobRecord.job_role_group) == "")
            )
            .scalar()
            or 0
        )
    else:
        uncategorised_roles_count = (
            db.session.query(func.count(JobRecord.id))
            .filter(
                (JobRecord.job_role.is_(None))
                | (func.trim(JobRecord.job_role) == "")
            )
            .scalar()
            or 0
        )

    return render_template(
        "insights.html",
        stats=stats_obj,
        options=options,
        filters=filters_map,
        filter_query=request.query_string.decode(),
        total_count=total,
        uncategorised_roles_count=uncategorised_roles_count,
    )


@bp.route("/insights/ai-analyze", methods=["POST"])
@login_required
def insights_ai_analyze():
    """
    Lightweight, deterministic 'AI-style' summary for the Insights page.
    Front-end sends: { filters: {...}, records: [{...}, ...] }
    We return JSON: { text: "...", html?: "..." }
    """
    payload = request.get_json(silent=True) or {}
    filters = payload.get("filters") or {}
    rows = payload.get("records") or []

    # Extract numeric hourly rates
    numeric_rates = [
        r.get("pay_rate")
        for r in rows
        if isinstance(r.get("pay_rate"), (int, float))
    ]
    n = len(numeric_rates)

    if not n:
        return jsonify({
            "text": (
                "I couldn’t see any numeric hourly rates in this view, so there’s "
                "nothing to summarise yet. Try widening the filters or removing any "
                "tight pay band constraints."
            )
        })

    numeric_rates.sort()

    def q(frac: float) -> float:
        """Simple linear quantile on the sorted list."""
        pos = (n - 1) * frac
        lo = int(pos)
        hi = min(lo + 1, n - 1)
        w = pos - lo
        return numeric_rates[lo] * (1 - w) + numeric_rates[hi] * w

    median = q(0.5)
    p25 = q(0.25)
    p75 = q(0.75)
    min_rate = numeric_rates[0]
    max_rate = numeric_rates[-1]
    avg_rate = stats.mean(numeric_rates)
    spread = p75 - p25
    range_width = max_rate - min_rate

    def most_common(field):
        values = [r.get(field) for r in rows if r.get(field)]
        if not values:
            return None, 0
        name, count = Counter(values).most_common(1)[0]
        return name, count

    top_sector, top_sector_n = most_common("sector")
    top_role, top_role_n = most_common("job_role")
    top_county, top_county_n = most_common("county")

    # ---- Build narrative text ----
    parts = []

    # Filters headline
    filter_bits = []
    if filters.get("sector"):
        filter_bits.append("sector(s): " + ", ".join(filters["sector"]))
    if filters.get("county"):
        filter_bits.append("county: " + ", ".join(filters["county"]))
    if filters.get("job_role"):
        filter_bits.append("role(s): " + ", ".join(filters["job_role"]))
    if filters.get("year"):
        filter_bits.append(f"year {filters['year']}")

    if filter_bits:
        headline = (
            "This view covers "
            + ", ".join(filter_bits)
            + f", with {n:,} pay records containing numeric hourly rates."
        )
    else:
        headline = f"This view covers the full dataset with {n:,} numeric pay records."

    parts.append(headline)

    # Central tendency + spread
    parts.append(
        "Typical pay sits around "
        f"£{median:0.2f} per hour (average £{avg_rate:0.2f}). "
        "The middle 50% of rates run from "
        f"£{p25:0.2f} to £{p75:0.2f} (a spread of £{spread:0.2f}). "
        "Overall the range goes from "
        f"£{min_rate:0.2f} to £{max_rate:0.2f} (width £{range_width:0.2f})."
    )

    # Concentration hotspots
    hotspot_bits = []
    if top_sector:
        hotspot_bits.append(
            f"Sector with most records: {top_sector} ({top_sector_n:,} records)."
        )
    if top_role:
        hotspot_bits.append(
            f"Most common role: {top_role} ({top_role_n:,} records)."
        )
    if top_county:
        hotspot_bits.append(
            f"County with most postings: {top_county} ({top_county_n:,} records)."
        )

    if hotspot_bits:
        parts.append("Concentration hotspots: " + " ".join(hotspot_bits))

    # Confidence / caveats
    if n < 50:
        parts.append(
            "Sample size is small, so treat these numbers as directional only and "
            "double-check individual records before using them in comms or decisions."
        )
    elif n < 250:
        parts.append(
            "Sample size is moderate. The median is a reasonable benchmark, but be "
            "cautious about thin sectors or counties inside this slice."
        )
    else:
        parts.append(
            "Sample size is strong. Focus on the interquartile range and any clear "
            "outliers rather than individual extreme records."
        )

    if filters.get("rate_min") or filters.get("rate_max"):
        parts.append(
            "A pay-band filter is applied, so extreme highs or lows outside this band "
            "are intentionally excluded from the view."
        )

    return jsonify({"text": "\n\n".join(parts)})
