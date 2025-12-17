# app/blueprints/dashboard.py
from __future__ import annotations

from datetime import date, datetime, time, timedelta

from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from sqlalchemy import func

from extensions import db
from models import (
    JobRecord,
    CronRunLog,
    JobRoleMapping,
    JobRoleSectorOverride,
)
from .utils import (
    build_filters_from_request,
    get_filter_options,
)

bp = Blueprint("dashboard", __name__)


def _fresh_filter_options():
    """
    Avoid TTL cache; query distincts directly so selects always populate
    on the dashboard filters.
    """

    def col_distinct(col):
        return [
            v[0]
            for v in db.session.query(col)
            .filter(col.isnot(None))
            .distinct()
            .order_by(col)
            .all()
        ]

    return {
        "sectors": col_distinct(JobRecord.sector),
        "roles": col_distinct(JobRecord.job_role),
        "counties": col_distinct(JobRecord.county),
        "months": col_distinct(JobRecord.imported_month),
        "years": col_distinct(JobRecord.imported_year),
    }


def _canonical_role_expr():
    """
    Canonical role for sector overrides:
    prefer job_role_group, fallback to job_role.
    """
    return func.coalesce(JobRecord.job_role_group, JobRecord.job_role)


def _effective_sector_expr():
    """
    Effective sector shown in reporting:
    override wins, else fall back to JobRecord.sector
    """
    return func.coalesce(JobRoleSectorOverride.canonical_sector, JobRecord.sector)


def _apply_effective_sector_filter(query, selected_sectors):
    """
    Apply sector filter against effective sector (override-aware).
    selected_sectors can be str, list[str], or None.
    """
    if not selected_sectors:
        return query

    if isinstance(selected_sectors, str):
        selected = [selected_sectors]
    else:
        selected = [s for s in selected_sectors if s]

    selected = [(s or "").strip() for s in selected if (s or "").strip()]
    if not selected:
        return query

    return query.filter(_effective_sector_expr().in_(selected))


def _json_safe_records_from_sq(sq, limit: int = 1000):
    """
    Build JSON-serialisable records for insights.html.
    Prevents 'Undefined is not JSON serializable' when using {{ records|tojson }}.
    """
    rows = (
        db.session.query(
            sq.c.id,
            sq.c.company_id,
            sq.c.company_name,
            sq.c.sector,
            sq.c.job_role,
            sq.c.postcode,
            sq.c.county,
            sq.c.pay_rate,
            sq.c.imported_year,
            sq.c.imported_month,
        )
        .order_by(sq.c.id.desc())
        .limit(limit)
        .all()
    )

    out = []
    for r in rows:
        out.append(
            {
                "id": int(r.id) if r.id is not None else None,
                "company_id": int(r.company_id) if r.company_id is not None else None,
                "company_name": r.company_name or "",
                "sector": r.sector or "",
                "job_role": r.job_role or "",
                "postcode": r.postcode or "",
                "county": r.county or "",
                "pay_rate": float(r.pay_rate) if r.pay_rate is not None else None,
                "imported_year": int(r.imported_year) if r.imported_year is not None else None,
                "imported_month": int(r.imported_month) if r.imported_month is not None else None,
            }
        )
    return out


@bp.route("/dashboard")
@login_required
def dashboard():
    """
    Dashboard landing with topline metrics.
    Provides:
      - avg_pay, min_pay, max_pay, total_records
      - total_companies, by_sector, by_county
      - scrape stats from CronRunLog (today + last 7 days)
      - uncategorised_roles_count for basic data hygiene visibility
    """
    # NOTE: dashboard.html uses name="role" for the job role filter.
    # We map that into "job_role" for build_filters_from_request.
    filters_map = {
        "q": request.args.get("q"),
        "sector": request.args.get("sector"),
        "job_role": request.args.get("role"),  # <-- form field "role"
        "county": request.args.get("county"),
        "month": request.args.get("month"),
        "year": request.args.get("year"),
        "rate_min": request.args.get("rate_min"),
        "rate_max": request.args.get("rate_max"),
    }
    filters, extra_search = build_filters_from_request(filters_map)

    base_q = JobRecord.query.filter(*filters)
    if extra_search is not None:
        base_q = extra_search(base_q)

    # Sector overrides join for effective sector (used in dashboard breakdown)
    role_expr = _canonical_role_expr()
    base_q = base_q.outerjoin(
        JobRoleSectorOverride,
        JobRoleSectorOverride.canonical_role == role_expr,
    )

    selected_sector = (filters_map.get("sector") or "").strip()
    if selected_sector:
        base_q = _apply_effective_sector_filter(base_q, selected_sector)

    # Subquery with the columns we need (use effective sector)
    sq = (
        base_q.with_entities(
            JobRecord.id.label("id"),
            JobRecord.pay_rate.label("pay_rate"),
            JobRecord.imported_year.label("imported_year"),
            JobRecord.imported_month.label("imported_month"),
            _effective_sector_expr().label("sector"),
            JobRecord.county.label("county"),
            JobRecord.company_id.label("company_id"),
        )
        .subquery(name="sq_dash")
    )

    # Aggregates
    agg_row = db.session.query(
        func.count(sq.c.id),
        func.avg(sq.c.pay_rate),
        func.min(sq.c.pay_rate),
        func.max(sq.c.pay_rate),
    ).first()

    total_records = int(agg_row[0] or 0)
    avg_pay = float(agg_row[1]) if agg_row[1] is not None else 0.0
    min_pay = float(agg_row[2]) if agg_row[2] is not None else 0.0
    max_pay = float(agg_row[3]) if agg_row[3] is not None else 0.0

    # Total distinct companies in the filtered dataset
    total_companies = (
        db.session.query(func.count(func.distinct(sq.c.company_id))).scalar() or 0
    )

    # Records by sector (effective sector)
    by_sector_rows = (
        db.session.query(
            sq.c.sector,
            func.count(sq.c.id),
            func.avg(sq.c.pay_rate),
        )
        .filter(sq.c.sector.isnot(None))
        .group_by(sq.c.sector)
        .order_by(func.count(sq.c.id).desc())
        .all()
    )
    by_sector = [
        (
            sector or "Unknown",
            int(count or 0),
            float(avg) if avg is not None else 0.0,
        )
        for sector, count, avg in by_sector_rows
    ]

    # Records by county
    by_county_rows = (
        db.session.query(
            sq.c.county,
            func.count(sq.c.id),
        )
        .filter(sq.c.county.isnot(None))
        .group_by(sq.c.county)
        .order_by(func.count(sq.c.id).desc())
        .all()
    )
    by_county = [
        (county or "Unknown", int(count or 0)) for county, count in by_county_rows
    ]

    # Recent uploads by month/year (optional widget)
    recent_uploads = (
        db.session.query(
            sq.c.imported_year,
            sq.c.imported_month,
            func.count(sq.c.id),
        )
        .group_by(sq.c.imported_year, sq.c.imported_month)
        .order_by(sq.c.imported_year.desc(), sq.c.imported_month.desc())
        .limit(6)
        .all()
    )
    recent_uploads = [
        {"year": y or "—", "month": m or "—", "count": int(n or 0)}
        for (y, m, n) in recent_uploads
    ]

    # Filter options for selects
    options = _fresh_filter_options()
    available_sectors = options["sectors"]
    available_roles = options["roles"]
    available_counties = options["counties"]

    # ------------------------------------------------------------------
    # Scrape stats from CronRunLog
    # ------------------------------------------------------------------
    today = date.today()
    start_of_today = datetime.combine(today, time.min)
    start_7d = datetime.combine(today - timedelta(days=6), time.min)

    # Jobs scraped today / last 7 days
    today_jobs_total = (
        db.session.query(func.coalesce(func.sum(CronRunLog.rows_scraped), 0))
        .filter(CronRunLog.started_at >= start_of_today)
        .scalar()
        or 0
    )
    week_jobs_total = (
        db.session.query(func.coalesce(func.sum(CronRunLog.rows_scraped), 0))
        .filter(CronRunLog.started_at >= start_7d)
        .scalar()
        or 0
    )

    # Errorful runs (status != 'success')
    today_error_runs = (
        db.session.query(func.count(CronRunLog.id))
        .filter(CronRunLog.started_at >= start_of_today)
        .filter(CronRunLog.status != "success")
        .scalar()
        or 0
    )
    week_error_runs = (
        db.session.query(func.count(CronRunLog.id))
        .filter(CronRunLog.started_at >= start_7d)
        .filter(CronRunLog.status != "success")
        .scalar()
        or 0
    )

    # Uncategorised roles (records where canonical job_role has not been set)
    uncategorised_roles_count = (
        db.session.query(func.count(JobRecord.id))
        .filter(JobRecord.job_role.is_(None))
        .scalar()
        or 0
    )

    return render_template(
        "dashboard.html",
        # Filters
        filters=filters_map,
        filter_query=request.query_string.decode(),
        available_sectors=available_sectors,
        available_roles=available_roles,
        available_counties=available_counties,
        # Topline metrics expected by template
        avg_pay=avg_pay,
        min_pay=min_pay,
        max_pay=max_pay,
        total_records=total_records,
        total_companies=total_companies,
        by_sector=by_sector,
        by_county=by_county,
        # Scrape stats
        today_jobs_total=today_jobs_total,
        week_jobs_total=week_jobs_total,
        today_error_runs=today_error_runs,
        week_error_runs=week_error_runs,
        # Data hygiene
        uncategorised_roles_count=uncategorised_roles_count,
        # Extra (safe if unused)
        recent_uploads=recent_uploads,
    )


@bp.route("/insights")
@login_required
def insights():
    """
    Insights over JobRecord with filters.

    Uses JobRoleMapping to prefer canonical roles in all analytics:
      job_role = COALESCE(JobRoleMapping.canonical_role, JobRecord.job_role)

    Uses JobRoleSectorOverride to prefer sector overrides in all analytics:
      sector = COALESCE(JobRoleSectorOverride.canonical_sector, JobRecord.sector)
    """
    selected_sectors = request.args.getlist("sector")

    filters_map = {
        "q": request.args.get("q"),
        "sector": selected_sectors,
        # Accept both ?job_role= and ?role= just in case
        "job_role": request.args.getlist("job_role") or request.args.getlist("role"),
        "county": request.args.getlist("county"),
        "month": request.args.get("month"),
        "year": request.args.get("year"),
        "rate_min": request.args.get("rate_min"),
        "rate_max": request.args.get("rate_max"),
    }

    # IMPORTANT: sector filtering is applied manually (override-aware)
    filters_map_for_build = dict(filters_map)
    filters_map_for_build["sector"] = []  # handled manually below

    filters, extra_search = build_filters_from_request(filters_map_for_build)

    base_q = JobRecord.query.filter(*filters)
    if extra_search is not None:
        base_q = extra_search(base_q)

    # Join to JobRoleMapping so we can use canonical roles where available
    base_q = base_q.outerjoin(
        JobRoleMapping,
        JobRecord.job_role == JobRoleMapping.raw_value,
    )

    # Join sector overrides using canonical role expression
    role_expr = _canonical_role_expr()
    base_q = base_q.outerjoin(
        JobRoleSectorOverride,
        JobRoleSectorOverride.canonical_role == role_expr,
    )

    # Apply sector filter against effective sector (override-aware)
    base_q = _apply_effective_sector_filter(base_q, selected_sectors)

    # Subquery with canonical job_role + effective sector label
    sq = base_q.with_entities(
        JobRecord.id.label("id"),
        JobRecord.company_id.label("company_id"),
        JobRecord.company_name.label("company_name"),
        _effective_sector_expr().label("sector"),
        func.coalesce(JobRoleMapping.canonical_role, JobRecord.job_role).label("job_role"),
        JobRecord.postcode.label("postcode"),
        JobRecord.county.label("county"),
        JobRecord.pay_rate.label("pay_rate"),
        JobRecord.imported_month.label("imported_month"),
        JobRecord.imported_year.label("imported_year"),
    ).subquery(name="sq_records")

    # JSON-safe list for insights.html charts/tables
    records = _json_safe_records_from_sq(sq, limit=1000)

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
    top_counties = [
        {"county": c or "—", "count": int(n or 0)} for c, n in top_counties_rows
    ]

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

    # Sector breakdown (effective sector)
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

    # Role mix by sector
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

    county_trends: dict[str, list[dict]] = {}
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

    # Role × sector matrix
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

    stats = {
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

    uncategorised_roles_count = (
        db.session.query(func.count(JobRecord.id))
        .filter(JobRecord.job_role.is_(None))
        .scalar()
        or 0
    )

    return render_template(
        "insights.html",
        stats=stats,
        records=records,  # ✅ prevents Undefined->tojson crash
        options=options,
        filters=filters_map,
        filter_query=request.query_string.decode(),
        total_count=total,
        uncategorised_roles_count=uncategorised_roles_count,
    )


# ----------------------------------------------------------------------
# Admin: Job Role Cleaner
# ----------------------------------------------------------------------


@bp.route("/admin/job-roles")
@login_required
def admin_job_roles():
    """
    Admin view to map raw job roles to canonical roles (JobRoleMapping).
    Designed to help clean up job_role values used in analytics.
    """
    # Ensure table exists (safe no-op if migrations already handle this)
    try:
        JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    search = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "all").strip().lower()
    if status not in ("all", "with", "without"):
        status = "all"

    # Raw role expression
    raw_expr = func.coalesce(JobRecord.job_role, "").label("raw_role")

    q = db.session.query(
        raw_expr,
        func.count(JobRecord.id).label("count"),
        func.avg(JobRecord.pay_rate).label("avg_pay"),
        func.min(JobRecord.pay_rate).label("min_pay"),
        func.max(JobRecord.pay_rate).label("max_pay"),
    ).filter(JobRecord.job_role.isnot(None)).filter(func.trim(JobRecord.job_role) != "")

    if search:
        pattern = f"%{search}%"
        q = q.filter(raw_expr.ilike(pattern))

    if status == "with":
        q = q.join(JobRoleMapping, JobRoleMapping.raw_value == raw_expr)
    elif status == "without":
        q = q.outerjoin(JobRoleMapping, JobRoleMapping.raw_value == raw_expr).filter(
            JobRoleMapping.id.is_(None)
        )

    rows = (
        q.group_by(raw_expr)
        .order_by(func.count(JobRecord.id).desc())
        .limit(500)
        .all()
    )

    mappings = {}
    try:
        all_maps = JobRoleMapping.query.order_by(JobRoleMapping.raw_value).all()
        mappings = {m.raw_value: m for m in all_maps}
    except Exception:
        mappings = {}

    # Canonical role suggestions: existing canonical roles in mapping table + job_role_group values
    canonical_opts = set()

    try:
        canon_from_maps = (
            db.session.query(JobRoleMapping.canonical_role)
            .filter(JobRoleMapping.canonical_role.isnot(None))
            .distinct()
            .order_by(JobRoleMapping.canonical_role)
            .all()
        )
        canonical_opts.update([v[0] for v in canon_from_maps if (v[0] or "").strip()])
    except Exception:
        pass

    try:
        canon_from_groups = (
            db.session.query(JobRecord.job_role_group)
            .filter(JobRecord.job_role_group.isnot(None))
            .distinct()
            .order_by(JobRecord.job_role_group)
            .all()
        )
        canonical_opts.update([v[0] for v in canon_from_groups if (v[0] or "").strip()])
    except Exception:
        pass

    canonical_options = sorted(canonical_opts)

    return render_template(
        "admin_job_roles.html",
        rows=rows,
        mappings=mappings,
        canonical_options=canonical_options,
        search=search,
        status=status,
    )


@bp.route("/admin/job-roles/map", methods=["POST"])
@login_required
def admin_job_roles_map():
    raw_value = (request.form.get("raw_value") or "").strip()
    canonical_role = (request.form.get("canonical_role") or "").strip()

    q_param = (request.form.get("q") or "").strip()
    status_param = (request.form.get("status") or "all").strip().lower()

    if not raw_value or not canonical_role:
        flash("Raw role and canonical role are required.", "error")
        return redirect(url_for("dashboard.admin_job_roles", q=q_param, status=status_param))

    try:
        JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    m = JobRoleMapping.query.filter_by(raw_value=raw_value).first()
    if m is None:
        m = JobRoleMapping(raw_value=raw_value, canonical_role=canonical_role)
    else:
        m.canonical_role = canonical_role

    db.session.add(m)
    db.session.commit()

    flash(f"Role mapping saved: '{raw_value}' → '{canonical_role}'.", "success")
    return redirect(url_for("dashboard.admin_job_roles", q=q_param, status=status_param))


@bp.route("/admin/job-roles/bulk-map", methods=["POST"])
@login_required
def admin_job_roles_bulk_map():
    raw_values = request.form.getlist("raw_values") or []
    raw_values = sorted({(r or "").strip() for r in raw_values if (r or "").strip()})

    canonical_role = (request.form.get("canonical_role") or "").strip()

    q_param = (request.form.get("q") or "").strip()
    status_param = (request.form.get("status") or "all").strip().lower()

    if not raw_values:
        flash("Select at least one raw role before using bulk assign.", "error")
        return redirect(url_for("dashboard.admin_job_roles", q=q_param, status=status_param))

    if not canonical_role:
        flash("Canonical role is required for bulk assignment.", "error")
        return redirect(url_for("dashboard.admin_job_roles", q=q_param, status=status_param))

    try:
        JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    updated = 0
    for rv in raw_values:
        m = JobRoleMapping.query.filter_by(raw_value=rv).first()
        if m is None:
            m = JobRoleMapping(raw_value=rv, canonical_role=canonical_role)
        else:
            m.canonical_role = canonical_role
        db.session.add(m)
        updated += 1

    db.session.commit()

    flash(f"Bulk role mapping applied: {updated} role(s) → '{canonical_role}'.", "success")
    return redirect(url_for("dashboard.admin_job_roles", q=q_param, status=status_param))


# ----------------------------------------------------------------------
# Admin: Sector Override Cleaner
# ----------------------------------------------------------------------


@bp.route("/admin/role-sectors")
@login_required
def admin_role_sectors():
    """
    Admin view to map canonical roles (job_role_group) to canonical sectors.
    Focus is: roles currently sitting in sector == "Other" (or missing).
    """
    # Ensure the overrides table exists
    try:
        JobRoleSectorOverride.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    search = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "all").strip().lower()
    if status not in ("all", "with", "without"):
        status = "all"

    only_other = (request.args.get("only_other") or "1").strip()
    only_other = only_other in ("1", "true", "yes", "on")

    # Canonical role: prefer job_role_group, fallback to job_role
    role_expr = func.coalesce(JobRecord.job_role_group, JobRecord.job_role).label("canonical_role")

    q = db.session.query(
        role_expr,
        func.count(JobRecord.id).label("count"),
        func.avg(JobRecord.pay_rate).label("avg_pay"),
        func.min(JobRecord.pay_rate).label("min_pay"),
        func.max(JobRecord.pay_rate).label("max_pay"),
    ).filter(role_expr.isnot(None))

    if only_other:
        q = q.filter(
            (JobRecord.sector.is_(None)) |
            (func.trim(JobRecord.sector) == "") |
            (func.lower(func.trim(JobRecord.sector)) == "other")
        )

    if search:
        pattern = f"%{search}%"
        q = q.filter(role_expr.ilike(pattern))

    if status == "with":
        q = q.join(JobRoleSectorOverride, JobRoleSectorOverride.canonical_role == role_expr)
    elif status == "without":
        q = q.outerjoin(JobRoleSectorOverride, JobRoleSectorOverride.canonical_role == role_expr).filter(
            JobRoleSectorOverride.id.is_(None)
        )

    rows = (
        q.group_by(role_expr)
        .order_by(func.count(JobRecord.id).desc())
        .limit(500)
        .all()
    )

    try:
        override_rows = JobRoleSectorOverride.query.order_by(JobRoleSectorOverride.canonical_role).all()
        overrides = {o.canonical_role: o for o in override_rows}
    except Exception:
        overrides = {}

    # Sector dropdown options (existing sectors + "Other")
    sector_opts = [
        v[0]
        for v in db.session.query(JobRecord.sector)
        .filter(JobRecord.sector.isnot(None))
        .distinct()
        .order_by(JobRecord.sector)
        .all()
    ]
    sector_opts = [s for s in sector_opts if (s or "").strip()]
    if "Other" not in sector_opts:
        sector_opts.append("Other")

    return render_template(
        "admin_role_sectors.html",
        rows=rows,
        overrides=overrides,
        sector_options=sector_opts,
        search=search,
        status=status,
        only_other=only_other,
    )


@bp.route("/admin/role-sectors/map", methods=["POST"])
@login_required
def admin_role_sectors_map():
    canonical_role = (request.form.get("canonical_role") or "").strip()
    canonical_sector = (request.form.get("canonical_sector") or "").strip()

    q_param = (request.form.get("q") or "").strip()
    status_param = (request.form.get("status") or "all").strip().lower()
    only_other_param = (request.form.get("only_other") or "1").strip()

    if not canonical_role or not canonical_sector:
        flash("Canonical role and canonical sector are required.", "error")
        return redirect(url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param))

    try:
        JobRoleSectorOverride.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    ov = JobRoleSectorOverride.query.filter_by(canonical_role=canonical_role).first()
    if ov is None:
        ov = JobRoleSectorOverride(canonical_role=canonical_role, canonical_sector=canonical_sector)
    else:
        ov.canonical_sector = canonical_sector

    db.session.add(ov)
    db.session.commit()

    flash(f"Sector override saved: '{canonical_role}' → '{canonical_sector}'.", "success")
    return redirect(url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param))


@bp.route("/admin/role-sectors/bulk-map", methods=["POST"])
@login_required
def admin_role_sectors_bulk_map():
    canonical_roles = request.form.getlist("canonical_roles") or []
    canonical_roles = sorted({(r or "").strip() for r in canonical_roles if (r or "").strip()})

    canonical_sector = (request.form.get("canonical_sector") or "").strip()

    q_param = (request.form.get("q") or "").strip()
    status_param = (request.form.get("status") or "all").strip().lower()
    only_other_param = (request.form.get("only_other") or "1").strip()

    if not canonical_roles:
        flash("Select at least one role before using bulk assign.", "error")
        return redirect(url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param))

    if not canonical_sector:
        flash("Canonical sector is required for bulk assignment.", "error")
        return redirect(url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param))

    try:
        JobRoleSectorOverride.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    updated = 0
    for role in canonical_roles:
        ov = JobRoleSectorOverride.query.filter_by(canonical_role=role).first()
        if ov is None:
            ov = JobRoleSectorOverride(canonical_role=role, canonical_sector=canonical_sector)
        else:
            ov.canonical_sector = canonical_sector
        db.session.add(ov)
        updated += 1

    db.session.commit()

    flash(f"Bulk sector override applied: {updated} role(s) → '{canonical_sector}'.", "success")
    return redirect(url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param))

