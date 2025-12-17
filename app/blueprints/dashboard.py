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
    Must match what Sector Cleaner uses.
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
    selected_sector = request.args.get("sector")

    # NOTE: dashboard.html uses name="role" for the job role filter.
    # We map that into "job_role" for build_filters_from_request.
    # IMPORTANT: we DO NOT pass sector into build_filters_from_request,
    # because we want sector to filter on *effective* sector (override-aware).
    filters_map = {
        "q": request.args.get("q"),
        "sector": selected_sector,  # keep for template display
        "job_role": request.args.get("role"),  # <-- form field "role"
        "county": request.args.get("county"),
        "month": request.args.get("month"),
        "year": request.args.get("year"),
        "rate_min": request.args.get("rate_min"),
        "rate_max": request.args.get("rate_max"),
    }

    filters_map_for_build = dict(filters_map)
    filters_map_for_build["sector"] = None  # handled manually below

    filters, extra_search = build_filters_from_request(filters_map_for_build)

    base_q = JobRecord.query.filter(*filters)
    if extra_search is not None:
        base_q = extra_search(base_q)

    # Join sector overrides so we can label/filter by effective sector
    role_expr = _canonical_role_expr()
    base_q = base_q.outerjoin(
        JobRoleSectorOverride,
        JobRoleSectorOverride.canonical_role == role_expr,
    )

    # Apply sector filter against effective sector (override-aware)
    base_q = _apply_effective_sector_filter(base_q, selected_sector)

    # Subquery with the columns we need
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

    Uses JobRoleMapping to prefer canonical roles in all analytics.
    Now also uses JobRoleSectorOverride to prefer sector overrides in all analytics:
      sector = COALESCE(JobRoleSectorOverride.canonical_sector, JobRecord.sector)
    """
    selected_sectors = request.args.getlist("sector")

    filters_map = {
        "q": request.args.get("q"),
        "sector": selected_sectors,  # keep for template display
        # Accept both ?job_role= and ?role= just in case
        "job_role": request.args.getlist("job_role") or request.args.getlist("role"),
        "county": request.args.getlist("county"),
        "month": request.args.get("month"),
        "year": request.args.get("year"),
        "rate_min": request.args.get("rate_min"),
        "rate_max": request.args.get("rate_max"),
    }

    filters_map_for_build = dict(filters_map)
    filters_map_for_build["sector"] = []  # handled manually below

    filters, extra_search = build_filters_from_request(filters_map_for_build)

    base_q = JobRecord.query.filter(*filters)
    if extra_search is not None:
        base_q = extra_search(base_q)

    # Join role mapping (canonical role labels)
    base_q = base_q.outerjoin(
        JobRoleMapping,
        JobRecord.job_role == JobRoleMapping.raw_value,
    )

    # Join sector overrides (override sector labels)
    role_expr = _canonical_role_expr()
    base_q = base_q.outerjoin(
        JobRoleSectorOverride,
        JobRoleSectorOverride.canonical_role == role_expr,
    )

    # Apply sector filter against effective sector (override-aware)
    base_q = _apply_effective_sector_filter(base_q, selected_sectors)

    # Subquery with canonical job_role label + effective sector label
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
            q = q.filter(
                sq.c.pay_rate >= lower if include_lower else sq.c.pay_rate > lower
            )
        if upper is not None:
            q = q.filter(
                sq.c.pay_rate <= upper if include_upper else sq.c.pay_rate < upper
            )
        return int(q.scalar() or 0)

    dist = [
        {"label": "< £11", "count": _band_count(None, 11, include_upper=False)},
        {
            "label": "£11–£12",
            "count": _band_count(11, 12, include_lower=True, include_upper=False),
        },
        {
            "label": "£12–£13",
            "count": _band_count(12, 13, include_lower=True, include_upper=False),
        },
        {
            "label": "£13–£14",
            "count": _band_count(13, 14, include_lower=True, include_upper=False),
        },
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
        {
            "year": y,
            "month": m,
            "avg_rate": float(a or 0.0) if a is not None else 0.0,
        }
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

    # Role mix by sector (effective sector + canonical role)
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
        {
            "sector": s or "Unknown",
            "role": r or "Unknown",
            "count": int(n or 0),
        }
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
                {
                    "year": y,
                    "month": m,
                    "avg_rate": float(a or 0.0) if a is not None else 0.0,
                }
            )

    # Role × sector matrix (effective sector + canonical role)
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
        options=options,
        filters=filters_map,
        filter_query=request.query_string.decode(),
        total_count=total,
        uncategorised_roles_count=uncategorised_roles_count,
    )


# ----------------------------------------------------------------------
# Admin: Job Role Cleaner (existing)
# ----------------------------------------------------------------------


@bp.route("/admin/job-roles")
@login_required
def admin_job_roles():
    ...
    # (UNCHANGED - keep your existing implementation here)
    ...


@bp.route("/admin/job-roles/map", methods=["POST"])
@login_required
def admin_job_roles_map():
    ...
    # (UNCHANGED - keep your existing implementation here)
    ...


@bp.route("/admin/job-roles/bulk-map", methods=["POST"])
@login_required
def admin_job_roles_bulk_map():
    ...
    # (UNCHANGED - keep your existing implementation here)
    ...


# ----------------------------------------------------------------------
# Admin: Sector Override Cleaner (NEW)
# ----------------------------------------------------------------------


@bp.route("/admin/role-sectors")
@login_required
def admin_role_sectors():
    ...
    # (UNCHANGED - keep your existing implementation here)
    ...


@bp.route("/admin/role-sectors/map", methods=["POST"])
@login_required
def admin_role_sectors_map():
    ...
    # (UNCHANGED - keep your existing implementation here)
    ...


@bp.route("/admin/role-sectors/bulk-map", methods=["POST"])
@login_required
def admin_role_sectors_bulk_map():
    ...
    # (UNCHANGED - keep your existing implementation here)
    ...
