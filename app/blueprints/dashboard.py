# app/blueprints/dashboard.py
from __future__ import annotations

from datetime import date, datetime, time, timedelta

from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from sqlalchemy import func

from extensions import db
from models import JobRecord, CronRunLog, JobRoleMapping
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

    # Subquery with the columns we need
    sq = (
        base_q.with_entities(
            JobRecord.id.label("id"),
            JobRecord.pay_rate.label("pay_rate"),
            JobRecord.imported_year.label("imported_year"),
            JobRecord.imported_month.label("imported_month"),
            JobRecord.sector.label("sector"),
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

    # Records by sector
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

    Now uses JobRoleMapping to prefer canonical roles in all analytics:
    job_role = COALESCE(JobRoleMapping.canonical_role, JobRecord.job_role)
    """
    filters_map = {
        "q": request.args.get("q"),
        "sector": request.args.getlist("sector"),
        # Accept both ?job_role= and ?role= just in case
        "job_role": request.args.getlist("job_role") or request.args.getlist("role"),
        "county": request.args.getlist("county"),
        "month": request.args.get("month"),
        "year": request.args.get("year"),
        "rate_min": request.args.get("rate_min"),
        "rate_max": request.args.get("rate_max"),
    }

    filters, extra_search = build_filters_from_request(filters_map)

    base_q = JobRecord.query.filter(*filters)
    if extra_search is not None:
        base_q = extra_search(base_q)

    # Join to JobRoleMapping so we can use canonical roles where available
    base_q = base_q.outerjoin(
        JobRoleMapping,
        JobRecord.job_role == JobRoleMapping.raw_value,
    )

    # Subquery with canonical job_role label
    sq = base_q.with_entities(
        JobRecord.id.label("id"),
        JobRecord.company_id.label("company_id"),
        JobRecord.company_name.label("company_name"),
        JobRecord.sector.label("sector"),
        func.coalesce(JobRoleMapping.canonical_role, JobRecord.job_role).label(
            "job_role"
        ),
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

    # Top roles (now canonical where mapping exists)
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

    # Lightweight set of rows for client-side UI in insights.html
    rows_for_client = (
        db.session.query(
            sq.c.id,
            sq.c.company_name,
            sq.c.sector,
            sq.c.job_role,
            sq.c.county,
            sq.c.pay_rate,
            sq.c.imported_month,
            sq.c.imported_year,
        )
        .order_by(
            sq.c.imported_year.desc(),
            sq.c.imported_month.desc(),
            sq.c.company_name.asc(),
        )
        .limit(200)
        .all()
    )
    records = [
        {
            "id": r[0],
            "company_name": r[1],
            "sector": r[2],
            "job_role": r[3],
            "county": r[4],
            "pay_rate": float(r[5]) if r[5] is not None else None,
            "imported_month": r[6],
            "imported_year": r[7],
        }
        for r in rows_for_client
    ]

    options = get_filter_options(force=True)

    # data hygiene metric reused here if you want to surface a banner
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
        records=records,
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
    Admin view to see distinct job_role values and map them to canonical roles.
    Self-healing: ensures job_role_mappings table exists before querying.
    Supports:
      - q: search over raw roles
      - status: all / with / without canonical mapping
    """
    # Make sure the mapping table exists (safe if already created)
    try:
        JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        # If this somehow fails, we still try to render with empty mappings below
        pass

    search = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "all").strip().lower()
    if status not in ("all", "with", "without"):
        status = "all"

    # Base query: distinct job_role values with counts
    q = db.session.query(
        JobRecord.job_role.label("raw_value"),
        func.count(JobRecord.id).label("count"),
    ).filter(JobRecord.job_role.isnot(None))

    if search:
        pattern = f"%{search}%"
        q = q.filter(JobRecord.job_role.ilike(pattern))

    # Apply status filter via join/outerjoin on JobRoleMapping
    if status == "with":
        q = q.join(
            JobRoleMapping,
            JobRecord.job_role == JobRoleMapping.raw_value,
        )
    elif status == "without":
        q = q.outerjoin(
            JobRoleMapping,
            JobRecord.job_role == JobRoleMapping.raw_value,
        ).filter(JobRoleMapping.id.is_(None))

    rows = (
        q.group_by(JobRecord.job_role)
        .order_by(func.count(JobRecord.id).desc())
        .limit(500)
        .all()
    )

    # Existing mappings keyed by raw_value; if table is still missing for some reason,
    # fall back to an empty dict rather than crashing.
    try:
        mapping_rows = JobRoleMapping.query.order_by(JobRoleMapping.raw_value).all()
        mappings = {m.raw_value: m for m in mapping_rows}
    except Exception:
        mappings = {}

    uncategorised_roles_count = (
        db.session.query(func.count(JobRecord.id))
        .filter(JobRecord.job_role.is_(None))
        .scalar()
        or 0
    )

    return render_template(
        "admin_job_roles.html",
        rows=rows,
        mappings=mappings,
        search=search,
        status=status,
        uncategorised_roles_count=uncategorised_roles_count,
    )


@bp.route("/admin/job-roles/map", methods=["POST"])
@login_required
def admin_job_roles_map():
    """
    Create/update a mapping for a raw job_role value to a canonical role.
    Optionally applies the change immediately to existing JobRecord rows.
    Redirects back to the current Job Role Cleaner filters (q, status).
    """
    raw_value = (request.form.get("raw_value") or "").strip()
    canonical_role = (request.form.get("canonical_role") or "").strip()
    apply_now = request.form.get("apply_now") == "1"

    # Preserve filters on redirect
    q_param = (request.form.get("q") or "").strip()
    status_param = (request.form.get("status") or "all").strip().lower()

    if not raw_value or not canonical_role:
        flash("Raw value and canonical role are required.", "error")
        return redirect(
            url_for("dashboard.admin_job_roles", q=q_param, status=status_param)
        )

    # Ensure table exists here as well, in case this endpoint is hit first.
    try:
        JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    mapping = JobRoleMapping.query.filter_by(raw_value=raw_value).first()
    if mapping is None:
        mapping = JobRoleMapping(raw_value=raw_value, canonical_role=canonical_role)
    else:
        mapping.canonical_role = canonical_role

    db.session.add(mapping)

    if apply_now:
        db.session.query(JobRecord).filter(JobRecord.job_role == raw_value).update(
            {JobRecord.job_role: canonical_role},
            synchronize_session=False,
        )

    db.session.commit()
    flash(f"Mapping saved for role '{raw_value}' → '{canonical_role}'.", "success")
    return redirect(
        url_for("dashboard.admin_job_roles", q=q_param, status=status_param)
    )


@bp.route("/admin/job-roles/bulk-map", methods=["POST"])
@login_required
def admin_job_roles_bulk_map():
    """
    Bulk-create/update mappings for multiple raw job_role values to a single canonical role.
    Optionally applies the change immediately to existing JobRecord rows.

    Expects:
      - raw_values: repeated form fields (one per selected checkbox)
      - canonical_role: the target canonical role
      - apply_now: "1" if JobRecord rows should be updated too
      - q, status: current filter state on the Job Role Cleaner page
    """
    raw_values = request.form.getlist("raw_values") or []
    # De-duplicate, strip empty
    raw_values = sorted({(rv or "").strip() for rv in raw_values if (rv or "").strip()})

    canonical_role = (request.form.get("canonical_role") or "").strip()
    apply_now = request.form.get("apply_now") == "1"

    q_param = (request.form.get("q") or "").strip()
    status_param = (request.form.get("status") or "all").strip().lower()

    if not raw_values:
        flash("Select at least one job title before using bulk assign.", "error")
        return redirect(
            url_for("dashboard.admin_job_roles", q=q_param, status=status_param)
        )

    if not canonical_role:
        flash("Canonical role is required for bulk assignment.", "error")
        return redirect(
            url_for("dashboard.admin_job_roles", q=q_param, status=status_param)
        )

    # Ensure table exists
    try:
        JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    updated_mappings = 0

    for raw_value in raw_values:
        mapping = JobRoleMapping.query.filter_by(raw_value=raw_value).first()
        if mapping is None:
            mapping = JobRoleMapping(raw_value=raw_value, canonical_role=canonical_role)
            db.session.add(mapping)
        else:
            mapping.canonical_role = canonical_role
        updated_mappings += 1

    if apply_now:
        # Update all JobRecord rows whose job_role is in the selected raw_values
        db.session.query(JobRecord).filter(JobRecord.job_role.in_(raw_values)).update(
            {JobRecord.job_role: canonical_role},
            synchronize_session=False,
        )

    db.session.commit()

    flash(
        f"Bulk mapping applied: {updated_mappings} raw role(s) → '{canonical_role}'.",
        "success",
    )
    return redirect(
        url_for("dashboard.admin_job_roles", q=q_param, status=status_param)
    )
