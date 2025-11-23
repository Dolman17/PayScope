# app/blueprints/dashboard.py
from __future__ import annotations

from datetime import date, datetime, time, timedelta

from flask import Blueprint, render_template, request
from flask_login import login_required
from sqlalchemy import func

from extensions import db
from models import JobRecord, CronRunLog
from .utils import (
    build_filters_from_request,
    get_filter_options,
)

bp = Blueprint("dashboard", __name__)


def _fresh_filter_options():
    # Avoid TTL cache; query distincts directly so selects always populate
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
    """
    # Filters for JobRecord
    filters_map = {
        "q": request.args.get("q"),
        "sector": request.args.get("sector"),
        "job_role": request.args.get("job_role"),
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

    # Recent uploads by month/year (optional small widget if you want it later)
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
        # Extra (safe if unused)
        recent_uploads=recent_uploads,
    )


@bp.route("/insights")
@login_required
def insights():
    """
    Insights over JobRecord with filters.
    - Unpacks (filters, extra_search) from build_filters_from_request
    - Uses a subquery alias for aggregates to avoid cartesian products
    - Supplies `records` for any client-side scripts in insights.html
    """
    filters_map = {
        "q": request.args.get("q"),
        "sector": request.args.get("sector"),
        "job_role": request.args.get("job_role"),
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

    # Subquery
    sq = base_q.with_entities(
        JobRecord.id.label("id"),
        JobRecord.company_id.label("company_id"),
        JobRecord.company_name.label("company_name"),
        JobRecord.sector.label("sector"),
        JobRecord.job_role.label("job_role"),
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
    top_counties = [{"county": c or "—", "count": n} for c, n in top_counties_rows]

    # Top roles
    top_roles_rows = (
        db.session.query(sq.c.job_role, func.count(sq.c.id))
        .filter(sq.c.job_role.isnot(None))
        .group_by(sq.c.job_role)
        .order_by(func.count(sq.c.id).desc())
        .limit(10)
        .all()
    )
    top_roles = [{"role": r or "—", "count": n} for r, n in top_roles_rows]

    # Distribution bands
    def _band_count(lower, upper, include_lower=True, include_upper=False):
        q = db.session.query(func.count(sq.c.id))
        if lower is not None:
            q = q.filter(sq.c.pay_rate >= lower if include_lower else sq.c.pay_rate > lower)
        if upper is not None:
            q = q.filter(sq.c.pay_rate <= upper if include_upper else sq.c.pay_rate < upper)
        return q.scalar() or 0

    dist = [
        {"label": "< £11",   "count": _band_count(None, 11, include_upper=False)},
        {"label": "£11–£12", "count": _band_count(11, 12, include_lower=True, include_upper=False)},
        {"label": "£12–£13", "count": _band_count(12, 13, include_lower=True, include_upper=False)},
        {"label": "£13–£14", "count": _band_count(13, 14, include_lower=True, include_upper=False)},
        {"label": "≥ £14",   "count": _band_count(14, None, include_lower=True)},
    ]

    stats = {
        "total": total,
        "avg_rate": avg_rate,
        "min_rate": min_rate,
        "max_rate": max_rate,
        "top_counties": top_counties,
        "top_roles": top_roles,
        "distribution": dist,
    }

    # Lightweight set of rows for client-side UI in insights.html
    rows_for_client = (
        db.session.query(
            sq.c.id,
            sq.c.company_name,
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
            "job_role": r[2],
            "county": r[3],
            "pay_rate": float(r[4]) if r[4] is not None else None,
            "imported_month": r[5],
            "imported_year": r[6],
        }
        for r in rows_for_client
    ]

    options = get_filter_options(force=True)

    return render_template(
        "insights.html",
        stats=stats,
        options=options,
        filters=filters_map,
        filter_query=request.query_string.decode(),
        records=records,
    )
