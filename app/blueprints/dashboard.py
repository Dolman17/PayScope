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
    Supports multi-select for sector / county / job_role.
    Provides extended stats for multiple charts.
    """
    # Multi-selects: repeated query params (?sector=A&sector=B)
    sectors_selected = request.args.getlist("sector")
    counties_selected = request.args.getlist("county")
    roles_selected = request.args.getlist("job_role") or request.args.getlist("role")

    # Scalar filters still use the shared helper
    scalar_filter_map = {
        "q": request.args.get("q"),
        "month": request.args.get("month"),
        "year": request.args.get("year"),
        "rate_min": request.args.get("rate_min"),
        "rate_max": request.args.get("rate_max"),
    }
    filters, extra_search = build_filters_from_request(scalar_filter_map)

    base_q = JobRecord.query.filter(*filters)
    if extra_search is not None:
        base_q = extra_search(base_q)

    # Apply multi-select filters via IN clauses
    if sectors_selected:
        base_q = base_q.filter(JobRecord.sector.in_(sectors_selected))
    if counties_selected:
        base_q = base_q.filter(JobRecord.county.in_(counties_selected))
    if roles_selected:
        base_q = base_q.filter(JobRecord.job_role.in_(roles_selected))

    # Subquery (from the now-filtered base_q)
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

    # Aggregates over full filtered dataset
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

    # Top counties (count)
    top_counties_rows = (
        db.session.query(sq.c.county, func.count(sq.c.id))
        .filter(sq.c.county.isnot(None))
        .group_by(sq.c.county)
        .order_by(func.count(sq.c.id).desc())
        .limit(10)
        .all()
    )
    top_counties = [{"county": c or "—", "count": int(n or 0)} for c, n in top_counties_rows]

    # Top roles (count)
    top_roles_rows = (
        db.session.query(sq.c.job_role, func.count(sq.c.id))
        .filter(sq.c.job_role.isnot(None))
        .group_by(sq.c.job_role)
        .order_by(func.count(sq.c.id).desc())
        .limit(10)
        .all()
    )
    top_roles = [{"role": r or "—", "count": int(n or 0)} for r, n in top_roles_rows]

    # Sector breakdown (count + avg + min/max + stddev)
    sector_rows = (
        db.session.query(
            sq.c.sector,
            func.count(sq.c.id),
            func.avg(sq.c.pay_rate),
            func.min(sq.c.pay_rate),
            func.max(sq.c.pay_rate),
            func.stddev_samp(sq.c.pay_rate),
        )
        .group_by(sq.c.sector)
        .order_by(func.count(sq.c.id).desc())
        .all()
    )

    sector_stats = []
    sector_ranges = []
    sector_volatility = []
    for s, n, a, mn, mx, sd in sector_rows:
        sector_name = s or "Unknown"
        count = int(n or 0)
        avg_val = float(a) if a is not None else 0.0
        min_val = float(mn) if mn is not None else 0.0
        max_val = float(mx) if mx is not None else 0.0
        sd_val = float(sd) if sd is not None else 0.0

        sector_stats.append(
            {
                "sector": sector_name,
                "count": count,
                "avg_rate": avg_val,
            }
        )
        sector_ranges.append(
            {
                "sector": sector_name,
                "count": count,
                "avg_rate": avg_val,
                "min_rate": min_val,
                "max_rate": max_val,
            }
        )
        sector_volatility.append(
            {
                "sector": sector_name,
                "stddev": sd_val,
                "count": count,
            }
        )

    # Distribution bands for histogram
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
        return q.scalar() or 0

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

    # Monthly trend (avg pay by year/month)
    trend_rows = (
        db.session.query(
            sq.c.imported_year,
            sq.c.imported_month,
            func.avg(sq.c.pay_rate),
        )
        .filter(sq.c.imported_year.isnot(None), sq.c.imported_month.isnot(None))
        .group_by(sq.c.imported_year, sq.c.imported_month)
        .order_by(sq.c.imported_year, sq.c.imported_month)
        .all()
    )
    monthly_trend = [
        {
            "year": y,
            "month": m,
            "avg_rate": float(a) if a is not None else None,
        }
        for (y, m, a) in trend_rows
    ]

    # Sector × county average pay (heatmap / stacked)
    heat_rows = (
        db.session.query(
            sq.c.sector,
            sq.c.county,
            func.avg(sq.c.pay_rate),
        )
        .filter(sq.c.sector.isnot(None), sq.c.county.isnot(None))
        .group_by(sq.c.sector, sq.c.county)
        .all()
    )
    sector_county_heat = [
        {
            "sector": s or "Unknown",
            "county": c or "Unknown",
            "avg_rate": float(a) if a is not None else None,
        }
        for (s, c, a) in heat_rows
    ]

    # Top companies by avg pay
    company_rows = (
        db.session.query(
            sq.c.company_name,
            func.avg(sq.c.pay_rate),
            func.count(sq.c.id),
        )
        .filter(sq.c.company_name.isnot(None))
        .group_by(sq.c.company_name)
        .order_by(func.avg(sq.c.pay_rate).desc())
        .limit(10)
        .all()
    )
    top_companies = [
        {
            "company_name": n or "Unknown",
            "avg_rate": float(a) if a is not None else None,
            "count": int(c or 0),
        }
        for (n, a, c) in company_rows
    ]

    # Role mix (role counts per sector)
    mix_rows = (
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
            "count": int(c or 0),
        }
        for (s, r, c) in mix_rows
    ]

    # County trend (for top 5 counties by count)
    top_county_names = [c["county"] for c in top_counties[:5]]
    county_trend_rows = []
    if top_county_names:
        county_trend_rows = (
            db.session.query(
                sq.c.county,
                sq.c.imported_year,
                sq.c.imported_month,
                func.avg(sq.c.pay_rate),
            )
            .filter(
                sq.c.county.isnot(None),
                sq.c.imported_year.isnot(None),
                sq.c.imported_month.isnot(None),
                sq.c.county.in_(top_county_names),
            )
            .group_by(sq.c.county, sq.c.imported_year, sq.c.imported_month)
            .order_by(sq.c.county, sq.c.imported_year, sq.c.imported_month)
            .all()
        )

    county_trends = {}
    for county, y, m, a in county_trend_rows:
        c_name = county or "Unknown"
        county_trends.setdefault(c_name, []).append(
            {"year": y, "month": m, "avg_rate": float(a) if a is not None else None}
        )

    # Role × sector matrix (avg pay per role/sector)
    matrix_rows = (
        db.session.query(
            sq.c.sector,
            sq.c.job_role,
            func.avg(sq.c.pay_rate),
        )
        .filter(sq.c.sector.isnot(None), sq.c.job_role.isnot(None))
        .group_by(sq.c.sector, sq.c.job_role)
        .all()
    )
    role_sector_matrix = [
        {
            "sector": s or "Unknown",
            "role": r or "Unknown",
            "avg_rate": float(a) if a is not None else None,
        }
        for (s, r, a) in matrix_rows
    ]

    stats = {
        "total": total,
        "total_count": total,
        "avg_rate": avg_rate,
        "min_rate": min_rate,
        "max_rate": max_rate,
        "top_counties": top_counties,
        "top_roles": top_roles,
        "sector_stats": sector_stats,
        "sector_ranges": sector_ranges,
        "sector_volatility": sector_volatility,
        "distribution": dist,
        "monthly_trend": monthly_trend,
        "sector_county_heat": sector_county_heat,
        "top_companies": top_companies,
        "role_mix": role_mix,
        "county_trends": county_trends,
        "role_sector_matrix": role_sector_matrix,
    }

    # Sample (max 200) sent to front-end / AI / scatter
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

    # Filter state for the template (lists for multi-selects / checkboxes)
    filter_state = {
        "q": request.args.get("q"),
        "sector": sectors_selected,
        "county": counties_selected,
        "job_role": roles_selected,
        "month": request.args.get("month"),
        "year": request.args.get("year"),
        "rate_min": request.args.get("rate_min"),
        "rate_max": request.args.get("rate_max"),
    }

    return render_template(
        "insights.html",
        stats=stats,
        options=options,
        filters=filter_state,
        filter_query=request.query_string.decode(),
        records=records,
        total_count=total,
    )

