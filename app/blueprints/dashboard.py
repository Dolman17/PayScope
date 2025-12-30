# app/blueprints/dashboard.py
from __future__ import annotations

from datetime import date, datetime, time, timedelta
import difflib
import re
import json
import csv
import io
from typing import Dict, List, Optional, Tuple

from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    jsonify,
    Response,
)
from flask_login import login_required, current_user
from sqlalchemy import func
from sqlalchemy.orm import aliased
from werkzeug.datastructures import MultiDict

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

# Optional fuzzy matcher (RapidFuzz preferred, fallback to difflib)
try:
    from rapidfuzz import fuzz  # type: ignore
except Exception:  # pragma: no cover
    fuzz = None  # type: ignore


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

    # Prefer canonical roles if present; fallback to raw job_role
    role_col = JobRecord.job_role
    if hasattr(JobRecord, "job_role_group"):
        role_col = JobRecord.job_role_group

    return {
        "sectors": col_distinct(JobRecord.sector),
        "roles": col_distinct(role_col),
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
    # Restrict dashboard to superusers only
    if not getattr(current_user, "is_superuser", None) or not current_user.is_superuser():
        flash("You do not have access to the dashboard. Use the main workspace instead.", "error")
        return redirect(url_for("auth.home"))

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
        .order_by(sq.c.job_role)
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
        stats=stats,
        options=options,
        filters=filters_map,
        filter_query=request.query_string.decode(),
        total_count=total,
        uncategorised_roles_count=uncategorised_roles_count,
    )


# ----------------------------------------------------------------------
# Job role hygiene helpers (rules + suggestions)
# ----------------------------------------------------------------------

# A small, opinionated ruleset for turning messy raw titles into canonical roles.
# This is intentionally conservative: we only auto-map when we're confident.
_ROLE_RULES: List[Tuple[re.Pattern[str], str]] = [
    # Registered Nurse variations
    (re.compile(r"\b(rn|rgn|registered\s*nurse|staff\s*nurse)\b", re.I), "Registered Nurse"),
    (re.compile(r"\b(nurse\s*associate)\b", re.I), "Nurse Associate"),
    (re.compile(r"\b(community\s*nurse)\b", re.I), "Registered Nurse"),
    # Care / support
    (re.compile(r"\b(care\s*assistant|carer|care\s*worker|health\s*care\s*assistant|hca)\b", re.I), "Care Assistant"),
    (re.compile(r"\b(senior\s*(care\s*assistant|carer|care\s*worker|hca))\b", re.I), "Senior Care Assistant"),
    (re.compile(r"\b(support\s*worker)\b", re.I), "Support Worker"),
    (re.compile(r"\b(senior\s*support\s*worker)\b", re.I), "Senior Support Worker"),
    (re.compile(r"\b(learning\s*disabilities?\s*support)\b", re.I), "Support Worker"),
    # Leadership / management
    (re.compile(r"\b(team\s*leader)\b", re.I), "Team Leader"),
    (re.compile(r"\b(deputy\s*manager)\b", re.I), "Deputy Manager"),
    (re.compile(r"\b(registered\s*manager|service\s*manager|home\s*manager)\b", re.I), "Registered Manager"),
    # Domestic / housekeeping
    (re.compile(r"\b(house\s*keeper|housekeeper|domestic\s*assistant|domestic)\b", re.I), "Domestic Assistant"),
    (re.compile(r"\b(cleaner|cleaning)\b", re.I), "Cleaner"),
    (re.compile(r"\b(cook|chef|kitchen\s*assistant)\b", re.I), "Kitchen Assistant"),
    # Maintenance
    (re.compile(r"\b(maintenance\s*(assistant|person|operative)|handyman)\b", re.I), "Maintenance"),
    (re.compile(r"\b(electrician)\b", re.I), "Electrician"),
    # Admin
    (re.compile(r"\b(administrator|admin\s*assistant|office\s*administrator)\b", re.I), "Administrator"),
]


def _clean_raw_job_title(raw: str) -> str:
    """Normalize a raw job title into a comparable 'cleaned' string."""
    s = (raw or "").strip()
    if not s:
        return ""

    # Remove bracketed noise: (Nights), [Temp], etc.
    s = re.sub(r"\([^\)]*\)", " ", s)
    s = re.sub(r"\[[^\]]*\]", " ", s)

    # Remove obvious pay fragments: £12.34, 12.34/hr, per hour
    s = re.sub(r"£\s*\d+(?:\.\d+)?", " ", s, flags=re.I)
    s = re.sub(r"\b\d+(?:\.\d+)?\s*(?:ph|p\/h|per\s*hour|\/hr|hr)\b", " ", s, flags=re.I)

    # Remove contract/time qualifiers (keep conservative)
    s = re.sub(
        r"\b(full\s*time|part\s*time|temp(?:orary)?|permanent|contract|bank|agency)\b",
        " ",
        s,
        flags=re.I,
    )
    s = re.sub(r"\b(days?|nights?|weekends?)\b", " ", s, flags=re.I)

    # Strip location-like suffixes after separators (common in scraped titles)
    s = re.split(r"\s[-–|•]\s", s, maxsplit=1)[0]

    # Lower, keep letters/numbers/spaces, collapse whitespace
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s\+\/]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    return s


def _rule_based_canonical(raw: str) -> Optional[str]:
    """Return a canonical role if a rule matches, else None."""
    if not raw:
        return None
    for pat, canonical in _ROLE_RULES:
        if pat.search(raw):
            return canonical
    return None


def _build_canonical_vocab() -> List[str]:
    """Build a stable list of canonical roles we can suggest against."""
    vocab: List[str] = []

    # 1) Existing canonical roles in mappings
    try:
        rows = db.session.query(JobRoleMapping.canonical_role).distinct().all()
        vocab.extend([r[0] for r in rows if (r and (r[0] or "").strip())])
    except Exception:
        pass

    # 2) Existing canonical roles already applied on JobRecord (job_role_group)
    try:
        if hasattr(JobRecord, "job_role_group"):
            rows = (
                db.session.query(JobRecord.job_role_group)
                .filter(
                    JobRecord.job_role_group.isnot(None),
                    func.trim(JobRecord.job_role_group) != "",
                )
                .distinct()
                .all()
            )
            vocab.extend([r[0] for r in rows if (r and (r[0] or "").strip())])
    except Exception:
        pass

    # 3) Built-in role taxonomy seeds (kept short on purpose)
    seed = [
        "Care Assistant",
        "Senior Care Assistant",
        "Support Worker",
        "Senior Support Worker",
        "Registered Nurse",
        "Nurse Associate",
        "Team Leader",
        "Deputy Manager",
        "Registered Manager",
        "Domestic Assistant",
        "Cleaner",
        "Kitchen Assistant",
        "Maintenance",
        "Electrician",
        "Administrator",
    ]
    vocab.extend(seed)

    # De-dupe, preserve order-ish
    seen = set()
    out: List[str] = []
    for v in vocab:
        vv = (v or "").strip()
        if not vv:
            continue
        key = vv.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(vv)

    return out


def _canonical_role_filter_options() -> List[str]:
    """
    Build a sorted list of canonical roles for the Insights Job Role filter.

    Uses:
      - JobRoleMapping.canonical_role
      - JobRecord.job_role_group (if present)

    Falls back to raw JobRecord.job_role only if nothing canonical is available,
    so the filter never appears empty.
    """
    labels: set[str] = set()

    # Canonical roles from mappings
    try:
        rows = (
            db.session.query(JobRoleMapping.canonical_role)
            .filter(
                JobRoleMapping.canonical_role.isnot(None),
                func.trim(JobRoleMapping.canonical_role) != "",
            )
            .distinct()
            .order_by(JobRoleMapping.canonical_role)
            .all()
        )
        for (val,) in rows:
            s = (val or "").strip()
            if s:
                labels.add(s)
    except Exception:
        pass

    # Canonical roles already written into job_role_group
    try:
        if hasattr(JobRecord, "job_role_group"):
            rows = (
                db.session.query(JobRecord.job_role_group)
                .filter(
                    JobRecord.job_role_group.isnot(None),
                    func.trim(JobRecord.job_role_group) != "",
                )
                .distinct()
                .order_by(JobRecord.job_role_group)
                .all()
            )
            for (val,) in rows:
                s = (val or "").strip()
                if s:
                    labels.add(s)
    except Exception:
        pass

    # If we genuinely have no canonical labels yet, fall back to raw job_role
    if not labels:
        try:
            rows = (
                db.session.query(JobRecord.job_role)
                .filter(
                    JobRecord.job_role.isnot(None),
                    func.trim(JobRecord.job_role) != "",
                )
                .distinct()
                .order_by(JobRecord.job_role)
                .limit(1000)
                .all()
            )
            for (val,) in rows:
                s = (val or "").strip()
                if s:
                    labels.add(s)
        except Exception:
            pass

    return sorted(labels, key=lambda x: x.lower())


def _fuzzy_best_match(query: str, choices: List[str]) -> Tuple[Optional[str], int]:
    """Return (best_choice, score 0-100)."""
    q = (query or "").strip()
    if not q or not choices:
        return (None, 0)

    if fuzz is not None:
        best = None
        best_score = 0
        for c in choices:
            sc = int(fuzz.token_set_ratio(q, c))
            if sc > best_score:
                best_score = sc
                best = c
        return (best, best_score)

    # difflib fallback
    best = None
    best_score = 0
    for c in choices:
        sc = int(100 * difflib.SequenceMatcher(None, q.lower(), (c or "").lower()).ratio())
        if sc > best_score:
            best_score = sc
            best = c
    return (best, best_score)


def _suggest_canonical_for_raw(raw: str, vocab: List[str]) -> Dict[str, object]:
    """Compute cleaned form + best suggestion + score + source."""
    cleaned = _clean_raw_job_title(raw)
    rule_hit = _rule_based_canonical(raw or "")

    if rule_hit:
        return {"cleaned": cleaned, "suggested": rule_hit, "score": 100, "source": "rule"}

    best, score = _fuzzy_best_match(cleaned, vocab)
    return {"cleaned": cleaned, "suggested": best, "score": int(score), "source": "fuzzy"}


# ----------------------------------------------------------------------
# Canonical label cleanup helper
# ----------------------------------------------------------------------

ROLE_LABEL_MAX_LEN = 80  # keep canonical labels short and tidy


def _clean_canonical_label(raw: str) -> str:
    """
    Best-effort normalisation for JobRoleMapping.canonical_role based on
    patterns seen in the export (markdown blobs, 'Canonical Job Role:', etc).

    Returns a cleaned label or an empty string if we can't confidently improve it.
    """
    if not raw:
        return ""

    s = str(raw)
    # Normalise newlines but keep them so we can reason about "first line"
    s = s.replace("\r\n", "\n").replace("\r", "\n").strip()

    # Quick bail-out: looks like an already clean, short, single-line label
    if (
        "\n" not in s
        and len(s) <= ROLE_LABEL_MAX_LEN
        and "**" not in s
        and not re.search(r"(canonical\s+job\s+role|job\s+role|job\s+title)\s*[:\-]", s, re.I)
    ):
        clean = re.sub(r"\s+", " ", s).strip()
        clean = clean.strip("*").strip()
        clean = re.sub(r"^[#\-\*\s]+", "", clean).strip()
        return clean

    original = s

    # 1) Try to extract after "Canonical Job Role:", "Job Role:", or "Job Title:"
    label_re = re.compile(
        r"(canonical\s+job\s+role|job\s+role|job\s+title)\s*[:\-]\s*(.+)",
        re.IGNORECASE,
    )
    m = label_re.search(s)
    if m:
        candidate = m.group(2).strip()
        # Strip surrounding markdown ** if present
        candidate = candidate.strip("*").strip()
        # Only use up to first line / markdown break
        candidate = candidate.split("\n", 1)[0].strip()
        if "**" in candidate:
            candidate = candidate.split("**", 1)[0].strip()

        # Final clean-up
        candidate = re.sub(r"\s+", " ", candidate).strip()
        if candidate and len(candidate) <= ROLE_LABEL_MAX_LEN:
            return candidate

    # 2) If the string starts with a bold block, take the first **…** as the label
    if s.startswith("**"):
        inner = s[2:]
        if "**" in inner:
            candidate = inner.split("**", 1)[0].strip()
            candidate = re.sub(r"\s+", " ", candidate).strip()
            if candidate and len(candidate) <= ROLE_LABEL_MAX_LEN:
                return candidate

    # 3) Fallback: use the first line, stripped of markdown headers / bullets
    first_line = original.split("\n", 1)[0]
    first_line = re.sub(r"^[#\-\*\s]+", "", first_line).strip()  # strip bullets / '#' etc
    first_line = first_line.strip("*").strip()
    first_line = re.sub(r"\s+", " ", first_line).strip()

    # Don’t keep obviously over-long lines as canonical labels
    if len(first_line) > ROLE_LABEL_MAX_LEN:
        return ""

    # Require at least one letter
    if not re.search(r"[A-Za-z]", first_line):
        return ""

    return first_line


# ----------------------------------------------------------------------
# Role hygiene scoring helpers (for report + unmapped hotspots)
# ----------------------------------------------------------------------

def _role_hygiene_flags(raw: str) -> Dict[str, bool]:
    """
    Lightweight heuristics to flag 'noisy' job titles.

    These are intentionally simple and cheap – just enough to surface
    the worst offenders in the admin report.
    """
    s = (raw or "").strip()
    has_letters = bool(re.search(r"[A-Za-z]", s))

    is_all_caps = has_letters and s.upper() == s

    has_pay_terms = bool(
        re.search(
            r"(£\s*\d|\b\d+(?:\.\d+)?\s*(ph|p\/h|per\s*hour|hourly|rate|salary))",
            s,
            re.IGNORECASE,
        )
    )

    has_location_terms = bool(
        re.search(
            r"\b(london|manchester|birmingham|leeds|liverpool|sheffield|nottingham|bristol|"
            r"cardiff|glasgow|scotland|wales|england|uk|united\s+kingdom|remote|hybrid)\b",
            s,
            re.IGNORECASE,
        )
    )

    has_agency_noise = bool(
        re.search(
            r"\b(agency|recruitment|recruiting|staffing|solutions|limited|ltd|plc)\b",
            s,
            re.IGNORECASE,
        )
    )

    has_brackets_or_codes = any(ch in s for ch in "[]()") or bool(
        re.search(r"\b(ref|reference)\s*[:#]\s*\w+", s, re.IGNORECASE)
    )

    has_shift_words = bool(
        re.search(
            r"\b(nights?|days?|weekends?|shifts?|rota|rotational)\b",
            s,
            re.IGNORECASE,
        )
    )

    return {
        "is_all_caps": is_all_caps,
        "has_pay_terms": has_pay_terms,
        "has_location_terms": has_location_terms,
        "has_agency_noise": has_agency_noise,
        "has_brackets_or_codes": has_brackets_or_codes,
        "has_shift_words": has_shift_words,
    }


def _role_hygiene_score(flags: Dict[str, bool]) -> int:
    """
    Convert hygiene flags into a 0–100 'cleanliness' score.

    100 = looks like a clean, reusable job title
      0 = very messy (location/pay/agency/code noise everywhere)
    """
    score = 100

    if flags.get("has_pay_terms"):
        score -= 25
    if flags.get("has_location_terms"):
        score -= 20
    if flags.get("has_agency_noise"):
        score -= 15
    if flags.get("has_brackets_or_codes"):
        score -= 10
    if flags.get("has_shift_words"):
        score -= 5
    if flags.get("is_all_caps"):
        score -= 5

    if score < 0:
        score = 0
    if score > 100:
        score = 100
    return score


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
      - q: search over raw roles (JobRecord.job_role)
      - canonical_search: search over canonical roles (JobRoleMapping.canonical_role)
      - status: all / with / without canonical mapping
    """
    # Make sure the mapping table exists (safe if already created)
    try:
        JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        # If this somehow fails, we still try to render with empty mappings below
        pass

    search = (request.args.get("q") or "").strip()
    canonical_search = (request.args.get("canonical_search") or "").strip()
    status = (request.args.get("status") or "all").strip().lower()
    if status not in ("all", "with", "without"):
        status = "all"

    # Base query: distinct job_role values with counts
    JRM = aliased(JobRoleMapping)

    q = db.session.query(
        JobRecord.job_role.label("raw_value"),
        func.count(JobRecord.id).label("count"),
    ).filter(JobRecord.job_role.isnot(None))

    if search:
        pattern = f"%{search}%"
        q = q.filter(JobRecord.job_role.ilike(pattern))

    # Join mode depends on status + canonical_search
    if status == "with" or canonical_search:
        # Need a join so we can filter on canonical_role
        q = q.join(JRM, JobRecord.job_role == JRM.raw_value)
    elif status == "without":
        q = q.outerjoin(JRM, JobRecord.job_role == JRM.raw_value).filter(JRM.id.is_(None))
    # status == "all" and no canonical_search: no join needed, we want everything

    if canonical_search:
        pattern_c = f"%{canonical_search}%"
        q = q.filter(JRM.canonical_role.ilike(pattern_c))

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

    # Keep consistent with the rest of the dashboard: count None OR empty
    # (This page is about raw roles, but the hygiene count should reflect canonical if available)
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

    # Suggestions (rules + fuzzy) for this page of raw roles
    vocab = _build_canonical_vocab()
    suggestions: Dict[str, Dict[str, object]] = {}
    for r in rows:
        rv = getattr(r, "raw_value", None)
        suggestions[rv] = _suggest_canonical_for_raw(rv or "", vocab)

    return render_template(
        "admin_job_roles.html",
        rows=rows,
        mappings=mappings,
        search=search,
        status=status,
        canonical_search=canonical_search,
        uncategorised_roles_count=uncategorised_roles_count,
        suggestions=suggestions,
    )


@bp.route("/admin/job-roles/map", methods=["POST"])
@login_required
def admin_job_roles_map():
    """
    Create/update a mapping for a raw job_role value to a canonical role.
    Optionally applies the change immediately to existing JobRecord rows.
    Redirects back to the current Job Role Cleaner filters (q, status, canonical_search).
    """
    raw_value = (request.form.get("raw_value") or "").strip()
    canonical_role = (request.form.get("canonical_role") or "").strip()
    apply_now = request.form.get("apply_now") == "1"

    # Preserve filters on redirect
    q_param = (request.form.get("q") or "").strip()
    status_param = (request.form.get("status") or "all").strip().lower()
    canonical_param = (request.form.get("canonical_search") or "").strip()

    if not raw_value or not canonical_role:
        flash("Raw value and canonical role are required.", "error")
        return redirect(
            url_for(
                "dashboard.admin_job_roles",
                q=q_param,
                status=status_param,
                canonical_search=canonical_param,
            )
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
        # Prefer writing canonical into job_role_group (preserves raw job_role for audit),
        # but fall back to overwriting job_role if the canonical column doesn't exist.
        if hasattr(JobRecord, "job_role_group"):
            db.session.query(JobRecord).filter(JobRecord.job_role == raw_value).update(
                {JobRecord.job_role_group: canonical_role},
                synchronize_session=False,
            )
        else:
            db.session.query(JobRecord).filter(JobRecord.job_role == raw_value).update(
                {JobRecord.job_role: canonical_role},
                synchronize_session=False,
            )

    db.session.commit()
    flash(f"Mapping saved for role '{raw_value}' → '{canonical_role}'.", "success")
    return redirect(
        url_for(
            "dashboard.admin_job_roles",
            q=q_param,
            status=status_param,
            canonical_search=canonical_param,
        )
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
      - q, status, canonical_search: current filter state on the Job Role Cleaner page
    """
    raw_values = request.form.getlist("raw_values") or []
    # De-duplicate, strip empty
    raw_values = sorted({(rv or "").strip() for rv in raw_values if (rv or "").strip()})

    canonical_role = (request.form.get("canonical_role") or "").strip()
    apply_now = request.form.get("apply_now") == "1"

    q_param = (request.form.get("q") or "").strip()
    status_param = (request.form.get("status") or "all").strip().lower()
    canonical_param = (request.form.get("canonical_search") or "").strip()

    if not raw_values:
        flash("Select at least one job title before using bulk assign.", "error")
        return redirect(
            url_for(
                "dashboard.admin_job_roles",
                q=q_param,
                status=status_param,
                canonical_search=canonical_param,
            )
        )

    if not canonical_role:
        flash("Canonical role is required for bulk assignment.", "error")
        return redirect(
            url_for(
                "dashboard.admin_job_roles",
                q=q_param,
                status=status_param,
                canonical_search=canonical_param,
            )
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
        # Prefer writing canonical into job_role_group (preserves raw job_role for audit),
        # but fall back to overwriting job_role if the canonical column doesn't exist.
        if hasattr(JobRecord, "job_role_group"):
            db.session.query(JobRecord).filter(JobRecord.job_role.in_(raw_values)).update(
                {JobRecord.job_role_group: canonical_role},
                synchronize_session=False,
            )
        else:
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
        url_for(
            "dashboard.admin_job_roles",
            q=q_param,
            status=status_param,
            canonical_search=canonical_param,
        )
    )


@bp.route("/admin/job-roles/auto-clean", methods=["POST"])
@login_required
def admin_job_roles_auto_clean():
    """Auto-clean + auto-map selected raw roles using rules + fuzzy suggestions.

    Expects:
      - raw_values: repeated fields
      - threshold: integer (0-100), default 88
      - apply_now: "1" to backfill existing JobRecord rows (writes to job_role_group if present)
      - q, status, canonical_search: preserved filter params
    """
    raw_values = request.form.getlist("raw_values") or []
    raw_values = sorted({(rv or "").strip() for rv in raw_values if (rv or "").strip()})

    q_param = (request.form.get("q") or "").strip()
    status_param = (request.form.get("status") or "all").strip().lower()
    canonical_param = (request.form.get("canonical_search") or "").strip()

    try:
        threshold = int((request.form.get("threshold") or "88").strip())
    except Exception:
        threshold = 88
    threshold = max(0, min(100, threshold))

    apply_now = request.form.get("apply_now") == "1"

    if not raw_values:
        flash("Select at least one job title before running auto-clean.", "error")
        return redirect(
            url_for(
                "dashboard.admin_job_roles",
                q=q_param,
                status=status_param,
                canonical_search=canonical_param,
            )
        )

    # Ensure mapping table exists
    try:
        JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    vocab = _build_canonical_vocab()

    mapped = 0
    skipped = 0

    # We'll also keep a list of (raw_value, canonical) for an efficient backfill update.
    backfill_pairs: List[Tuple[str, str]] = []

    for raw in raw_values:
        suggestion = _suggest_canonical_for_raw(raw, vocab)
        canonical = suggestion.get("suggested")  # type: ignore
        score = int(suggestion.get("score") or 0)  # type: ignore

        if not canonical or score < threshold:
            skipped += 1
            continue

        canonical_role = str(canonical).strip()
        if not canonical_role:
            skipped += 1
            continue

        mapping = JobRoleMapping.query.filter_by(raw_value=raw).first()
        if mapping is None:
            mapping = JobRoleMapping(raw_value=raw, canonical_role=canonical_role)
            db.session.add(mapping)
        else:
            mapping.canonical_role = canonical_role

        mapped += 1
        if apply_now:
            backfill_pairs.append((raw, canonical_role))

    if apply_now and backfill_pairs:
        # Backfill existing JobRecord rows. Prefer job_role_group if available.
        if hasattr(JobRecord, "job_role_group"):
            for raw, canonical_role in backfill_pairs:
                db.session.query(JobRecord).filter(JobRecord.job_role == raw).update(
                    {JobRecord.job_role_group: canonical_role},
                    synchronize_session=False,
                )
        else:
            for raw, canonical_role in backfill_pairs:
                db.session.query(JobRecord).filter(JobRecord.job_role == raw).update(
                    {JobRecord.job_role: canonical_role},
                    synchronize_session=False,
                )

    db.session.commit()

    if mapped and skipped:
        flash(
            f"Auto-clean mapped {mapped} role(s). Skipped {skipped} below the {threshold}% confidence threshold.",
            "success",
        )
    elif mapped:
        flash(f"Auto-clean mapped {mapped} role(s) (threshold {threshold}%).", "success")
    else:
        flash(
            f"No roles were auto-mapped. Try lowering the threshold (currently {threshold}%).",
            "info",
        )

    return redirect(
        url_for(
            "dashboard.admin_job_roles",
            q=q_param,
            status=status_param,
            canonical_search=canonical_param,
        )
    )


@bp.route("/admin/job-roles/ai-suggest", methods=["POST"])
@login_required
def admin_job_roles_ai_suggest():
    """
    Lightweight AI helper:
      - Reuses existing JobRoleMapping as a cache (no cost).
      - Falls back to our rules + fuzzy logic (no cost).
      - Only calls OpenAI if heuristics are low-confidence.
    Returns JSON:
      { ok, canonical_role, score, source, model, reason }
    """
    data = request.get_json(silent=True) or {}
    raw_value = (data.get("raw_value") or "").strip()

    if not raw_value:
        return jsonify({"ok": False, "error": "No raw job title provided."}), 400

    # 1) If we already have a mapping, treat it as cached and avoid AI entirely
    mapping = JobRoleMapping.query.filter_by(raw_value=raw_value).first()
    if mapping and (mapping.canonical_role or "").strip():
        return jsonify(
            {
                "ok": True,
                "canonical_role": mapping.canonical_role.strip(),
                "score": 100,
                "source": "cache",
                "model": None,
                "reason": "Existing mapping from job_role_mappings used as cache.",
            }
        )

    # 2) Use our deterministic rules + fuzzy matching first (cheap)
    vocab = _build_canonical_vocab()
    suggestion = _suggest_canonical_for_raw(raw_value, vocab)
    heuristic_canonical = (suggestion.get("suggested") or "").strip()  # type: ignore
    heuristic_score = int(suggestion.get("score") or 0)  # type: ignore
    heuristic_source = suggestion.get("source") or "heuristic"  # type: ignore

    # If the heuristic is strong enough, just use that and skip AI
    HEURISTIC_THRESHOLD = 90
    if heuristic_canonical and heuristic_score >= HEURISTIC_THRESHOLD:
        return jsonify(
            {
                "ok": True,
                "canonical_role": heuristic_canonical,
                "score": heuristic_score,
                "source": heuristic_source,
                "model": None,
                "reason": "High-confidence heuristic (rules/fuzzy) – no AI call needed.",
            }
        )

    # 3) Call OpenAI as a last resort
    try:
        from openai import OpenAI  # type: ignore

        client = OpenAI()
    except Exception:
        # OpenAI not installed or not configured
        # Still return the heuristic if we have *something*
        if heuristic_canonical:
            return jsonify(
                {
                    "ok": True,
                    "canonical_role": heuristic_canonical,
                    "score": heuristic_score,
                    "source": heuristic_source,
                    "model": None,
                    "reason": "OpenAI client not available; returned best heuristic match instead.",
                }
            )
        return jsonify(
            {
                "ok": False,
                "error": "AI client not configured on server and no high-confidence heuristic match was found.",
            }
        ), 500

    # Keep candidate list reasonably small for cost
    candidate_roles = _build_canonical_vocab()[:60]
    bullets = "\n".join(f"- {r}" for r in candidate_roles)

    system_prompt = (
        "You are a data cleaning assistant for UK social care job adverts.\n"
        "Your job is to map messy raw job titles into a clean, standardised canonical job role.\n"
        "Only choose from the provided canonical roles list. If nothing fits, return an empty string.\n"
        "Be conservative and aim for accuracy over recall."
    )

    user_prompt = (
        f'Raw job title: "{raw_value}"\n\n'
        "Candidate canonical roles:\n"
        f"{bullets}\n\n"
        "Return a SINGLE JSON object with keys:\n"
        '  - "canonical_role": either one of the candidate roles above, or "" if none is suitable\n'
        '  - "confidence": integer 0–100 reflecting how confident you are in the mapping\n'
        '  - "reason": a short explanation (max 2 sentences)\n'
        "Do not include any extra text, only valid JSON."
    )

    try:
        completion = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            max_tokens=200,
            temperature=0.1,
        )
        text = completion.choices[0].message.content or ""
    except Exception:
        # If AI call fails, fall back to heuristic if we have anything
        if heuristic_canonical:
            return jsonify(
                {
                    "ok": True,
                    "canonical_role": heuristic_canonical,
                    "score": heuristic_score,
                    "source": heuristic_source,
                    "model": None,
                    "reason": "AI backend error; returned best heuristic match instead.",
                }
            )
        return jsonify(
            {
                "ok": False,
                "error": "AI backend error and no high-confidence heuristic match was found.",
            }
        ), 500

    # Try to extract JSON from the AI response
    try:
        # Handle possible code fences
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1:
            json_str = text[start : end + 1]
        else:
            json_str = text

        payload = json.loads(json_str)
    except Exception:
        # If parsing fails, again fall back to heuristic if possible
        if heuristic_canonical:
            return jsonify(
                {
                    "ok": True,
                    "canonical_role": heuristic_canonical,
                    "score": heuristic_score,
                    "source": heuristic_source,
                    "model": "gpt-4o-mini",
                    "reason": "AI response was not valid JSON; returned best heuristic match instead.",
                }
            )
        return jsonify(
            {
                "ok": False,
                "error": "AI response was not valid JSON and no high-confidence heuristic match was found.",
            }
        ), 500

    canonical_role = (payload.get("canonical_role") or "").strip()
    confidence = int(payload.get("confidence") or 0)
    reason = (payload.get("reason") or "").strip()

    # If AI says "none suitable", surface that gently
    if not canonical_role or canonical_role not in candidate_roles:
        # Still return ok=True so UI can show the explanation
        return jsonify(
            {
                "ok": True,
                "canonical_role": "",
                "score": confidence,
                "source": "ai",
                "model": "gpt-4o-mini",
                "reason": reason
                or "AI could not confidently map this title to any canonical role.",
            }
        )

    return jsonify(
        {
            "ok": True,
            "canonical_role": canonical_role,
            "score": confidence,
            "source": "ai",
            "model": "gpt-4o-mini",
            "reason": reason,
        }
    )


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
            (JobRecord.sector.is_(None))
            | (func.trim(JobRecord.sector) == "")
            | (func.lower(func.trim(JobRecord.sector)) == "other")
        )

    if search:
        pattern = f"%{search}%"
        q = q.filter(role_expr.ilike(pattern))

    if status == "with":
        q = q.join(JobRoleSectorOverride, JobRoleSectorOverride.canonical_role == role_expr)
    elif status == "without":
        q = q.outerjoin(
            JobRoleSectorOverride,
            JobRoleSectorOverride.canonical_role == role_expr,
        ).filter(JobRoleSectorOverride.id.is_(None))

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
        return redirect(
            url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param)
        )

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
    return redirect(
        url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param)
    )


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
        return redirect(
            url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param)
        )

    if not canonical_sector:
        flash("Canonical sector is required for bulk assignment.", "error")
        return redirect(
            url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param)
        )

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
    return redirect(
        url_for("dashboard.admin_role_sectors", q=q_param, status=status_param, only_other=only_other_param)
    )


# ----------------------------------------------------------------------
# Admin: Job Role Mapping Report (HTML + CSV export)
# ----------------------------------------------------------------------

def _job_roles_report_data() -> Tuple[List[Dict[str, object]], Dict[str, List[Dict[str, object]]]]:
    """
    Shared query for job role mapping report.

    Returns:
      summary: list of {
          canonical_role,
          raw_variants,
          total_count,
          share_total_pct,
          worst_hygiene_score,
          noisy_variants
      }
      grouped_roles: {
          canonical_role: [
              {
                  raw_value,
                  count,
                  share_total_pct,
                  hygiene_flags,
                  hygiene_score,
              },
              ...
          ]
      }
    """
    # Global total for % of whole dataset
    total_records = db.session.query(func.count(JobRecord.id)).scalar() or 0

    # Join JobRoleMapping -> JobRecord to get counts per raw_value
    q = (
        db.session.query(
            JobRoleMapping.canonical_role.label("canonical_role"),
            JobRoleMapping.raw_value.label("raw_value"),
            func.count(JobRecord.id).label("count"),
        )
        .outerjoin(JobRecord, JobRecord.job_role == JobRoleMapping.raw_value)
        .group_by(JobRoleMapping.canonical_role, JobRoleMapping.raw_value)
        .order_by(JobRoleMapping.canonical_role.asc(), func.count(JobRecord.id).desc())
    )

    rows = q.all()

    grouped_roles: Dict[str, List[Dict[str, object]]] = {}
    for canonical_role, raw_value, count in rows:
        cr = canonical_role or "—"
        rv = raw_value or "—"
        c = int(count or 0)

        flags = _role_hygiene_flags(rv)
        hygiene_score = _role_hygiene_score(flags)

        if total_records > 0:
            share_total_pct = round((c / total_records) * 100.0, 2)
        else:
            share_total_pct = 0.0

        grouped_roles.setdefault(cr, []).append(
            {
                "raw_value": rv,
                "count": c,
                "share_total_pct": share_total_pct,
                "hygiene_flags": flags,
                "hygiene_score": hygiene_score,
            }
        )

    # Summary table: one row per canonical_role
    summary: List[Dict[str, object]] = []
    for canonical_role, raw_list in grouped_roles.items():
        total_count = sum(r["count"] for r in raw_list)
        raw_variants = len(raw_list)
        worst_hygiene_score = 100
        noisy_variants = 0

        for r in raw_list:
            score = int(r.get("hygiene_score") or 0)
            if score < worst_hygiene_score:
                worst_hygiene_score = score
            if score < 80:
                noisy_variants += 1

        if total_records > 0:
            share_total_pct = round((total_count / total_records) * 100.0, 2)
        else:
            share_total_pct = 0.0

        summary.append(
            {
                "canonical_role": canonical_role,
                "raw_variants": raw_variants,
                "total_count": total_count,
                "share_total_pct": share_total_pct,
                "worst_hygiene_score": worst_hygiene_score,
                "noisy_variants": noisy_variants,
            }
        )

    # Sort summary by total_count desc so biggest roles float to the top
    summary.sort(key=lambda r: r["total_count"], reverse=True)

    return summary, grouped_roles


def _unmapped_role_hotspots(limit: int = 50) -> List[Dict[str, object]]:
    """
    Top unmapped raw job_role values by volume, with hygiene metrics.

    Intended for a 'Unmapped hotspots' widget on the report.
    """
    total_records = db.session.query(func.count(JobRecord.id)).scalar() or 0

    q = (
        db.session.query(
            JobRecord.job_role.label("raw_value"),
            func.count(JobRecord.id).label("count"),
        )
        .filter(JobRecord.job_role.isnot(None), func.trim(JobRecord.job_role) != "")
        .outerjoin(
            JobRoleMapping,
            JobRecord.job_role == JobRoleMapping.raw_value,
        )
        .filter(JobRoleMapping.id.is_(None))
        .group_by(JobRecord.job_role)
        .order_by(func.count(JobRecord.id).desc())
        .limit(limit)
    )

    rows = q.all()
    hotspots: List[Dict[str, object]] = []

    for raw_value, count in rows:
        rv = raw_value or "—"
        c = int(count or 0)
        flags = _role_hygiene_flags(rv)
        hygiene_score = _role_hygiene_score(flags)

        if total_records > 0:
            share_total_pct = round((c / total_records) * 100.0, 2)
        else:
            share_total_pct = 0.0

        hotspots.append(
            {
                "raw_value": rv,
                "count": c,
                "share_total_pct": share_total_pct,
                "hygiene_flags": flags,
                "hygiene_score": hygiene_score,
            }
        )

    return hotspots


def _sector_override_mismatches(
    min_rows: int = 20,
    dominance_threshold: float = 0.5,
) -> List[Dict[str, object]]:
    """
    For each JobRoleSectorOverride, compare the override sector with the
    dominant observed sector in JobRecord for that canonical role.

    Returns a list of likely mismatches to surface as gentle warnings.
    """
    # Use canonical role expression consistent with admin_role_sectors
    role_expr = func.coalesce(JobRecord.job_role_group, JobRecord.job_role)

    q = (
        db.session.query(
            JobRoleSectorOverride.canonical_role.label("canonical_role"),
            JobRoleSectorOverride.canonical_sector.label("override_sector"),
            JobRecord.sector.label("observed_sector"),
            func.count(JobRecord.id).label("count"),
        )
        .outerjoin(JobRecord, role_expr == JobRoleSectorOverride.canonical_role)
        .group_by(
            JobRoleSectorOverride.canonical_role,
            JobRoleSectorOverride.canonical_sector,
            JobRecord.sector,
        )
    )

    rows = q.all()
    by_role: Dict[str, Dict[str, object]] = {}

    for canonical_role, override_sector, observed_sector, count in rows:
        cr = canonical_role or "—"
        ov_sector = (override_sector or "Unknown") or "Unknown"
        obs_sector = (observed_sector or "Unknown") or "Unknown"
        c = int(count or 0)

        if cr not in by_role:
            by_role[cr] = {
                "override_sector": ov_sector,
                "sector_counts": {},
                "total_rows": 0,
            }

        role_entry = by_role[cr]
        sector_counts = role_entry["sector_counts"]  # type: ignore[assignment]
        sector_counts[obs_sector] = sector_counts.get(obs_sector, 0) + c  # type: ignore[index]
        role_entry["total_rows"] = int(role_entry["total_rows"]) + c  # type: ignore[index]

    mismatches: List[Dict[str, object]] = []

    for canonical_role, data in by_role.items():
        override_sector = data["override_sector"]  # type: ignore[assignment]
        sector_counts: Dict[str, int] = data["sector_counts"]  # type: ignore[assignment]
        total_rows = int(data["total_rows"])  # type: ignore[assignment]

        if total_rows < min_rows:
            continue

        # Find dominant observed sector
        if not sector_counts:
            continue
        dominant_sector, dominant_count = max(sector_counts.items(), key=lambda kv: kv[1])

        if total_rows > 0:
            dominant_share = dominant_count / float(total_rows)
        else:
            dominant_share = 0.0

        # Only flag when the dominant observed sector strongly disagrees
        if (
            dominant_sector
            and override_sector
            and dominant_sector != override_sector
            and dominant_share >= dominance_threshold
        ):
            mismatches.append(
                {
                    "canonical_role": canonical_role,
                    "override_sector": override_sector,
                    "dominant_sector": dominant_sector,
                    "dominant_share": round(dominant_share * 100.0, 1),
                    "total_rows": total_rows,
                }
            )

    # Sort by importance: biggest total_rows first
    mismatches.sort(key=lambda r: r["total_rows"], reverse=True)
    return mismatches


@bp.route("/admin/job-roles/report")
@login_required
def admin_job_roles_report():
    """
    Report: for each canonical role, show which raw job_role values map to it,
    plus counts of JobRecord rows per raw value.

    This is read-only and safe for export.
    """
    # Keep consistent with dashboard access: superusers only.
    if not getattr(current_user, "is_superuser", None) or not current_user.is_superuser():
        flash("You do not have access to the Job Role Mapping report.", "error")
        return redirect(url_for("auth.home"))

    summary, grouped_roles = _job_roles_report_data()
    unmapped_hotspots = _unmapped_role_hotspots(limit=50)
    sector_mismatches = _sector_override_mismatches()

    return render_template(
        "admin_job_roles_report.html",
        summary=summary,
        grouped_roles=grouped_roles,
        unmapped_hotspots=unmapped_hotspots,
        sector_mismatches=sector_mismatches,
    )


@bp.route("/admin/job-roles/report/export")
@login_required
def admin_job_roles_report_export():
    """
    CSV export for the Job Role Mapping report.

    One row per (canonical_role, raw_value) with JobRecord count:
      canonical_role, raw_value, jobrecord_count
    """
    # Same access control as the HTML report
    if not getattr(current_user, "is_superuser", None) or not current_user.is_superuser():
        flash("You do not have access to the Job Role Mapping export.", "error")
        return redirect(url_for("auth.home"))

    _summary, grouped_roles = _job_roles_report_data()

    # Flatten into rows for CSV
    output = io.StringIO()
    writer = csv.writer(output)

    # Header
    writer.writerow(["canonical_role", "raw_value", "jobrecord_count"])

    for canonical_role, raw_list in grouped_roles.items():
        cr = canonical_role or "—"
        for item in raw_list:
            writer.writerow(
                [
                    cr,
                    item.get("raw_value") or "—",
                    item.get("count") or 0,
                ]
            )

    csv_data = output.getvalue()
    output.close()

    resp = Response(csv_data, mimetype="text/csv")
    resp.headers["Content-Disposition"] = 'attachment; filename="job_role_mapping_report.csv"'
    return resp


# ----------------------------------------------------------------------
# Admin: One-off Canonical Label Cleaner
# ----------------------------------------------------------------------

@bp.route("/admin/job-roles/clean-canonical", methods=["POST"])
@login_required
def admin_job_roles_clean_canonical():
    """
    One-off (but safe to re-run) canonical label cleaner.

    It:
      - scans all JobRoleMapping rows
      - identifies labels that look like long AI paragraphs / summaries
      - replaces them with a shorter, job-title-style label via _clean_canonical_label
      - leaves already-clean labels unchanged
    """
    # Same access rules as other admin hygiene tools
    if not getattr(current_user, "is_superuser", None) or not current_user.is_superuser():
        flash("You do not have access to the canonical role cleaner.", "error")
        return redirect(url_for("auth.home"))

    try:
        JobRoleMapping.__table__.create(bind=db.engine, checkfirst=True)
    except Exception:
        pass

    mappings = JobRoleMapping.query.all()
    updated = 0
    skipped = 0

    for m in mappings:
        old = (m.canonical_role or "").strip()
        if not old:
            skipped += 1
            continue

        new = _clean_canonical_label(old)

        # Only write if the helper actually changed the label
        if new and new != old:
            m.canonical_role = new
            updated += 1
        else:
            skipped += 1

    if updated:
        db.session.commit()

    if updated:
        flash(
            f"Canonical label cleaner updated {updated} mapping(s). "
            f"{skipped} left unchanged.",
            "success",
        )
    else:
        flash(
            "Canonical label cleaner did not change any mappings. "
            "Existing labels already look clean.",
            "info",
        )

    return redirect(url_for("dashboard.admin_job_roles_report"))
