# app/blueprints/dashboard/quick_search.py
from __future__ import annotations

from typing import Dict, List

from flask import render_template, request
from flask_login import login_required
from sqlalchemy import func, case, cast, Integer, or_

from extensions import db
from models import JobRecord
from . import bp


@bp.route("/quick-search")
@login_required
def quick_search():
    """
    Fast search by (role, location) with:
      - total records
      - avg/min/max rate
      - distinct companies
      - simple month-on-month and 3-month deltas
      - recent records table

    Role is matched against canonical role where available:
      COALESCE(job_role_group, job_role) ILIKE %role%
    Location is matched against county, region (if present), and postcode prefix.
    """

    # ------------------------------------------------------------------
    # Dropdown options (always populated)
    # ------------------------------------------------------------------
    role_expr = func.coalesce(JobRecord.job_role_group, JobRecord.job_role)

    role_options = [
        r[0]
        for r in (
            db.session.query(role_expr)
            .filter(role_expr.isnot(None), func.trim(role_expr) != "")
            .distinct()
            .order_by(role_expr)
            .all()
        )
    ]

    location_options = [
        c[0]
        for c in (
            db.session.query(JobRecord.county)
            .filter(JobRecord.county.isnot(None), func.trim(JobRecord.county) != "")
            .distinct()
            .order_by(JobRecord.county)
            .all()
        )
    ]

    # ------------------------------------------------------------------
    # Incoming query
    # ------------------------------------------------------------------
    role_raw = (request.args.get("role") or "").strip()
    loc_raw = (request.args.get("location") or "").strip()
    had_query = bool(role_raw or loc_raw)

    query = {
        "role": role_raw,
        "location": loc_raw,
    }

    # No query yet – just show the shell page
    if not had_query:
        return render_template(
            "quick_search.html",
            query=query,
            role_options=role_options,
            location_options=location_options,
            had_query=False,
            summary=None,
            deltas=None,
            monthly_buckets=[],
            recent_records=[],
        )

    # ------------------------------------------------------------------
    # Base filters
    # ------------------------------------------------------------------
    base_q = JobRecord.query

    if role_raw:
        pattern = f"%{role_raw}%"
        base_q = base_q.filter(role_expr.ilike(pattern))

    if loc_raw:
        loc_pattern = f"%{loc_raw}%"
        pc_pattern = f"{loc_raw}%"

        location_clauses = [
            JobRecord.county.ilike(loc_pattern),
            JobRecord.postcode.ilike(pc_pattern),
        ]

        # If a region column exists, include it in the location match
        if hasattr(JobRecord, "region"):
            location_clauses.append(JobRecord.region.ilike(loc_pattern))

        base_q = base_q.filter(or_(*location_clauses))

    # ------------------------------------------------------------------
    # Summary metrics
    # ------------------------------------------------------------------
    total_records = base_q.with_entities(func.count(JobRecord.id)).scalar() or 0
    avg_rate = base_q.with_entities(func.avg(JobRecord.pay_rate)).scalar()
    min_rate = base_q.with_entities(func.min(JobRecord.pay_rate)).scalar()
    max_rate = base_q.with_entities(func.max(JobRecord.pay_rate)).scalar()
    total_companies = (
        base_q.with_entities(func.count(func.distinct(JobRecord.company_id))).scalar() or 0
    )

    summary = {
        "total_records": int(total_records),
        "avg_rate": float(avg_rate or 0.0) if avg_rate is not None else None,
        "min_rate": float(min_rate or 0.0) if min_rate is not None else None,
        "max_rate": float(max_rate or 0.0) if max_rate is not None else None,
        "total_companies": int(total_companies),
    }

    # ------------------------------------------------------------------
    # Monthly buckets + deltas
    # ------------------------------------------------------------------
    # imported_year / imported_month may now be text like "2024" and "December".
    # Safely normalise them into numeric year/month using CASE expressions.
    year_str = func.nullif(JobRecord.imported_year, "")
    month_str = func.nullif(JobRecord.imported_month, "")

    # Numeric year only (e.g. "2024")
    year_int = case(
        (year_str.op("~")(r"^[0-9]{4}$"), cast(year_str, Integer)),
        else_=None,
    )

    # Month: support both numeric ("12") and text ("December", "Dec")
    month_int = case(
        # Numeric month 1–12
        (month_str.op("~")(r"^[0-9]+$"), cast(month_str, Integer)),
        # Full month names
        (func.lower(month_str) == "january", 1),
        (func.lower(month_str) == "february", 2),
        (func.lower(month_str) == "march", 3),
        (func.lower(month_str) == "april", 4),
        (func.lower(month_str) == "may", 5),
        (func.lower(month_str) == "june", 6),
        (func.lower(month_str) == "july", 7),
        (func.lower(month_str) == "august", 8),
        (func.lower(month_str) == "september", 9),
        (func.lower(month_str) == "october", 10),
        (func.lower(month_str) == "november", 11),
        (func.lower(month_str) == "december", 12),
        # Common three-letter abbreviations, just in case
        (func.lower(month_str) == "jan", 1),
        (func.lower(month_str) == "feb", 2),
        (func.lower(month_str) == "mar", 3),
        (func.lower(month_str) == "apr", 4),
        (func.lower(month_str) == "jun", 6),
        (func.lower(month_str) == "jul", 7),
        (func.lower(month_str) == "aug", 8),
        (func.lower(month_str) == "sep", 9),
        (func.lower(month_str) == "oct", 10),
        (func.lower(month_str) == "nov", 11),
        (func.lower(month_str) == "dec", 12),
        else_=None,
    )

    period_expr = year_int * 100 + month_int

    # Only keep rows where year/month resolved to integers for the buckets.
    buckets_q = (
        base_q.with_entities(
            period_expr.label("period"),
            year_int.label("year"),
            month_int.label("month"),
            func.count(JobRecord.id).label("count"),
            func.avg(JobRecord.pay_rate).label("avg_rate"),
        )
        .filter(year_int.isnot(None), month_int.isnot(None))
        .group_by("period", "year", "month")
        .order_by("period")
    )

    bucket_rows = buckets_q.all()

    monthly_buckets: List[Dict[str, object]] = []
    for row in bucket_rows:
        monthly_buckets.append(
            {
                "period": int(row.period),
                "year": int(row.year),
                "month": int(row.month),
                "count": int(row.count or 0),
                "avg_rate": float(row.avg_rate or 0.0),
            }
        )

    # Simple deltas
    deltas = {
        "have_data": bool(monthly_buckets),
        "latest": None,
        "prev_1m": None,
        "delta_1m_count": None,
        "delta_1m_rate": None,
        "last_3m": None,
        "prev_3m": None,
        "delta_3m_count": None,
        "delta_3m_rate": None,
    }

    if monthly_buckets:
        latest = monthly_buckets[-1]
        deltas["latest"] = latest

        # 1-month change (if we have at least 2 buckets)
        if len(monthly_buckets) >= 2:
            prev = monthly_buckets[-2]
            deltas["prev_1m"] = prev
            deltas["delta_1m_count"] = latest["count"] - prev["count"]
            deltas["delta_1m_rate"] = latest["avg_rate"] - prev["avg_rate"]

        # 3-month rolling change (if we have at least 6 buckets)
        if len(monthly_buckets) >= 6:
            last3 = monthly_buckets[-3:]
            prev3 = monthly_buckets[-6:-3]

            last3_count = sum(b["count"] for b in last3)
            prev3_count = sum(b["count"] for b in prev3)
            last3_rate = (
                sum(b["avg_rate"] * b["count"] for b in last3) / last3_count
                if last3_count
                else 0.0
            )
            prev3_rate = (
                sum(b["avg_rate"] * b["count"] for b in prev3) / prev3_count
                if prev3_count
                else 0.0
            )

            deltas["last_3m"] = {
                "count": last3_count,
                "avg_rate": last3_rate,
            }
            deltas["prev_3m"] = {
                "count": prev3_count,
                "avg_rate": prev3_rate,
            }
            deltas["delta_3m_count"] = last3_count - prev3_count
            deltas["delta_3m_rate"] = last3_rate - prev3_rate

    # ------------------------------------------------------------------
    # Recent records (top 20 in this slice)
    # ------------------------------------------------------------------
    recent_rows = (
        base_q.with_entities(
            role_expr.label("role"),
            JobRecord.sector,
            JobRecord.company_name,
            JobRecord.county,
            JobRecord.postcode,
            JobRecord.pay_rate,
            year_int.label("year"),
            month_int.label("month"),
        )
        .order_by(
            year_int.desc().nullslast(),
            month_int.desc().nullslast(),
            JobRecord.id.desc(),
        )
        .limit(20)
        .all()
    )

    recent_records: List[Dict[str, object]] = []
    for row in recent_rows:
        recent_records.append(
            {
                "role": row.role,
                "sector": row.sector,
                "company_name": row.company_name,
                "county": row.county,
                "postcode": row.postcode,
                "pay_rate": float(row.pay_rate) if row.pay_rate is not None else None,
                "imported_year": int(row.year) if row.year is not None else None,
                "imported_month": int(row.month) if row.month is not None else None,
            }
        )

    return render_template(
        "quick_search.html",
        query=query,
        role_options=role_options,
        location_options=location_options,
        had_query=had_query,
        summary=summary,
        deltas=deltas,
        monthly_buckets=monthly_buckets,
        recent_records=recent_records,
    )
