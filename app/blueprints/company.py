# app/blueprints/company.py
from __future__ import annotations

from collections import Counter
from statistics import mean, median
from typing import Optional

from flask import Blueprint, render_template, abort
from flask_login import login_required
from extensions import db
from models import JobRecord, Company
from app.blueprints.utils import (
    logo_url_for,
    company_has_logo,
    _clean_company_name,
    _slugify,
)

bp = Blueprint("company", __name__)


def _find_company_by_slug(slug: str) -> Optional[Company]:
    """
    Given a company_id slug, try to resolve it to a Company row.

    We derive a slug from (canonical_name or name) using the same
    _clean_company_name + _slugify logic used elsewhere.
    """
    if not slug:
        return None

    candidates = Company.query.all()
    for c in candidates:
        base_name = (c.canonical_name or c.name or "").strip()
        cleaned = _clean_company_name(base_name)
        if not cleaned:
            continue
        cand_slug = _slugify(cleaned)
        if cand_slug == slug:
            return c
    return None


@bp.route("/company/<company_id>")
@login_required
def company_profile(company_id: str):
    """
    Company profile page.

    - Looks up all JobRecords with this company_id
    - Tries to resolve a matching Company row
    - Shows pay stats, locations, roles, etc.
    """
    company_id = (company_id or "").strip()
    if not company_id:
        abort(404)

    # All jobs linked to this company_id
    jobs = (
        JobRecord.query.filter_by(company_id=company_id)
        .order_by(JobRecord.pay_rate.desc())
        .all()
    )

    if not jobs:
        # No records for this slug at all
        abort(404)

    # Try to resolve a Company row
    company = _find_company_by_slug(company_id)

    # Derive a display name
    display_name = None
    if company and company.name:
        display_name = company.name
    else:
        # Fallback to the most common name in JobRecords
        names = [j.company_name for j in jobs if j.company_name]
        if names:
            display_name = Counter(names).most_common(1)[0][0]
        else:
            display_name = company_id

    # Stats
    rates = [float(j.pay_rate) for j in jobs if j.pay_rate is not None]
    avg_rate = mean(rates) if rates else None
    med_rate = median(rates) if rates else None
    min_rate = min(rates) if rates else None
    max_rate = max(rates) if rates else None

    # Distinct roles, counties, years
    roles = sorted({j.job_role for j in jobs if j.job_role})
    counties = sorted({j.county for j in jobs if j.county})
    years = sorted({j.imported_year for j in jobs if j.imported_year})

    logo_url = logo_url_for(company_id)
    has_logo = company_has_logo(company_id)

    # Simple histogram buckets for display
    buckets = [
        ("< £11", lambda v: v < 11),
        ("£11–£12", lambda v: 11 <= v < 12),
        ("£12–£13", lambda v: 12 <= v < 13),
        ("£13–£14", lambda v: 13 <= v < 14),
        ("≥ £14", lambda v: v >= 14),
    ]
    hist = []
    for label, test in buckets:
        count = sum(1 for v in rates if test(v))
        hist.append({"label": label, "count": count})

    return render_template(
        "company/profile.html",
        company=company,
        company_id=company_id,
        display_name=display_name,
        jobs=jobs,
        avg_rate=avg_rate,
        med_rate=med_rate,
        min_rate=min_rate,
        max_rate=max_rate,
        roles=roles,
        counties=counties,
        years=years,
        logo_url=logo_url,
        has_logo=has_logo,
        hist=hist,
    )
