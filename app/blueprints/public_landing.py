from __future__ import annotations

from datetime import date
from sqlalchemy import func
from flask import Blueprint, render_template, request, redirect, url_for, flash

from extensions import db
from models import WaitlistSignup, AccessRequest

# Optional imports if these exist in your app; safe fallback if they don't.
try:
    from models import JobSummaryDaily, JobRecord, SectorMapping  # type: ignore
except Exception:
    JobSummaryDaily = None  # type: ignore
    JobRecord = None  # type: ignore
    SectorMapping = None  # type: ignore


bp = Blueprint("public", __name__)

CONTACT_EMAIL = "hello@payscope.uk"


def _safe_int(x):
    try:
        return int(x)
    except Exception:
        return 0


def _get_public_stats():
    """
    Lightweight, safe stats for the landing page.
    All optional: if a table isn't present, return None/0 without breaking the page.
    """
    stats = {
        "last_data_date": None,
        "job_records_count": None,
        "sectors_mapped_count": None,
        "coverage_monitored": True,  # you do monitor this operationally
    }

    # Freshness: latest daily summary date
    if JobSummaryDaily is not None:
        try:
            stats["last_data_date"] = db.session.query(func.max(JobSummaryDaily.summary_date)).scalar()
        except Exception:
            stats["last_data_date"] = None

    # Social proof: total records
    if JobRecord is not None:
        try:
            stats["job_records_count"] = _safe_int(db.session.query(func.count(JobRecord.id)).scalar())
        except Exception:
            stats["job_records_count"] = None

    # Social proof: sector mappings
    if SectorMapping is not None:
        try:
            stats["sectors_mapped_count"] = _safe_int(db.session.query(func.count(SectorMapping.id)).scalar())
        except Exception:
            stats["sectors_mapped_count"] = None

    return stats


@bp.route("/", methods=["GET"])
def landing():
    stats = _get_public_stats()
    return render_template(
        "index.html",
        contact_email=CONTACT_EMAIL,
        stats=stats,
        today=date.today(),
    )


@bp.route("/waitlist", methods=["POST"])
def waitlist_submit():
    email = (request.form.get("email") or "").strip().lower()
    notes = (request.form.get("notes") or "").strip()
    source = (request.form.get("source") or "landing").strip()

    if not email or "@" not in email or "." not in email:
        flash("Please enter a valid email address.", "error")
        return redirect(url_for("public.landing") + "#waitlist")

    existing = db.session.query(WaitlistSignup).filter_by(email=email).first()
    if existing:
        flash("You’re already on the waitlist — thanks!", "success")
        return redirect(url_for("public.landing") + "#waitlist")

    db.session.add(WaitlistSignup(email=email, notes=notes or None, source=source))
    try:
        db.session.commit()
        flash("You’re on the waitlist — we’ll be in touch.", "success")
    except Exception:
        db.session.rollback()
        flash("Sorry — we couldn’t save that. Please try again.", "error")

    return redirect(url_for("public.landing") + "#waitlist")


@bp.route("/request-access", methods=["POST"])
def request_access_submit():
    email = (request.form.get("email") or "").strip().lower()
    notes = (request.form.get("notes") or "").strip()
    source = (request.form.get("source") or "landing").strip()

    # email optional here (some people will just write notes)
    if email and (("@" not in email) or ("." not in email)):
        flash("Please enter a valid email address (or leave it blank).", "error")
        return redirect(url_for("public.landing") + "#request-access")

    db.session.add(AccessRequest(email=email or None, notes=notes or None, source=source))
    try:
        db.session.commit()
        flash("Request received — we’ll reply from hello@payscope.uk.", "success")
    except Exception:
        db.session.rollback()
        flash("Sorry — we couldn’t save that. Please try again.", "error")

    return redirect(url_for("public.landing") + "#request-access")


@bp.route("/privacy", methods=["GET"])
def privacy():
    return render_template("legal/privacy.html", contact_email=CONTACT_EMAIL)


@bp.route("/terms", methods=["GET"])
def terms():
    return render_template("legal/terms.html", contact_email=CONTACT_EMAIL)


@bp.route("/cookies", methods=["GET"])
def cookies():
    return render_template("legal/cookies.html", contact_email=CONTACT_EMAIL)
