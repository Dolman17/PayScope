# app/blueprints/admin.py
from __future__ import annotations

from flask import Blueprint, render_template, request, redirect, url_for, flash, abort
from flask_login import login_required, current_user
from sqlalchemy import desc, or_, cast, String

from extensions import db
from models import User, AIAnalysisLog
from .utils import commit_or_rollback

bp = Blueprint("admin", __name__)

def _require_superuser():
    if not current_user.is_authenticated or not current_user.is_superuser():
        flash("Access denied – superuser only.", "error")
        return False
    return True

@bp.route("/users", methods=["GET", "POST"])
@login_required
def manage_users():
    if not _require_superuser():
        return redirect(url_for("home"))

    if request.method == "POST":
        action = request.form.get("action")

        if action == "add":
            from werkzeug.security import generate_password_hash
            username = (request.form.get("username") or "").strip()
            password = (request.form.get("password") or "").strip()
            admin_level = int(request.form.get("admin_level", 0))

            if not username or not password:
                flash("Username and password are required.", "error")
            elif db.session.query(User).filter_by(username=username).first():
                flash("Username already exists.", "error")
            else:
                new_user = User(
                    username=username,
                    password=generate_password_hash(password),
                    admin_level=admin_level,
                )
                db.session.add(new_user)
                try:
                    commit_or_rollback()
                    flash(f"User '{username}' added.", "success")
                except Exception:
                    flash("Failed to add user.", "error")

        elif action == "delete":
            user_id = request.form.get("user_id")
            if not user_id:
                flash("User ID missing.", "error")
            elif int(user_id) == current_user.id:
                flash("You cannot delete your own account.", "error")
            else:
                user = db.session.get(User, int(user_id))
                if user:
                    db.session.delete(user)
                    try:
                        commit_or_rollback()
                        flash("User deleted.", "info")
                    except Exception:
                        flash("Failed to delete user.", "error")
                else:
                    flash("User not found.", "error")

        elif action == "update":
            user_id = request.form.get("user_id")
            admin_level = request.form.get("admin_level")
            if not (user_id and admin_level is not None):
                flash("Missing user ID or admin level.", "error")
            else:
                user = db.session.get(User, int(user_id))
                if user:
                    user.admin_level = int(admin_level)
                    try:
                        commit_or_rollback()
                        flash("User updated.", "success")
                    except Exception:
                        flash("Failed to update user.", "error")
                else:
                    flash("User not found.", "error")

    users = User.query.all()
    return render_template("manage_users.html", users=users)

@bp.route("/backfill-counties")
@login_required
def backfill_counties():
    if not _require_superuser():
        return redirect(url_for("home"))

    from geopy.geocoders import Nominatim
    from geopy.extra.rate_limiter import RateLimiter
    from models import JobRecord

    geolocator = Nominatim(user_agent="pay-rate-map")
    reverse = RateLimiter(geolocator.reverse, min_delay_seconds=2)

    updated = 0
    skipped = 0

    q = JobRecord.query.filter((JobRecord.county == None) | (JobRecord.county == ""))  # noqa: E711
    limit = request.args.get("limit", type=int)
    if limit:
        q = q.limit(limit)
    missing = q.all()

    for record in missing:
        if record.latitude and record.longitude:
            try:
                location = reverse((record.latitude, record.longitude), exactly_one=True)
                if location and "county" in location.raw.get("address", {}):
                    record.county = location.raw["address"]["county"]
                    updated += 1
                else:
                    skipped += 1
            except Exception as e:
                print(f"❌ Error reverse geocoding ID {record.id}: {e}")
                skipped += 1

    try:
        commit_or_rollback()
        flash(f"✅ County backfill complete. Updated: {updated}, Skipped: {skipped}", "success")
    except Exception:
        flash("Failed to save backfill results.", "error")
    return redirect(url_for("upload.upload"))

@bp.route("/ai-logs", methods=["GET"])
@login_required
def ai_logs():
    # Only admins
    if getattr(current_user, "admin_level", 0) not in (1, 2):
        abort(403)

    q = (request.args.get("q") or "").strip()
    page = max(int(request.args.get("page", 1) or 1), 1)
    per_page = min(max(int(request.args.get("per_page", 20) or 20), 5), 100)

    query = AIAnalysisLog.query.order_by(desc(AIAnalysisLog.created_at))

    if q:
        like = f"%{q}%"
        query = (
            query.join(User, AIAnalysisLog.user, isouter=True)
                 .filter(
                     or_(
                         AIAnalysisLog.filters.ilike(like),
                         AIAnalysisLog.output_html.ilike(like),
                         cast(AIAnalysisLog.record_count, String).ilike(like),
                         User.username.ilike(like),
                     )
                 )
        )

    pagination = query.paginate(page=page, per_page=per_page, error_out=False)
    logs = pagination.items

    from flask import render_template
    return render_template("ai_logs.html", logs=logs, pagination=pagination, q=q, per_page=per_page)

@bp.route("/ai-logs/<int:log_id>", methods=["GET"])
@login_required
def ai_logs_get(log_id: int):
    if getattr(current_user, "admin_level", 0) not in (1, 2):
        abort(403)
    row = AIAnalysisLog.query.get_or_404(log_id)
    from flask import jsonify
    return jsonify({
        "id": row.id,
        "user_id": row.user_id,
        "created_at": row.created_at.isoformat(),
        "record_count": row.record_count,
        "filters": row.filters,
        "output_html": row.output_html,
    })
