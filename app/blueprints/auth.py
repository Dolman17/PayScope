# app/blueprints/auth.py
from __future__ import annotations

import os
from datetime import datetime

from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, logout_user, login_required
from werkzeug.security import check_password_hash, generate_password_hash

from extensions import db
from models import User

bp = Blueprint("auth", __name__)


@bp.route("/login", methods=["GET", "POST"])
def login():
    """
    Login handler.

    Behaviour:
    - Successful login redirects to ?next= if present and safe
    - Otherwise redirects to the post-login home page
    """
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        remember = "remember" in request.form

        user = User.query.filter_by(username=username).first()

        if user and check_password_hash(user.password, password):
            # Log the user in
            login_user(user, remember=remember)

            # Track last login time for monitoring / auditing
            user.last_login_at = datetime.utcnow()
            try:
                db.session.commit()
            except Exception:
                db.session.rollback()
                # Don't block login if this fails – just log/flash mildly if you want
                # For now, stay silent to avoid noise.

            flash("Logged in successfully.", "success")

            # Respect next param if it is a safe relative path
            next_page = request.args.get("next")
            if next_page and next_page.startswith("/"):
                return redirect(next_page)

            # Default post-login landing page (not the dashboard)
            return redirect(url_for("auth.home"))

        flash("Invalid username or password.", "error")

    return render_template("login.html")


@bp.route("/home")
@login_required
def home():
    """
    Post-login landing page with tiles for key functions.
    """
    return render_template("home.html")


@bp.route("/logout", methods=["POST"])
def logout():
    """
    Log the user out and return them to the public landing page.
    """
    logout_user()
    flash("Logged out.", "info")
    # Go back to marketing / public index (index.html)
    return redirect(url_for("public.landing"))


@bp.route("/init-admin")
def init_admin():
    """
    One-time bootstrap route to create an initial admin user.

    Rules:
    - Only works if NO users exist in the database
    - Username/password pulled from env vars:
        INITIAL_ADMIN_USERNAME
        INITIAL_ADMIN_PASSWORD
    """
    existing_count = User.query.count()
    if existing_count > 0:
        return (
            f"Init admin aborted: {existing_count} user(s) already exist.",
            400,
        )

    username = os.getenv("INITIAL_ADMIN_USERNAME", "admin")
    raw_password = os.getenv("INITIAL_ADMIN_PASSWORD", "changeme")

    if not raw_password:
        return (
            "INITIAL_ADMIN_PASSWORD env var is not set. "
            "Set it and redeploy, then hit /init-admin again.",
            400,
        )

    user = User(
        username=username,
        password=generate_password_hash(raw_password),
        admin_level=1,  # superuser
    )

    db.session.add(user)
    db.session.commit()

    msg = (
        "✅ Admin user created.\n\n"
        f"Username: {username}\n"
        "Password: (value of INITIAL_ADMIN_PASSWORD env var)"
    )

    return msg, 201
