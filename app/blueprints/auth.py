# app/blueprints/auth.py

from __future__ import annotations

from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, logout_user
from werkzeug.security import check_password_hash

from werkzeug.security import generate_password_hash
import os
from extensions import db
from models import User

bp = Blueprint("auth", __name__)

@bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        remember = "remember" in request.form

        user = User.query.filter_by(username=username).first()
        if user and check_password_hash(user.password, password):
            login_user(user, remember=remember)
            flash("Logged in successfully.", "success")
            next_page = request.args.get("next")
            return redirect(next_page) if next_page else redirect(url_for("home"))
        flash("Invalid username or password.", "error")

    return render_template("login.html")

@bp.route("/logout")
def logout():
    logout_user()
    flash("Logged out.", "info")
    return redirect(url_for("auth.login"))


@bp.route("/init-admin")
def init_admin():
    """
    One-time bootstrap route to create an initial admin user.

    - Only works if there are currently NO users in the database.
    - Username/password are taken from env vars:
        INITIAL_ADMIN_USERNAME
        INITIAL_ADMIN_PASSWORD
    """
    # Only allow if DB has no users yet
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
        f"✅ Admin user created.\n\n"
        f"Username: {username}\n"
        f"Password: (value of INITIAL_ADMIN_PASSWORD env var)"
    )
    # Plain text response is fine here
    return msg, 201
