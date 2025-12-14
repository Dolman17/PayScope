# app/blueprints/main.py
from flask import Blueprint, render_template, redirect, url_for
from flask_login import current_user

bp = Blueprint("main", __name__)

@bp.route("/")
def index():
    # Logged-in users go straight to the app dashboard
    if current_user.is_authenticated:
        return redirect(url_for("dashboard.dashboard"))

    # Logged-out users see the marketing landing page
    return render_template("index.html", hide_nav=True)
