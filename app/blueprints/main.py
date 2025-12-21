from flask import Blueprint, render_template, redirect, url_for
from flask_login import current_user, login_required

# Renamed blueprint to avoid conflict with public_landing.py
bp = Blueprint("main", __name__)


@bp.route("/")
def index():
    # Logged-in users go straight to the app HOME tiles page
    if current_user.is_authenticated:
        return redirect(url_for("auth.home"))

    # Logged-out users see the marketing landing page
    return render_template("index.html", hide_nav=True)


@bp.route("/home")
@login_required
def home():
    # Post-login landing page (tiles for Upload, Map, Pay Explorer, Insights, Records)
    return render_template("home.html")
