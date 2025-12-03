# app/__init__.py
from __future__ import annotations

import os
from datetime import datetime, timezone
from dotenv import load_dotenv
load_dotenv()  # Load .env before anything else

from flask import Flask, app, render_template
from flask_login import LoginManager, login_required

from extensions import db, migrate
from models import User  # Must be importable after db is defined


login_manager = LoginManager()
login_manager.login_view = "auth.login"


def create_app():
    app = Flask(__name__)

    # ---------------------------------------------------------
    # Load config (Postgres from Railway or SQLite fallback)
    # ---------------------------------------------------------
    project_root = os.path.abspath(os.path.join(app.root_path, os.pardir))
    app.config.from_pyfile(os.path.join(project_root, "config.py"))

    # SECRET KEY (required for sessions)
    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-key-change-me")

    # Sensible defaults
    app.config.setdefault("UPLOAD_FOLDER", os.path.join(app.root_path, "uploads"))
    app.config.setdefault("MAX_CONTENT_LENGTH", 20 * 1024 * 1024)  # 20 MB
    app.config.setdefault("ALLOWED_EXTENSIONS", {".xlsx", ".xls", ".csv"})

    # Ensure upload folder exists
    if app.config.get("UPLOAD_FOLDER"):
        os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)

    # ---------------------------------------------------------
    # Initialise extensions
    # ---------------------------------------------------------
    db.init_app(app)
    migrate.init_app(app, db)
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id: str):
        try:
            return db.session.get(User, int(user_id))
        except Exception:
            return None

    @app.context_processor
    def inject_now():
        return {"current_year": datetime.now(timezone.utc).year}

    # ---------------------------------------------------------
    # IMPORTANT: Removed create_all() for Postgres + Alembic
    # ---------------------------------------------------------
    # Alembic handles all schema creation.
    # db.create_all() should not be used in a production app with migrations.
    # ---------------------------------------------------------

    # ---------------------------------------------------------
    # Register blueprints
    # ---------------------------------------------------------
    from .blueprints.auth import bp as auth_bp
    from .blueprints.admin import bp as admin_bp
    from .blueprints.records import bp as records_bp
    from .blueprints.maps import bp as maps_bp
    from .blueprints.dashboard import bp as dashboard_bp
    from .blueprints.upload import bp as upload_bp
    from .blueprints.main import bp as main_bp

    app.register_blueprint(main_bp) 
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp, url_prefix="/admin")
    app.register_blueprint(records_bp)
    app.register_blueprint(maps_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(upload_bp)

    # ---------------------------------------------------------
    # Home route
    # ---------------------------------------------------------
    @app.route("/")
    @login_required
    def home():
        return render_template("index.html", now=lambda: datetime.now(timezone.utc))

    return app
