# app/__init__.py
from __future__ import annotations

import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from flask import Flask, render_template
from flask_login import LoginManager, login_required
from sqlalchemy import exc as sa_exc

from extensions import db, migrate
from models import User  # must be importable after db is defined

load_dotenv()

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
    # MULTI-WORKER SAFE create_all()
    # ---------------------------------------------------------
    with app.app_context():
        try:
            db.create_all()
        except sa_exc.ProgrammingError as e:
            # Happens when multiple workers try creating the same tables
            app.logger.warning(f"db.create_all() warning (safe to ignore): {e}")
        except Exception as e:
            app.logger.error(f"db.create_all() failed: {e}")
            raise

    # ---------------------------------------------------------
    # Register blueprints
    # ---------------------------------------------------------
    from .blueprints.auth import bp as auth_bp
    from .blueprints.admin import bp as admin_bp
    from .blueprints.records import bp as records_bp
    from .blueprints.maps import bp as maps_bp
    from .blueprints.dashboard import bp as dashboard_bp
    from .blueprints.upload import bp as upload_bp

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
