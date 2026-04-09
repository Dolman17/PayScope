from __future__ import annotations

import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from flask import Flask, render_template
from flask_login import LoginManager, login_required

from extensions import db, migrate
from models import User

load_dotenv()  # Load .env before anything else

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
    # Register blueprints
    # ---------------------------------------------------------
    from .blueprints.auth import bp as auth_bp
    from .blueprints.admin import bp as admin_bp
    from .blueprints.records import bp as records_bp
    from .blueprints.maps import bp as maps_bp
    from .blueprints.dashboard import bp as dashboard_bp
    from .blueprints.upload import bp as upload_bp
    from .blueprints.main import bp as main_bp
    from .blueprints.api import bp as api_bp
    from .blueprints.marketing import marketing_bp
    from .blueprints.public_landing import bp as public_bp
    from .blueprints.insights import bp as insights_bp
    from .blueprints.recruiter import bp as recruiter_bp

    app.register_blueprint(main_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp, url_prefix="/admin")
    app.register_blueprint(records_bp)
    app.register_blueprint(maps_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(upload_bp)
    app.register_blueprint(api_bp, url_prefix="/api")
    app.register_blueprint(marketing_bp)
    app.register_blueprint(public_bp)
    app.register_blueprint(insights_bp)
    app.register_blueprint(recruiter_bp)

    # ---------------------------------------------------------
    # Home route
    # ---------------------------------------------------------
    @app.route("/")
    @login_required
    def home():
        return render_template("index.html", now=lambda: datetime.now(timezone.utc))

    return app