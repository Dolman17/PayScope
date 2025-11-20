from __future__ import annotations

import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from flask import Flask, render_template
from flask_login import LoginManager, login_required

from extensions import db, migrate
from models import User

load_dotenv()

login_manager = LoginManager()
login_manager.login_view = "auth.login"


def create_app():
    app = Flask(__name__)

    # Load config.py
    project_root = os.path.abspath(os.path.join(app.root_path, os.pardir))
    app.config.from_pyfile(os.path.join(project_root, "config.py"))

    print("📡 Using DB:", app.config["SQLALCHEMY_DATABASE_URI"])

    # Upload config
    upload_dir = os.path.join(project_root, "uploads")
    app.config.setdefault("UPLOAD_FOLDER", upload_dir)
    os.makedirs(upload_dir, exist_ok=True)

    app.config.setdefault("MAX_CONTENT_LENGTH", 20 * 1024 * 1024)
    app.config.setdefault("ALLOWED_EXTENSIONS", {".xlsx", ".xls", ".csv"})

    # Init extensions
    db.init_app(app)
    migrate.init_app(app, db)
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(uid):
        try:
            return db.session.get(User, int(uid))
        except Exception:
            return None

    @app.context_processor
    def inject_now():
        return {"current_year": datetime.now(timezone.utc).year}

    # Blueprints
    from .blueprints.auth import bp as auth_bp
    from .blueprints.admin import bp as admin_bp
    from .blueprints.records import bp as records_bp
    from .blueprints.maps import bp as maps_bp
    from .blueprints.dashboard import bp as dashboard_bp
    from .blueprints.upload import bp as upload_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(records_bp)
    app.register_blueprint(maps_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(upload_bp)

    @app.route("/")
    @login_required
    def home():
        return render_template("index.html")

    @app.route("/health")
    def health():
        return "OK", 200

    return app
