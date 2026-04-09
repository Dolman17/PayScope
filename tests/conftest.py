from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import pytest
from werkzeug.security import generate_password_hash


@pytest.fixture(scope="session")
def app(tmp_path_factory: pytest.TempPathFactory):
    db_dir = tmp_path_factory.mktemp("db")
    db_path = db_dir / "test_payscope.sqlite"

    os.environ["DATABASE_URL"] = f"sqlite:///{db_path}"
    os.environ["SECRET_KEY"] = "pytest-secret-key"
    os.environ["INITIAL_ADMIN_USERNAME"] = "bootstrap_admin"
    os.environ["INITIAL_ADMIN_PASSWORD"] = "bootstrap_password"

    from app import create_app
    from extensions import db

    app = create_app()
    app.config.update(
        TESTING=True,
        WTF_CSRF_ENABLED=False,
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        SERVER_NAME="localhost",
    )

    with app.app_context():
        db.drop_all()
        db.create_all()

    yield app

    with app.app_context():
        db.session.remove()
        db.drop_all()

    try:
        Path(db_path).unlink(missing_ok=True)
    except Exception:
        pass


@pytest.fixture(autouse=True)
def clean_db(app):
    from extensions import db

    with app.app_context():
        db.session.remove()
        for table in reversed(db.metadata.sorted_tables):
            db.session.execute(table.delete())
        db.session.commit()

    yield

    with app.app_context():
        db.session.remove()


@pytest.fixture()
def client(app):
    return app.test_client()


@pytest.fixture()
def runner(app):
    return app.test_cli_runner()


@pytest.fixture()
def organisation(app):
    from extensions import db
    from models import Organisation

    org = Organisation(name="Alpha Care", slug="alpha-care")
    with app.app_context():
        db.session.add(org)
        db.session.commit()
        db.session.refresh(org)
        return org


@pytest.fixture()
def second_organisation(app):
    from extensions import db
    from models import Organisation

    org = Organisation(name="Beta Care", slug="beta-care")
    with app.app_context():
        db.session.add(org)
        db.session.commit()
        db.session.refresh(org)
        return org


@pytest.fixture()
def normal_user(app, organisation):
    from extensions import db
    from models import User

    user = User(
        username="member_user",
        password=generate_password_hash("password123"),
        admin_level=0,
        org_role="member",
        organisation_id=organisation.id,
    )
    with app.app_context():
        db.session.add(user)
        db.session.commit()
        db.session.refresh(user)
        return user


@pytest.fixture()
def superuser(app, organisation):
    from extensions import db
    from models import User

    user = User(
        username="super_user",
        password=generate_password_hash("password123"),
        admin_level=1,
        org_role="owner",
        organisation_id=organisation.id,
    )
    with app.app_context():
        db.session.add(user)
        db.session.commit()
        db.session.refresh(user)
        return user


@pytest.fixture()
def admin_user(app, organisation):
    from extensions import db
    from models import User

    user = User(
        username="admin_user",
        password=generate_password_hash("password123"),
        admin_level=2,
        org_role="admin",
        organisation_id=organisation.id,
    )
    with app.app_context():
        db.session.add(user)
        db.session.commit()
        db.session.refresh(user)
        return user


@pytest.fixture()
def login(client):
    def _login(username: str, password: str = "password123", follow_redirects: bool = True):
        return client.post(
            "/login",
            data={"username": username, "password": password},
            follow_redirects=follow_redirects,
        )

    return _login


@pytest.fixture()
def logged_in_client(client, normal_user, login):
    response = login(normal_user.username, "password123", follow_redirects=True)
    assert response.status_code == 200
    return client


@pytest.fixture()
def admin_client(client, admin_user, login):
    response = login(admin_user.username, "password123", follow_redirects=True)
    assert response.status_code == 200
    return client


@pytest.fixture()
def superuser_client(client, superuser, login):
    response = login(superuser.username, "password123", follow_redirects=True)
    assert response.status_code == 200
    return client


@pytest.fixture()
def sample_job_records(app):
    from extensions import db
    from models import JobRecord

    now = datetime.utcnow()

    records = [
        JobRecord(
            company_id="C1",
            company_name="Alpha Care",
            sector="Social Care",
            job_role="Support Worker",
            job_role_group="Care & Support Worker",
            county="Staffordshire",
            postcode="WS1 1AA",
            pay_rate=12.50,
            imported_month="01",
            imported_year="2026",
            created_at=now - timedelta(days=7),
        ),
        JobRecord(
            company_id="C2",
            company_name="Bravo Care",
            sector="Social Care",
            job_role="Senior Support Worker",
            job_role_group="Care & Support Worker",
            county="Staffordshire",
            postcode="WS1 2BB",
            pay_rate=13.20,
            imported_month="01",
            imported_year="2026",
            created_at=now - timedelta(days=3),
        ),
        JobRecord(
            company_id="C3",
            company_name="City Nursing",
            sector="Nursing",
            job_role="Registered Nurse",
            job_role_group="Registered Nurse",
            county="Cheshire",
            postcode="CW1 3CC",
            pay_rate=21.75,
            imported_month="02",
            imported_year="2026",
            created_at=now - timedelta(days=2),
        ),
        JobRecord(
            company_id="C4",
            company_name="Delta HR",
            sector="HR",
            job_role="HR Advisor",
            job_role_group="HR Advisor",
            county="Greater Manchester",
            postcode="M1 4DD",
            pay_rate=17.00,
            imported_month="02",
            imported_year="2026",
            created_at=now - timedelta(days=1),
        ),
    ]

    with app.app_context():
        db.session.add_all(records)
        db.session.commit()
        return records