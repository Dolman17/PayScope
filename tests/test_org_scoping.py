from __future__ import annotations

import pytest


def test_users_can_belong_to_different_organisations(app, normal_user, second_organisation):
    from extensions import db
    from models import User
    from werkzeug.security import generate_password_hash

    with app.app_context():
        other_user = User(
            username="beta_member",
            password=generate_password_hash("password123"),
            admin_level=0,
            org_role="member",
            organisation_id=second_organisation.id,
        )
        db.session.add(other_user)
        db.session.commit()

        users = User.query.order_by(User.username).all()
        assert len(users) == 2
        assert users[0].organisation_id != users[1].organisation_id


@pytest.mark.xfail(
    reason=(
        "JobRecord does not currently expose organisation_id in the live public model. "
        "Replace this with true tenant-scoping assertions once record-level tenancy "
        "exists in the schema or query layer."
    ),
    strict=True,
)
@pytest.mark.xfail_architecture
def test_job_records_should_eventually_be_scoped_by_organisation():
    from models import JobRecord

    assert hasattr(JobRecord, "organisation_id")