from __future__ import annotations

import pytest

from tests.helpers import first_dashboard_rule


def test_dashboard_like_route_redirects_when_logged_out(app, client):
    rule = first_dashboard_rule(app)
    if not rule:
        pytest.skip("No dashboard/insights GET route discovered.")

    response = client.get(rule.rule, follow_redirects=False)
    assert response.status_code in (301, 302, 401, 403)


def test_dashboard_like_route_loads_when_logged_in(app, logged_in_client, sample_job_records):
    rule = first_dashboard_rule(app)
    if not rule:
        pytest.skip("No dashboard/insights GET route discovered.")

    response = logged_in_client.get(rule.rule, follow_redirects=True)
    assert response.status_code == 200