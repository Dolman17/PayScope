from __future__ import annotations

import pytest


def test_pay_compare_endpoint_returns_json(logged_in_client, sample_job_records):
    response = logged_in_client.get("/api/pay-compare")
    assert response.status_code == 200
    assert response.is_json


def test_pay_compare_accepts_expected_query_params(logged_in_client, sample_job_records):
    response = logged_in_client.get(
        "/api/pay-compare",
        query_string={
            "sector": "Social Care",
            "job_role_group": "Care & Support Worker",
            "group_by": "county",
            "start_date": "2026-01-01",
            "end_date": "2026-12-31",
        },
    )
    assert response.status_code == 200
    assert response.is_json


@pytest.mark.xfail(
    reason=(
        "Current /api/pay-compare is a login-protected page-backing endpoint, "
        "not yet an external API with API-key auth. Convert this test when "
        "external API auth is added under /api/v1."
    ),
    strict=True,
)
@pytest.mark.xfail_architecture
def test_external_api_should_require_auth_when_hardened(client):
    response = client.get("/api/pay-compare")
    assert response.status_code in (401, 403)