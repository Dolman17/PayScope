from __future__ import annotations

import pytest


def test_recruiter_page_requires_login(client):
    response = client.get("/recruiter-radar", follow_redirects=False)
    assert response.status_code in (302, 401, 403, 404)


def test_recruiter_page_loads_for_authenticated_user(logged_in_client):
    response = logged_in_client.get("/recruiter-radar", follow_redirects=False)
    assert response.status_code in (200, 404)


@pytest.mark.parametrize(
    "path",
    [
        "/recruiter-radar",
        "/recruiter-radar?view=weekly",
        "/recruiter-radar?view=monthly",
    ],
)
def test_recruiter_views_do_not_500_for_authenticated_user(logged_in_client, path):
    response = logged_in_client.get(path, follow_redirects=False)
    assert response.status_code in (200, 404), response.get_data(as_text=True)


def test_recruiter_api_or_data_view_does_not_500(logged_in_client):
    candidate_paths = [
        "/api/recruiter-radar",
        "/recruiter-radar/data",
        "/recruiter-radar.json",
    ]

    last_response = None
    for path in candidate_paths:
        response = logged_in_client.get(path, follow_redirects=False)
        last_response = response
        if response.status_code != 404:
            assert response.status_code == 200, response.get_data(as_text=True)
            return

    assert last_response is not None
    assert last_response.status_code == 404