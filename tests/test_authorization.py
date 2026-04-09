from __future__ import annotations

import pytest


def _is_redirect_response(response) -> bool:
    return response.status_code in (301, 302, 303, 307, 308)


def _assert_login_protected(response) -> None:
    assert response.status_code in (302, 401, 403), (
        f"Expected auth protection, got {response.status_code}"
    )


def _assert_forbidden_or_redirect(response) -> None:
    assert response.status_code in (302, 403), (
        f"Expected forbidden or redirect, got {response.status_code}"
    )


@pytest.mark.parametrize(
    "path",
    [
        "/home",
        "/api/pay-compare",
    ],
)
def test_authenticated_pages_require_login(client, path):
    response = client.get(path, follow_redirects=False)
    _assert_login_protected(response)


def test_public_root_is_still_public(client):
    response = client.get("/")
    assert response.status_code == 200


def test_logout_rejects_get(client):
    response = client.get("/logout", follow_redirects=False)
    assert response.status_code in (405, 400)


@pytest.mark.parametrize(
    "path",
    [
        "/admin",
        "/admin/",
        "/admin/dashboard",
        "/admin/tools",
        "/admin/cron-runs",
        "/admin/coverage",
    ],
)
def test_admin_routes_block_anonymous_users(client, path):
    response = client.get(path, follow_redirects=False)
    assert response.status_code in (302, 401, 403, 404)


@pytest.mark.parametrize(
    "path",
    [
        "/admin",
        "/admin/",
        "/admin/dashboard",
        "/admin/tools",
        "/admin/cron-runs",
        "/admin/coverage",
    ],
)
def test_admin_routes_block_standard_users(client, logged_in_client, path):
    response = logged_in_client.get(path, follow_redirects=False)
    assert response.status_code in (302, 403, 404)