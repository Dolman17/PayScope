from __future__ import annotations

import pytest


def _assert_login_protected(response) -> None:
    assert response.status_code in (302, 401, 403), (
        f"Expected auth protection, got {response.status_code}"
    )


@pytest.mark.parametrize(
    "path",
    [
        "/admin/tools",
        "/admin/coverage",
        "/admin/cron-runs",
    ],
)
def test_admin_pages_require_login(client, path):
    response = client.get(path, follow_redirects=False)
    _assert_login_protected(response)


@pytest.mark.parametrize(
    "path,expected_statuses",
    [
        ("/admin/tools", {302, 403}),
        ("/admin/coverage", {302, 403}),
        ("/admin/cron-runs", {302, 403}),
    ],
)
def test_normal_user_is_blocked_from_admin_pages(logged_in_client, path, expected_statuses):
    response = logged_in_client.get(path, follow_redirects=False)
    assert response.status_code in expected_statuses, (
        f"Expected one of {expected_statuses}, got {response.status_code}"
    )


@pytest.mark.parametrize(
    "path,expected_statuses",
    [
        ("/admin/tools", {403}),
        ("/admin/coverage", {403}),
        ("/admin/cron-runs", {302, 403}),
    ],
)
def test_admin_level_user_is_blocked_from_superuser_pages(admin_client, path, expected_statuses):
    response = admin_client.get(path, follow_redirects=False)
    assert response.status_code in expected_statuses, (
        f"Expected one of {expected_statuses}, got {response.status_code}"
    )


@pytest.mark.parametrize(
    "path",
    [
        "/admin/tools",
        "/admin/coverage",
        "/admin/cron-runs",
    ],
)
def test_superuser_can_access_core_admin_pages(superuser_client, path):
    response = superuser_client.get(path, follow_redirects=False)
    assert response.status_code == 200, response.get_data(as_text=True)


def test_status_json_returns_ok_payload(client):
    response = client.get("/admin/status.json", follow_redirects=False)
    assert response.status_code == 200
    assert response.is_json

    data = response.get_json()
    assert isinstance(data, dict)
    assert "ok" in data
    assert "status" in data
    assert "db_ok" in data


def test_status_json_includes_expected_top_level_keys(client):
    response = client.get("/admin/status.json", follow_redirects=False)
    assert response.status_code == 200

    data = response.get_json()
    expected_keys = {
        "ok",
        "status",
        "app_time_utc",
        "elapsed_ms",
        "db_ok",
        "db_error",
        "counts",
        "freshness",
        "coverage",
        "cron_summary",
        "cron",
        "recent_logins",
    }

    assert expected_keys.issubset(set(data.keys()))