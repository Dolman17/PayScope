from __future__ import annotations

import pytest

from tests.helpers import first_admin_rule


def test_login_page_loads(client):
    response = client.get("/login")
    assert response.status_code == 200


def test_valid_login_succeeds_and_redirects_home(client, normal_user, login):
    response = login(normal_user.username, "password123", follow_redirects=False)
    assert response.status_code in (301, 302)
    assert response.headers["Location"].endswith("/home")


def test_invalid_login_shows_error(client, normal_user, login):
    response = login(normal_user.username, "wrong-password", follow_redirects=True)
    assert response.status_code == 200
    assert b"Invalid username or password" in response.data


def test_home_page_available_after_login(logged_in_client):
    response = logged_in_client.get("/home")
    assert response.status_code == 200


def test_logout_is_post_only(client, normal_user, login):
    login(normal_user.username, "password123", follow_redirects=True)

    get_response = client.get("/logout", follow_redirects=False)
    assert get_response.status_code in (405, 308)

    post_response = client.post("/logout", follow_redirects=False)
    assert post_response.status_code in (301, 302)


def test_non_admin_blocked_from_admin_area(app, logged_in_client):
    rule = first_admin_rule(app)
    if not rule:
        pytest.skip("No GET admin route discovered to smoke-test.")

    response = logged_in_client.get(rule.rule, follow_redirects=False)
    assert response.status_code in (302, 403)


def test_admin_can_access_admin_area(app, admin_client):
    rule = first_admin_rule(app)
    if not rule:
        pytest.skip("No GET admin route discovered to smoke-test.")

    response = admin_client.get(rule.rule, follow_redirects=True)
    assert response.status_code == 200