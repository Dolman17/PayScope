from __future__ import annotations


def test_create_app_returns_flask_app(app):
    assert app is not None
    assert app.config["TESTING"] is True


def test_expected_blueprints_registered(app):
    expected = {"auth", "admin", "main", "api", "public", "insights"}
    registered = set(app.blueprints.keys())
    missing = expected - registered
    assert not missing, f"Missing blueprints: {sorted(missing)}"


def test_root_route_exists(app):
    routes = {rule.rule for rule in app.url_map.iter_rules()}
    assert "/" in routes


def test_root_route_loads_publicly(client):
    response = client.get("/", follow_redirects=False)
    assert response.status_code == 200


def test_home_route_exists(app):
    routes = {rule.rule for rule in app.url_map.iter_rules()}
    assert "/home" in routes


def test_api_pay_compare_route_exists(app):
    routes = {rule.rule for rule in app.url_map.iter_rules()}
    assert "/api/pay-compare" in routes