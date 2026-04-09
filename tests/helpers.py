from __future__ import annotations

from typing import Iterable, Optional


def find_rule_by_endpoint_prefix(app, prefix: str, methods: Optional[Iterable[str]] = None):
    wanted_methods = set(methods or ["GET"])

    for rule in app.url_map.iter_rules():
        if not rule.endpoint.startswith(prefix):
            continue
        if rule.endpoint == "static":
            continue
        if not wanted_methods.issubset(rule.methods):
            continue
        return rule
    return None


def find_rule_by_path_fragment(app, fragment: str, methods: Optional[Iterable[str]] = None):
    wanted_methods = set(methods or ["GET"])

    for rule in app.url_map.iter_rules():
        if fragment not in rule.rule:
            continue
        if rule.endpoint == "static":
            continue
        if not wanted_methods.issubset(rule.methods):
            continue
        return rule
    return None


def first_admin_rule(app):
    rule = find_rule_by_endpoint_prefix(app, "admin.", methods=["GET"])
    if rule:
        return rule
    return find_rule_by_path_fragment(app, "/admin", methods=["GET"])


def first_dashboard_rule(app):
    for prefix in ("dashboard.", "insights."):
        rule = find_rule_by_endpoint_prefix(app, prefix, methods=["GET"])
        if rule:
            return rule

    for fragment in ("/dashboard", "/insights"):
        rule = find_rule_by_path_fragment(app, fragment, methods=["GET"])
        if rule:
            return rule

    return None