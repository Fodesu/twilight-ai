"""Pure dashboard contracts and variable interpolation used by live verification."""
import json
import re

ROLES = ("owner", "worker", "model-backend", "tool-backend")


def check_layout(dashboard):
    variables = {v["name"]: v for v in dashboard["templating"]["list"]}
    assert set(variables) == {"component", "pod"}
    assert variables["component"]["multi"] and variables["component"]["includeAll"]
    assert {v["value"] for v in variables["component"]["options"] if v["value"] != "$__all"} == set(ROLES)
    assert 'role=~"${component:regex}"' in variables["pod"]["definition"]
    assert variables["pod"]["includeAll"] and variables["pod"]["allValue"] == ".*"
    panels = dashboard["panels"]
    ids = [p["id"] for p in panels]
    assert len(ids) == len(set(ids)), "duplicate panel IDs"
    rows = [p for p in panels if p["type"] == "row"]
    assert len(rows) == 1 and rows[0]["repeat"] == "component"
    assert not rows[0]["collapsed"] and not rows[0]["panels"]
    row_index = panels.index(rows[0])
    details = panels[row_index + 1:]
    assert details and all(p["type"] != "row" for p in details)
    for p in panels:
        for target in p.get("targets", []):
            expr = target["expr"]
            assert 'namespace="twilight-validation"' in expr
            assert 'role=~"${component:regex}"' in expr
            assert 'pod=~"${pod:regex}"' in expr
    for p in details:
        if p.get("datasource", {}).get("type") == "prometheus":
            for target in p["targets"]:
                assert "{{pod}}" in target["legendFormat"], p["title"]
                expr = target["expr"]
                if "twilight_http_requests_total" in expr or "twilight_http_request_duration_seconds_bucket" in expr:
                    assert 'route!~' in expr or 'route=~"GET /(healthz|readyz)"' in expr
                assert "twilight_http_requests_in_flight{" not in expr, "mixed in-flight metric used as a specific class"
    return panels[:row_index], details


def interpolate(expr, role=None, pod=None):
    # Called only with fixed roles and actual Kubernetes Pod names. Escape regex
    # values AND their enclosing PromQL/LogQL strings, as Grafana interpolation does.
    role_regex = re.escape(role) if role else "|".join(ROLES)
    pod_regex = re.escape(pod) if pod else ".*"
    for token, value in [("${component:regex}", role_regex), ("${pod:regex}", pod_regex)]:
        expr = expr.replace(token, json.dumps(value)[1:-1])
    expr = expr.replace("$__rate_interval", "1m").replace("$__range", "30m")
    assert "$" not in expr, expr
    return expr
