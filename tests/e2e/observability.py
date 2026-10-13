#!/usr/bin/env python3
"""Verify live service APIs through Kubernetes; never opens a port-forward.

Without --require-application, absent application traffic is reported, not
interpreted as proof that application observability is working.
"""
from collections import Counter
import datetime
import json
from deploy.validation.common import APP_NS, OBS_NS, CONTEXT, ROLES, service_api as api
from .dashboard import check_layout, interpolate


def prom(query, grafana=False):
    service = "grafana:3000" if grafana else "prometheus:9090"
    path = ("/api/datasources/proxy/uid/twilight-prometheus/api/v1/query"
            if grafana else "/api/v1/query")
    return api(service, path, {"query": query})["data"]["result"]


def check(args):
    print("Verified at", datetime.datetime.now(datetime.timezone.utc).isoformat())
    print("Context:", CONTEXT, "namespace:", OBS_NS)
    health = api("grafana:3000", "/api/health")
    assert health["database"] == "ok", health
    print("Grafana health:", health)
    assert api("loki:3100", "/ready", text=True) == "ready"
    assert api("alloy:12345", "/-/ready", text=True) == "Alloy is ready."
    print("Loki / Alloy readiness: OK")
    for uid in ["twilight-prometheus", "twilight-loki"]:
        health = api("grafana:3000", f"/api/datasources/uid/{uid}/health")
        assert health["status"] == "OK", health
        print("Grafana datasource:", uid, health["status"], health["message"])
    dashboard = api("grafana:3000", "/api/dashboards/uid/twilight-validation")
    assert dashboard["dashboard"]["title"] == "Twilight Validation"
    overview, details = check_layout(dashboard["dashboard"])
    print("Dashboard: component/POD filters and repeated component row verified; anonymous read OK")
    for role, panels in [(None, overview)] + [(role, details) for role in ROLES]:
        for panel in panels:
            if panel.get("datasource", {}).get("type") == "prometheus":
                for query in panel["targets"]:
                    rows = prom(interpolate(query["expr"], role), grafana=True)
                    if role:
                        assert all(r["metric"].get("pod", "").startswith(role + "-") for r in rows), (role, panel["title"], rows)
                    print("Panel query:", role or "overview", panel["title"], query["refId"], "OK; series =", len(rows))
            elif panel.get("datasource", {}).get("type") == "loki":
                logs = api("grafana:3000", "/api/datasources/proxy/uid/twilight-loki/loki/api/v1/query_range",
                           {"query": interpolate(panel["targets"][0]["expr"], role), "since": args.since, "limit": 10})
                assert all(s["stream"]["role"] == role for s in logs["data"]["result"]), (role, logs)
                print("Log panel:", role, "OK; streams =", len(logs["data"]["result"]))
    targets = api("prometheus:9090", "/api/v1/targets")["data"]["activeTargets"]
    for target in targets:
        assert target["labels"]["namespace"] == APP_NS, target
        assert target["labels"]["role"] in ["owner", "worker", "model-backend", "tool-backend"]
        assert target["discoveredLabels"]["__meta_kubernetes_pod_container_port_name"] == "http"
        pod_ip = target["discoveredLabels"]["__meta_kubernetes_pod_ip"]
        assert target["scrapeUrl"].startswith(f"http://{pod_ip}:")
        print("Pod target:", target["labels"]["pod"], target["scrapeUrl"],
              target["health"], target["lastError"])
    up = prom(f'up{{namespace="{APP_NS}"}}', grafana=True)
    ready = prom(f'twilight_ready{{namespace="{APP_NS}"}}')
    current_pods = {target["labels"]["pod"] for target in targets}
    current_up = [r for r in up if r["metric"]["pod"] in current_pods]
    current_ready = [r for r in ready if r["metric"]["pod"] in current_pods]
    print("Grafana Prometheus proxy query: up series =", len(up), "current targets =", len(current_up))
    if len(up) != len(current_up):
        print("BOUNDARY: old rollout Pod samples remain within Prometheus lookback; excluded from current-target checks")
    print("Application readiness:", [(r["metric"]["pod"], r["value"][1]) for r in current_ready])
    # Verify the POD dropdown's series lookup and all six individual POD filters,
    # not just the all-components/all-pods overview.
    selector = interpolate('up{namespace="twilight-validation",role=~"${component:regex}"}')
    options = api("grafana:3000", "/api/datasources/proxy/uid/twilight-prometheus/api/v1/series", {"match[]": selector})["data"]
    assert current_pods <= {s["pod"] for s in options}, "POD dropdown missing current targets"
    for target in targets:
        role, pod = target["labels"]["role"], target["labels"]["pod"]
        rows = prom(interpolate('up{namespace="twilight-validation",role=~"${component:regex}",pod=~"${pod:regex}"}', role, pod), grafana=True)
        assert len(rows) == 1 and rows[0]["metric"]["pod"] == pod, (role, pod, rows)
    classes = prom(f'twilight_http_requests_in_flight_by_class{{namespace="{APP_NS}"}}')
    current_classes = [r for r in classes if r["metric"]["pod"] in current_pods]
    print("Current per-class gauges:", len(current_classes))
    for metric in ["twilight_http_requests_total", "twilight_http_request_duration_seconds_count",
                   "twilight_http_requests_in_flight", "twilight_http_requests_in_flight_by_class", "twilight_operations_total",
                   "twilight_operation_duration_seconds_count", "twilight_event_frames_total",
                   "twilight_background_warnings_total", "go_memstats_heap_alloc_bytes",
                   "process_resident_memory_bytes"]:
        rows = prom(f'{metric}{{namespace="{APP_NS}"}}')
        assert all(not ({"session", "session_id", "execution", "execution_id"} & set(r["metric"])) for r in rows)
        print("Metric:", metric, "series =", len(rows),
              "(no samples yet)" if not rows else "OK")
    unknown = prom(f'twilight_operations_total{{namespace="{APP_NS}",result="dispatch_unknown"}}')
    print("dispatch_unknown series =", len(unknown), "(requires a matching operation to occur)")
    params = {"query": f'{{namespace="{APP_NS}",role=~"owner|worker|model-backend|tool-backend",container=~"owner|worker|model-backend|tool-backend"}}',
              "since": args.since, "limit": 100}
    # Query through Grafana too: health alone does not prove data reaches panels.
    logs = api("grafana:3000", "/api/datasources/proxy/uid/twilight-loki/loki/api/v1/query_range", params)
    streams = logs["data"]["result"]
    json_count = 0
    for stream in streams:
        assert stream["stream"]["namespace"] == APP_NS
        assert not ({"session", "session_id", "execution", "execution_id"} & set(stream["stream"]))
        for _, line in stream["values"]:
            try:
                json.loads(line)
                json_count += 1
            except json.JSONDecodeError:
                pass
    print("Grafana Loki proxy query: streams =", len(streams), "JSON log entries =", json_count)
    print("Log roles:", sorted({s["stream"]["role"] for s in streams}))
    namespaces = api("loki:3100", "/loki/api/v1/label/namespace/values", {"since": args.since}).get("data", [])
    assert all(ns == APP_NS for ns in namespaces), namespaces
    print("Loki observed namespaces:", namespaces)
    indexed = api("loki:3100", "/loki/api/v1/series",
                  {"match[]": f'{{namespace="{APP_NS}"}}', "since": args.since}).get("data", [])
    allowed_labels = {"namespace", "pod", "container", "role"}
    exact_streams = [s for s in indexed if set(s) == allowed_labels]
    assert all(not ({"session", "session_id", "execution", "execution_id", "request_id"} & set(s)) for s in indexed)
    print("Loki index: exact four-label streams =", len(exact_streams),
          "; older static-label streams =", len(indexed) - len(exact_streams))
    if args.require_application:
        role_counts = Counter(t["labels"]["role"] for t in targets)
        assert role_counts == {"owner": 2, "worker": 2, "model-backend": 1, "tool-backend": 1}, role_counts
        print("Expected current topology verified:", dict(role_counts))
        assert targets and len(current_up) == len(targets) and all(r["value"][1] == "1" for r in current_up)
        assert len(current_ready) == len(targets) and all(r["value"][1] == "1" for r in current_ready)
        for pod in current_pods:
            assert {r["metric"]["class"] for r in current_classes if r["metric"]["pod"] == pod} == {"probe", "rpc", "stream"}, pod
        assert json_count > 0, "No real JSON application logs returned"
        assert exact_streams, "No four-label Loki index streams returned"
    elif not targets or not json_count:
        print("BOUNDARY: stack API works, but application targets/logs are not yet proven")
    print("Verification completed. Empty event/operation metrics do not prove those paths ran.")
