"""Opt-in deletion of ONE validation Owner to prove centralized log retention.

This is not active execution recovery/fencing. Refuse deletion until Loki has
indexed a harmless read marker; never operates in the existing twilight NS.
"""
import json
import subprocess
import time
import uuid

from deploy.validation.common import APP_NS, http_json, kube, kube_json, run


def check(args):
    if not args.delete_owner:
        raise ValueError("Log retention needs an explicit --delete-owner")
    pods = kube_json(["get", "pods", "-l", "app.kubernetes.io/name=owner", "-o", "json"])["items"]
    ready = [p for p in pods if any(c.get("type") == "Ready" and c.get("status") == "True" for c in p["status"].get("conditions", []))]
    if len(ready) != 2:
        raise RuntimeError("Expected two Ready validation Owners before deletion")
    pod = ready[0]["metadata"]["name"]
    marker = "obs-retention-" + uuid.uuid4().hex[:12]
    url = f"/api/v1/namespaces/{APP_NS}/pods/{pod}:8080/proxy/sessions/{marker}/turns"
    request = subprocess.run(kube() + ["get", "--raw", url], capture_output=True, text=True, timeout=30)
    if request.returncode and not ("NotFound" in request.stderr or "not_found" in request.stderr):
        raise RuntimeError("Marker request failed before reaching the application")
    query = '{namespace="' + APP_NS + '",pod=' + json.dumps(pod) + ',container="owner"} |= ' + json.dumps(marker)

    def count():
        endpoint = args.grafana_url.rstrip("/") + "/api/datasources/proxy/uid/twilight-loki/loki/api/v1/query_range"
        data = http_json(endpoint, {"query": query, "since": "10m", "limit": 100})
        return sum(len(s["values"]) for s in data["data"]["result"])

    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        before = count()
        if before:
            break
        time.sleep(1)
    else:
        raise TimeoutError("Marker not ingested into Loki; refusing to delete")
    run(kube() + ["delete", "pod", pod, "--force", "--grace-period=0", "--wait=true"])
    run(kube() + ["rollout", "status", "deployment/owner", "--timeout=90s"])
    after = count()
    if after < before:
        raise RuntimeError("Old Pod marker missing after deletion")
    print(json.dumps({"deleted_owner_pod": pod, "marker_session_id": marker,
        "loki_entries_before": before, "loki_entries_after": after,
        "verified": "centralized log retention, not execution recovery/fencing"}))
