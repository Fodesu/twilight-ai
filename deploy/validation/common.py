"""Shared helpers for the isolated development environment, not production APIs."""
import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
CONTEXT = "k3d-twilight"
APP_NS = "twilight-validation"
OBS_NS = "twilight-observability"
ROLES = ("owner", "worker", "model-backend", "tool-backend")
OWNER_URL = "http://127.0.0.1:18080"
GRAFANA_URL = "http://127.0.0.1:13000"


def run(argv, **kwargs):
    kwargs.setdefault("check", True)
    kwargs.setdefault("timeout", 600)
    return subprocess.run([str(v) for v in argv], **kwargs)


def kube(namespace=APP_NS):
    return ["kubectl", "--context", CONTEXT, "--namespace", namespace]


def kube_json(arguments, namespace=APP_NS):
    result = run(kube(namespace) + arguments, capture_output=True, text=True, timeout=45)
    return json.loads(result.stdout)


def helm():
    binary = shutil.which(os.environ.get("HELM_BIN") or "helm")
    if not binary:
        raise RuntimeError("Helm 3 required: add it to PATH or set HELM_BIN")
    return binary


def http_json(url, params=None, timeout=20):
    if params:
        url += "?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.load(response)


def service_api(service, path, params=None, text=False, namespace=OBS_NS):
    url = f"/api/v1/namespaces/{namespace}/services/http:{service}/proxy{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    result = run(kube(namespace) + ["get", "--raw", url], capture_output=True, text=True, timeout=45)
    if text:
        return result.stdout.strip()
    response = json.loads(result.stdout)
    if response.get("status") == "error":
        raise RuntimeError("Service API reported an error")
    return response


class OwnerClient:
    def __init__(self, session, request_id, base_url=OWNER_URL):
        self.session, self.request_id, self.base_url = session, request_id, base_url.rstrip("/")

    def request(self, method, path, body=None):
        return urllib.request.Request(self.base_url + path,
            data=None if body is None else json.dumps(body).encode(), method=method,
            headers={"Content-Type": "application/json", "X-Request-ID": self.request_id})

    def call(self, method, path, body=None):
        with urllib.request.urlopen(self.request(method, path, body), timeout=20) as response:
            data = response.read()
            return json.loads(data) if data else None

    def command(self, cid, kind, payload):
        # Enqueue once; polling only reads the durable command result. Never
        # blindly retry an ambiguous write after a lost HTTP response.
        prefix = f"/sessions/{self.session}/commands"
        self.call("POST", prefix, {"id": cid, "kind": kind, "payload": payload})
        for _ in range(4):
            entry = self.call("GET", f"{prefix}/{cid}?wait=10s")
            status = entry.get("result", {}).get("status")
            if status == "applied":
                return entry
            if status and status != "pending":
                raise RuntimeError(f"Command {cid} rejected; status={status}")
        raise TimeoutError("Command not applied within bounded wait")


def private_directory(path):
    os.umask(0o077)
    folder = Path(path)
    folder.mkdir(mode=0o700, parents=False, exist_ok=False)
    return folder


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()
