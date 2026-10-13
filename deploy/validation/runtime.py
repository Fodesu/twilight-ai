"""Operator actions for the fixed, isolated validation namespaces."""
import json
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import subprocess
import time

from .common import APP_NS, OBS_NS, CONTEXT, ROOT, helm, kube, kube_json, private_directory, run, utc_now


def ensure_namespace(namespace):
    doc = {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": namespace}}
    run(["kubectl", "--context", CONTEXT, "apply", "-f", "-"], input=json.dumps(doc), text=True)


def seed_mock_secret():
    existing = run(kube() + ["get", "secret", "twilight-secrets", "--ignore-not-found", "-o", "name"], capture_output=True, text=True).stdout.strip()
    if existing:
        print("Existing validation DB/development Secret kept; no password rotation.")
        return
    password = secrets.token_urlsafe(32)
    doc = {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
           "metadata": {"name": "twilight-secrets", "namespace": APP_NS},
           "stringData": {"postgres-password": password,
               "postgres-dsn": f"postgres://twilight:{password}@postgres:5432/twilight?sslmode=disable",
               "OPENAI_API_KEY": "validation-mock-not-a-provider-key"}}
    run(kube() + ["create", "-f", "-"], input=json.dumps(doc), text=True)


def deploy(args):
    binary = helm()
    if args.component == "app":
        if not args.tag or not re.fullmatch(r"validation-[A-Za-z0-9_.-]+", args.tag):
            raise ValueError("deploy app requires a new --tag validation-*; never reuse dev")
        ensure_namespace(APP_NS)
        seed_mock_secret()
        run(["docker", "build", "-t", "twilight:" + args.tag, "."], cwd=ROOT)
        run(["k3d", "image", "import", "twilight:" + args.tag, "-c", "twilight"])
        run(["kubectl", "--context", CONTEXT, "apply", "-k", ROOT / "deploy/validation/mock"])
        chart = ROOT / "deploy/helm/twilight"
        overlays = ["-f", ROOT / "deploy/validation/values.yaml", "-f", ROOT / "deploy/validation/k3d-values.yaml", "--set-string", "image.tag=" + args.tag]
        run([binary, "lint", chart, "--strict"] + overlays)
        run([binary, "upgrade", "--install", "twilight-validation", chart, "--kube-context", CONTEXT,
             "-n", APP_NS] + overlays + ["--wait", "--timeout", "5m"])
        run(kube() + ["rollout", "status", "deployment/mock-model", "--timeout=180s"])
        print("Mock catalog deployed. To use a real provider, run: validate.py model real --env-file .env")
    else:
        run(["kubectl", "--context", CONTEXT, "get", "namespace", APP_NS])
        ensure_namespace(OBS_NS)
        chart = ROOT / "deploy/observability"
        run([binary, "lint", chart, "--namespace", OBS_NS])
        run([binary, "upgrade", "--install", "twilight-observability", chart, "--kube-context", CONTEXT,
             "--namespace", OBS_NS, "--wait", "--timeout", "5m"])
        for component in ("prometheus", "grafana", "loki", "alloy"):
            run(kube(OBS_NS) + ["rollout", "status", "deployment/" + component, "--timeout=120s"])


def access(args):
    services = [("owner", APP_NS, 18080, 8080), ("grafana", OBS_NS, 13000, 3000),
                ("prometheus", OBS_NS, 19090, 9090), ("loki", OBS_NS, 13100, 3100)]
    os.umask(0o077)
    folder = Path(args.log_dir)
    folder.mkdir(mode=0o700, parents=True, exist_ok=True)
    for name, namespace, local, _ in services:
        run(kube(namespace) + ["get", "service", name, "-o", "name"], stdout=subprocess.DEVNULL)
        with socket.socket() as sock:
            try:
                sock.bind(("127.0.0.1", local))
            except OSError:
                raise RuntimeError(f"Port {local} already in use; keep the existing supervisor, do not start duplicates") from None
    stopping = False
    children, logs = {}, {}
    handlers = {}

    def stop(_signal, _frame):
        nonlocal stopping
        stopping = True

    def start(service):
        name, namespace, local, remote = service
        children[name] = subprocess.Popen(kube(namespace) + ["port-forward", "--address", "127.0.0.1",
            "service/" + name, f"{local}:{remote}"], stdin=subprocess.DEVNULL, stdout=logs[name], stderr=subprocess.STDOUT)

    for sig in (signal.SIGTERM, signal.SIGINT):
        handlers[sig] = signal.signal(sig, stop)
    try:
        for service in services:
            name = service[0]
            logs[name] = (folder / (name + ".log")).open("a")
            start(service)
            print(f"{name}: http://127.0.0.1:{service[2]}", flush=True)
        (folder / "supervisor.pid").write_text(str(os.getpid()) + "\n")
        while not stopping:
            for service in services:
                if children[service[0]].poll() is not None:
                    print(f"Restarting {service[0]} port-forward", flush=True)
                    start(service)
            time.sleep(1)
    finally:
        for child in children.values():
            if child.poll() is None:
                child.terminate()
        for child in children.values():
            try:
                child.wait(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        for log in logs.values():
            log.close()
        (folder / "supervisor.pid").unlink(missing_ok=True)
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


def collect(args):
    folder = private_directory(args.output)
    (folder / "collection.txt").write_text(f"context={CONTEXT}\nnamespace={APP_NS}\nstarted_at={utc_now()}\n")

    def capture(arguments, path):
        result = subprocess.run(kube() + ["--request-timeout=15s"] + arguments,
                                capture_output=True, text=True, timeout=25)
        path.write_text(result.stdout)
        if result.stderr or result.returncode:
            path.with_suffix(path.suffix + ".error").write_text(result.stderr or f"exit={result.returncode}\n")
        return result.returncode

    for name, arguments in [("pods.txt", ["get", "pods", "-o", "wide"]),
            ("workloads.txt", ["get", "deployments,statefulsets", "-o", "wide"]),
            ("events.txt", ["get", "events", "--sort-by=.metadata.creationTimestamp"])]:
        if capture(arguments, folder / name):
            raise RuntimeError("Evidence inventory failed; partial evidence retained")
    for pod in kube_json(["get", "pods", "-l", "app.kubernetes.io/part-of=twilight", "-o", "json"])["items"]:
        name = pod["metadata"]["name"]
        target = folder / name
        target.mkdir(mode=0o700)
        for container in pod["spec"].get("initContainers", []) + pod["spec"]["containers"]:
            cid = container["name"]
            for previous in (False, True):
                flags = ["--previous"] if previous else ["--since=30m"]
                suffix = ".previous.log" if previous else ".log"
                capture(["logs", name, "-c", cid, "--timestamps"] + flags, target / (cid + suffix))
        if pod["metadata"]["labels"].get("app.kubernetes.io/name") not in ("owner", "worker", "model-backend", "tool-backend"):
            continue
        ports = [p["containerPort"] for p in pod["spec"]["containers"][0].get("ports", []) if p["name"] == "http"]
        if not ports:
            (target / "proxy.error").write_text("No HTTP port\n")
            continue
        for endpoint in ("metrics", "healthz", "readyz"):
            capture(["get", "--raw", f"/api/v1/namespaces/{APP_NS}/pods/{name}:{ports[0]}/proxy/{endpoint}"], target / (endpoint + ".txt"))
    with (folder / "collection.txt").open("a") as output:
        output.write("finished_at=" + utc_now() + "\n")
    print(f"Evidence saved to {folder}; review sensitive logs before sharing")
