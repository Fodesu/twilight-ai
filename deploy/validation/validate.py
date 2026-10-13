#!/usr/bin/env python3
"""One operator entrypoint for isolated deployment, access and e2e acceptance.

Live checks are explicitly named; importing modules or running unit discovery
never opens ports, changes Kubernetes resources or calls a model provider.
"""
import argparse
import importlib
import json
import os
from pathlib import Path
import sys

# Works both as a file command and as python -m deploy.validation.validate.
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from deploy.validation.common import GRAFANA_URL, OWNER_URL, ROOT, kube_json, run


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    commands = p.add_subparsers(dest="action", required=True)
    deploy = commands.add_parser("deploy", help="explicitly deploy an isolated component")
    deploy.add_argument("component", choices=["app", "observability"])
    deploy.add_argument("--tag", help="new validation-* tag, required for app; app deploy selects Mock")
    access = commands.add_parser("access", help="foreground localhost port-forward supervisor")
    access.add_argument("--log-dir", default="/tmp/twilight-validation-access")
    model = commands.add_parser("model", help="switch validation model catalog; changes one backend")
    model.add_argument("mode", choices=["real", "mock"])
    model.add_argument("--env-file", default=str(ROOT / ".env"))
    collect = commands.add_parser("collect", help="read-only evidence into a NEW private directory")
    collect.add_argument("--output", required=True)
    check = commands.add_parser("check", help="explicit live acceptance; observability is read-only")
    checks = check.add_subparsers(dest="profile", required=True)
    obs = checks.add_parser("observability")
    obs.add_argument("--stack-only", action="store_true", help="allow missing app traffic/topology")
    obs.add_argument("--since", default="30m", help="Loki log/index lookback; e.g. 24h for a quiet environment")
    mock = checks.add_parser("mock", help="writes a test session; requires Mock catalog")
    mock.add_argument("--tool", action="store_true")
    mock.add_argument("--timeout", type=float, default=90)
    mock.add_argument("--base-url", default=OWNER_URL)
    real = checks.add_parser("real", help="billable real-model loop; requires explicit API opt-in")
    real.add_argument("--allow-real-api", action="store_true", required=True)
    real.add_argument("--output", required=True)
    real.add_argument("--timeout", type=float, default=180)
    real.add_argument("--base-url", default=OWNER_URL)
    logs = checks.add_parser("logs", help="deletes ONE validation Owner only after marker ingestion")
    logs.add_argument("--delete-owner", action="store_true", required=True)
    logs.add_argument("--grafana-url", default=GRAFANA_URL)
    browser = checks.add_parser("browser", help="optional Node/Chrome rendered-dashboard acceptance")
    browser.add_argument("--output", help="NEW private evidence directory; defaults to a fresh temp directory")
    dashboard = commands.add_parser("dashboard", help="regenerate dashboard artifact or check drift")
    dashboard.add_argument("--check", action="store_true")
    return p


def require_catalog(profile):
    # Prevent a Mock assertion from accidentally spending real provider tokens,
    # and never claim real-provider acceptance when the catalog still points at Mock.
    doc = kube_json(["get", "configmap", "twilight-models", "-o", "json"])
    entries = json.loads(doc["data"]["models.json"])["models"]
    chat = next(e for e in entries if e["ref"] == "chat")
    is_mock = chat.get("baseURL", "").startswith("http://mock-model:") and chat["model"] == "validation-mock"
    if is_mock != (profile == "mock"):
        raise RuntimeError(f"Catalog mode does not match check {profile}; switch explicitly with model real|mock")


def main(argv=None):
    args = parser().parse_args(argv)
    if args.action == "check":
        if args.profile == "browser":
            env = os.environ.copy()
            if args.output:
                env["VALIDATION_BROWSER_OUTPUT"] = str(Path(args.output).resolve())
            run(["node", ROOT / "tests/e2e/browser.mjs"], env=env)
        else:
            if args.profile in ("mock", "real"):
                require_catalog(args.profile)
            if args.profile == "observability":
                args.require_application = not args.stack_only
            importlib.import_module("tests.e2e." + args.profile).check(args)
    elif args.action == "model":
        from deploy.validation.model import configure
        configure(args)
    elif args.action == "dashboard":
        run([sys.executable, ROOT / "deploy/observability/build_dashboard.py"] + (["--check"] if args.check else []))
    else:
        from deploy.validation import runtime
        getattr(runtime, args.action)(args)


if __name__ == "__main__":
    main()
