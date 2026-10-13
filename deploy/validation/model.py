#!/usr/bin/env python3
"""Switch only the isolated validation model catalog, using an approved .env.

The key travels only over kubectl stdin into a model-backend-only Secret. It is
never passed in command arguments, printed, stored in Helm values or copied
from the existing twilight deployment. `model mock` restores the fixed catalog.
"""
import json
from pathlib import Path
import shlex
import tempfile
import urllib.parse

from .common import APP_NS, CONTEXT, ROOT, helm, kube, run

SECRET = "twilight-validation-real-model"


def load_env(path):
    values = {}
    for line in Path(path).read_text().splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        if text.startswith("export "):
            text = text[7:]
        name, separator, raw = text.partition("=")
        if not separator:
            raise ValueError("Invalid .env assignment (contents not shown)")
        parts = shlex.split(raw, comments=True)
        if len(parts) > 1:
            raise ValueError("Quote .env values containing whitespace (contents not shown)")
        values[name.strip()] = parts[0] if parts else ""
    required = ["OPENAI_API_KEY", "OPENAI_BASE_URL", "OPENAI_MODEL"]
    if any(not values.get(k) for k in required):
        raise ValueError("Required .env keys: " + ", ".join(required))
    url = urllib.parse.urlsplit(values["OPENAI_BASE_URL"])
    if url.scheme != "https" or not url.hostname or url.username or url.password or url.query or url.fragment:
        raise ValueError("Use an HTTPS provider URL without embedded credentials/query/fragment")
    return values


def configure(args):
    binary = helm()  # Fail before changing Secrets if Helm is unavailable.
    if args.mode == "mock":
        entry = {"ref": "chat", "kind": "openai", "model": "validation-mock",
                 "baseURL": "http://mock-model:8090/v1", "apiKeySecret": "OPENAI_API_KEY"}
        provider_secret = ""
    else:
        env = load_env(args.env_file)
        doc = {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
               "metadata": {"name": SECRET, "namespace": APP_NS},
               "stringData": {"REAL_MODEL_API_KEY": env["OPENAI_API_KEY"]}}
        run(kube() + ["apply", "--server-side", "--field-manager=twilight-validation-real-model", "-f", "-"],
                       input=json.dumps(doc), text=True, check=True, timeout=30)
        entry = {"ref": "chat", "kind": "openai", "model": env["OPENAI_MODEL"],
                 "baseURL": env["OPENAI_BASE_URL"].rstrip("/"), "apiKeySecret": "REAL_MODEL_API_KEY"}
        provider_secret = SECRET
    override = {"models": [entry], "services": {"modelBackend": {"providerSecret": provider_secret}}}
    with tempfile.TemporaryDirectory(prefix="twilight-real-model-") as folder:
        path = Path(folder) / "values.json"
        path.write_text(json.dumps(override))  # Only endpoint/model/Secret names, no credentials.
        path.chmod(0o600)
        run([binary, "upgrade", "twilight-validation", ROOT / "deploy/helm/twilight",
                        "--kube-context", CONTEXT, "--namespace", APP_NS, "--reuse-values",
                        "--values", str(path), "--wait", "--timeout", "5m"], check=True, timeout=330)
    print(json.dumps({"namespace": APP_NS, "mode": args.mode,
                      "model": entry["model"], "base_url": entry["baseURL"],
                      "provider_secret": provider_secret or "shared development mock only"}))
    print("Key rotations with an unchanged catalog require a model-backend rollout restart.")
