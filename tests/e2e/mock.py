"""Normal-path mock smoke; not recovery/fencing or a real-provider test."""
import json
import time
import urllib.request
import uuid

from deploy.validation.common import OwnerClient


def check(args):
    sid = "obs-smoke-" + uuid.uuid4().hex[:12]
    request_id = "smoke-" + sid
    client = OwnerClient(sid, request_id, args.base_url)
    client.call("GET", "/readyz")
    client.call("PUT", "/sessions/" + sid)
    if args.tool:
        workspace = client.call("POST", "/workspaces", {"project": "observability-validation"})
        client.command("bind", "bind_workspace", {"workspaceId": workspace["id"]})
    client.command("submit", "submit", {"inputId": "input-1", "text": "[validation-tool]" if args.tool else "validation smoke test"})
    deadline = time.monotonic() + args.timeout
    while time.monotonic() < deadline:
        surface = client.call("GET", f"/sessions/{sid}/turns")
        turns = surface.get("turns", {})
        if turns and all(t["status"] == "completed" for t in turns.values()):
            break
        if any(t["status"] not in ("active", "completed") for t in turns.values()):
            raise RuntimeError("Mock turn failed")
        time.sleep(0.5)
    else:
        raise TimeoutError("Mock turn did not complete for " + sid)
    tid = surface["order"][-1]
    turn = client.call("GET", f"/sessions/{sid}/turns/{tid}")
    expected = "validation mock: tool done" if args.tool else "validation mock: done"
    if turn.get("reply") != expected:
        raise RuntimeError("Unexpected mock reply; ensure the deployed catalog is Mock, not real")
    # Only the first durable event, not a full event-order acceptance test.
    with urllib.request.urlopen(client.request("GET", f"/sessions/{sid}/events?from=0"), timeout=15) as response:
        for raw in response:
            if raw.startswith(b"data: "):
                event = json.loads(raw[6:])
                if event.get("type"):
                    position = event["position"]
                    break
        else:
            raise RuntimeError("No durable event returned")
    print(json.dumps({"session_id": sid, "request_id": request_id, "turn_id": tid,
        "tool": args.tool, "status": turn["turn"]["status"], "reply": turn["reply"],
        "first_durable_position": position}))
