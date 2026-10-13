#!/usr/bin/env python3
"""Real provider -> model-issued shell calls -> second model completion.

The answer is unpredictable from the prompt: the model creates a random UUID
file inside the Sandbox and reads/hashes it. Acceptance independently reads the
file through Kubernetes and checks both UUID and SHA256 plus durable loop facts.
No direct tool call is injected by this script. Evidence is private (0700/0600).
"""
import hashlib
import json
import re
import subprocess
import time
import urllib.request
import uuid

from deploy.validation.common import OwnerClient, kube, private_directory


def check(args):
    folder = private_directory(args.output)
    sid = "real-loop-" + uuid.uuid4().hex[:12]
    request_id = "accept-" + sid
    file_path = "/tmp/" + sid + "/challenge.txt"
    client = OwnerClient(sid, request_id, args.base_url)
    call, command = client.call, client.command
    tid = None
    submitted = False

    def save(name, data):
        (folder / name).write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")

    try:
        call("GET", "/readyz")
        call("PUT", "/sessions/" + sid)
        workspace = call("POST", "/workspaces", {"project": "real-model-loop-validation"})
        command("bind", "bind_workspace", {"workspaceId": workspace["id"]})
        prompt = (
            "请完成一个实际沙箱任务，不要猜测或模拟结果。只能操作目录 " + file_path.rsplit("/", 1)[0] + "。"
            "先用shell工具创建该目录，读取Linux的/proc/sys/kernel/random/uuid，将读到的UUID加一个换行写入challenge.txt。"
            "第一步成功后，用第二次独立shell工具调用读取此文件并计算它的SHA256（包括文件末尾换行）。"
            "必须实际执行至少两次shell调用。禁止网络访问，不读取环境变量、凭据或其他用户文件。"
            "最终只返回JSON对象，字段uuid为文件中的UUID字符串，sha256为此文件的64位小写十六进制摘要；不要Markdown或解释。"
        )
        save("identity.json", {"session_id": sid, "request_id": request_id, "workspace_id": workspace["id"], "file": file_path})
        save("prompt.json", {"text": prompt})
        # Mark before enqueue so ambiguous submission failures also attempt a
        # durable stop; write requests are never blindly retried.
        submitted = True
        command("submit", "submit", {"inputId": "input-1", "text": prompt})
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            surface = call("GET", f"/sessions/{sid}/turns")
            if surface.get("order"):
                tid = surface["order"][-1]
                status = surface["turns"][tid]["status"]
                if status == "completed":
                    break
                if status != "active":
                    save("failed-surface.json", surface)
                    raise RuntimeError("Real model turn ended with status=" + status)
            time.sleep(0.5)
        else:
            raise TimeoutError("Real model turn exceeded acceptance deadline")
        turn = call("GET", f"/sessions/{sid}/turns/{tid}")
        save("turn.json", turn)
        run_id = turn["turn"]["runId"]
        events = []
        req = client.request("GET", f"/sessions/{sid}/events?from=0")
        with urllib.request.urlopen(req, timeout=20) as response:
            for raw in response:
                if not raw.startswith(b"data: "):
                    continue
                event = json.loads(raw[6:])
                if event.get("error"):
                    raise RuntimeError("Durable event stream failed; body suppressed")
                if event.get("type"):
                    events.append(event)
                value = event.get("value", {})
                if event.get("type") == "twilight/run/run_ended" and value.get("RunID") == run_id:
                    break
                if len(events) > 1000:
                    raise RuntimeError("Too many events for bounded loop acceptance")
        save("events.json", events)
        facts = [(e["type"].removeprefix("twilight/run/"), e["value"]["Fact"]) for e in events
                 if e["type"].startswith("twilight/run/") and e.get("value", {}).get("RunID") == run_id]
        names = [name for name, _ in facts]
        models = [f for name, f in facts if name == "model_step_completed"]
        calls = [c for name, f in facts if name == "tool_step_opened" for c in f["calls"]]
        completed = [f for name, f in facts if name == "tool_call_completed"]
        assert len(models) >= 3, "Expected model -> tool -> model -> tool -> model"
        assert len([c for c in calls if c["toolRef"] == "shell"]) >= 2, "Model did not issue two shell calls"
        assert len(completed) >= 2, "Two shell results were not durably completed"
        assert names.index("model_step_completed") < names.index("tool_call_completed") < len(names) - 1
        assert facts[-1][0] == "run_ended" and "completed" in facts[-1][1], "Run did not end successfully"

        # Independent verification executes only our fixed read/hash program in
        # validation Sandbox Pods. It never executes model-generated commands.
        kubectl = kube()
        pods = json.loads(subprocess.check_output(kubectl + ["get", "pods", "-o", "json"], timeout=20))["items"]
        matches = []
        for pod in pods:
            name = pod["metadata"]["name"]
            if not name.startswith("twilight-validation-sandbox-pool-"):
                continue
            result = subprocess.run(kubectl + ["exec", name, "--", "sh", "-c",
                'test -f "$1" || exit 44; cat "$1"; sha256sum "$1"', "verify", file_path],
                capture_output=True, text=True, timeout=20)
            if result.returncode == 0:
                lines = result.stdout.splitlines()
                matches.append((name, lines[0], lines[1].split()[0]))
        assert len(matches) == 1, "Expected challenge file in exactly one validation Sandbox"
        sandbox, actual_uuid, actual_sha = matches[0]
        assert str(uuid.UUID(actual_uuid)) == actual_uuid
        assert hashlib.sha256((actual_uuid + "\n").encode()).hexdigest() == actual_sha
        reply = turn["reply"].strip()
        reply = re.sub(r"^```(?:json)?\s*|\s*```$", "", reply)
        answer = json.loads(reply)
        assert answer["uuid"] == actual_uuid and answer["sha256"] == actual_sha, "Model answer does not match independent filesystem evidence"
        report = {"status": "passed", "session_id": sid, "request_id": request_id, "turn_id": tid,
                  "run_id": run_id, "sandbox_pod": sandbox, "model_completions": len(models),
                  "model_issued_shell_calls": len([c for c in calls if c["toolRef"] == "shell"]),
                  "durable_tool_completions": len(completed), "answer": answer,
                  "usage": [m["usage"] for m in models], "evidence": str(folder)}
        save("result.json", report)
        print(json.dumps(report, ensure_ascii=False))
    except (Exception, KeyboardInterrupt) as error:
        stop = {"status": "not_submitted"}
        if submitted:
            try:
                payload = {"reason": "bounded real-model acceptance failed or timed out"}
                if tid:
                    payload["turnId"] = tid
                command("acceptance-stop", "stop", payload)
                stop = {"status": "applied"}
            except Exception as stop_error:
                stop = {"status": "unconfirmed", "error_type": type(stop_error).__name__}
        save("failure.json", {"session_id": sid, "turn_id": tid, "error_type": type(error).__name__, "stop": stop})
        print(json.dumps({"status": "failed", "session_id": sid, "turn_id": tid,
                          "error_type": type(error).__name__, "stop": stop, "evidence": str(folder)}))
        raise
