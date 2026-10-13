"""Deterministic, development-only OpenAI-compatible model. No provider access.

POST /control {"delay_seconds": 0..120} sets future request latency.
A user message containing [validation-tool] requests one harmless shell call;
otherwise the result is fixed. No prompts or credentials are logged.
"""
import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_lock = threading.Lock()
_delay_seconds = 0.0


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        pass

    def json_reply(self, status, body):
        raw = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path == "/healthz":
            self.send_response(204)
            self.end_headers()
        else:
            self.json_reply(404, {"error": "not found"})

    def do_POST(self):
        global _delay_seconds
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 0 or length > 4 * 1024 * 1024:
                self.json_reply(413, {"error": "request too large"})
                self.close_connection = True
                return
            body = json.loads(self.rfile.read(length))
            if not isinstance(body, dict):
                raise ValueError("body must be object")
        except (ValueError, TypeError):
            self.json_reply(400, {"error": "invalid JSON"})
            return
        if self.path == "/control":
            try:
                delay = float(body["delay_seconds"])
                if not 0 <= delay <= 120:
                    raise ValueError("invalid delay")
            except (KeyError, ValueError, TypeError):
                self.json_reply(400, {"error": "delay_seconds must be 0..120"})
                return
            with _lock:
                _delay_seconds = delay
            self.json_reply(200, {"delay_seconds": delay})
            return
        if self.path != "/v1/chat/completions":
            self.json_reply(404, {"error": "not found"})
            return
        with _lock:
            delay = _delay_seconds
        request_id = "mock-" + uuid.uuid4().hex
        print(json.dumps({"event": "mock_model_started", "request_id": request_id, "delay_seconds": delay}), flush=True)
        time.sleep(delay)
        messages = body.get("messages", [])
        tool_result = any(m.get("role") == "tool" for m in messages)
        use_tool = not tool_result and any(
            m.get("role") == "user" and "[validation-tool]" in json.dumps(m.get("content", ""))
            for m in messages
        )
        call = {"id": "validation-shell", "type": "function", "function": {
            "name": "shell", "arguments": json.dumps({"command": "printf 'validation-tool-ok\\n'"})}}
        tool_success = any(
            m.get("role") == "tool" and "validation-tool-ok" in json.dumps(m.get("content", ""))
            for m in messages
        )
        reply = ("validation mock: tool done" if tool_success else "validation mock: tool failed") if tool_result else "validation mock: done"
        finish = "tool_calls" if use_tool else "stop"
        usage = {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}
        common = {"id": request_id, "created": int(time.time()), "model": "validation-mock"}
        try:
            if not body.get("stream"):
                message = {"role": "assistant", "content": None if use_tool else reply}
                if use_tool:
                    message["tool_calls"] = [call]
                self.json_reply(200, {**common, "object": "chat.completion", "choices": [
                    {"index": 0, "message": message, "finish_reason": finish}], "usage": usage})
            else:
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.end_headers()
                self.close_connection = True

                def chunk(delta, finish_reason=None):
                    obj = {**common, "object": "chat.completion.chunk", "choices": [
                        {"index": 0, "delta": delta, "finish_reason": finish_reason}]}
                    self.wfile.write(("data: " + json.dumps(obj) + "\n\n").encode())
                    self.wfile.flush()

                chunk({"role": "assistant"})
                chunk({"tool_calls": [{"index": 0, **call}]} if use_tool else {"content": reply})
                chunk({}, finish)
                self.wfile.write(("data: " + json.dumps({**common, "object": "chat.completion.chunk", "choices": [], "usage": usage}) + "\n\ndata: [DONE]\n\n").encode())
                self.wfile.flush()
            print(json.dumps({"event": "mock_model_completed", "request_id": request_id, "finish_reason": finish}), flush=True)
        except (BrokenPipeError, ConnectionResetError):
            print(json.dumps({"event": "mock_model_client_disconnected", "request_id": request_id}), flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
