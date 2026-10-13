#!/usr/bin/env python3
"""Build the provisioned beginner-friendly dashboard (no runtime dependencies).

The expanded row is repeated by Grafana's component variable; its children
follow it in the flat panel list, as required by Grafana's expanded-row schema.
Keep this generator and its JSON artifact together; --check detects drift.
"""
import argparse
import json
from pathlib import Path

NS = "twilight-validation"
ROLES = {
    "owner": "Owner（会话与命令）",
    "worker": "Worker（执行与恢复）",
    "model-backend": "Model Backend（模型执行）",
    "tool-backend": "Tool Backend（工具与工作区）",
}
PROM = {"type": "prometheus", "uid": "twilight-prometheus"}
LOKI = {"type": "loki", "uid": "twilight-loki"}
PROBE = r"GET /(healthz|readyz)"
STREAM = r"GET /sessions/[^/]+/events|POST /(progress|settlements|notices)"
SCOPE = f'namespace="{NS}",role=~"${{component:regex}}",pod=~"${{pod:regex}}"'
RPC = SCOPE + ',route!~' + json.dumps(PROBE) + ',route!~' + json.dumps(STREAM)
PROBES = SCOPE + ',route=~' + json.dumps(PROBE)
PANELS = []


def panel(title, kind, x, y, width, height, description="", targets=None, unit=None):
    result = {
        "id": len(PANELS) + 1, "title": title, "type": kind,
        "gridPos": {"x": x, "y": y, "w": width, "h": height},
        "description": description,
    }
    if targets is not None:
        result["datasource"] = PROM
        result["targets"] = [
            {"refId": chr(65 + i), "expr": expr, "legendFormat": legend}
            for i, (expr, legend) in enumerate(targets)
        ]
        result["fieldConfig"] = {"defaults": {"noValue": "暂无样本"}, "overrides": []}
        if unit:
            result["fieldConfig"]["defaults"].update(unit="suffix:次/分钟" if unit == "rpm" else unit, min=0)
        if kind == "timeseries":
            result["options"] = {
                "legend": {"displayMode": "table", "placement": "bottom", "calcs": ["lastNotNull"]},
                "tooltip": {"mode": "multi", "sort": "desc"},
            }
    PANELS.append(result)
    return result


def rate(metric, selector, group, scale=1):
    return f'sum by ({group}) (rate({metric}{{{selector}}}[$__rate_interval]))' + (f" * {scale}" if scale != 1 else "")


intro = panel("先看这里：如何使用面板", "text", 0, 0, 24, 7)
intro["options"] = {"mode": "markdown", "content": """### 总览 → 组件 → 副本 → 日志
- 顶部 **组件** 默认全部展开；选择一个组件后，只看它的详情。**Pod** 用来定位具体副本；选单个 Pod 时，建议同时选择其所属组件。
- 四个组件：**Owner** 接收命令/订阅事件；**Worker** 调度、核对和恢复执行；**Model Backend** 执行模型；**Tool Backend** 执行工具/管理工作区。
- **普通 RPC、健康探针、流式端点分别展示**。RPC 完成数不是模型调用数；流式活跃请求不是每秒输出的 token 数。
- `No data` / `暂无样本` 不代表故障：未发生某类请求或样本不足时可能空白。先看存活/就绪；没有 HTTP 错误也不能证明业务恢复成功。
- 所有详情保留 **Pod** 区分。日志里的 Session/Execution ID 用于搜索，不是 metrics/stream labels。"""}

for title, metric, x, desc in [
    ("存活：每个 Pod 的指标端点", "up", 0, "1=Prometheus 能抓取 /metrics；0=抓取失败。不证明业务循环、数据库或模型供应商正常。"),
    ("就绪：最近一次检查", "twilight_ready", 12, "1=最近一次 /readyz 成功；DB 组件检查本地数据库。不是持续依赖监控，也不检查外部模型供应商。"),
]:
    p = panel(title, "table", x, 7, 12, 8, desc, [(f'{metric}{{{SCOPE}}}', "{{role}} / {{pod}}")])
    p["targets"][0].update(instant=True, range=False, format="table")
    p["transformations"] = [{"id": "organize", "options": {
        "excludeByName": {key: True for key in ["Time", "__name__", "namespace", "instance", "job"]},
        "indexByName": {"role": 0, "pod": 1, "Value": 2},
        "renameByName": {"role": "组件", "pod": "副本", "Value": "状态"},
    }}]
    p["fieldConfig"]["defaults"].update(min=0, max=1, thresholds={"mode": "absolute", "steps": [
        {"color": "red", "value": None}, {"color": "green", "value": 1}]})
    p["fieldConfig"]["overrides"] = [
        {"matcher": {"id": "byName", "options": "状态"}, "properties": [
            {"id": "custom.cellOptions", "value": {"type": "color-background"}},
            {"id": "custom.width", "value": 90},
            {"id": "mappings", "value": [{"type": "value", "options": {"0": {"text": "失败", "color": "red"}, "1": {"text": "正常", "color": "green"}}}]},
        ]},
        {"matcher": {"id": "byName", "options": "组件"}, "properties": [{"id": "custom.width", "value": 125}]},
    ]
    p["options"] = {"showHeader": True, "cellHeight": "sm"}

panel("总览：普通 RPC 完成数 / 分钟（按组件）", "timeseries", 0, 15, 12, 7,
    "仅短请求：排除 /healthz、/readyz 和 SSE 端点。只在请求结束时计数；下方按组件/POD进一步定位。",
    [(rate("twilight_http_requests_total", RPC, "role", 60), "{{role}}")], "rpm")
panel("总览：执行边界结果（按组件）", "timeseries", 12, 15, 12, 7,
    "采样窗口内的 operation 次数。owned/not_ready 等可能是正常协议结果；dispatch_unknown 不是确定失败。没有相应操作时不伪造零样本。",
    [(f'sum by (role, result) (increase(twilight_operations_total{{{SCOPE}}}[$__rate_interval]))', "{{role}} · {{result}}")], "short")

row = panel("${component:text} · 组件详情", "row", 0, 22, 24, 1)
row.update(collapsed=False, panels=[], repeat="component", repeatDirection="v")

panel("普通 RPC 完成数 / 分钟", "timeseries", 0, 18, 12, 7,
    "当前组件各 Pod/接口的完成速率。不含健康探针和 SSE；不是模型或工具执行吞吐量。",
    [(rate("twilight_http_requests_total", RPC, "pod, route", 60), "{{pod}} · {{route}}")], "rpm")
errors = rate("twilight_http_requests_total", RPC + ',status=~"[45].."', "pod, status", 60)
# A zero is justified only for Pods whose RPC counter has actual rate samples;
# targets without RPC traffic/samples remain absent, rather than fabricated 0s.
zero_baseline = rate("twilight_http_requests_total", RPC, "pod", 60) + " * 0"
panel("普通 RPC：4xx / 5xx 次数 / 分钟", "timeseries", 12, 18, 12, 7,
    "4xx 多为无效请求/冲突/不存在，5xx 多为服务错误。已采到 RPC 流量但没有错误时显示零；未采到 RPC 流量时无样本。HTTP 成功不等于业务成功。",
    [(errors + " or on (pod) (" + zero_baseline + ")", "{{pod}} · {{status}}")], "rpm")
latency = panel("普通 RPC：P95 耗时", "timeseries", 0, 25, 12, 7,
    "按 Pod/接口的直方图估计值：约95%的已完成短请求低于此值。排除探针和流式连接；低流量时精度有限，NaN 表示样本不足。",
    [(f'histogram_quantile(0.95, sum by (le, pod, route) (rate(twilight_http_request_duration_seconds_bucket{{{RPC}}}[$__rate_interval])))', "{{pod}} · {{route}}")], "s")
latency["fieldConfig"]["defaults"]["mappings"] = [{"type": "special", "options": {"match": "nan", "result": {"text": "样本不足"}}}]
panel("普通 RPC：当前活跃请求", "timeseries", 12, 25, 12, 7,
    "当前组件各 Pod 正在处理的普通请求，不含探针/流式端点。执行可在HTTP返回后继续，0不等于没有运行中的任务。15s抓取可能看不到很短的请求。旧版没有新指标时留空，不拿混合总数代替。",
    [(f'twilight_http_requests_in_flight_by_class{{{SCOPE},class="rpc"}}', "{{pod}}")], "short")
panel("流式端点：当前活跃请求", "timeseries", 0, 32, 12, 7,
    "与普通 RPC 分开：events/progress/settlements/notices 的活跃请求，包含握手，不仅是已经接收的SSE连接。不用连接寿命冒充RPC延迟；长期保持非零可能正常。",
    [(f'twilight_http_requests_in_flight_by_class{{{SCOPE},class="stream"}}', "{{pod}}")], "short")
panel("健康探针：完成数 / 分钟", "timeseries", 12, 32, 12, 7,
    "只有 /healthz 和 /readyz。即使没有对话也会有流量；状态503表示未就绪。探针不计入左上方的业务RPC速率/延迟。",
    [(rate("twilight_http_requests_total", PROBES, "pod, route, status", 60), "{{pod}} · {{route}} · {{status}}")], "rpm")
panel("操作结果：采样窗口内次数", "timeseries", 0, 39, 12, 7,
    "当前组件的 Session/dispatch/outcome/recover/backend 边界。每条线保留 Pod/operation/result。not_ready/owned 未必异常；没有样本不证明恢复路径已执行。",
    [(f'sum by (pod, operation, result) (increase(twilight_operations_total{{{SCOPE}}}[$__rate_interval]))', "{{pod}} · {{operation}} · {{result}}")], "short")
panel("操作：P95 耗时", "timeseries", 12, 39, 12, 7,
    "对应具体边界操作，不是整轮对话耗时；慢 owner open/recover 与慢HTTP应分别定位。",
    [(f'histogram_quantile(0.95, sum by (le, pod, operation) (rate(twilight_operation_duration_seconds_bucket{{{SCOPE}}}[$__rate_interval])))', "{{pod}} · {{operation}}")], "s")
panel("内存：Go 堆 / 进程 RSS", "timeseries", 0, 46, 12, 7,
    "每个 Pod 分开。Go堆是进程内存的一部分，RSS还包含栈/运行时等；不要将两条线相加。",
    [(f'go_memstats_heap_alloc_bytes{{{SCOPE}}}', "堆 · {{pod}}"),
     (f'process_resident_memory_bytes{{{SCOPE}}}', "RSS · {{pod}}")], "bytes")
panel("CPU：100% = 一个核", "timeseries", 12, 46, 12, 7,
    "进程CPU秒的增长速率；100%=使用一个CPU核，多核进程可能超过100%。不是整个节点的CPU使用率。",
    [(f'rate(process_cpu_seconds_total{{{SCOPE}}}[$__rate_interval]) * 100', "{{pod}}")], "percent")
logs = panel("当前组件日志（可展开详情搜索 Session / Execution）", "logs", 0, 53, 24, 10,
    "只显示当前组件/所选Pod的主container。动态ID保留在JSON正文；可在Explore继续用 |= 过滤。选定Pod后不会混入其他副本日志。")
logs["datasource"] = LOKI
logs["targets"] = [{"refId": "A", "expr": '{' + SCOPE + ',container=~"${component:regex}"}', "queryType": "range"}]
logs["options"] = {"showTime": True, "showLabels": True, "wrapLogMessage": True,
    "sortOrder": "Descending", "enableLogDetails": True, "dedupStrategy": "none"}
# The taller, readable overview shifts the repeated component's whole body.
for child in PANELS[PANELS.index(row) + 1:]:
    child["gridPos"]["y"] += 5


def dashboard():
    role_options = [{"text": "All", "value": "$__all", "selected": True}] + [
        {"text": text, "value": value, "selected": False} for value, text in ROLES.items()]
    pod_query = f'label_values(up{{namespace="{NS}",role=~"${{component:regex}}"}}, pod)'
    return {
        "uid": "twilight-validation", "title": "Twilight Validation",
        "tags": ["twilight", "validation"], "schemaVersion": 41, "version": 2,
        "editable": False, "timezone": "browser", "refresh": "15s",
        "time": {"from": "now-30m", "to": "now"}, "annotations": {"list": []},
        "templating": {"list": [
            {"name": "component", "label": "组件", "type": "custom", "hide": 0,
             "query": ", ".join(text + " : " + value for value, text in ROLES.items()),
             "multi": True, "includeAll": True, "options": role_options,
             "current": {"text": "All", "value": "$__all", "selected": True},
             "skipUrlSync": False},
            {"name": "pod", "label": "Pod / 副本", "type": "query", "hide": 0,
             "datasource": PROM, "query": {"query": pod_query, "refId": "StandardVariableQuery"},
             "definition": pod_query, "refresh": 2, "sort": 1, "regex": "",
             "multi": True, "includeAll": True, "allValue": ".*", "options": [],
             "current": {"text": "All", "value": "$__all", "selected": True},
             "skipUrlSync": False},
        ]},
        "panels": PANELS,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    path = Path(__file__).parent / "dashboards" / "twilight-validation.json"
    content = json.dumps(dashboard(), ensure_ascii=False, indent=2) + "\n"
    if args.check:
        if path.read_text() != content:
            raise SystemExit("Dashboard artifact differs; run build_dashboard.py")
        print("Dashboard generator/artifact match")
    else:
        path.write_text(content)
        print("Built", path)


if __name__ == "__main__":
    main()
