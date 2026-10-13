import copy
import json
from pathlib import Path
import re
import unittest

from deploy.observability import build_dashboard as build
from tests.e2e.dashboard import check_layout, interpolate, ROLES


class DashboardTests(unittest.TestCase):
    def test_generator_matches_committed_artifact(self):
        doc = json.loads((Path(build.__file__).parent / "dashboards/twilight-validation.json").read_text())
        self.assertEqual(build.dashboard(), doc)
        check_layout(doc)

    def test_all_and_single_component_and_pod_filters(self):
        _, details = check_layout(build.dashboard())
        for role in ROLES:
            for panel in details:
                for target in panel.get("targets", []):
                    expr = interpolate(target["expr"], role, "owner-a.b-123")
                    self.assertNotIn("$", expr)
                    self.assertIn('role=~' + json.dumps(re.escape(role)), expr)
                    self.assertIn('pod=~' + json.dumps(re.escape("owner-a.b-123")), expr)
        all_query = interpolate('up{' + build.SCOPE + '}')
        self.assertIn('role=~"owner|worker|model-backend|tool-backend"', all_query)
        self.assertIn('pod=~".*"', all_query)

    def test_probe_and_stream_routes_are_not_rpc(self):
        for route in ("GET /healthz", "GET /readyz", "GET /sessions/{sid}/events", "POST /progress", "POST /settlements", "POST /notices"):
            self.assertTrue(re.fullmatch(build.PROBE, route) or re.fullmatch(build.STREAM, route), route)
        for route in ("POST /dispatch", "POST /recover", "POST /outcome", "GET /sessions/{sid}/turns", "unmatched"):
            self.assertIsNone(re.fullmatch(build.PROBE, route))
            self.assertIsNone(re.fullmatch(build.STREAM, route))

    def test_scope_regression_is_rejected(self):
        bad = copy.deepcopy(build.dashboard())
        next(p for p in bad["panels"] if p["type"] == "logs")["targets"][0]["expr"] = '{namespace="twilight-validation"}'
        with self.assertRaises(AssertionError):
            check_layout(bad)

    def test_no_mixed_inflight_fallback(self):
        expressions = [t["expr"] for p in build.dashboard()["panels"] for t in p.get("targets", [])]
        self.assertTrue(any('class="rpc"' in e for e in expressions))
        self.assertTrue(any('class="stream"' in e for e in expressions))
        self.assertFalse(any("twilight_http_requests_in_flight{" in e for e in expressions))


if __name__ == "__main__":
    unittest.main()
