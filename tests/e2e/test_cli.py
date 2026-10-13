"""Offline tests: no credentials, Kubernetes mutations or model API calls."""
import contextlib
import importlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from deploy.validation import common, runtime, validate


class CLITests(unittest.TestCase):
    def test_live_modules_import_without_side_effects(self):
        with patch('subprocess.run', side_effect=AssertionError('unexpected process')), patch('urllib.request.urlopen', side_effect=AssertionError('unexpected HTTP')):
            for module in ('mock', 'real', 'logs', 'observability'):
                importlib.reload(importlib.import_module('tests.e2e.' + module))

    def test_real_api_and_delete_need_explicit_opt_in(self):
        for argv in (['check', 'real', '--output', '/tmp/unused'], ['check', 'logs']):
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                validate.parser().parse_args(argv)

    def test_mock_check_refuses_real_catalog(self):
        doc = {'data': {'models.json': '{"models":[{"ref":"chat","model":"real","baseURL":"https://example.invalid/v1"}]}'}}
        with patch.object(validate, 'kube_json', return_value=doc), self.assertRaises(RuntimeError):
            validate.require_catalog('mock')

    def test_invalid_image_tag_fails_before_mutation(self):
        args = validate.parser().parse_args(['deploy', 'app', '--tag', 'dev'])
        with patch.object(runtime, 'helm', return_value='helm'), patch.object(runtime, 'run', side_effect=AssertionError('unexpected mutation')):
            with self.assertRaises(ValueError):
                runtime.deploy(args)

    def test_private_output_never_overwrites_evidence(self):
        with tempfile.TemporaryDirectory() as parent:
            path = Path(parent) / 'evidence'
            common.private_directory(path)
            self.assertEqual(path.stat().st_mode & 0o777, 0o700)
            with self.assertRaises(FileExistsError):
                common.private_directory(path)

    def test_ambiguous_write_is_not_retried(self):
        client = common.OwnerClient('fixture-session', 'fixture-request')
        with patch.object(client, 'call', side_effect=ConnectionError('lost response')) as call:
            with self.assertRaises(ConnectionError):
                client.command('id', 'submit', {'inputId': 'fixture'})
            self.assertEqual(call.call_count, 1)
            self.assertEqual(call.call_args.args[0], 'POST')

    def test_missing_helm_executable_fails_early(self):
        with patch.object(common.shutil, 'which', return_value=None), self.assertRaises(RuntimeError):
            common.helm()

    def test_real_interruption_records_stop_outcome(self):
        from tests.e2e import real
        for stop_error in (None, ConnectionError('lost stop response')):
            with tempfile.TemporaryDirectory() as parent, patch.object(real, 'OwnerClient') as client:
                args = validate.parser().parse_args(['check', 'real', '--allow-real-api', '--output', str(Path(parent) / 'evidence')])
                client.return_value.call.side_effect = [None, None, {'id': 'fixture-ws'}]
                client.return_value.command.side_effect = [None, KeyboardInterrupt(), stop_error]
                with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(KeyboardInterrupt):
                    real.check(args)
                self.assertEqual(client.return_value.command.call_args.args[0], 'acceptance-stop')
                failure = json.loads((Path(args.output) / 'failure.json').read_text())
                self.assertEqual(failure['stop']['status'], 'applied' if stop_error is None else 'unconfirmed')

    def test_kubernetes_scope_is_explicit_and_isolated(self):
        self.assertEqual(common.kube()[1:5], ['--context', 'k3d-twilight', '--namespace', 'twilight-validation'])
        self.assertNotIn('twilight', common.kube())


if __name__ == '__main__':
    unittest.main()
