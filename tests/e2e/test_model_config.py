from pathlib import Path
import tempfile
import unittest

from deploy.validation.model import load_env


class ConfigTests(unittest.TestCase):
    def load(self, text):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / '.env'
            path.write_text(text)
            return load_env(path)

    def test_quotes_comments_and_export_without_shell_execution(self):
        values = self.load('export OPENAI_API_KEY="fake-test-key" # comment\nOPENAI_BASE_URL=https://example.invalid/v1\nOPENAI_MODEL="test-model"\n')
        self.assertEqual(values['OPENAI_API_KEY'], 'fake-test-key')
        self.assertEqual(values['OPENAI_MODEL'], 'test-model')

    def test_missing_key_error_does_not_expose_values(self):
        with self.assertRaises(ValueError) as err:
            self.load('OPENAI_API_KEY=secret-do-not-print\n')
        self.assertNotIn('secret-do-not-print', str(err.exception))

    def test_credentials_cannot_enter_helm_url_values(self):
        for url in ('http://example.invalid/v1', 'https://name:secret@example.invalid/v1', 'https://example.invalid/v1?key=secret'):
            with self.assertRaises(ValueError):
                self.load(f'OPENAI_API_KEY=fake\nOPENAI_MODEL=test\nOPENAI_BASE_URL={url}\n')


if __name__ == '__main__':
    unittest.main()
