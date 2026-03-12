import os
import sys
import unittest
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
APP_DIR = TESTS_DIR.parent / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from mmcore.secret_store import (
    decrypt_state_value,
    encrypt_state_value,
    is_encrypted_state_value,
    is_secret_state_key,
    _state_fernet,
)


class SecretStoreTests(unittest.TestCase):
    def setUp(self):
        os.environ["MANAGEMOVIE_STATE_CRYPT_KEY"] = "unit-test-secret"
        _state_fernet.cache_clear()

    def tearDown(self):
        os.environ.pop("MANAGEMOVIE_STATE_CRYPT_KEY", None)
        _state_fernet.cache_clear()

    def test_secret_key_encrypted_roundtrip(self):
        key = "settings.gemini_api"
        plain = "abc123"
        enc = encrypt_state_value(key, plain)
        self.assertNotEqual(enc, plain)
        self.assertTrue(is_encrypted_state_value(enc))
        self.assertEqual(decrypt_state_value(key, enc), plain)

    def test_non_secret_key_passthrough(self):
        key = "web.last_mode"
        plain = "copy"
        stored = encrypt_state_value(key, plain)
        self.assertEqual(stored, plain)
        self.assertEqual(decrypt_state_value(key, stored), plain)

    def test_plain_legacy_secret_remains_readable(self):
        key = "settings.tmdb_api"
        plain = "legacy-value"
        self.assertEqual(decrypt_state_value(key, plain), plain)

    def test_secret_key_registry(self):
        self.assertTrue(is_secret_state_key("settings.plex_api"))
        self.assertFalse(is_secret_state_key("settings.plex_ip"))


if __name__ == "__main__":
    unittest.main()
