import importlib.util
import unittest
from pathlib import Path


TESTS_DIR = Path(__file__).resolve().parent
WEB_DIR = TESTS_DIR.parent / "web"
APP_PATH = WEB_DIR / "app.py"
SPEC = importlib.util.spec_from_file_location("managemovie_web_app_imdb", APP_PATH)
APP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(APP)


class WebImdbNormalizationTests(unittest.TestCase):
    def test_normalize_editor_imdb_id_accepts_10_digit_prefixed(self) -> None:
        self.assertEqual(APP.normalize_editor_imdb_id("tt1234567890"), "tt1234567890")

    def test_normalize_editor_imdb_id_accepts_10_digit_numeric(self) -> None:
        self.assertEqual(APP.normalize_editor_imdb_id("1234567890"), "tt1234567890")

    def test_normalize_editor_rows_payload_keeps_10_digit_imdb_id(self) -> None:
        rows = APP.normalize_editor_rows_payload(
            [{"source_name": "A.mkv", "imdb_id": "tt1234567890"}],
            "/tmp",
            rebuild_targets=False,
        )
        self.assertEqual(rows[0]["imdb_id"], "tt1234567890")


if __name__ == "__main__":
    unittest.main()
