import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


TESTS_DIR = Path(__file__).resolve().parent
WEB_DIR = TESTS_DIR.parent / "web"
APP_PATH = WEB_DIR / "app.py"
SPEC = importlib.util.spec_from_file_location("managemovie_web_app", APP_PATH)
APP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(APP)


class EditorManifestCleanTests(unittest.TestCase):
    def test_editor_manifest_clean_uses_source_filter(self) -> None:
        pending = {
            "state": "pending",
            "mode": "copy",
            "token": "tok",
            "_token": "tok",
            "_start_folder": "/tmp/start",
        }
        rows = [
            {"source_name": "A/File1.mkv", "target_name": "__OUT/Movie/File1.mkv"},
            {"source_name": "B/File2.mkv", "target_name": "__OUT/Movie/File2.mkv"},
        ]
        cleaned_payload = {
            "rows": 1,
            "source_sidecars_deleted": 1,
            "target_sidecars_deleted": 1,
            "track_entries_deleted": 1,
            "track_files_touched": 1,
            "failed": 0,
        }

        with APP.app.test_client() as client:
            with patch.object(APP, "get_pending_confirmation_for_token", return_value=(pending, "")):
                with patch.object(APP, "collect_editor_rows_from_payload", return_value=rows):
                    with patch.object(APP, "clean_manifest_for_editor_rows", return_value=(True, cleaned_payload, "")) as clean_mock:
                        with patch.object(APP, "clear_editor_override_cache_rows", return_value=1) as clear_editor_mock:
                            with patch.object(APP, "clear_processed_history_cache_rows", return_value=1) as clear_history_mock:
                                response = client.post(
                                    "/api/confirm/editor/manifest/clean",
                                    json={"token": "tok", "source_names": ["A/File1.mkv"]},
                                )

        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["rows"], 1)
        self.assertEqual(payload["track_entries_deleted"], 1)
        self.assertEqual(payload["editor_cache_cleared"], 1)
        self.assertEqual(payload["history_cache_cleared"], 1)
        clean_mock.assert_called_once()
        clear_editor_mock.assert_called_once_with(["A/File1.mkv"])
        clear_history_mock.assert_called_once_with(["A/File1.mkv"])
        _, kwargs = clean_mock.call_args
        self.assertIn("source_filter_set", kwargs)
        self.assertEqual(kwargs["source_filter_set"], {"a/file1.mkv"})


if __name__ == "__main__":
    unittest.main()
