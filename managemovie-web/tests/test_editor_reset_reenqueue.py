import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


TESTS_DIR = Path(__file__).resolve().parent
WEB_DIR = TESTS_DIR.parent / "web"
APP_PATH = WEB_DIR / "app.py"
SPEC = importlib.util.spec_from_file_location("managemovie_web_app", APP_PATH)
APP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(APP)


class EditorResetReenqueueFallbackTests(unittest.TestCase):
    def test_session_reset_augments_rows_from_runtime_when_reenqueue_path_changed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            start = Path(tmp_dir) / "START"
            start.mkdir(parents=True)
            reenqueue_root = start / "__RE-ENQUEUE"
            reenqueue_root.mkdir(parents=True)
            (start / "keep.mkv").write_text("keep", encoding="utf-8")
            (reenqueue_root / "nested").mkdir(parents=True, exist_ok=True)
            (reenqueue_root / "nested" / "lost.mkv").write_text("lost", encoding="utf-8")

            pending = {
                "state": "pending",
                "mode": "analyze",
                "token": "tok",
                "start_folder": str(start),
                "editor_rows": [{"source_name": "keep.mkv"}],
                "editor_rows_original": [{"source_name": "keep.mkv"}],
                "editor_rows_session_start": [{"source_name": "keep.mkv"}],
                "file_count": 1,
            }

            runtime_rows = [
                {"Quellname": "keep.mkv"},
                {"Quellname": "Movies/lost.mkv"},
            ]
            runtime_json = json.dumps(runtime_rows, ensure_ascii=False)
            restored_sources: list[str] = []

            def fake_read_state(key: str) -> str:
                if key == "runtime.gemini_rows_json":
                    return runtime_json
                return ""

            def fake_move_from_reenqueue(start_folder: str, source_name: str):
                restored_sources.append(source_name)
                return True, {"moved_back_files": 1, "moved_back_sidecars": 0}, ""

            with APP.app.test_client() as client:
                with patch.object(APP, "get_pending_confirmation_for_token", return_value=(pending, "")):
                    with patch.object(APP, "init_state_store", return_value=True):
                        with patch.object(APP, "resolve_reenqueue_root_for_start", return_value=reenqueue_root):
                            with patch.object(APP.STATE_DB_STORE, "read_state", side_effect=fake_read_state):
                                with patch.object(APP, "move_source_from_reenqueue", side_effect=fake_move_from_reenqueue):
                                    with patch.object(APP, "clear_editor_override_cache_rows", return_value=0):
                                        with patch.object(APP, "write_confirmation_payload", return_value=True):
                                            response = client.post(
                                                "/api/confirm/editor/reset",
                                                json={"token": "tok", "reset_scope": "session_start"},
                                            )

            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["session_restored"], 2)
            self.assertEqual(payload["reverted_reenqueue"], 1)
            self.assertEqual(payload["moved_back_files"], 1)
            self.assertEqual(restored_sources, ["Movies/lost.mkv"])
            self.assertEqual([item["source_name"] for item in payload["rows"]], ["keep.mkv", "Movies/lost.mkv"])
            self.assertEqual(len(pending["editor_rows"]), 2)


if __name__ == "__main__":
    unittest.main()
