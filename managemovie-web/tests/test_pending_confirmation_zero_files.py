import os
import runpy
import sys
import tempfile
import unittest
from pathlib import Path


TESTS_DIR = Path(__file__).resolve().parent
APP_DIR = TESTS_DIR.parent / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


class PendingConfirmationZeroFilesTests(unittest.TestCase):
    def test_clear_web_confirmation_file_removes_relative_path_under_target_dir(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            target_dir = Path(temp_dir) / "MovieManager"
            target_dir.mkdir(parents=True, exist_ok=True)
            confirm_file = target_dir / "web-confirm.json"
            confirm_file.write_text('{"state":"pending"}', encoding="utf-8")

            old_target = os.environ.get("MANAGEMOVIE_WORKDIR")
            old_confirm = os.environ.get("MANAGEMOVIE_WEB_CONFIRM_FILE")
            try:
                os.environ["MANAGEMOVIE_WORKDIR"] = str(target_dir)
                os.environ["MANAGEMOVIE_WEB_CONFIRM_FILE"] = "web-confirm.json"
                module_globals = runpy.run_path(str(APP_DIR / "managemovie.py"), run_name="managemovie_test_pending_zero")
                module_globals["clear_web_confirmation_file"]()
            finally:
                if old_target is None:
                    os.environ.pop("MANAGEMOVIE_WORKDIR", None)
                else:
                    os.environ["MANAGEMOVIE_WORKDIR"] = old_target
                if old_confirm is None:
                    os.environ.pop("MANAGEMOVIE_WEB_CONFIRM_FILE", None)
                else:
                    os.environ["MANAGEMOVIE_WEB_CONFIRM_FILE"] = old_confirm

            self.assertFalse(confirm_file.exists())


if __name__ == "__main__":
    unittest.main()
