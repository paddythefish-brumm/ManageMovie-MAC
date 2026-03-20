import runpy
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch


TESTS_DIR = Path(__file__).resolve().parent
APP_DIR = TESTS_DIR.parent / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


class RunnerPermissionTests(unittest.TestCase):
    def test_ensure_clean_dir_reports_parent_on_create_permission_error(self) -> None:
        script = APP_DIR / "managemovie.py"
        module_globals = runpy.run_path(str(script), run_name="managemovie_test_permissions")
        ensure_clean_dir = module_globals["ensure_clean_dir"]

        with TemporaryDirectory() as tmp_dir:
            target = Path(tmp_dir) / "__OUT"
            with patch("pathlib.Path.mkdir", side_effect=PermissionError("blocked")):
                with self.assertRaises(RuntimeError) as ctx:
                    ensure_clean_dir(target)

        message = str(ctx.exception)
        self.assertIn("Zielpfad nicht anlegbar", message)
        self.assertIn(str(target), message)
        self.assertIn(f"parent={target.parent}", message)


if __name__ == "__main__":
    unittest.main()
