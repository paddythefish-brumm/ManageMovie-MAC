import runpy
import sys
import unittest
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
APP_DIR = TESTS_DIR.parent / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))


class RunnerImportTests(unittest.TestCase):
    def test_runner_module_loads(self):
        script = APP_DIR / "managemovie.py"
        module_globals = runpy.run_path(str(script), run_name="managemovie_test_import")
        self.assertEqual(module_globals.get("REENQUEUE_DIR_NAME"), "__RE-ENQUEUE")
        self.assertIn("TARGET_REENQUEUE_PATH", module_globals)


if __name__ == "__main__":
    unittest.main()
