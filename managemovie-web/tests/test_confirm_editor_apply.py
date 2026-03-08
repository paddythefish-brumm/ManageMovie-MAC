import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


TESTS_DIR = Path(__file__).resolve().parent
APP_DIR = TESTS_DIR.parent / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
APP_PATH = APP_DIR / "managemovie.py"
SPEC = importlib.util.spec_from_file_location("managemovie_core_app", APP_PATH)
MM = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MM)


class ConfirmEditorApplyTests(unittest.TestCase):
    def test_apply_confirmation_editor_rows_clears_manifest_skip_after_done_reset(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            start_folder = Path(tmp_dir)
            rows = [
                {
                    "Quellname": "Movie/Test.mkv",
                    "Name des Film/Serie": "Test",
                    "Erscheinungsjahr": "2024",
                    "Zielname": "__OUT/Movie/Test (2024)/Test (2024).mkv",
                    "MANIFEST-SKIP": "1",
                    "MANIFEST-MODE": "copy",
                    "MANIFEST-TARGET": "__OUT/Movie/Test (2024)/Test (2024).mkv",
                    "VERARBEITET": "1",
                    "Speed": "copied",
                    "ETA": "copied",
                    "Z-GB": "12.3",
                }
            ]
            editor_rows = [
                {
                    "source_name": "Movie/Test.mkv",
                    "target_name": "__OUT/Movie/Test (2024)/Test (2024).mkv",
                    "title": "Test",
                    "year": "2024",
                    "season": "",
                    "episode": "",
                    "imdb_id": "tt1234567",
                    "speed": "",
                    "eta": "",
                    "z_gb": "",
                    "e_gb": "",
                    "lzeit": "",
                    "manual": False,
                }
            ]

            MM.apply_confirmation_editor_rows(rows, start_folder, "c", editor_rows)

        row = rows[0]
        self.assertEqual(row.get("MANIFEST-SKIP", ""), "")
        self.assertEqual(row.get("MANIFEST-MODE", ""), "")
        self.assertEqual(row.get("VERARBEITET", ""), "")
        self.assertEqual(row.get("Speed", ""), "")
        self.assertEqual(row.get("ETA", ""), "")
        self.assertEqual(row.get("Z-GB", ""), "")


if __name__ == "__main__":
    unittest.main()
