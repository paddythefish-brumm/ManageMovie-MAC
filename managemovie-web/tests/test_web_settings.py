import sys
import unittest
from pathlib import Path

TESTS_DIR = Path(__file__).resolve().parent
APP_DIR = TESTS_DIR.parent / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from mmcore.web_settings import (
    SECRET_PLACEHOLDER,
    apply_secret_update,
    build_public_runtime_settings,
    mask_secret_for_ui,
)


class WebSettingsTests(unittest.TestCase):
    def test_mask_secret_for_ui(self):
        self.assertEqual(mask_secret_for_ui(""), "")
        self.assertEqual(mask_secret_for_ui("  "), "")
        self.assertEqual(mask_secret_for_ui("token"), SECRET_PLACEHOLDER)

    def test_apply_secret_update(self):
        current = "existing"
        self.assertEqual(apply_secret_update({}, "plex_api", current), "existing")
        self.assertEqual(
            apply_secret_update({"plex_api": SECRET_PLACEHOLDER}, "plex_api", current),
            "existing",
        )
        self.assertEqual(
            apply_secret_update({"plex_api": " new-token "}, "plex_api", current),
            "new-token",
        )
        self.assertEqual(
            apply_secret_update({"plex_api": ""}, "plex_api", current),
            "",
        )

    def test_build_public_runtime_settings(self):
        settings = {
            "target_nfs_path": "/mnt/out",
            "target_out_path": "__OUT",
            "nas_ip": "10.0.0.4",
            "plex_ip": "10.0.0.2",
            "plex_api": "p",
            "tmdb_api": "",
            "gemini_api": "g",
            "skip_4k_h265_encode": "1",
            "precheck_egb": "0",
            "start_on_boot": "0",
            "initial_setup_done": "0",
            "initial_setup_required": "1",
        }
        out = build_public_runtime_settings(settings, mode="copy", encoder="cpu")
        self.assertEqual(out["target_nfs_path"], "/mnt/out")
        self.assertEqual(out["target_out_path"], "__OUT")
        self.assertEqual(out["nas_ip"], "10.0.0.4")
        self.assertEqual(out["plex_ip"], "10.0.0.2")
        self.assertEqual(out["mode"], "copy")
        self.assertEqual(out["encoder"], "cpu")
        self.assertEqual(out["plex_api"], SECRET_PLACEHOLDER)
        self.assertEqual(out["tmdb_api"], "")
        self.assertEqual(out["gemini_api"], SECRET_PLACEHOLDER)
        self.assertTrue(out["has_plex_api"])
        self.assertFalse(out["has_tmdb_api"])
        self.assertTrue(out["has_gemini_api"])
        self.assertTrue(out["skip_4k_h265_encode"])
        self.assertFalse(out["precheck_egb"])
        self.assertFalse(out["start_on_boot"])
        self.assertFalse(out["initial_setup_done"])
        self.assertTrue(out["initial_setup_required"])
        self.assertNotIn("skip_h265_encode", out)

    def test_build_public_runtime_settings_reports_missing_keys(self):
        settings = {
            "plex_api": "",
            "tmdb_api": "",
            "gemini_api": "",
            "initial_setup_done": "0",
            "initial_setup_required": "1",
        }
        out = build_public_runtime_settings(settings, mode="analyze", encoder="cpu")
        self.assertFalse(out["has_plex_api"])
        self.assertFalse(out["has_tmdb_api"])
        self.assertFalse(out["has_gemini_api"])
        self.assertFalse(out["initial_setup_done"])
        self.assertTrue(out["initial_setup_required"])
        self.assertTrue(out["start_on_boot"])


if __name__ == "__main__":
    unittest.main()
