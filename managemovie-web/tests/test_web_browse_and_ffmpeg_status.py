import importlib.util
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


TESTS_DIR = Path(__file__).resolve().parent
WEB_APP_PATH = TESTS_DIR.parent / "web" / "app.py"
RUNNER_APP_PATH = TESTS_DIR.parent / "app" / "managemovie.py"

WEB_SPEC = importlib.util.spec_from_file_location("managemovie_web_app_browse", WEB_APP_PATH)
WEB_APP = importlib.util.module_from_spec(WEB_SPEC)
WEB_SPEC.loader.exec_module(WEB_APP)

RUNNER_SPEC = importlib.util.spec_from_file_location("managemovie_runner_status", RUNNER_APP_PATH)
RUNNER_APP = importlib.util.module_from_spec(RUNNER_SPEC)
RUNNER_SPEC.loader.exec_module(RUNNER_APP)


class WebBrowseAndFfmpegStatusTests(unittest.TestCase):
    def test_mount_iso_readonly_uses_7zip_extract_on_linux(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            iso_path = Path(tmpdir) / "disc.iso"
            iso_path.write_bytes(b"iso")
            extract_root = Path(tmpdir) / "extract"
            temp_root = Path(tmpdir) / "tmp"

            class FakeProcess:
                def __init__(self) -> None:
                    self.returncode = 0
                    self._polled = False
                    self.stderr = io.StringIO("")

                def poll(self):  # type: ignore[no-untyped-def]
                    if self._polled:
                        return self.returncode
                    self._polled = True
                    return self.returncode

            def fake_popen(cmd, stdout=None, stderr=None, text=None):  # type: ignore[no-untyped-def]
                self.assertEqual(cmd[0], "/usr/bin/7z")
                self.assertEqual(cmd[1], "x")
                self.assertIn(str(iso_path), cmd)
                extract_root.mkdir(parents=True, exist_ok=True)
                (extract_root / "VIDEO_TS").mkdir(parents=True, exist_ok=True)
                return FakeProcess()

            with patch.object(RUNNER_APP.sys, "platform", "linux"), \
                 patch.object(RUNNER_APP, "resolve_7zip_binary", return_value="/usr/bin/7z"), \
                 patch.object(RUNNER_APP, "TMP_DIR", temp_root), \
                 patch.object(RUNNER_APP.mount_iso_readonly.__globals__["tempfile"], "mkdtemp", return_value=str(extract_root)), \
                 patch.object(RUNNER_APP.mount_iso_readonly.__globals__["subprocess"], "Popen", side_effect=fake_popen), \
                 patch.object(RUNNER_APP.mount_iso_readonly.__globals__["time"], "sleep", return_value=None):
                mount_point, cleanup = RUNNER_APP.mount_iso_readonly(iso_path)

            self.assertEqual(mount_point, extract_root)
            self.assertTrue((mount_point / "VIDEO_TS").exists())
            cleanup()
            self.assertFalse(extract_root.exists())

    def test_mount_iso_readonly_requires_7zip_on_linux(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            iso_path = Path(tmpdir) / "disc.iso"
            iso_path.write_bytes(b"iso")
            with patch.object(RUNNER_APP.sys, "platform", "linux"), \
                 patch.object(RUNNER_APP, "resolve_7zip_binary", return_value=""):
                with self.assertRaisesRegex(RuntimeError, "7z/7zz fehlt"):
                    RUNNER_APP.mount_iso_readonly(iso_path)

    def test_home_page_renders_skip_4k_h265_checkbox(self) -> None:
        with WEB_APP.app.test_client() as client:
            response = client.get("/")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn('id="skip4kH265EncodeSetting"', html)
        self.assertIn('id="precheckEgbSetting"', html)
        self.assertIn("4k/h265 nicht encoden", html)
        self.assertIn("Pre-Check E-GB", html)
        self.assertNotIn('skipH265EncodeSetting', html)
        self.assertLess(html.find('skip4kH265EncodeSetting'), html.find('<summary>Advanced</summary>'))

    def test_home_page_places_exit_button_next_to_editor_in_pending_confirmation_actions(self) -> None:
        with WEB_APP.app.test_client() as client:
            response = client.get("/")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        editor_index = html.find('id="confirmEditBtn"')
        exit_index = html.find('id="confirmExitBtn"')
        popout_index = html.find('id="confirmEditPopoutBtn"')
        self.assertGreaterEqual(editor_index, 0)
        self.assertGreaterEqual(exit_index, 0)
        self.assertGreaterEqual(popout_index, 0)
        self.assertLess(editor_index, exit_index)
        self.assertLess(exit_index, popout_index)

    def test_home_page_uses_back_and_abort_labels_in_inline_confirmation(self) -> None:
        with WEB_APP.app.test_client() as client:
            response = client.get("/")
        self.assertEqual(response.status_code, 200)
        html = response.get_data(as_text=True)
        self.assertIn(">Zurück</button>", html)
        self.assertIn(">Abbruch</button>", html)
        self.assertNotIn(">OK</button>", html)

    def test_normalize_browse_path_defaults_to_last_started_folder(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            folder = Path(tmpdir).resolve()
            with patch.object(WEB_APP, "BROWSE_ROOT", Path("/")):
                with patch.object(WEB_APP, "read_last_started_folder", return_value=str(folder)):
                    self.assertEqual(WEB_APP.normalize_browse_path(None), folder)

    def test_browse_page_hides_row_select_buttons(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir).resolve()
            (root / "alpha").mkdir()
            with patch.object(WEB_APP, "BROWSE_ROOT", root):
                with WEB_APP.app.test_client() as client:
                    response = client.get(f"/browse?folder={root}&target=folder")
            self.assertEqual(response.status_code, 200)
            html = response.get_data(as_text=True)
            self.assertNotIn(">Wählen</a>", html)
            self.assertIn(">Zurück</a>", html)

    def test_format_live_gb_text_keeps_two_decimals(self) -> None:
        self.assertEqual(RUNNER_APP.format_live_gb_text(0.0), "0.00")
        self.assertEqual(RUNNER_APP.format_live_gb_text(1.357), "1.36")

    def test_format_speed_text_uses_single_decimal(self) -> None:
        self.assertEqual(RUNNER_APP.format_speed_text(4.47), "4.5x")

    def test_ffmpeg_target_video_codec_is_h264(self) -> None:
        self.assertEqual(RUNNER_APP.FFMPEG_TARGET_VIDEO_CODEC, "h264")

    def test_ffmpeg_target_audio_codec_is_ac3(self) -> None:
        self.assertEqual(RUNNER_APP.FFMPEG_TARGET_AUDIO_CODEC, "ac3")

    def test_ffmpeg_cpu_encoder_args_use_h264_compatible_profile(self) -> None:
        args = RUNNER_APP.ffmpeg_video_encoder_args("cpu")
        self.assertIn("libx264", args)
        self.assertIn("high", args)
        self.assertIn("4.1", args)
        self.assertIn("yuv420p", args)

    def test_ffmpeg_apple_encoder_args_do_not_force_unsupported_4k_level(self) -> None:
        args = RUNNER_APP.ffmpeg_video_encoder_args("apple")
        self.assertIn("h264_videotoolbox", args)
        self.assertIn("yuv420p", args)
        self.assertNotIn("4.1", args)
        self.assertNotIn("high", args)

    def test_ffmpeg_audio_encoder_args_use_ac3(self) -> None:
        args = RUNNER_APP.ffmpeg_audio_encoder_args()
        self.assertEqual(args, ["-c:a", "ac3", "-b:a", "640k"])

    def test_ffmpeg_encoder_choices_hide_apple_on_linux(self) -> None:
        with patch.object(RUNNER_APP.sys, "platform", "linux"):
            self.assertEqual(RUNNER_APP.ffmpeg_encoder_choices_text(), "cpu/intel_qsv")

    def test_validate_ffmpeg_runtime_encoder_mode_rejects_unavailable_qsv(self) -> None:
        with patch.object(RUNNER_APP, "detect_intel_qsv_support", return_value=(False, "kein /dev/dri/renderD128 im Container")), \
             patch.object(RUNNER_APP, "detect_intel_vaapi_support", return_value=(False, "kein /dev/dri/renderD128 im Container")):
            ok, reason = RUNNER_APP.validate_ffmpeg_runtime_encoder_mode("intel_qsv")
        self.assertFalse(ok)
        self.assertIn("Intel Hardware-Encoding nicht verfügbar", reason)

    def test_resolve_ffmpeg_runtime_encoder_mode_uses_vaapi_when_qsv_fails(self) -> None:
        with patch.object(RUNNER_APP, "detect_intel_qsv_support", return_value=(False, "MFX session -9")), \
             patch.object(RUNNER_APP, "detect_intel_vaapi_support", return_value=(True, "")):
            ok, reason, mode = RUNNER_APP.resolve_ffmpeg_runtime_encoder_mode("intel_qsv")
        self.assertTrue(ok)
        self.assertEqual(mode, "intel_vaapi")
        self.assertIn("Nutze Intel VAAPI", reason)

    def test_force_target_rel_codec_uses_h264_for_ffmpeg_mode(self) -> None:
        rel = RUNNER_APP.force_target_rel_codec(
            "Movie/Foo (2026)/Foo.2026.1080p.h265.{tt1234567}.mkv",
            RUNNER_APP.FFMPEG_TARGET_VIDEO_CODEC,
        )
        self.assertIn(".h264.", rel)
        self.assertNotIn(".h265.", rel)

    def test_processing_log_writes_to_processing_log_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            processing_log = Path(tmpdir) / "processing_log.txt"
            with patch.object(RUNNER_APP, "LOGS_ENABLED", True):
                with patch.object(RUNNER_APP, "PROCESSING_LOG_FILE", processing_log):
                    RUNNER_APP.processing_log("[TEST] runner log")
            self.assertTrue(processing_log.exists())
            self.assertIn("[TEST] runner log", processing_log.read_text(encoding="utf-8"))

    def test_sync_out_tree_to_nas_suppresses_zero_progress_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            start_folder = root / "input"
            out_file = start_folder / "__OUT" / "Serien" / "Foo (2026)" / "S01" / "Foo.2026.S01.E01.h264.{tt1234567}.mkv"
            out_file.parent.mkdir(parents=True, exist_ok=True)
            out_file.write_bytes(b"x" * 1024)
            nas_root = root / "nas"
            progress: list[str] = []
            ok, stats = RUNNER_APP.sync_out_tree_to_nas(
                start_folder,
                nas_root,
                on_progress=progress.append,
                progress_interval_sec=0.0,
            )
            self.assertTrue(ok)
            self.assertTrue(progress)
            self.assertFalse(any("Speed = 0.0 MB/s" in line and "ETA = n/a" in line for line in progress))
            self.assertEqual(int(stats.get("failed", 0) or 0), 0)

    def test_verify_source_targets_exist_accepts_local_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            start_folder = root / "input"
            target_rel = "Serien/Foo (2026)/S01/Foo.2026.S01.E01.h264.{tt1234567}.mkv"
            target_file = start_folder / target_rel
            target_file.parent.mkdir(parents=True, exist_ok=True)
            target_file.write_bytes(b"ok")
            ok, missing, checked = RUNNER_APP.verify_source_targets_exist(
                [{"Quellname": "Foo.mkv", "Zielname": target_rel}],
                start_folder,
                root / "nas",
            )
            self.assertTrue(ok)
            self.assertEqual(missing, [])
            self.assertEqual(checked, 1)

    def test_verify_source_targets_exist_accepts_nas_mirror(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            start_folder = root / "input"
            nas_root = root / "nas"
            target_rel = "Serien/Foo (2026)/S01/Foo.2026.S01.E01.h264.{tt1234567}.mkv"
            nas_target = nas_root / "Serien" / "Foo (2026)" / "S01" / "Foo.2026.S01.E01.h264.{tt1234567}.mkv"
            nas_target.parent.mkdir(parents=True, exist_ok=True)
            nas_target.write_bytes(b"ok")
            ok, missing, checked = RUNNER_APP.verify_source_targets_exist(
                [{"Quellname": "Foo.mkv", "Zielname": target_rel}],
                start_folder,
                nas_root,
            )
            self.assertTrue(ok)
            self.assertEqual(missing, [])
            self.assertEqual(checked, 1)

    def test_api_state_does_not_reuse_last_runner_log_when_processing_log_is_empty_and_job_is_idle(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            runner_log = tmp / "runner.log"
            processing_log = tmp / "processing_log.txt"
            runner_log.write_text("stale runner log\n", encoding="utf-8")
            processing_log.write_text("", encoding="utf-8")

            fake_job = {
                "exists": True,
                "job_id": "last-run",
                "mode": "copy",
                "folder": "/tmp",
                "encoder": "cpu",
                "sync_nas": False,
                "sync_plex": False,
                "del_out": False,
                "del_source": False,
                "started_at": None,
                "ended_at": None,
                "running": False,
                "exit_code": 0,
                "log_path": str(runner_log),
                "release_version": "0.2.0",
            }

            with patch.object(WEB_APP, "PROCESSING_LOG_FILE", processing_log):
                with patch.object(WEB_APP, "current_job", None):
                    with patch.object(WEB_APP, "fallback_job_data", return_value=fake_job):
                        with patch.object(WEB_APP, "read_runtime_settings", return_value={}):
                            with patch.object(WEB_APP, "read_state_values", return_value={}):
                                with patch.object(WEB_APP, "read_pending_confirmation_payload", return_value=None):
                                    with WEB_APP.app.test_client() as client:
                                        response = client.get("/api/state")
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertEqual(payload["processing_log"], "")

    def test_api_state_exposes_initial_setup_gate_flags(self) -> None:
        with patch.object(WEB_APP, "current_job", None):
            with patch.object(WEB_APP, "fallback_job_data", return_value={"exists": False}):
                with patch.object(
                    WEB_APP,
                    "read_runtime_settings",
                    return_value={
                        "target_nfs_path": "/mnt/data",
                        "target_out_path": "__OUT",
                        "target_reenqueue_path": "__RE-ENQUEUE",
                        "nas_ip": "192.168.52.4",
                        "plex_ip": "192.168.52.5",
                        "initial_setup_done": "0",
                        "initial_setup_required": "1",
                    },
                ):
                    with patch.object(WEB_APP, "read_state_values", return_value={}):
                        with patch.object(WEB_APP, "read_pending_confirmation_payload", return_value=None):
                            with WEB_APP.app.test_client() as client:
                                response = client.get("/api/state")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        self.assertFalse(payload["settings"]["initial_setup_done"])
        self.assertTrue(payload["settings"]["initial_setup_required"])

    def test_effective_encode_speed_blends_ffmpeg_and_live_progress(self) -> None:
        speed = RUNNER_APP.effective_encode_speed(
            speed_val=4.47,
            progress_sec=2688.0,
            started_ts=100.0,
            now_ts=700.0,
        )
        self.assertGreater(speed, 4.47)
        self.assertLess(speed, 4.48)

    def test_skip_encode_for_h265_uses_filename_token(self) -> None:
        with patch.object(RUNNER_APP, "SKIP_H265_ENCODE_ENABLED", True):
            self.assertTrue(
                RUNNER_APP.should_skip_encode_for_h265(
                    Path("/tmp/Reacher.2022.S01.E01.2160p.h265.mkv"),
                    "Reacher.2022.S01.E01.2160p.h265.mkv",
                )
            )

    def test_skip_encode_for_h265_uses_probe_when_filename_is_ambiguous(self) -> None:
        with patch.object(RUNNER_APP, "SKIP_H265_ENCODE_ENABLED", True):
            with patch.object(RUNNER_APP, "probe_codec_from_video", return_value="h265") as probe_mock:
                self.assertTrue(
                    RUNNER_APP.should_skip_encode_for_h265(
                        Path("/tmp/source.mkv"),
                        "source.mkv",
                    )
                )
                probe_mock.assert_called_once()

    def test_skip_encode_for_4k_h265_uses_filename_tokens(self) -> None:
        with patch.object(RUNNER_APP, "SKIP_4K_H265_ENCODE_ENABLED", True):
            self.assertTrue(
                RUNNER_APP.should_skip_encode_for_4k_h265(
                    Path("/tmp/Reacher.2022.S01.E01.2160p.h265.mkv"),
                    "Reacher.2022.S01.E01.2160p.h265.mkv",
                )
            )

    def test_skip_encode_for_4k_h265_uses_probe_when_filename_is_ambiguous(self) -> None:
        with patch.object(RUNNER_APP, "SKIP_4K_H265_ENCODE_ENABLED", True):
            with patch.object(RUNNER_APP, "probe_resolution_label", return_value="4k") as resolution_mock:
                with patch.object(RUNNER_APP, "probe_codec_from_video", return_value="h265") as codec_mock:
                    self.assertTrue(
                        RUNNER_APP.should_skip_encode_for_4k_h265(
                            Path("/tmp/source.mkv"),
                            "source.mkv",
                        )
                    )
                    resolution_mock.assert_called_once()
                    codec_mock.assert_called_once()

    def test_skip_encode_for_4k_h265_rejects_non_4k_source(self) -> None:
        with patch.object(RUNNER_APP, "SKIP_4K_H265_ENCODE_ENABLED", True):
            self.assertFalse(
                RUNNER_APP.should_skip_encode_for_4k_h265(
                    Path("/tmp/Reacher.2022.S01.E01.1080p.h265.mkv"),
                    "Reacher.2022.S01.E01.1080p.h265.mkv",
                )
            )

    def test_preferred_copy_codec_prefers_source_name_before_target_probe(self) -> None:
        codec = RUNNER_APP.preferred_copy_codec(
            Path("/tmp/Reacher.2022.S01.E01.2160p.h265.mkv"),
            Path("/tmp/target.codec.mkv"),
        )
        self.assertEqual(codec, "h265")

    def test_preferred_copy_codec_probes_source_before_target(self) -> None:
        with patch.object(RUNNER_APP, "probe_codec_from_video", side_effect=["h264", "h265"]) as probe_mock:
            codec = RUNNER_APP.preferred_copy_codec(
                Path("/tmp/source.mkv"),
                Path("/tmp/target.codec.mkv"),
            )
        self.assertEqual(codec, "h264")
        self.assertEqual(probe_mock.call_count, 1)

    def test_native_copy_is_enabled_on_linux_for_regular_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source = root / "source.mkv"
            target = root / "out" / "target.mkv"
            target.parent.mkdir(parents=True, exist_ok=True)
            source.write_bytes(b"x")
            with patch.object(RUNNER_APP.sys, "platform", "linux"):
                self.assertTrue(RUNNER_APP.should_use_native_copy(source, target))


if __name__ == "__main__":
    unittest.main()
