#!/usr/bin/env python3
from __future__ import annotations
"""
macOS helper script to pick a folder in Finder and save its directory tree
into a temp file. Designed to run inside a virtual environment; will warn
if not.

Version: 0.2.31
"""
import argparse
import csv
import hashlib
import io
import json
import os
import plistlib
import queue
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Callable

from mmcore.db_cache import GeminiDbStore

VERSION = "0.2.31"
SCRIPT_NAME = f"managemovie_v{VERSION}.py"
TMDB_ENABLED = (os.environ.get("MANAGEMOVIE_TMDB_ENABLED", "1") or "1").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}
REQUIRED_PACKAGES = ["pymysql"]
LOGS_ENABLED = True
SCRIPT_DIR = Path(__file__).resolve().parent
TARGET_DIR = Path(os.environ.get("MANAGEMOVIE_WORKDIR", "WORK")).expanduser()
if not TARGET_DIR.is_absolute():
    TARGET_DIR = (SCRIPT_DIR / TARGET_DIR).resolve()
TMP_DIR = TARGET_DIR / "tmp"


def resolve_binary(binary_name: str, env_name: str = "") -> str:
    override = (os.environ.get(env_name, "") or "").strip() if env_name else ""
    if override:
        candidate = Path(override).expanduser()
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)

    found = shutil.which(binary_name)
    if found:
        return found

    candidates = [
        Path("/opt/homebrew/bin") / binary_name,
        Path("/usr/local/bin") / binary_name,
        Path("/opt/local/bin") / binary_name,
        Path("/usr/bin") / binary_name,
    ]
    for candidate in candidates:
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    return binary_name


FFMPEG_BIN = resolve_binary("ffmpeg", "MANAGEMOVIE_FFMPEG_BIN")
FFPROBE_BIN = resolve_binary("ffprobe", "MANAGEMOVIE_FFPROBE_BIN")
QSV_RUNTIME_STATUS_CACHE: tuple[bool, str] | None = None
VAAPI_RUNTIME_STATUS_CACHE: tuple[bool, str] | None = None


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = (os.environ.get(name, str(default)) or str(default)).strip()
    try:
        value = int(raw)
    except Exception:
        return default
    if value < minimum:
        return minimum
    if value > maximum:
        return maximum
    return value


def normalize_copy_fsync_mode(value: str) -> str:
    raw = (value or "").strip().lower()
    if raw in {"always", "on", "1", "true", "yes", "y"}:
        return "always"
    if raw in {"never", "off", "0", "false", "no", "n"}:
        return "never"
    return "auto"


def detect_host_memory_gib() -> float:
    try:
        if sys.platform.startswith("linux"):
            meminfo = Path("/proc/meminfo")
            if meminfo.exists():
                for line in meminfo.read_text(encoding="utf-8", errors="ignore").splitlines():
                    if not line.startswith("MemTotal:"):
                        continue
                    parts = line.split()
                    if len(parts) >= 2:
                        kib = float(parts[1])
                        if kib > 0:
                            return kib / 1024.0 / 1024.0
        if sys.platform == "darwin":
            result = subprocess.run(
                ["sysctl", "-n", "hw.memsize"],
                capture_output=True,
                text=True,
                check=False,
            )
            if result.returncode == 0:
                raw = (result.stdout or "").strip()
                if raw:
                    mem_bytes = float(raw)
                    if mem_bytes > 0:
                        return mem_bytes / 1024.0 / 1024.0 / 1024.0
    except Exception:
        return 0.0
    return 0.0


HOST_CPU_COUNT = max(1, int(os.cpu_count() or 1))
HOST_MEMORY_GIB = detect_host_memory_gib()


def default_copy_chunk_mib() -> int:
    if HOST_MEMORY_GIB >= 20 and HOST_CPU_COUNT >= 12:
        return 96
    if HOST_MEMORY_GIB >= 8 and HOST_CPU_COUNT >= 8:
        return 64
    return 32


def parse_copy_chunk_mib(value: str) -> int:
    raw = (value or "").strip()
    if not raw:
        return default_copy_chunk_mib()
    try:
        num = int(raw)
    except Exception:
        return default_copy_chunk_mib()
    if num < 1:
        return 1
    if num > 128:
        return 128
    return num


def parse_ffmpeg_threads(value: str) -> int:
    raw = (value or "").strip().lower()
    if not raw or raw == "auto":
        if HOST_CPU_COUNT <= 2:
            return 1
        if HOST_CPU_COUNT <= 4:
            return max(1, HOST_CPU_COUNT - 1)
        return max(2, min(16, HOST_CPU_COUNT - 1))
    try:
        num = int(raw)
    except Exception:
        return parse_ffmpeg_threads("auto")
    if num < 1:
        return 1
    if num > 32:
        return 32
    return num


COPY_CHUNK_MIB = parse_copy_chunk_mib(os.environ.get("MANAGEMOVIE_COPY_CHUNK_MIB", ""))
COPY_CHUNK_SIZE_BYTES = COPY_CHUNK_MIB * 1024 * 1024
COPY_FSYNC_MODE = normalize_copy_fsync_mode(os.environ.get("MANAGEMOVIE_COPY_FSYNC", ""))
FFMPEG_THREADS = parse_ffmpeg_threads(os.environ.get("MANAGEMOVIE_FFMPEG_THREADS", "auto"))
FFMPEG_TARGET_VIDEO_CODEC = "h264"
FFMPEG_TARGET_AUDIO_CODEC = "ac3"
FFMPEG_PROGRESS_LOG_INTERVAL_SEC = float(env_int("MANAGEMOVIE_FFMPEG_LOG_INTERVAL_SEC", 60, 15, 600))
COPY_FS_TYPE_CACHE: dict[str, str] = {}
COPY_SETTINGS_REPORTED = False
NAS_MOUNT_LAST_ATTEMPT_TS: dict[str, float] = {}
NAS_MOUNT_RETRY_INTERVAL_SEC = 12.0
RUNTIME_PROBE_MODE = (os.environ.get("MANAGEMOVIE_RUNTIME_PROBE", "auto") or "auto").strip().lower()
if RUNTIME_PROBE_MODE not in {"auto", "always", "never"}:
    RUNTIME_PROBE_MODE = "auto"
RUNTIME_PROBE_REPORTED = False
POST_ACTION_SUMMARY_LINES: list[str] = []
REENQUEUE_DIR_NAME = "__RE-ENQUEUE"
TMDB_CACHE_RETENTION_DAYS = 365
TMDB_CACHE_STATE_PREFIX_LEGACY = "tmdb.cache.v1."
TMDB_CACHE_PROMPT_PREFIX = "tmdb.cache.v2."
TMDB_LANGUAGE = "de-DE"
TMDB_TITLE_STOPWORDS = {
    "a",
    "an",
    "and",
    "das",
    "de",
    "der",
    "des",
    "die",
    "ein",
    "eine",
    "el",
    "en",
    "for",
    "im",
    "in",
    "la",
    "las",
    "le",
    "les",
    "los",
    "of",
    "the",
    "to",
    "um",
    "und",
    "von",
    "vom",
    "zu",
    "zum",
    "zur",
}


def env_flag(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


SYNC_NAS_ENABLED = env_flag("MANAGEMOVIE_SYNC_NAS", False)
SYNC_PLEX_ENABLED = env_flag("MANAGEMOVIE_SYNC_PLEX", False)
DEL_OUT_ENABLED = env_flag("MANAGEMOVIE_DEL_OUT", False)
DEL_SOURCE_ENABLED = env_flag("MANAGEMOVIE_DEL_SOURCE", False)
DEL_SOURCE_CONFIRMED = env_flag("MANAGEMOVIE_DEL_SOURCE_CONFIRMED", False)
ANALYZE_RUNTIME_PROBE = env_flag("MANAGEMOVIE_ANALYZE_RUNTIME_PROBE", False)
AI_QUERY_DISABLED = env_flag("MANAGEMOVIE_DISABLE_AI_QUERY", True)
SKIP_H265_ENCODE_ENABLED = env_flag("MANAGEMOVIE_SKIP_H265_ENCODE", False)
SKIP_4K_H265_ENCODE_ENABLED = env_flag("MANAGEMOVIE_SKIP_4K_H265_ENCODE", False)
PRECHECK_EGB_ENABLED = env_flag("MANAGEMOVIE_PRECHECK_EGB", True)
# Keep Terminal windows opt-in; default is pure web operation.
TERMINAL_UI_ENABLED = env_flag("MANAGEMOVIE_TERMINAL_UI", False)
TARGET_NFS_PATH_RAW = (os.environ.get("MANAGEMOVIE_TARGET_NFS_PATH", "") or "").strip() or "/Volumes/Data/Movie/"
TARGET_NFS_PATH = Path(TARGET_NFS_PATH_RAW).expanduser()
TARGET_OUT_PATH_RAW = (os.environ.get("MANAGEMOVIE_TARGET_OUT_PATH", "") or "").strip() or "__OUT"
TARGET_OUT_PATH = Path(TARGET_OUT_PATH_RAW).expanduser()
TARGET_REENQUEUE_PATH_RAW = (
    os.environ.get("MANAGEMOVIE_TARGET_REENQUEUE_PATH", "") or ""
).strip()
TARGET_REENQUEUE_PATH = Path(TARGET_REENQUEUE_PATH_RAW).expanduser() if TARGET_REENQUEUE_PATH_RAW else Path(REENQUEUE_DIR_NAME)
NAS_IP_RAW = (os.environ.get("MANAGEMOVIE_NAS_IP", "") or "").strip()
PLEX_IP_RAW = (os.environ.get("MANAGEMOVIE_PLEX_IP", "") or "").strip()
PLEX_TOKEN_RAW = (os.environ.get("MANAGEMOVIE_PLEX_API", "") or "").strip()


def resolve_secret_file() -> Path:
    env_secret = os.environ.get("MANAGEMOVIE_SECRET_FILE", "").strip()
    if env_secret:
        candidate = Path(env_secret).expanduser()
        return candidate if candidate.is_absolute() else (TARGET_DIR / candidate).resolve()

    local_secret = TARGET_DIR / "IMDB.secret"
    if local_secret.exists():
        return local_secret

    # Optional fallback for existing setups that keep IMDB.secret in iCloud.
    icloud_secret = Path.home() / "Library" / "Mobile Documents" / "com~apple~CloudDocs" / "IMDB.secret"
    if icloud_secret.exists():
        return icloud_secret

    return local_secret


SECRET_FILE = resolve_secret_file()
GEMINI_LAST_MODEL_FILE = TARGET_DIR / "gemini_last_model.txt"
GEMINI_HITS_CACHE_FILE = TARGET_DIR / "gemini_hits_cache.json"
GEMINI_SOURCE_ROW_CACHE_PREFIX = "gemini.source.row."
EDITOR_SOURCE_ROW_CACHE_PREFIX = "editor.source.row."
EDITOR_SOURCE_ROW_RETENTION_DAYS = 365
PROCESSED_SOURCE_ROW_CACHE_PREFIX = "processed.source.row."
PROCESSED_SOURCE_ROW_RETENTION_DAYS = 365
STATUS_TABLE_FILE = TARGET_DIR / "gemini-status-table.txt"
OUT_PLAN_FILE = TARGET_DIR / "out_plan.txt"
OUT_TREE_FILE = TARGET_DIR / "out_tree.txt"
PROCESSING_LOG_FILE = TARGET_DIR / "processing_log.txt"
OUT_TREE_DONE_FILE = TARGET_DIR / "out_tree.done"
STATUS_DONE_FILE = TARGET_DIR / "status.done"
LEGACY_GEMINI_TREE_FILE = TARGET_DIR / "gemini-tree.txt"
FFMPEG_ENCODER_PREF_FILE = TARGET_DIR / "ffmpeg_encoder_mode.txt"
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
MIRROR_LOG_TO_PROCESSING = False
LOG_TO_STDOUT = (os.environ.get("MANAGEMOVIE_LOG_TO_STDOUT", "") or "").strip().lower() in {
    "1",
    "true",
    "yes",
    "y",
    "on",
}
SCRIPT_START_TS = time.time()
GEMINI_QUOTA_BLOCKED = False
MIN_VALID_ISO_MKV_BYTES = 200 * 1024 * 1024
EST_SAMPLE_SECONDS = 45.0
EST_SAMPLE_RATIOS = (0.05, 0.20, 0.35, 0.55, 0.75, 0.95)
EST_VIDEO_OVERHEAD_FACTOR = 1.015
CSV_HEADERS = [
    "Quellname",
    "Name des Film/Serie",
    "Erscheinungsjahr",
    "Staffel",
    "Episode",
    "Laufzeit",
    "IMDB-ID",
]
MODEL_FALLBACKS = ["gemini-2.0-flash", "gemini-2.0-flash-lite", "gemini-1.5-flash"]
VIDEO_EXTENSIONS = {
    ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm",
    ".m4v", ".mpg", ".mpeg", ".ts", ".mts", ".m2ts", ".3gp",
}
ISO_EXTENSIONS = {".iso"}
SUBTITLE_EXTENSIONS = {".srt", ".ass", ".ssa", ".vtt", ".sub", ".idx"}
MANAGEMOVIE_TRACK_FILE_NAME = ".managemovie.txt"
MANAGEMOVIE_TRACK_FILE_ALIASES = (".managemovie.txt", ".managamovie.txt")
MANAGEMOVIE_VIDEO_MANIFEST_SUFFIX = ".managemovie.txt"
MANUAL_DIR_NAME = "__MANUAL"
SERIES_METADATA: dict[str, dict[str, Any]] = {}
MARIADB_HOST = (os.environ.get("MANAGEMOVIE_DB_HOST", "127.0.0.1") or "127.0.0.1").strip()
MARIADB_PORT = env_int("MANAGEMOVIE_DB_PORT", 3306, 1, 65535)
MARIADB_DB = (os.environ.get("MANAGEMOVIE_DB_NAME", "managemovie") or "managemovie").strip()
MARIADB_USER = (os.environ.get("MANAGEMOVIE_DB_USER", "managemovie") or "managemovie").strip()
MARIADB_PASSWORD = os.environ.get("MANAGEMOVIE_DB_PASSWORD", "")
MARIADB_CACHE_RETENTION_DAYS = env_int("MANAGEMOVIE_DB_RETENTION_DAYS", 365, 1, 3650)
MARIADB_CONNECT_TIMEOUT_SEC = env_int("MANAGEMOVIE_DB_CONNECT_TIMEOUT_SEC", 8, 1, 120)
MARIADB_READ_TIMEOUT_SEC = env_int("MANAGEMOVIE_DB_READ_TIMEOUT_SEC", 20, 1, 300)
MARIADB_WRITE_TIMEOUT_SEC = env_int("MANAGEMOVIE_DB_WRITE_TIMEOUT_SEC", 20, 1, 300)
GEMINI_DB_STORE = GeminiDbStore(
    host=MARIADB_HOST,
    port=MARIADB_PORT,
    database=MARIADB_DB,
    user=MARIADB_USER,
    password=MARIADB_PASSWORD,
    retention_days=MARIADB_CACHE_RETENTION_DAYS,
    connect_timeout_sec=MARIADB_CONNECT_TIMEOUT_SEC,
    read_timeout_sec=MARIADB_READ_TIMEOUT_SEC,
    write_timeout_sec=MARIADB_WRITE_TIMEOUT_SEC,
)


def configure_local_tmpdir() -> None:
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = str(TMP_DIR)
    os.environ["TMPDIR"] = tmp_path
    os.environ["TMP"] = tmp_path
    os.environ["TEMP"] = tmp_path
    tempfile.tempdir = tmp_path


def format_path(path: Path) -> str:
    try:
        return str(path.relative_to(TARGET_DIR))
    except ValueError:
        return str(path)


def normalize_ffmpeg_encoder_mode(value: str) -> str:
    raw = (value or "").strip().lower()
    if raw in {"cpu", "cpi", "software", "sw", "x265", "libx265"}:
        return "cpu"
    if raw in {"intel", "intel_qsv", "qsv", "hevc_qsv", "quicksync", "quick sync"}:
        return "intel_qsv"
    if raw in {"apple", "videotoolbox", "vt", "hevc_videotoolbox"}:
        return "apple"
    if raw in {"hardware", "hw", "gpu"}:
        return "apple" if sys.platform == "darwin" else "intel_qsv"
    return ""


def read_ffmpeg_encoder_default() -> str:
    try:
        if FFMPEG_ENCODER_PREF_FILE.exists():
            stored = normalize_ffmpeg_encoder_mode(FFMPEG_ENCODER_PREF_FILE.read_text(encoding="utf-8"))
            if stored:
                return stored
    except Exception:
        pass
    return "cpu"


def write_ffmpeg_encoder_default(mode: str) -> None:
    normalized = normalize_ffmpeg_encoder_mode(mode)
    if not normalized:
        return
    try:
        TARGET_DIR.mkdir(parents=True, exist_ok=True)
        FFMPEG_ENCODER_PREF_FILE.write_text(normalized + "\n", encoding="utf-8")
    except Exception:
        pass


def available_ffmpeg_encoder_modes() -> list[str]:
    modes = ["cpu", "intel_qsv"]
    if sys.platform == "darwin":
        modes.append("apple")
    return modes


def ffmpeg_encoder_choices_text() -> str:
    return "/".join(available_ffmpeg_encoder_modes())


def ffmpeg_video_encoder_args(encoder_mode: str) -> list[str]:
    mode = normalize_ffmpeg_encoder_mode(encoder_mode) or "cpu"
    if mode == "intel_qsv":
        return [
            "-c:v",
            "h264_qsv",
            "-preset",
            "medium",
            "-global_quality",
            "23",
            "-profile:v",
            "high",
            "-level",
            "41",
            "-pix_fmt",
            "yuv420p",
        ]
    if mode == "intel_vaapi":
        return [
            "-vaapi_device",
            "/dev/dri/renderD128",
            "-vf",
            "format=nv12,hwupload",
            "-c:v",
            "h264_vaapi",
            "-qp",
            "23",
            "-profile:v",
            "high",
            "-level",
            "41",
        ]
    if mode == "apple":
        return [
            "-c:v",
            "h264_videotoolbox",
            "-realtime",
            "0",
            "-pix_fmt",
            "yuv420p",
        ]
    return [
        "-threads",
        str(FFMPEG_THREADS),
        "-c:v",
        "libx264",
        "-preset",
        "medium",
        "-crf",
        "20",
        "-profile:v",
        "high",
        "-level:v",
        "4.1",
        "-pix_fmt",
        "yuv420p",
    ]


def detect_intel_qsv_support() -> tuple[bool, str]:
    global QSV_RUNTIME_STATUS_CACHE
    if QSV_RUNTIME_STATUS_CACHE is not None:
        return QSV_RUNTIME_STATUS_CACHE

    dri_dir = Path("/dev/dri")
    render_node = dri_dir / "renderD128"
    if not dri_dir.exists() or not render_node.exists():
        QSV_RUNTIME_STATUS_CACHE = (False, "kein /dev/dri/renderD128 im Container")
        return QSV_RUNTIME_STATUS_CACHE

    try:
        result = subprocess.run(
            [
                FFMPEG_BIN,
                "-hide_banner",
                "-v",
                "error",
                "-init_hw_device",
                "qsv=hw",
                "-f",
                "lavfi",
                "-i",
                "color=size=16x16:rate=1",
                "-frames:v",
                "1",
                "-c:v",
                "h264_qsv",
                "-f",
                "null",
                "-",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        QSV_RUNTIME_STATUS_CACHE = (False, "ffmpeg fehlt im PATH")
        return QSV_RUNTIME_STATUS_CACHE
    except Exception as exc:
        QSV_RUNTIME_STATUS_CACHE = (False, f"QSV-Selbsttest fehlgeschlagen: {exc}")
        return QSV_RUNTIME_STATUS_CACHE

    if result.returncode == 0:
        QSV_RUNTIME_STATUS_CACHE = (True, "")
        return QSV_RUNTIME_STATUS_CACHE

    details = (result.stderr or result.stdout or "").strip().splitlines()
    summary = details[-1].strip() if details else f"Exit-Code {result.returncode}"
    QSV_RUNTIME_STATUS_CACHE = (False, summary)
    return QSV_RUNTIME_STATUS_CACHE


def detect_intel_vaapi_support() -> tuple[bool, str]:
    global VAAPI_RUNTIME_STATUS_CACHE
    if VAAPI_RUNTIME_STATUS_CACHE is not None:
        return VAAPI_RUNTIME_STATUS_CACHE

    dri_dir = Path("/dev/dri")
    render_node = dri_dir / "renderD128"
    if not dri_dir.exists() or not render_node.exists():
        VAAPI_RUNTIME_STATUS_CACHE = (False, "kein /dev/dri/renderD128 im Container")
        return VAAPI_RUNTIME_STATUS_CACHE

    env = os.environ.copy()
    env.setdefault("LIBVA_DRIVER_NAME", "iHD")
    try:
        result = subprocess.run(
            [
                FFMPEG_BIN,
                "-hide_banner",
                "-v",
                "error",
                "-vaapi_device",
                str(render_node),
                "-f",
                "lavfi",
                "-i",
                "testsrc2=size=128x72:rate=30",
                "-frames:v",
                "1",
                "-vf",
                "format=nv12,hwupload",
                "-c:v",
                "h264_vaapi",
                "-f",
                "null",
                "-",
            ],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
    except FileNotFoundError:
        VAAPI_RUNTIME_STATUS_CACHE = (False, "ffmpeg fehlt im PATH")
        return VAAPI_RUNTIME_STATUS_CACHE
    except Exception as exc:
        VAAPI_RUNTIME_STATUS_CACHE = (False, f"VAAPI-Selbsttest fehlgeschlagen: {exc}")
        return VAAPI_RUNTIME_STATUS_CACHE

    if result.returncode == 0:
        VAAPI_RUNTIME_STATUS_CACHE = (True, "")
        return VAAPI_RUNTIME_STATUS_CACHE

    details = (result.stderr or result.stdout or "").strip().splitlines()
    summary = details[-1].strip() if details else f"Exit-Code {result.returncode}"
    VAAPI_RUNTIME_STATUS_CACHE = (False, summary)
    return VAAPI_RUNTIME_STATUS_CACHE


def resolve_ffmpeg_runtime_encoder_mode(requested_mode: str) -> tuple[bool, str, str]:
    mode_norm = normalize_ffmpeg_encoder_mode(requested_mode) or "cpu"
    if mode_norm == "intel_qsv":
        ok_qsv, reason_qsv = detect_intel_qsv_support()
        if ok_qsv:
            return True, "", "intel_qsv"
        ok_vaapi, reason_vaapi = detect_intel_vaapi_support()
        if ok_vaapi:
            return True, f"Intel Quick Sync nicht verfügbar ({reason_qsv}). Nutze Intel VAAPI.", "intel_vaapi"
        return False, f"Intel Hardware-Encoding nicht verfügbar. QSV: {reason_qsv} | VAAPI: {reason_vaapi}", mode_norm
    if mode_norm == "apple" and sys.platform != "darwin":
        return False, "Apple VideoToolbox ist auf diesem Host nicht verfügbar", mode_norm
    return True, "", mode_norm


def validate_ffmpeg_runtime_encoder_mode(requested_mode: str) -> tuple[bool, str]:
    ok, reason, _effective_mode = resolve_ffmpeg_runtime_encoder_mode(requested_mode)
    return ok, reason


def ffmpeg_apple_rate_control_args(source_video: Path, duration_sec: float, q_gb: float) -> list[str]:
    resolution = probe_resolution_label(source_video)
    # Conservative defaults for AppleTV/Plex compatible h264 output.
    target_kbps = 5200.0
    min_kbps = 2600.0
    if resolution == "4k":
        target_kbps = 9000.0
        min_kbps = 5200.0
    elif resolution == "720p":
        target_kbps = 3200.0
        min_kbps = 1800.0
    elif resolution == "480p":
        target_kbps = 2200.0
        min_kbps = 1200.0

    source_total_kbps = 0.0
    if q_gb > 0 and duration_sec > 0:
        source_total_kbps = (q_gb * 1024.0 * 1024.0 * 8.0) / max(1.0, duration_sec)

    if source_total_kbps > 0:
        # Keep compression below source bitrate while preserving a floor for quality.
        capped = min(target_kbps, source_total_kbps * 0.78)
        target_kbps = max(min_kbps, capped)
        if source_total_kbps > 1800.0:
            target_kbps = min(target_kbps, source_total_kbps * 0.92)

    maxrate_kbps = max(target_kbps + 300.0, target_kbps * 1.30)
    bufsize_kbps = max(maxrate_kbps * 2.0, target_kbps * 2.60)
    return [
        "-b:v",
        f"{int(round(target_kbps))}k",
        "-maxrate",
        f"{int(round(maxrate_kbps))}k",
        "-bufsize",
        f"{int(round(bufsize_kbps))}k",
    ]


def ffmpeg_audio_encoder_args() -> list[str]:
    return [
        "-c:a",
        FFMPEG_TARGET_AUDIO_CODEC,
        "-b:a",
        "640k",
    ]


def log(level: str, message: str) -> None:
    message = normalize_display_umlauts(message)
    if not LOGS_ENABLED:
        if level == "ERROR":
            print(f"[ERROR] {message}", file=sys.stderr)
        return
    line = f"[{level}] {message}"
    if LOG_TO_STDOUT:
        print(line)
    if MIRROR_LOG_TO_PROCESSING:
        try:
            with PROCESSING_LOG_FILE.open("a", encoding="utf-8") as handle:
                ts = time.strftime("%H:%M:%S")
                handle.write(f"[{ts}] {line}\n")
        except Exception:
            pass


def overwrite_text_file(path: Path, text: str, *, encoding: str = "utf-8") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(text, encoding=encoding)
        return path
    except PermissionError:
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        path.write_text(text, encoding=encoding)
        return path


def is_web_ui_only_mode() -> bool:
    truthy = {"1", "true", "yes", "y"}
    web_ui_only = (os.environ.get("MANAGEMOVIE_WEB_UI_ONLY", "") or "").strip().lower()
    autostart = (os.environ.get("MANAGEMOVIE_AUTOSTART", "") or "").strip().lower()
    web_confirm_file = (os.environ.get("MANAGEMOVIE_WEB_CONFIRM_FILE", "") or "").strip()
    return web_ui_only in truthy or autostart in truthy or bool(web_confirm_file)

def pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def terminate_old_managemovie_processes() -> None:
    current_pid = os.getpid()
    script_name_re = re.compile(r"^managemovie(?:_v\d+\.\d+\.\d+)?\.py$", re.IGNORECASE)
    script_dir_resolved = SCRIPT_DIR.resolve()
    try:
        result = subprocess.run(
            ["ps", "-axo", "pid=,args="],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        return

    pids: list[int] = []
    for raw in (result.stdout or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if len(parts) < 2:
            continue
        try:
            pid = int(parts[0])
        except Exception:
            continue
        if pid == current_pid:
            continue
        cmdline = parts[1]
        try:
            argv = shlex.split(cmdline)
        except Exception:
            continue
        if not argv:
            continue

        exe_name = Path(argv[0]).name.lower()
        if "python" not in exe_name:
            continue

        script_token = ""
        seen_exec_mode = False
        for idx, tok in enumerate(argv[1:], start=1):
            if tok in {"-c", "-m"}:
                seen_exec_mode = True
                break
            if tok.startswith("-"):
                continue
            cleaned = tok.strip("'\"")
            if cleaned.lower().endswith(".py"):
                script_token = cleaned
                break
            if idx == 1:
                break

        if seen_exec_mode or not script_token:
            continue

        script_basename = Path(script_token).name
        if not script_name_re.match(script_basename):
            continue

        script_path = Path(script_token)
        if script_path.is_absolute():
            try:
                resolved_script = script_path.resolve()
            except Exception:
                continue
            if resolved_script.parent != script_dir_resolved:
                continue

        pids.append(pid)

    targets = sorted(set(pids))
    if not targets:
        return

    for sig, wait_sec in ((15, 2.0), (9, 1.0)):
        alive_before = [pid for pid in targets if pid_is_alive(pid)]
        if not alive_before:
            break
        for pid in alive_before:
            try:
                os.kill(pid, sig)
            except Exception:
                pass
        deadline = time.time() + wait_sec
        while time.time() < deadline:
            if not any(pid_is_alive(pid) for pid in alive_before):
                break
            time.sleep(0.1)


def processing_log(message: str) -> None:
    message = normalize_display_umlauts(message)
    if not LOGS_ENABLED:
        return
    ts = time.strftime("%H:%M:%S")
    try:
        PROCESSING_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with PROCESSING_LOG_FILE.open("a", encoding="utf-8") as handle:
            handle.write(f"[{ts}] {message}\n")
    except Exception:
        # Logging must never crash the processing pipeline.
        pass


def normalize_display_umlauts(value: str) -> str:
    text = str(value or "")
    replacements = (
        ("Bestaetig", "Bestätig"),
        ("bestaetig", "bestätig"),
        ("Pruef", "Prüf"),
        ("pruef", "prüf"),
        ("Uebers", "Übers"),
        ("uebers", "übers"),
        ("Ueber", "Über"),
        ("ueber", "über"),
        ("Zurueck", "Zurück"),
        ("zurueck", "zurück"),
        ("Geloesch", "Gelösch"),
        ("geloesch", "gelösch"),
        ("Koenn", "Könn"),
        ("koenn", "könn"),
        ("Aender", "Änder"),
        ("aender", "änder"),
        ("Waehr", "Währ"),
        ("waehr", "währ"),
        ("Laeuft", "Läuft"),
        ("laeuft", "läuft"),
        ("Oeffn", "Öffn"),
        ("oeffn", "öffn"),
        ("Fuer", "Für"),
        ("fuer", "für"),
        ("Eintraege", "Einträge"),
        ("eintraege", "einträge"),
        ("Loes", "Lös"),
        ("loes", "lös"),
        ("ausfuehr", "ausführ"),
        ("Ausfuehr", "Ausführ"),
        ("unveraendert", "unverändert"),
        ("Unveraendert", "Unverändert"),
    )
    for src, dst in replacements:
        text = text.replace(src, dst)
    return text


def elapsed_hhmm() -> str:
    seconds = max(0, int(time.time() - SCRIPT_START_TS))
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def format_hh_mm_ss(seconds: float) -> str:
    total = max(0, int(round(seconds)))
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def banner_lines() -> list[str]:
    border = "####################################"
    title = f"ManageMovie  {VERSION}"
    middle = f"#   {title:<31}#"
    return [border, middle, border]


def in_venv() -> bool:
    return sys.prefix != sys.base_prefix


def ensure_dependencies() -> None:
    if not REQUIRED_PACKAGES:
        log("INFO", "Keine externen Pakete erforderlich.")
        return

    missing = []
    for pkg in REQUIRED_PACKAGES:
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)

    if not missing:
        log("INFO", "Alle benötigten Pakete vorhanden.")
        return

    pip_cmd = [sys.executable, "-m", "pip", "install"]
    if not in_venv():
        pip_cmd.append("--user")
    pip_cmd.extend(missing)

    result = subprocess.run(pip_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"pip install fehlgeschlagen: {result.stderr.strip() or result.stdout.strip()}")
    log("OK", "Pakete installiert.")


def ensure_venv() -> None:
    if not in_venv():
        log("INFO", "Kein venv aktiv. Nutze System-/User-Site für Abhängigkeiten.")


def read_secret_keys(secret_file: Path) -> tuple[str, str]:
    def read_state_secret(key: str) -> str:
        try:
            raw = (GEMINI_DB_STORE.read_state(key) or "").strip()
        except Exception:
            return ""
        if not raw:
            return ""
        try:
            from mmcore.secret_store import decrypt_state_value

            value = (decrypt_state_value(key, raw) or "").strip()
            if value:
                return value
        except Exception:
            pass
        if raw.startswith("enc:v1:"):
            return ""
        return raw

    gemini_key = (os.environ.get("MANAGEMOVIE_GEMINI_KEY", "") or "").strip()
    tmdb_key = (os.environ.get("MANAGEMOVIE_TMDB_KEY", "") or "").strip()
    if not gemini_key:
        gemini_key = read_state_secret("settings.gemini_api")
    if not tmdb_key:
        tmdb_key = read_state_secret("settings.tmdb_api")

    if not gemini_key and not AI_QUERY_DISABLED:
        log(
            "WARN",
            "KI-Key fehlt: settings.gemini_api ist leer "
            "und MANAGEMOVIE_GEMINI_KEY ist nicht gesetzt.",
        )
    if TMDB_ENABLED and not tmdb_key:
        log("WARN", "TMDB aktiv, aber kein TMDB-Key gefunden (settings.tmdb_api / MANAGEMOVIE_TMDB_KEY).")
    return gemini_key, tmdb_key


def fetch_json(url: str) -> dict:
    req = urllib.request.Request(url, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_text(url: str, headers: dict[str, str] | None = None, timeout: int = 30) -> str:
    req_headers = {"User-Agent": "ManageMovie/1.0", "Accept": "*/*"}
    if headers:
        req_headers.update(headers)
    req = urllib.request.Request(url, headers=req_headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return resp.read().decode(charset, errors="replace")


def post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def post_json_with_heartbeat(url: str, payload: dict, heartbeat_label: str, heartbeat_sec: int = 10) -> dict:
    result_q: queue.Queue = queue.Queue(maxsize=1)

    def worker() -> None:
        try:
            data = post_json(url, payload)
            result_q.put(("ok", data))
        except Exception as exc:
            result_q.put(("err", exc))

    thread = threading.Thread(target=worker, daemon=True, name="gemini-request")
    thread.start()

    waited = 0
    while thread.is_alive():
        thread.join(timeout=max(1, int(heartbeat_sec)))
        if thread.is_alive():
            waited += max(1, int(heartbeat_sec))
            processing_log(f"[KI] {heartbeat_label} | warte auf Antwort... {waited}s")

    if result_q.empty():
        raise RuntimeError("KI-Request ohne Ergebnis beendet.")

    state, value = result_q.get()
    if state == "err":
        raise value
    return value


def get_http_error_message(exc: urllib.error.HTTPError) -> str:
    try:
        raw = exc.read().decode("utf-8", errors="ignore")
    except Exception:
        return ""
    if not raw:
        return ""
    try:
        parsed = json.loads(raw)
        return parsed.get("error", {}).get("message", "") or raw
    except Exception:
        return raw


def is_quota_exhausted_message(message: str) -> bool:
    text = (message or "").lower()
    return (
        "resource_exhausted" in text
        or "exceeded your current quota" in text
        or "quota exceeded" in text
        or "limit: 0" in text
    )


def sanitize_model_text(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        cleaned = cleaned.removeprefix("csv").strip()
    return cleaned.strip()


def strip_fences(text: str) -> str:
    cleaned = text.strip()
    if cleaned.startswith("```") and cleaned.endswith("```"):
        cleaned = "\n".join(cleaned.splitlines()[1:-1]).strip()
    return cleaned


def extract_first_json_array(text: str) -> list[dict[str, Any]]:
    cleaned = strip_fences(text)
    try:
        value = json.loads(cleaned)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    except Exception:
        pass

    match = re.search(r"\[\s*\{.*\}\s*\]", cleaned, flags=re.DOTALL)
    if not match:
        return []
    try:
        value = json.loads(match.group(0))
    except Exception:
        return []
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def check_gemini_connection_and_tokens(gemini_key: str) -> None:
    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={gemini_key}"
    try:
        data = fetch_json(url)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Gemini-Verbindung fehlgeschlagen (HTTP {exc.code}).") from exc
    except Exception as exc:
        raise RuntimeError(f"Gemini-Verbindung fehlgeschlagen ({exc}).") from exc

    models = data.get("models", [])
    if not models:
        raise RuntimeError("Gemini-Verbindung OK, aber keine Modelle gefunden.")

    model = None
    for item in models:
        if "gemini-2.0-flash" in item.get("name", ""):
            model = item
            break
    if model is None:
        model = models[0]

    model_name = model.get("name", "unbekannt")
    in_limit = model.get("inputTokenLimit", "unbekannt")
    out_limit = model.get("outputTokenLimit", "unbekannt")
    total_limit = model.get("totalTokenLimit", "unbekannt")

    log("OK", f"Gemini-Verbindung erfolgreich ({model_name}).")
    log("INFO", f"Token-Kontingent (pro Request): input={in_limit}, output={out_limit}, total={total_limit}")


def list_gemini_generate_models(gemini_key: str) -> list[str]:
    last_model = read_last_successful_model()
    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={gemini_key}"
    try:
        data = fetch_json(url)
    except Exception:
        candidates = []
        if last_model:
            candidates.append(last_model)
        for model in MODEL_FALLBACKS:
            if model not in candidates:
                candidates.append(model)
        return candidates

    available = []
    for model in data.get("models", []):
        name = model.get("name", "")
        if not name.startswith("models/"):
            continue
        methods = model.get("supportedGenerationMethods", [])
        if "generateContent" not in methods:
            continue
        model_name = name.split("/", 1)[1]
        if not model_name.startswith("gemini-"):
            continue
        available.append(model_name)

    if not available:
        candidates = []
        if last_model:
            candidates.append(last_model)
        for model in MODEL_FALLBACKS:
            if model not in candidates:
                candidates.append(model)
        return candidates

    if last_model and last_model in available:
        available.remove(last_model)
        available.insert(0, last_model)

    return available


def init_mariadb_schema() -> None:
    GEMINI_DB_STORE.init_schema(info_log=lambda message: log("INFO", message))


def read_last_successful_model() -> str:
    init_mariadb_schema()
    return GEMINI_DB_STORE.read_last_successful_model(legacy_model_file=GEMINI_LAST_MODEL_FILE)


def write_last_successful_model(model: str) -> None:
    init_mariadb_schema()
    GEMINI_DB_STORE.write_last_successful_model(model)


def gemini_prompt_cache_key(prompt: str) -> str:
    return GEMINI_DB_STORE.prompt_hash(prompt)


def migrate_legacy_gemini_cache_file() -> None:
    init_mariadb_schema()
    imported = GEMINI_DB_STORE.migrate_legacy_cache_file(GEMINI_HITS_CACHE_FILE)
    if imported > 0:
        log("INFO", f"[TMDB-CACHE] Legacy-Dateicache nach MariaDB importiert: {imported} Eintraege")


def purge_legacy_gemini_files() -> None:
    legacy_files = (
        GEMINI_HITS_CACHE_FILE,
        GEMINI_LAST_MODEL_FILE,
        LEGACY_GEMINI_TREE_FILE,
    )
    for file_path in legacy_files:
        try:
            if file_path.exists():
                file_path.unlink()
                log("INFO", f"[TMDB-CACHE] Legacy-Datei entfernt: {format_path(file_path)}")
        except Exception:
            pass


def get_cached_gemini_rows(prompt: str) -> list[dict[str, Any]]:
    init_mariadb_schema()
    rows, model = GEMINI_DB_STORE.get_cached_rows(prompt)
    if not rows:
        return []
    key = gemini_prompt_cache_key(prompt)
    log("INFO", f"[TMDB-CACHE] Treffer aus MariaDB: key={key[:12]}, rows={len(rows)}, model={model}")
    processing_log(f"[TMDB-CACHE] Treffer aus MariaDB: rows={len(rows)}, model={model}")
    return rows


def store_gemini_rows_in_cache(prompt: str, rows: list[dict[str, Any]], model: str) -> None:
    init_mariadb_schema()
    GEMINI_DB_STORE.store_rows(prompt, rows, model)


def tmdb_cache_identity(path: str, params: dict | None = None) -> dict[str, Any]:
    query_pairs: list[tuple[str, str]] = []
    if isinstance(params, dict):
        for key in sorted(params.keys()):
            query_pairs.append((str(key), str(params.get(key, "") or "")))
    return {
        "path": str(path or ""),
        "params": query_pairs,
    }


def tmdb_state_cache_key(path: str, params: dict | None = None) -> str:
    raw = json.dumps(
        tmdb_cache_identity(path, params),
        ensure_ascii=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return f"{TMDB_CACHE_STATE_PREFIX_LEGACY}{digest}"


def tmdb_cache_prompt(path: str, params: dict | None = None) -> str:
    raw = json.dumps(
        tmdb_cache_identity(path, params),
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return f"{TMDB_CACHE_PROMPT_PREFIX}{raw}"


def load_tmdb_response_from_db(path: str, params: dict | None = None) -> dict | None:
    init_mariadb_schema()
    prompt = tmdb_cache_prompt(path, params)
    has_row_cache = callable(getattr(GEMINI_DB_STORE, "get_cached_rows", None))
    if has_row_cache:
        rows, _ = GEMINI_DB_STORE.get_cached_rows(prompt)
        if rows:
            cached = rows[0]
            if isinstance(cached, dict):
                data = cached.get("data")
                if isinstance(data, dict) and data:
                    return data

    # Legacy fallback from app_state (v1) with lazy migration into tmdb_cache.
    cache_key = tmdb_state_cache_key(path, params)
    raw = str(GEMINI_DB_STORE.read_state(cache_key) or "").strip()
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if not isinstance(data, dict):
        return None
    try:
        saved_unix = int(payload.get("saved_unix", 0) or 0)
    except Exception:
        saved_unix = 0
    if saved_unix <= 0:
        return None
    max_age_sec = TMDB_CACHE_RETENTION_DAYS * 86400
    if int(time.time()) - saved_unix > max_age_sec:
        return None
    if has_row_cache:
        try:
            store_tmdb_response_to_db(path, params, data)
            GEMINI_DB_STORE.delete_state_many([cache_key])
        except Exception:
            pass
    return data


def store_tmdb_response_to_db(path: str, params: dict | None, data: dict) -> None:
    if not isinstance(data, dict) or not data:
        return
    init_mariadb_schema()
    has_row_cache = callable(getattr(GEMINI_DB_STORE, "store_rows", None))
    if has_row_cache:
        prompt = tmdb_cache_prompt(path, params)
        payload_row = {
            "path": str(path or ""),
            "params": tmdb_cache_identity(path, params).get("params", []),
            "data": data,
        }
        GEMINI_DB_STORE.store_rows(prompt, [payload_row], "tmdb-api")
        try:
            GEMINI_DB_STORE.delete_state_many([tmdb_state_cache_key(path, params)])
        except Exception:
            pass
        return

    # Compatibility path for test fakes or minimal state stores.
    cache_key = tmdb_state_cache_key(path, params)
    payload = {
        "saved_unix": int(time.time()),
        "data": data,
    }
    GEMINI_DB_STORE.write_state(cache_key, json.dumps(payload, ensure_ascii=False))


def normalize_source_row_name(source_name: str) -> str:
    normalized = str(source_name or "").strip().replace("\\", "/")
    normalized = re.sub(r"/{2,}", "/", normalized)
    normalized = re.sub(r"^(?:\./)+", "", normalized)
    if normalized.startswith("/"):
        normalized = normalized[1:]
    return normalized.lower()


def source_row_cache_key(prefix: str, source_name: str) -> str:
    normalized = normalize_source_row_name(source_name)
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()
    return f"{prefix}{digest}"


def gemini_source_row_cache_key(source_name: str) -> str:
    return source_row_cache_key(GEMINI_SOURCE_ROW_CACHE_PREFIX, source_name)


def editor_source_row_cache_key(source_name: str) -> str:
    return source_row_cache_key(EDITOR_SOURCE_ROW_CACHE_PREFIX, source_name)


def _row_cache_payload_to_row(
    parsed: Any,
    *,
    source_name: str,
    retention_days: int = 0,
) -> dict[str, str] | None:
    if not isinstance(parsed, dict):
        return None

    payload = parsed
    saved_unix = 0
    if isinstance(parsed.get("row"), dict):
        payload = parsed.get("row") or {}
        try:
            saved_unix = int(parsed.get("saved_unix", 0) or 0)
        except Exception:
            saved_unix = 0

    if retention_days > 0:
        if saved_unix <= 0:
            return None
        max_age_sec = max(1, int(retention_days)) * 86400
        if int(time.time()) - saved_unix > max_age_sec:
            return None

    row = {header: str(payload.get(header, "") or "").strip() for header in CSV_HEADERS}
    row["Quellname"] = source_name
    if not any((row.get(header, "") or "").strip() for header in CSV_HEADERS if header != "Quellname"):
        return None
    return row


def load_cached_source_rows(
    source_files: list[Path],
    *,
    cache_prefix: str = GEMINI_SOURCE_ROW_CACHE_PREFIX,
    retention_days: int = 0,
    cache_label: str = "CACHE",
) -> tuple[dict[str, dict[str, str]], list[Path]]:
    init_mariadb_schema()
    cached: dict[str, dict[str, str]] = {}
    missing: list[Path] = []

    source_meta: list[tuple[Path, str, str, str]] = []
    cache_keys: list[str] = []
    for rel in source_files:
        source_name = str(rel)
        source_key = normalize_source_row_name(source_name)
        cache_key = source_row_cache_key(cache_prefix, source_name)
        source_meta.append((rel, source_name, source_key, cache_key))
        cache_keys.append(cache_key)

    state_map = GEMINI_DB_STORE.read_state_many(cache_keys)

    for rel, source_name, source_key, cache_key in source_meta:
        raw = str(state_map.get(cache_key, "") or "").strip()
        if not raw:
            missing.append(rel)
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            missing.append(rel)
            continue
        row = _row_cache_payload_to_row(parsed, source_name=source_name, retention_days=retention_days)
        if not row:
            missing.append(rel)
            continue

        cached[source_key] = row

    if cached:
        processing_log(f"[{cache_label}] Treffer: {len(cached)}/{len(source_files)}")
    return cached, missing


def load_runtime_rows_seed(
    source_files: list[Path],
    *,
    cache_label: str = "DB-RUNTIME",
) -> dict[str, dict[str, str]]:
    if not source_files:
        return {}
    init_mariadb_schema()
    wanted: dict[str, str] = {}
    for rel in source_files:
        rel_text = str(rel)
        key = normalize_source_row_name(rel_text)
        if key:
            wanted[key] = rel_text
    if not wanted:
        return {}

    rows_any: list[Any] = []
    state_map = GEMINI_DB_STORE.read_state_many(
        [
            "runtime.rows_json",
            "runtime.rows_csv",
            "runtime.gemini_rows_json",
            "runtime.gemini_csv",
        ]
    )
    raw_json = str(state_map.get("runtime.rows_json", "") or "").strip()
    if not raw_json:
        raw_json = str(state_map.get("runtime.gemini_rows_json", "") or "").strip()
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, list):
                rows_any = parsed
        except Exception:
            rows_any = []

    if not rows_any:
        raw_csv = str(state_map.get("runtime.rows_csv", "") or "").strip()
        if not raw_csv:
            raw_csv = str(state_map.get("runtime.gemini_csv", "") or "").strip()
        if raw_csv:
            rows_any = parse_csv_rows(raw_csv)

    cached: dict[str, dict[str, str]] = {}
    for item in rows_any:
        if not isinstance(item, dict):
            continue
        row = coerce_row_from_any(item)
        source_name = (row.get("Quellname", "") or "").strip()
        if not source_name:
            source_name = str(item.get("source_name", "") or "").strip()
        if not source_name:
            continue
        source_key = normalize_source_row_name(source_name)
        wanted_source = wanted.get(source_key)
        if not wanted_source:
            continue
        row["Quellname"] = wanted_source
        if not any((row.get(header, "") or "").strip() for header in CSV_HEADERS if header != "Quellname"):
            continue
        cached[source_key] = row

    if cached:
        processing_log(f"[{cache_label}] Treffer: {len(cached)}/{len(source_files)}")
    return cached


def store_source_rows_cache(
    rows: list[dict[str, str]],
    *,
    cache_prefix: str = GEMINI_SOURCE_ROW_CACHE_PREFIX,
    cache_label: str = "CACHE",
    overwrite: bool = True,
    prefer_richer_existing: bool = False,
) -> int:
    init_mariadb_schema()
    saved_unix = int(time.time())
    payload_by_key: dict[str, str] = {}
    row_by_key: dict[str, dict[str, str]] = {}
    source_name_by_key: dict[str, str] = {}
    for row in rows:
        source_name = (row.get("Quellname", "") or "").strip()
        if not source_name:
            continue
        row_payload = {header: (row.get(header, "") or "").strip() for header in CSV_HEADERS}
        row_payload["Quellname"] = source_name
        if not any((row_payload.get(header, "") or "").strip() for header in CSV_HEADERS if header != "Quellname"):
            continue
        payload = {
            "saved_unix": saved_unix,
            "source_name": source_name,
            "row": row_payload,
        }
        cache_key = source_row_cache_key(cache_prefix, source_name)
        payload_by_key[cache_key] = json.dumps(payload, ensure_ascii=False)
        row_by_key[cache_key] = row_payload
        source_name_by_key[cache_key] = source_name

    if not payload_by_key:
        return 0

    if not overwrite:
        existing_map = GEMINI_DB_STORE.read_state_many(list(payload_by_key.keys()))
        for cache_key in list(payload_by_key.keys()):
            if str(existing_map.get(cache_key, "") or "").strip():
                payload_by_key.pop(cache_key, None)
    elif prefer_richer_existing:
        existing_map = GEMINI_DB_STORE.read_state_many(list(payload_by_key.keys()))
        for cache_key in list(payload_by_key.keys()):
            raw = str(existing_map.get(cache_key, "") or "").strip()
            if not raw:
                continue
            try:
                parsed = json.loads(raw)
            except Exception:
                continue
            source_name = source_name_by_key.get(cache_key, "")
            existing_row = _row_cache_payload_to_row(parsed, source_name=source_name, retention_days=0)
            new_row = row_by_key.get(cache_key, {})
            if not existing_row or not new_row:
                continue
            same_row = True
            for header in CSV_HEADERS:
                if (existing_row.get(header, "") or "").strip() != (new_row.get(header, "") or "").strip():
                    same_row = False
                    break
            if same_row:
                payload_by_key.pop(cache_key, None)
                continue
            try:
                existing_quality = float(row_quality_score(existing_row))
                new_quality = float(row_quality_score(new_row))
            except Exception:
                existing_quality = 0.0
                new_quality = 0.0
            if new_quality < (existing_quality - 1e-9):
                payload_by_key.pop(cache_key, None)

    if not payload_by_key:
        return 0

    stored = GEMINI_DB_STORE.write_state_many(list(payload_by_key.items()))
    if stored > 0:
        processing_log(f"[{cache_label}] gespeichert: {stored}")
    return stored


def processed_source_row_cache_key(source_name: str) -> str:
    return source_row_cache_key(PROCESSED_SOURCE_ROW_CACHE_PREFIX, source_name)


def _processed_cache_payload_to_entry(
    parsed: Any,
    *,
    source_name: str,
    retention_days: int = PROCESSED_SOURCE_ROW_RETENTION_DAYS,
) -> dict[str, Any] | None:
    if not isinstance(parsed, dict):
        return None

    saved_unix_raw = parsed.get("saved_unix")
    try:
        saved_unix = int(saved_unix_raw or 0)
    except Exception:
        return None
    if saved_unix <= 0:
        return None

    if retention_days > 0:
        max_age = int(retention_days * 86400)
        if saved_unix < int(time.time()) - max_age:
            return None

    source_payload = str(parsed.get("source_name", "") or "").strip()
    resolved_source = source_name or source_payload
    if not resolved_source:
        return None

    mode = normalize_manifest_mode(str(parsed.get("mode", "") or ""))
    if mode not in {"copy", "encode"}:
        return None

    target_rel = str(parsed.get("target_rel", "") or "").strip()
    z_gb = str(parsed.get("z_gb", "") or "").strip() or "n/a"
    row_any = parsed.get("row")
    row_payload: dict[str, str] = {}
    if isinstance(row_any, dict):
        for key, value in row_any.items():
            key_text = str(key or "").strip()
            if not key_text:
                continue
            value_text = str(value or "").strip()
            if not value_text:
                continue
            row_payload[key_text] = value_text
    row_payload["Quellname"] = resolved_source
    if target_rel and not row_payload.get("Zielname"):
        row_payload["Zielname"] = target_rel
    if z_gb and z_gb != "n/a" and not row_payload.get("Z-GB"):
        row_payload["Z-GB"] = z_gb

    return {
        "saved_unix": saved_unix,
        "source_name": resolved_source,
        "mode": mode,
        "target_rel": target_rel,
        "z_gb": z_gb,
        "row": row_payload,
    }


def load_processed_source_history(
    source_files: list[Path],
    *,
    retention_days: int = PROCESSED_SOURCE_ROW_RETENTION_DAYS,
    cache_label: str = "DB-HISTORY",
) -> tuple[dict[str, dict[str, Any]], list[Path]]:
    init_mariadb_schema()
    cached: dict[str, dict[str, Any]] = {}
    missing: list[Path] = []
    source_meta: list[tuple[Path, str, str, str]] = []
    cache_keys: list[str] = []
    for rel in source_files:
        source_name = str(rel)
        source_key = normalize_source_row_name(source_name)
        cache_key = processed_source_row_cache_key(source_name)
        source_meta.append((rel, source_name, source_key, cache_key))
        cache_keys.append(cache_key)

    state_map = GEMINI_DB_STORE.read_state_many(cache_keys)
    for rel, source_name, source_key, cache_key in source_meta:
        raw = str(state_map.get(cache_key, "") or "").strip()
        if not raw:
            missing.append(rel)
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            missing.append(rel)
            continue
        entry = _processed_cache_payload_to_entry(
            parsed,
            source_name=source_name,
            retention_days=retention_days,
        )
        if not entry:
            missing.append(rel)
            continue
        cached[source_key] = entry

    if cached:
        processing_log(f"[{cache_label}] Treffer: {len(cached)}/{len(source_files)}")
    return cached, missing


def store_processed_source_history_row(
    source_name: str,
    row: dict[str, Any] | None,
    *,
    processed_mode: str,
    target_rel: str = "",
    z_gb: str = "",
) -> bool:
    init_mariadb_schema()
    source_text = str(source_name or "").strip()
    mode_norm = normalize_manifest_mode(processed_mode)
    if not source_text or mode_norm not in {"copy", "encode"}:
        return False

    row_payload: dict[str, str] = {}
    if isinstance(row, dict):
        for key, value in row.items():
            key_text = str(key or "").strip()
            if not key_text:
                continue
            value_text = str(value or "").strip()
            if not value_text:
                continue
            row_payload[key_text] = value_text
    row_payload["Quellname"] = source_text

    target_text = str(target_rel or "").strip()
    if not target_text:
        target_text = str((row_payload.get("Zielname", "") or "")).strip()
    if target_text:
        row_payload["Zielname"] = target_text

    z_value = str(z_gb or "").strip()
    if not z_value:
        z_value = str((row_payload.get("Z-GB", "") or "")).strip() or "n/a"
    row_payload["Z-GB"] = z_value
    row_payload["MANIFEST-SKIP"] = "1"
    row_payload["MANIFEST-MODE"] = mode_norm
    row_payload["VERARBEITET"] = "1"

    payload = {
        "saved_unix": int(time.time()),
        "source_name": source_text,
        "mode": mode_norm,
        "target_rel": target_text,
        "z_gb": z_value,
        "row": row_payload,
    }
    cache_key = processed_source_row_cache_key(source_text)
    GEMINI_DB_STORE.write_state(cache_key, json.dumps(payload, ensure_ascii=False))
    return True


def tmdb_get_json(path: str, tmdb_key: str, params: dict | None = None) -> dict:
    query = {"api_key": tmdb_key}
    if params:
        query.update(params)
    qs = urllib.parse.urlencode(query)
    url = f"https://api.themoviedb.org/3{path}?{qs}"
    return fetch_json(url)


def check_tmdb_connection(tmdb_key: str) -> None:
    if not TMDB_ENABLED:
        log("INFO", "TMDB-Abfrage ist deaktiviert.")
        return
    try:
        data = tmdb_get_json("/configuration", tmdb_key)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"TMDB-Verbindung fehlgeschlagen (HTTP {exc.code}).") from exc
    except Exception as exc:
        raise RuntimeError(f"TMDB-Verbindung fehlgeschlagen ({exc}).") from exc
    images = data.get("images", {})
    if "secure_base_url" not in images:
        raise RuntimeError("TMDB-Verbindung unklar: Antwort ohne erwartete Daten.")
    log("OK", "TMDB-Verbindung erfolgreich.")


def choose_folder() -> Path:
    script = 'set theFolder to POSIX path of (choose folder with prompt "Ordner wählen")' \
             '\nreturn theFolder'
    result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Ordnerwahl abgebrochen oder fehlgeschlagen: {result.stderr.strip()}")
    folder = result.stdout.strip()
    if not folder:
        raise RuntimeError("Keine Auswahl getroffen.")
    return Path(folder)


def is_video_file(path: Path) -> bool:
    return path.suffix.lower() in VIDEO_EXTENSIONS


def is_ignored_path(path: Path, root: Path | None = None) -> bool:
    root_name = ""
    if root is not None:
        try:
            root_name = root.resolve().name.lower()
        except Exception:
            root_name = root.name.lower()
    ignored = {"sample", "__out", "__"}
    for part in path.parts:
        p = part.lower()
        if p == REENQUEUE_DIR_NAME.lower():
            if root_name != REENQUEUE_DIR_NAME.lower():
                return True
            continue
        if p in ignored:
            return True
        if p.startswith("._"):
            return True
    return False


def collect_video_rel_paths(root: Path) -> list[Path]:
    return sorted(
        (
            file_path.relative_to(root)
            for file_path in root.rglob("*")
            if file_path.is_file()
            and is_video_file(file_path)
            and not is_ignored_path(file_path.relative_to(root), root)
        ),
        key=lambda rel: str(rel).lower(),
    )


def normalize_target_rel_key(value: str) -> str:
    text = (value or "").strip().replace("\\", "/")
    text = re.sub(r"/+", "/", text).strip("/")
    return text.lower()


def normalize_target_rel_codecless_key(value: str) -> str:
    key = normalize_target_rel_key(value)
    if not key:
        return ""
    return re.sub(r"\.(?:codec|x264|h264|avc|x265|h265|hevc|mpeg2)\.", ".video.", key)

def normalize_manifest_mode(value: str) -> str:
    raw = (value or "").strip().lower()
    if raw in {"copy", "copied", "c"}:
        return "copy"
    if raw in {"encode", "encoded", "ffmpeg", "f"}:
        return "encode"
    return ""


def manifest_action_label(mode: str) -> str:
    mode_norm = normalize_manifest_mode(mode)
    if mode_norm == "copy":
        return "Copy"
    if mode_norm == "encode":
        return "Encode"
    return ""


def split_manifest_parts(raw_line: str) -> list[str]:
    if "	" in raw_line:
        return [part.strip() for part in raw_line.split("	")]
    return [part.strip() for part in raw_line.split("|")]


def video_manifest_sidecar_path(video_path: Path) -> Path:
    return video_path.with_name(video_path.name + MANAGEMOVIE_VIDEO_MANIFEST_SUFFIX)


def parse_video_manifest_line(line: str) -> tuple[str, str] | None:
    raw = (line or "").strip()
    if not raw or raw.startswith("#"):
        return None
    mode_match = re.search(r"(?i)\b(copy|copied|encode|encoded|ffmpeg|c|f)\b", raw)
    if not mode_match:
        return None
    mode = normalize_manifest_mode(mode_match.group(1))
    if not mode:
        return None
    z_match = re.search(r"(?i)\bz[-_ ]?gb\s*[:=]\s*([0-9]+(?:[.,][0-9]+)?)\b", raw)
    z_gb = (z_match.group(1).replace(",", ".") if z_match else "n/a").strip() or "n/a"
    return mode, z_gb


def read_video_manifest_entry(video_path: Path) -> dict[str, str] | None:
    manifest_path = video_manifest_sidecar_path(video_path)
    if not manifest_path.exists() or not manifest_path.is_file():
        return None
    text = read_text_best_effort(manifest_path)
    if not text:
        return None
    for line in text.splitlines():
        parsed = parse_video_manifest_line(line)
        if not parsed:
            continue
        mode, z_gb = parsed
        return {
            "target_rel": "",
            "mode": mode,
            "z_gb": z_gb,
        }
    return None


def write_video_manifest_sidecar(video_path: Path, mode: str) -> None:
    mode_norm = normalize_manifest_mode(mode)
    action = manifest_action_label(mode_norm)
    if not action:
        return
    manifest_path = video_manifest_sidecar_path(video_path)
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(f"{timestamp}\t{action}\n", encoding="utf-8")


def parse_managemovie_manifest_line(line: str, manifest_parent_rel: Path, root: Path) -> dict[str, str] | None:
    raw = (line or "").strip()
    if not raw or raw.startswith("#"):
        return None

    parts = split_manifest_parts(raw)
    if len(parts) < 3:
        return None

    target_part = parts[0].strip()
    mode = normalize_manifest_mode(parts[1])
    z_gb = parts[2].strip() or "n/a"
    if not target_part or not mode:
        return None

    target_path = Path(target_part)
    if target_path.is_absolute():
        try:
            target_rel = target_path.relative_to(root)
        except Exception:
            return None
    elif len(target_path.parts) == 1:
        target_rel = manifest_parent_rel / target_path.name
    else:
        target_rel = target_path

    target_rel_str = str(target_rel)
    if not target_rel_str:
        return None

    return {
        "target_rel": target_rel_str,
        "mode": mode,
        "z_gb": z_gb,
    }


def scan_managemovie_tree(root: Path) -> dict[str, dict[str, str]]:
    entries: dict[str, dict[str, str]] = {}
    manifest_map: dict[str, Path] = {}
    for alias in MANAGEMOVIE_TRACK_FILE_ALIASES:
        try:
            for file_path in root.rglob(alias):
                manifest_map[str(file_path)] = file_path
        except Exception:
            continue
    manifest_files = sorted(manifest_map.values(), key=lambda p: str(p).lower())

    for manifest_file in manifest_files:
        if not manifest_file.is_file():
            continue
        try:
            parent_rel = manifest_file.parent.relative_to(root)
        except Exception:
            continue
        if is_ignored_path(parent_rel, root):
            continue
        text = read_text_best_effort(manifest_file)
        if not text:
            continue

        for line in text.splitlines():
            parsed = parse_managemovie_manifest_line(line, parent_rel, root)
            if not parsed:
                continue
            key = normalize_target_rel_key(parsed["target_rel"])
            if not key:
                continue
            entries[key] = {
                "target_rel": parsed["target_rel"],
                "mode": parsed["mode"],
                "z_gb": parsed["z_gb"],
            }
    return entries


def write_managemovie_entry(root: Path, target_video: Path, mode: str, z_gb: str) -> None:
    mode_norm = normalize_manifest_mode(mode)
    if mode_norm not in {"copy", "encode"}:
        return

    manifest_path = target_video.parent / MANAGEMOVIE_TRACK_FILE_NAME
    entries: dict[str, tuple[str, str, str]] = {}

    if manifest_path.exists():
        existing = read_text_best_effort(manifest_path)
        for line in existing.splitlines():
            parts = split_manifest_parts(line.strip())
            if len(parts) < 3:
                continue
            name = Path(parts[0].strip()).name
            line_mode = normalize_manifest_mode(parts[1])
            line_z = parts[2].strip() or "n/a"
            if not name or not line_mode:
                continue
            entries[name.lower()] = (name, line_mode, line_z)

    line_z = (z_gb or "").strip() or "n/a"
    entries[target_video.name.lower()] = (target_video.name, mode_norm, line_z)

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    lines_out = [f"{name}	{line_mode}	{line_z}" for name, line_mode, line_z in sorted(entries.values(), key=lambda item: item[0].lower())]
    manifest_path.write_text("\n".join(lines_out) + ("\n" if lines_out else ""), encoding="utf-8")


def _title_norm_for_manifest(value: str) -> str:
    title = clean_title_noise(pretty_title(value)) or pretty_title(value)
    # Ignore common release/source tags during manifest prefilter matching.
    noise = {
        "amzn", "amazon", "nf", "netflix", "hmax", "max", "atvp", "appletv",
        "dsnp", "disney", "web", "dl", "german", "internal", "repack", "proper",
    }
    tokens = [t for t in re.split(r"\s+", title.lower()) if t and t not in noise]
    compact = " ".join(tokens).strip() or title
    return normalize_match_token(compact)


def build_manifest_match_keys_from_source_name(source_name: str) -> list[str]:
    guess = normalize_title_guess(source_name)
    title_norm = _title_norm_for_manifest(guess.get("title", "") or Path(source_name).stem)
    if not title_norm:
        return []

    season = format_season_episode(guess.get("season", ""))
    episode = format_season_episode(guess.get("episode", ""))
    if season and episode:
        return [f"S|{title_norm}|S{season}|E{episode}"]

    year = normalize_year(guess.get("year", "")) or normalize_year(source_year_hint(source_name)) or "0000"
    return [f"M|{title_norm}|{year}", f"M|{title_norm}|*"]


def build_manifest_match_keys_from_target_rel(target_rel: str) -> list[str]:
    stem = Path((target_rel or "").strip()).stem
    tokens = [t for t in stem.split(".") if t]
    if not tokens:
        return []

    year_idx = -1
    year_val = ""
    for i, tok in enumerate(tokens):
        y = normalize_year(tok)
        if y:
            year_idx = i
            year_val = y
            break
        if tok.lower() == "0000":
            year_idx = i
            year_val = "0000"
            break

    season = ""
    episode = ""
    season_idx = -1
    for i in range(len(tokens) - 1):
        m1 = re.fullmatch(r"(?i)s(\d{1,2})", tokens[i])
        m2 = re.fullmatch(r"(?i)e(\d{1,2})", tokens[i + 1])
        if m1 and m2:
            season = f"{int(m1.group(1)):02d}"
            episode = f"{int(m2.group(1)):02d}"
            season_idx = i
            break

    stop_idx = len(tokens)
    for idx_candidate in (year_idx, season_idx):
        if idx_candidate > 0:
            stop_idx = min(stop_idx, idx_candidate)

    title_tokens = tokens[:stop_idx] if stop_idx > 0 else tokens[:1]
    title_norm = _title_norm_for_manifest(" ".join(title_tokens))
    if not title_norm:
        return []

    if season and episode:
        return [f"S|{title_norm}|S{season}|E{episode}"]

    year = year_val or "0000"
    return [f"M|{title_norm}|{year}", f"M|{title_norm}|*"]


def prefilter_source_files_by_manifest(source_files: list[Path], manifest_entries: dict[str, dict[str, str]]) -> dict[str, dict[str, str]]:
    if not manifest_entries:
        return {}

    target_key_map: dict[str, dict[str, str]] = {}
    for entry in manifest_entries.values():
        target_rel = (entry.get("target_rel", "") or "").strip()
        if not target_rel:
            continue
        for key in build_manifest_match_keys_from_target_rel(target_rel):
            if key and key not in target_key_map:
                target_key_map[key] = entry

    matched: dict[str, dict[str, str]] = {}
    for rel_path in source_files:
        source_name = str(rel_path)
        source_key = normalize_source_row_name(source_name)
        for key in build_manifest_match_keys_from_source_name(source_name):
            entry = target_key_map.get(key)
            if not entry:
                continue
            matched[source_key] = entry
            break
    return matched


def collect_iso_rel_paths(root: Path) -> list[Path]:
    return sorted(
        (
            file_path.relative_to(root)
            for file_path in root.rglob("*")
            if file_path.is_file()
            and file_path.suffix.lower() in ISO_EXTENSIONS
            and not is_ignored_path(file_path.relative_to(root), root)
        ),
        key=lambda rel: str(rel).lower(),
    )


def iso_looks_series(name: str) -> bool:
    return bool(re.search(r"(?i)\b(s\d{1,2}|season|staffel|episode|episoden|disc\s*\d|dvd\s*\d)\b", name or ""))


def mount_iso_readonly(iso_path: Path) -> Path:
    cmd = [
        "hdiutil",
        "attach",
        "-nobrowse",
        "-readonly",
        "-noverify",
        "-plist",
        str(iso_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"ISO-Mount fehlgeschlagen: {iso_path.name}")
    try:
        payload = plistlib.loads(result.stdout.encode("utf-8"))
    except Exception as exc:
        raise RuntimeError(f"ISO-Mount-Antwort unlesbar: {iso_path.name}") from exc
    entities = payload.get("system-entities", [])
    for entity in entities:
        mount_point = entity.get("mount-point")
        if mount_point:
            return Path(mount_point)
    raise RuntimeError(f"Kein Mountpoint gefunden: {iso_path.name}")


def detach_iso_mount(mount_point: Path) -> None:
    subprocess.run(
        ["hdiutil", "detach", str(mount_point), "-force"],
        capture_output=True,
        text=True,
        check=False,
    )


def run_ffmpeg_with_progress(cmd: list[str], source_iso: Path, out_mkv: Path, label: str) -> None:
    process = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        source_bytes = max(0, int(source_iso.stat().st_size))
    except Exception:
        source_bytes = 0
    try:
        out_start_bytes = max(0, int(out_mkv.stat().st_size))
    except Exception:
        out_start_bytes = 0
    started_ts = time.time()
    next_log = time.time() + 60.0
    while process.poll() is None:
        now_ts = time.time()
        if now_ts >= next_log:
            try:
                out_bytes = max(0, int(out_mkv.stat().st_size))
            except Exception:
                out_bytes = 0
            produced_bytes = max(0, out_bytes - out_start_bytes)
            elapsed_sec = max(1.0, now_ts - started_ts)
            speed_mb_s = produced_bytes / elapsed_sec / (1024.0 ** 2)
            src_gb = float(source_bytes) / (1024.0 ** 3) if source_bytes > 0 else 0.0
            z_gb = float(out_bytes) / (1024.0 ** 3)
            progress_pct = 0.0
            if source_bytes > 0:
                progress_pct = min(100.0, max(0.0, (produced_bytes / float(source_bytes)) * 100.0))
            eta_text = "-"
            if source_bytes > 0 and speed_mb_s > 0:
                remaining_bytes = max(0.0, float(source_bytes - produced_bytes))
                eta_seconds = remaining_bytes / (speed_mb_s * (1024.0 ** 2))
                eta_text = format_hh_mm_ss(eta_seconds)
            processing_log(
                f"[ISO] {label} | Q-GB: {src_gb:.1f} | Z-GB: {z_gb:.1f} | Fortschritt: {progress_pct:.1f}% | "
                f"Speed: {speed_mb_s:.1f} MB/s | Laufzeit: {format_hh_mm_ss(elapsed_sec)} | ETA: {eta_text}"
            )
            next_log = now_ts + 60.0
        time.sleep(1.0)
    if process.returncode != 0:
        raise RuntimeError(f"ISO-Extraktion fehlgeschlagen: {label}")


def scan_dvd_candidates(video_ts_dir: Path) -> list[dict[str, Any]]:
    buckets: dict[int, list[tuple[int, Path]]] = {}
    pattern = re.compile(r"VTS_(\d{2})_(\d+)\.VOB$", re.IGNORECASE)
    for file_path in video_ts_dir.glob("VTS_*_*.VOB"):
        match = pattern.match(file_path.name)
        if not match:
            continue
        title_id = int(match.group(1))
        part_id = int(match.group(2))
        if part_id == 0:
            continue
        buckets.setdefault(title_id, []).append((part_id, file_path))

    candidates: list[dict[str, Any]] = []
    for title_id, entries in buckets.items():
        parts = [path for _, path in sorted(entries, key=lambda item: item[0])]
        if not parts:
            continue
        total_size = sum(path.stat().st_size for path in parts if path.exists())
        candidates.append(
            {
                "type": "dvd",
                "order": title_id,
                "name": f"VTS_{title_id:02d}",
                "parts": parts,
                "size": total_size,
            }
        )
    return candidates


def scan_bluray_candidates(bdmv_dir: Path) -> list[dict[str, Any]]:
    stream_dir = bdmv_dir / "STREAM"
    if not stream_dir.exists():
        return []
    candidates: list[dict[str, Any]] = []
    for idx, file_path in enumerate(sorted(stream_dir.glob("*.m2ts"), key=lambda p: p.name), start=1):
        try:
            size = file_path.stat().st_size
        except Exception:
            continue
        if size < 200 * 1024 * 1024:
            continue
        candidates.append(
            {
                "type": "bdmv",
                "order": idx,
                "name": file_path.stem,
                "path": file_path,
                "size": size,
            }
        )
    return candidates


def select_iso_titles(candidates: list[dict[str, Any]], iso_name: str) -> list[dict[str, Any]]:
    if not candidates:
        return []
    ranked = sorted(candidates, key=lambda item: item["size"], reverse=True)
    largest = ranked[0]["size"]
    series_hint = iso_looks_series(iso_name)

    if not series_hint and len(ranked) >= 3:
        if ranked[1]["size"] >= largest * 0.75 and ranked[2]["size"] >= largest * 0.75:
            series_hint = True

    if not series_hint:
        return [ranked[0]]

    selected = [item for item in ranked if item["size"] >= largest * 0.35]
    if len(selected) < 2:
        selected = ranked[: min(4, len(ranked))]
    return sorted(selected, key=lambda item: item["order"])


def extract_dvd_candidate(candidate: dict[str, Any], iso_path: Path, out_file: Path) -> None:
    with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8", prefix="managemovie_iso_", suffix=".txt") as handle:
        concat_file = Path(handle.name)
        for part in candidate["parts"]:
            escaped = str(part).replace("'", "'\\''")
            handle.write(f"file '{escaped}'\n")
    try:
        cmd = [
            FFMPEG_BIN,
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-y",
            "-fflags",
            "+genpts",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            str(concat_file),
            "-map",
            "0:v:0",
            "-map",
            "0:a?",
            "-map",
            "0:s?",
            "-c",
            "copy",
            "-avoid_negative_ts",
            "make_zero",
            str(out_file),
        ]
        run_ffmpeg_with_progress(cmd, iso_path, out_file, f"{iso_path.name} {candidate['name']}")
    finally:
        try:
            concat_file.unlink()
        except Exception:
            pass


def extract_bluray_candidate(candidate: dict[str, Any], iso_path: Path, out_file: Path) -> None:
    cmd = [
        FFMPEG_BIN,
        "-hide_banner",
        "-loglevel",
        "error",
        "-nostdin",
        "-y",
        "-i",
        str(candidate["path"]),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-map",
        "0:s?",
        "-c",
        "copy",
        str(out_file),
    ]
    run_ffmpeg_with_progress(cmd, iso_path, out_file, f"{iso_path.name} {candidate['name']}")


def extract_iso_file(iso_path: Path) -> None:
    processing_log(f"[ISO] Analysiere: {iso_path.name}")
    mount_point = mount_iso_readonly(iso_path)
    try:
        video_ts = mount_point / "VIDEO_TS"
        bdmv = mount_point / "BDMV"
        if video_ts.exists():
            candidates = scan_dvd_candidates(video_ts)
            source_type = "DVD"
        elif bdmv.exists():
            candidates = scan_bluray_candidates(bdmv)
            source_type = "BluRay"
        else:
            processing_log(f"[ISO] Unbekannte Struktur, uebersprungen: {iso_path.name}")
            return

        selected = select_iso_titles(candidates, iso_path.stem)
        if not selected:
            processing_log(f"[ISO] Keine geeigneten Titel gefunden: {iso_path.name}")
            return

        processing_log(f"[ISO] {iso_path.name}: Typ={source_type}, Titel={len(selected)}")
        single = len(selected) == 1
        for idx, candidate in enumerate(selected, start=1):
            if single:
                out_file = iso_path.with_suffix(".mkv")
            else:
                out_file = iso_path.parent / f"{iso_path.stem}.E{idx:02d}.mkv"
            if out_file.exists():
                if is_valid_iso_mkv(out_file):
                    processing_log(f"[ISO] Vorhandene MKV genutzt: {out_file.name}")
                    continue
                processing_log(f"[ISO] Ungueltige MKV erkannt (zu klein), erstelle neu: {out_file.name}")
                try:
                    out_file.unlink()
                except Exception:
                    pass
            processing_log(f"[ISO] Extrahiere {idx}/{len(selected)}: {out_file.name}")
            if candidate["type"] == "dvd":
                extract_dvd_candidate(candidate, iso_path, out_file)
            else:
                extract_bluray_candidate(candidate, iso_path, out_file)
            if not is_valid_iso_mkv(out_file):
                raise RuntimeError(f"Extrahierte MKV ungueltig/zu klein: {out_file.name}")
            processing_log(f"[ISO] Fertig: {out_file.name} ({file_size_human(out_file)} GB)")
    finally:
        detach_iso_mount(mount_point)


def extract_isos_in_tree(root: Path) -> None:
    iso_rel_paths = collect_iso_rel_paths(root)
    if not iso_rel_paths:
        return
    processing_log(f"[ISO] Gefundene ISOs: {len(iso_rel_paths)}")
    for rel_path in iso_rel_paths:
        iso_path = root / rel_path
        try:
            extract_iso_file(iso_path)
        except Exception as exc:
            processing_log(f"[ISO] Fehler bei {iso_path.name}: {exc}")


def render_tree(node: dict, prefix: str, lines: list[str]) -> None:
    entries = sorted(node.items(), key=lambda item: item[0].lower())
    for idx, (name, child) in enumerate(entries):
        is_last = idx == len(entries) - 1
        connector = "└── " if is_last else "├── "
        lines.append(f"{prefix}{connector}{name}")
        if isinstance(child, dict):
            next_prefix = f"{prefix}{'    ' if is_last else '│   '}"
            render_tree(child, next_prefix, lines)


def build_tree_from_paths(root_name: str, rel_paths: list[Path]) -> str:
    lines = [root_name]
    if not rel_paths:
        lines.append("(keine videodateien gefunden)")
        return "\n".join(lines)

    tree: dict[str, dict | None] = {}
    for rel_path in rel_paths:
        node = tree
        parts = rel_path.parts
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = None

    render_tree(tree, "", lines)
    return "\n".join(lines)


def write_tree_file(content: str) -> Path:
    TARGET_DIR.mkdir(parents=True, exist_ok=True)
    out_file = TARGET_DIR / "folder_tree.txt"
    out_file.write_text(content + "\n", encoding="utf-8")
    return out_file


def cleanup_previous_output(mode: str) -> list[Path]:
    reset: list[Path] = []
    mode_norm = str(mode or "").strip().lower()
    logged_files: list[Path] = []
    if mode_norm == "a":
        # Analyze-Ausgabe (Baum) bleibt als einzige sichtbare Arbeitsdatei bestehen.
        logged_files.append(TARGET_DIR / "folder_tree.txt")

    # Laufzeit-Arbeitsstände intern zurücksetzen (ohne extra Log-Spam).
    silent_files = (STATUS_TABLE_FILE, OUT_PLAN_FILE, OUT_TREE_FILE, PROCESSING_LOG_FILE)
    marker_files = (OUT_TREE_DONE_FILE, STATUS_DONE_FILE)

    for old_file in logged_files:
        if old_file.exists():
            overwrite_text_file(old_file, "")
            reset.append(old_file)

    for old_file in silent_files:
        try:
            if old_file.exists():
                overwrite_text_file(old_file, "")
        except Exception:
            pass

    for marker in marker_files:
        try:
            if marker.exists():
                marker.unlink()
        except Exception:
            pass
    return reset


def choose_start_mode() -> str:
    script = r'''
set choices to {"Analyze", "Copy", "FFMPEG"}
set selected to choose from list choices with prompt "Startparameter wählen" default items {"Analyze"} OK button name "Start" cancel button name "Abbrechen"
if selected is false then
    return ""
end if
return item 1 of selected
'''
    result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Startparameter-Auswahl fehlgeschlagen: {result.stderr.strip()}")
    choice = result.stdout.strip().lower()
    mapping = {"analyze": "a", "copy": "c", "ffmpeg": "f"}
    return mapping.get(choice, "")


def _is_truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _normalize_rel_path_text(value: str) -> str:
    return str(value or "").strip().replace("\\", "/").lstrip("./")


def is_manual_target_path(target_rel: str, start_folder: Path) -> bool:
    raw = str(target_rel or "").strip()
    if not raw:
        return False

    candidate = Path(raw)
    manual_root = resolve_target_manual_root(start_folder)
    if candidate.is_absolute():
        try:
            resolved = candidate.resolve()
        except Exception:
            resolved = candidate
        return resolved == manual_root or manual_root in resolved.parents

    norm = _normalize_rel_path_text(raw).lower()
    manual_prefix = _normalize_rel_path_text(str(target_manual_prefix_for_rows(start_folder))).lower()
    if not norm:
        return False
    if norm == manual_prefix or norm.startswith(manual_prefix + "/"):
        return True
    return norm == MANUAL_DIR_NAME.lower() or norm.startswith(MANUAL_DIR_NAME.lower() + "/")


def build_manual_target_rel_path(source_rel: str, start_folder: Path) -> str:
    manual_prefix = target_manual_prefix_for_rows(start_folder)
    source = Path(_normalize_rel_path_text(source_rel))
    if source.is_absolute():
        source = Path(source.name)
    return str(manual_prefix / source)


def build_confirmation_editor_rows(rows: list[dict[str, str]], start_folder: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        source_name = str(row.get("Quellname", "") or "").strip()
        target_name = str(row.get("Zielname", "") or "").strip()
        if not source_name:
            continue
        out.append(
            {
                "nr": idx,
                "source_name": source_name,
                "target_name": target_name,
                "title": str(row.get("Name des Film/Serie", "") or "").strip(),
                "year": str(row.get("Erscheinungsjahr", "") or "").strip(),
                "season": str(row.get("Staffel", "") or "").strip(),
                "episode": str(row.get("Episode", "") or "").strip(),
                "imdb_id": str(row.get("IMDB-ID", "") or "").strip(),
                "q_gb": str(row.get("Q-GB", "") or row.get("Groesse", "") or "").strip(),
                "z_gb": str(row.get("Z-GB", "") or "").strip(),
                "e_gb": str(row.get("E-GB", "") or "").strip(),
                "lzeit": str(row.get("Laufzeit (f)", "") or row.get("Laufzeit", "") or "").strip(),
                "speed": str(row.get("Speed", "") or "").strip(),
                "eta": str(row.get("ETA", "") or "").strip(),
                "completed": _is_truthy(row.get("VERARBEITET", "")),
                "manual": is_manual_target_path(target_name, start_folder) or _is_truthy(row.get("MANUAL", "")),
            }
        )
    return out


def apply_confirmation_editor_rows(
    rows: list[dict[str, str]],
    start_folder: Path,
    mode: str,
    editor_rows: Any,
) -> None:
    if not isinstance(editor_rows, list) or not rows:
        return

    by_source: dict[str, dict[str, str]] = {}
    allowed_sources: set[str] = set()
    for row in rows:
        source_name = str(row.get("Quellname", "") or "").strip()
        if source_name:
            by_source[normalize_source_row_name(source_name)] = row

    target_out_prefix = target_out_prefix_for_rows(start_folder)
    changed = 0
    for item in editor_rows:
        if not isinstance(item, dict):
            continue
        source_name = str(item.get("source_name", item.get("Quellname", "")) or "").strip()
        if not source_name:
            continue
        allowed_sources.add(normalize_source_row_name(source_name))

    requeue_marked = 0
    for row in rows:
        source_name = str(row.get("Quellname", "") or "").strip()
        source_key = normalize_source_row_name(source_name)
        if source_key and source_key not in allowed_sources:
            row["REQUEUE"] = "1"
            row["VERARBEITET"] = "1"
            row["Speed"] = "re-queue"
            row["ETA"] = "re-queue"
            if mode == "f":
                row["FPS"] = "n/a"
            requeue_marked += 1
            continue
        row.pop("REQUEUE", None)

    for item in editor_rows:
        if not isinstance(item, dict):
            continue
        source_name = str(item.get("source_name", item.get("Quellname", "")) or "").strip()
        if not source_name:
            continue
        row = by_source.get(normalize_source_row_name(source_name))
        if not row:
            continue

        incoming_speed = str(item.get("speed", item.get("Speed", "")) or "").strip()
        incoming_eta = str(item.get("eta", item.get("ETA", "")) or "").strip()
        incoming_z_gb = str(item.get("z_gb", item.get("Z-GB", "")) or "").strip()
        incoming_e_gb = str(item.get("e_gb", item.get("E-GB", "")) or "").strip()
        incoming_lzeit = str(item.get("lzeit", item.get("Lzeit", item.get("Laufzeit", ""))) or "").strip()
        should_clear_done_state = (
            ((row.get("MANIFEST-SKIP", "") or "").strip() == "1" or (row.get("VERARBEITET", "") or "").strip() == "1")
            and not incoming_speed
            and not incoming_eta
            and not incoming_z_gb
            and not incoming_e_gb
            and not incoming_lzeit
        )
        if should_clear_done_state:
            for key in (
                "MANIFEST-SKIP",
                "MANIFEST-MODE",
                "MANIFEST-TARGET",
                "MANIFEST-SOURCE",
                "MANIFEST-ZGB",
                "VERARBEITET",
                "Speed",
                "ETA",
                "Z-GB",
                "E-GB",
                "E-GB-BAND",
                "E-GB-STATUS",
                "FPS",
                "Laufzeit (f)",
                "Lzeit",
            ):
                if key in row:
                    row[key] = ""

        title_raw = str(item.get("title", item.get("Name des Film/Serie", "")) or "").strip()
        if title_raw:
            row["Name des Film/Serie"] = clean_title_noise(title_raw) or title_raw

        year_raw = str(item.get("year", item.get("Erscheinungsjahr", "")) or "").strip()
        if year_raw != "":
            row["Erscheinungsjahr"] = normalize_year(year_raw) or "0000"

        season_raw = str(item.get("season", item.get("Staffel", "")) or "").strip()
        episode_raw = str(item.get("episode", item.get("Episode", "")) or "").strip()
        season = format_season_episode(season_raw)
        episode = format_season_episode(episode_raw)
        if season and episode:
            row["Staffel"] = season
            row["Episode"] = episode
        else:
            row["Staffel"] = ""
            row["Episode"] = ""

        imdb_raw = str(item.get("imdb_id", item.get("IMDB-ID", "")) or "").strip()
        if imdb_raw != "":
            row["IMDB-ID"] = normalize_imdb_id(imdb_raw) or "tt0000000"

        manual = _is_truthy(item.get("manual", "0"))
        if manual:
            row["MANUAL"] = "1"
            row["Zielname"] = build_manual_target_rel_path(source_name, start_folder)
            row["E-GB"] = "copy"
            row["E-GB-BAND"] = ""
            row["E-GB-STATUS"] = "copy"
        else:
            row.pop("MANUAL", None)
            target_name = str(item.get("target_name", item.get("Zielname", "")) or "").strip()
            if target_name:
                row["Zielname"] = target_name
            else:
                row["Zielname"] = build_target_rel_path(row, target_out_prefix)
            if mode == "f":
                row["Zielname"] = force_target_rel_codec(row["Zielname"], FFMPEG_TARGET_VIDEO_CODEC)
        changed += 1

    if requeue_marked > 0:
        processing_log(f"[CONFIRM] RE-QUEUE ausgenommen: {requeue_marked} Datei(en).")
    if changed > 0:
        processing_log(f"[CONFIRM] Editor-Aenderungen uebernommen: {changed} Datei(en).")



def confirm_processing_start(
    mode: str,
    file_count: int,
    start_folder: Path,
    rows_for_edit: list[dict[str, str]] | None = None,
    rows_for_edit_original: list[dict[str, str]] | None = None,
) -> tuple[bool, str]:
    default_encoder = read_ffmpeg_encoder_default()
    action_labels = {
        "a": "Analyze (Tree-Dateien)",
        "c": "Copy",
        "f": "Encode (FFMPEG)",
    }
    action_label = action_labels.get(mode, "Verarbeitung")
    folder_text = str(start_folder).replace('"', "'")

    auto_start = (os.environ.get("MANAGEMOVIE_AUTOSTART", "") or "").strip().lower() in {"1", "true", "yes", "y"}
    if auto_start:
        confirm_file_raw = (os.environ.get("MANAGEMOVIE_WEB_CONFIRM_FILE", "") or "").strip()
        if mode in {"a", "c", "f"} and confirm_file_raw:
            try:
                confirm_file = Path(confirm_file_raw).expanduser()
                if not confirm_file.is_absolute():
                    confirm_file = (TARGET_DIR / confirm_file).resolve()

                token = f"{int(time.time() * 1000)}-{os.getpid()}"
                payload = {
                    "state": "pending",
                    "token": token,
                    "mode": mode,
                    "file_count": int(file_count),
                    "start_folder": str(start_folder),
                    "default_encoder": default_encoder,
                    "created_at": int(time.time()),
                }
                if rows_for_edit:
                    editor_rows = build_confirmation_editor_rows(rows_for_edit, start_folder)
                    payload["editor_rows"] = editor_rows
                    # Snapshot der beim Editorstart sichtbaren Daten (inkl. bestehender Korrekturen).
                    payload["editor_rows_session_start"] = build_confirmation_editor_rows(rows_for_edit, start_folder)
                    if rows_for_edit_original:
                        # Gemini-/Analyse-Baseline ohne Editor-Overrides fuer "echten" Zeilen-Reset.
                        payload["editor_rows_original"] = build_confirmation_editor_rows(rows_for_edit_original, start_folder)
                    else:
                        payload["editor_rows_original"] = editor_rows
                confirm_file.parent.mkdir(parents=True, exist_ok=True)
                confirm_file.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
                processing_log(f"[CONFIRM] Warte auf Freigabe: {action_label} nach Analyse.")

                while True:
                    time.sleep(0.5)
                    try:
                        current = json.loads(confirm_file.read_text(encoding="utf-8"))
                    except Exception:
                        continue

                    if not isinstance(current, dict):
                        continue
                    if str(current.get("token", "")) != token:
                        continue

                    state = str(current.get("state", "")).strip().lower()
                    if state in {"", "pending"}:
                        continue

                    try:
                        confirm_file.unlink()
                    except Exception:
                        pass

                    if state != "start":
                        return False, default_encoder

                    if rows_for_edit:
                        apply_confirmation_editor_rows(
                            rows_for_edit,
                            start_folder,
                            mode,
                            current.get("editor_rows", []),
                        )

                    if mode == "f":
                        selected = normalize_ffmpeg_encoder_mode(current.get("encoder", "")) or normalize_ffmpeg_encoder_mode(os.environ.get("MANAGEMOVIE_AUTOSTART_ENCODER", default_encoder)) or default_encoder
                        write_ffmpeg_encoder_default(selected)
                        processing_log(f"[CONFIRM] Freigabe erhalten: {action_label} | Encoder={selected}")
                        log("INFO", f"Freigabe erhalten: {action_label}, Encoder={selected}")
                        return True, selected

                    processing_log(f"[CONFIRM] Freigabe erhalten: {action_label}")
                    log("INFO", f"Freigabe erhalten: {action_label}")
                    return True, default_encoder
            except Exception as exc:
                log("WARN", f"Web-Freigabe fehlgeschlagen, nutze Auto-Start direkt: {exc}")

        if mode == "f":
            forced = normalize_ffmpeg_encoder_mode(os.environ.get("MANAGEMOVIE_AUTOSTART_ENCODER", default_encoder)) or default_encoder
            write_ffmpeg_encoder_default(forced)
            log("INFO", f"Auto-Start aktiv: {action_label}, Encoder={forced}")
            return True, forced
        log("INFO", f"Auto-Start aktiv: {action_label}")
        return True, default_encoder


    if mode == "a":
        dialog_text = (
            f"Analyse abgeschlossen. Tree-Dateien erstellen? Dateien: {file_count}. Ordner: {folder_text}"
        )
        escaped_text = dialog_text.replace('"', '\\"')
        script = (
            f'set dlg to display dialog "{escaped_text}" '
            f'buttons {{"Abbrechen", "OK"}} default button "OK" '
            f'cancel button "Abbrechen" with title "ManageMovie {VERSION}"\n'
            'return button returned of dlg'
        )
        result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, check=False)
        if result.returncode == 0:
            return (result.stdout or "").strip().lower() == "ok", default_encoder

        stderr_text = (result.stderr or "").lower()
        if "cancel" in stderr_text or "abgebrochen" in stderr_text:
            return False, default_encoder

        if sys.stdin.isatty():
            try:
                start_answer = input("Analyse abgeschlossen. Tree-Dateien erstellen? [j/N]: ").strip().lower()
            except EOFError:
                return False, default_encoder
            return start_answer in {"j", "ja", "y", "yes", "ok", "start", "s"}, default_encoder

        raise RuntimeError(f"Start-Bestaetigung fehlgeschlagen: {result.stderr.strip()}")


    if mode == "f":
        dialog_text = (
            f"Analyse abgeschlossen. {action_label} starten? "
            f"Dateien: {file_count}. Ordner: {folder_text}. "
            f"Encoder ({ffmpeg_encoder_choices_text()}):"
        )
        escaped_text = dialog_text.replace('"', '\\"')
        script = (
            f'set dlg to display dialog "{escaped_text}" '
            f'default answer "{default_encoder}" '
            f'buttons {{"Abbrechen", "Start"}} default button "Start" '
            f'cancel button "Abbrechen" with title "ManageMovie {VERSION}"\n'
            'return (button returned of dlg) & "|" & (text returned of dlg)'
        )
        result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, check=False)
        if result.returncode == 0:
            payload = (result.stdout or "").strip()
            button, _, text_value = payload.partition("|")
            if button.strip().lower() != "start":
                return False, default_encoder
            selected = normalize_ffmpeg_encoder_mode(text_value)
            if not selected:
                selected = default_encoder
                log("WARN", f"Unbekannte Encoder-Auswahl '{text_value.strip()}'. Nutze Default: {selected}.")
            write_ffmpeg_encoder_default(selected)
            return True, selected

        stderr_text = (result.stderr or "").lower()
        if "cancel" in stderr_text or "abgebrochen" in stderr_text:
            return False, default_encoder

        if sys.stdin.isatty():
            try:
                start_answer = input("Analyse abgeschlossen. Encode starten? [j/N]: ").strip().lower()
            except EOFError:
                return False, default_encoder
            if start_answer not in {"j", "ja", "y", "yes", "start", "s"}:
                return False, default_encoder
            try:
                raw_mode = input(f"Encoder waehlen ({ffmpeg_encoder_choices_text()}) [{default_encoder}]: ").strip().lower()
            except EOFError:
                raw_mode = ""
            selected = normalize_ffmpeg_encoder_mode(raw_mode) or default_encoder
            write_ffmpeg_encoder_default(selected)
            return True, selected

        raise RuntimeError(f"Start-Bestaetigung fehlgeschlagen: {result.stderr.strip()}")

    dialog_text = (
        f"Analyse abgeschlossen. {action_label} starten? "
        f"Dateien: {file_count}. Ordner: {folder_text}"
    )
    escaped_text = dialog_text.replace('"', '\\"')
    script = (
        f'set dlg to display dialog "{escaped_text}" '
        f'buttons {{"Abbrechen", "Start"}} default button "Start" '
        f'cancel button "Abbrechen" with title "ManageMovie {VERSION}"\n'
        'return button returned of dlg'
    )
    result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, check=False)
    if result.returncode == 0:
        return result.stdout.strip().lower() == "start", default_encoder

    stderr_text = (result.stderr or "").lower()
    if "cancel" in stderr_text or "abgebrochen" in stderr_text:
        return False, default_encoder

    if sys.stdin.isatty():
        try:
            answer = input(f"Analyse abgeschlossen. {action_label} starten? [j/N]: ").strip().lower()
        except EOFError:
            return False, default_encoder
        return answer in {"j", "ja", "y", "yes", "start", "s"}, default_encoder

    raise RuntimeError(f"Start-Bestaetigung fehlgeschlagen: {result.stderr.strip()}")

def parse_csv_rows(csv_text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    try:
        reader = csv.DictReader(io.StringIO(csv_text))
    except Exception:
        return rows

    if not reader.fieldnames:
        return rows

    for row in reader:
        normalized = {header: (row.get(header, "") or "").strip() for header in CSV_HEADERS}
        if any(normalized.values()):
            rows.append(normalized)
    return rows


def coerce_row_from_any(item: dict[str, Any]) -> dict[str, str]:
    row = {header: "" for header in CSV_HEADERS}
    aliases = {
        "Quellname": ["Quellname", "source", "file", "filename"],
        "Name des Film/Serie": ["Name des Film/Serie", "name", "title"],
        "Erscheinungsjahr": ["Erscheinungsjahr", "year"],
        "Staffel": ["Staffel", "season"],
        "Episode": ["Episode", "episode"],
        "Laufzeit": ["Laufzeit", "runtime", "duration"],
        "IMDB-ID": ["IMDB-ID", "imdb", "imdb_id"],
    }
    for target, keys in aliases.items():
        for key in keys:
            value = item.get(key)
            if value is None:
                continue
            row[target] = str(value).strip()
            if row[target]:
                break
    return row


def get_alias_value(item: dict[str, Any], keys: list[str]) -> str:
    for key in keys:
        value = item.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def normalize_title_guess(source_name: str) -> dict[str, str]:
    source_path = Path(source_name)
    stem = source_path.stem
    full = source_name

    # Prefer a directory segment that already encodes title/year/season info.
    candidates = [stem]
    for part in source_path.parts[:-1]:
        p = part.strip()
        if p:
            candidates.append(p)
    best = max(
        candidates,
        key=lambda s: (
            1 if re.search(r"(?i)s\d{1,2}[ ._\\-]*e\d{1,2}|\b(19\d{2}|20\d{2})\b", s) else 0,
            len(s),
        ),
    )
    stem = best

    season, episode = extract_season_episode_from_source(source_name)

    year_match = re.search(r"\b(19\d{2}|20\d{2})\b", full)
    year = year_match.group(1) if year_match else ""

    cleaned = re.sub(r"(?i)s\d{1,2}[ ._\\-]*e\d{1,2}(?:[ ._\\-]*e\d{1,2})*", " ", stem)
    cleaned = re.sub(r"(?i)\b\d{1,2}x\d{1,2}(?:[ ._\\-]*(?:x)?\d{1,2})*\b", " ", cleaned)
    cleaned = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", cleaned)
    cleaned = re.sub(r"[._-]+", " ", cleaned)

    bad = {
        "german", "dl", "web", "webrip", "webdl", "web-dl", "bluray", "bdrip", "dvdrip",
        "hdr", "h264", "h265", "x264", "x265", "1080p", "2160p", "720p", "uhd", "hevc",
        "knoedel", "sauerkraut", "wvf", "w4k", "wayne", "p73", "rsg",
        "repack", "proper", "readnfo", "rerip", "internal", "avc", "4sf", "18p", "ml", "intention",
        "multi", "complete", "pal", "dvd9",
        "ac3", "dts", "dtshd", "truehd", "uncut", "extended", "edition", "dubbed",
    }

    tokens = []
    for token in cleaned.split():
        t = token.strip()
        if not t:
            continue
        if t.lower() in bad:
            continue
        tokens.append(t)

    # Generic scene-group cleanup for release-like tails (e.g. "... BLURAY-MONUMENT").
    if len(tokens) >= 2:
        last = tokens[-1]
        if re.fullmatch(r"[A-Z0-9]{4,}", last):
            tokens = tokens[:-1]

    title = " ".join(tokens).strip()
    title = re.sub(r"(?i)\bfogofwar\b", "Fog of War", title)
    if title and title.lower().startswith("gma "):
        title = title[4:].strip()
    if not title:
        title = stem

    return {
        "title": title,
        "year": year,
        "season": season,
        "episode": episode,
        "is_series": "1" if season and episode else "0",
    }


def extract_season_episode_from_source(source_name: str) -> tuple[str, str]:
    source_path = Path(source_name)
    full = str(source_name or "")
    stem = source_path.stem

    se_match = re.search(
        r"(?i)(?:^|[ ./_\\-])s(\d{1,2})[ ._\\-]*e(\d{1,3})(?:[ ._\\-]*e\d{1,3})*(?:$|[ ./_\\-])",
        full,
    )
    if se_match:
        return str(int(se_match.group(1))), str(int(se_match.group(2)))

    x_match = re.search(
        r"(?i)(?:^|[ ./_\\-])(\d{1,2})x(\d{1,3})(?:[ ._\\-]*(?:x)?\d{1,3})*(?:$|[ ./_\\-])",
        full,
    )
    if x_match:
        # Guard against false positives like movie titles "4x4" without series context.
        context = re.sub(
            r"(?i)(?:^|[ ./_\\-])\d{1,2}x\d{1,3}(?:[ ._\\-]*(?:x)?\d{1,3})*(?:$|[ ./_\\-])",
            " ",
            stem,
        )
        context = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", context)
        context = re.sub(
            r"(?i)\b(2160p|1080p|720p|480p|4k|uhd|webrip|webdl|web-dl|bluray|bdrip|dvdrip|x264|x265|h264|h265|hevc)\b",
            " ",
            context,
        )
        context = re.sub(r"[._-]+", " ", context)
        context = re.sub(r"\s+", " ", context).strip()
        context_tokens = [token.strip().lower() for token in context.split() if token.strip()]
        release_noise = {
            "german", "dl", "web", "webrip", "webdl", "web-dl", "bluray", "bdrip", "dvdrip",
            "hdr", "x264", "h264", "x265", "h265", "hevc", "uhd", "avc",
            "repack", "proper", "readnfo", "rerip", "internal",
            "complete", "pal", "dvd9", "multi", "ac3", "dts", "dtshd", "truehd",
        }
        cleaned_tokens = [
            token
            for token in context_tokens
            if token not in release_noise and not re.fullmatch(r"\d+", token)
        ]
        has_local_title = any(re.search(r"[a-zäöüß]{3,}", token) for token in cleaned_tokens)
        has_parent_title = any(
            bool(re.search(r"[A-Za-zÄÖÜäöüß]{3,}", str(part or "")))
            for part in source_path.parts[:-1]
        )
        if has_local_title or has_parent_title:
            return str(int(x_match.group(1))), str(int(x_match.group(2)))

    season_from_dir = ""
    for raw_part in reversed(source_path.parts[:-1]):
        part = str(raw_part or "").strip()
        if not part:
            continue
        season_dir_match = re.fullmatch(r"(?i)s(?:taffel|eason)?[ ._\\-]?(\d{1,2})", part)
        if not season_dir_match:
            season_dir_match = re.fullmatch(r"(?i)(?:staffel|season)[ ._\\-]?(\d{1,2})", part)
        if season_dir_match:
            season_from_dir = str(int(season_dir_match.group(1)))
            break
    if season_from_dir:
        ep_match = re.search(r"(?i)(?:^|[ ._\\-])(?:e|ep|episode|folge|f)[ ._\\-]?(\d{1,3})(?:$|[ ._\\-])", stem)
        if not ep_match:
            ep_match = re.search(r"(?i)(?:^|[ ._\\-])(?:teil|part)[ ._\\-]?(\d{1,3})(?:$|[ ._\\-])", stem)
        if ep_match:
            return season_from_dir, str(int(ep_match.group(1)))

    return "", ""


def series_title_from_source(source_name: str) -> str:
    source_path = Path(source_name)
    # Prefer a clean parent-folder title when available (common for series trees:
    # "<Series>/S01/<release>/file.mkv"). This avoids noisy/translated release aliases.
    parent_generic = {"serien", "series", "tv", "shows", "show"}
    season_dir_re = re.compile(r"(?i)^(?:s(?:taffel|eason)?[ ._\\-]?\d{1,2}|(?:staffel|season)[ ._\\-]?\d{1,2})$")
    for raw_part in source_path.parts[:-1]:
        part = str(raw_part or "").strip()
        if not part:
            continue
        if season_dir_re.fullmatch(part):
            continue
        if re.search(r"(?i)(?:^|[ ._\\-])s\d{1,2}[ ._\\-]*e\d{1,3}(?:$|[ ._\\-])", part):
            continue
        if re.search(r"(?i)(?:^|[ ._\\-])\d{1,2}x\d{1,3}(?:$|[ ._\\-])", part):
            continue
        if not re.search(r"[A-Za-zÄÖÜäöüß]{3,}", part):
            continue
        cleaned_parent = clean_title_noise(part) or part
        compact_parent = normalize_match_token(cleaned_parent)
        if not compact_parent or compact_parent in parent_generic or len(compact_parent) < 3:
            continue
        return cleaned_parent

    candidates: list[tuple[str, bool]] = []
    for part in source_path.parts[:-1]:
        p = part.strip()
        if p:
            candidates.append((p, True))
    candidates.append((source_path.stem, False))

    best = ""
    best_score: tuple[int, int, int] | None = None
    for raw, is_parent in candidates:
        text = raw.strip()
        if not text:
            continue
        match = re.search(
            r"(?i)^(.*?)(?:[ ._\\-]s\d{1,2}[ ._\\-]*e\d{1,2}(?:[ ._\\-]*e\d{1,2})*)(?:$|[ ._\\-])",
            text,
        )
        if not match:
            match = re.search(
                r"(?i)^(.*?)(?:[ ._\\-]\d{1,2}x\d{1,2}(?:[ ._\\-]*(?:x)?\d{1,2})*)(?:$|[ ._\\-])",
                text,
            )
        if not match:
            match = re.search(r"(?i)^(.*?)(?:[ ._\\-]s\d{1,2})(?:$|[ ._\\-])", text)
        if not match:
            continue
        head = match.group(1).strip(" ._-")
        head = re.sub(r"\b(19\d{2}|20\d{2})\b", " ", head)
        head = re.sub(r"\s+", " ", head).strip(" ._-")
        cleaned = clean_title_noise(head) or head
        compact = normalize_match_token(cleaned)
        if not compact:
            continue
        score = (
            1 if is_parent else 0,
            1 if not re.search(r"\d", cleaned) else 0,
            len(cleaned),
        )
        if best_score is None or score > best_score:
            best = cleaned
            best_score = score

    if not best:
        return ""
    return best


class TmdbClient:
    def __init__(self, tmdb_key: str):
        self.tmdb_key = tmdb_key
        self.cache: dict[str, dict] = {}
        self.db_hits = 0
        self.db_writes = 0

    def _cached_get(self, key: str, path: str, params: dict | None = None) -> dict:
        if key in self.cache:
            return self.cache[key]
        data: dict | None = None
        try:
            data = load_tmdb_response_from_db(path, params)
        except Exception:
            data = None
        if isinstance(data, dict):
            self.db_hits += 1
            self.cache[key] = data
            return data

        data = tmdb_get_json(path, self.tmdb_key, params)
        try:
            store_tmdb_response_to_db(path, params, data)
            self.db_writes += 1
        except Exception:
            pass
        self.cache[key] = data
        return data

    def search_tv(self, title: str, year_hint: str = "") -> dict | None:
        key = f"search_tv:{title.lower()}:{TMDB_LANGUAGE}"
        data = self._cached_get(key, "/search/tv", {"query": title, "language": TMDB_LANGUAGE})
        results = data.get("results", [])
        return pick_best_tmdb_search_result(
            title,
            results,
            title_keys=("name", "original_name"),
            year_hint=year_hint,
            date_keys=("first_air_date",),
        )

    def search_movie(self, title: str, year: str) -> dict | None:
        params = {"query": title, "language": TMDB_LANGUAGE}
        if year:
            params["year"] = year
        key = f"search_movie:{title.lower()}:{year}:{TMDB_LANGUAGE}"
        data = self._cached_get(key, "/search/movie", params)
        results = data.get("results", [])
        return pick_best_tmdb_search_result(
            title,
            results,
            title_keys=("title", "original_title"),
            year_hint=year,
            date_keys=("release_date",),
        )

    def tv_details(self, tv_id: int) -> dict:
        key = f"tv_details:{tv_id}:{TMDB_LANGUAGE}"
        return self._cached_get(
            key,
            f"/tv/{tv_id}",
            {"append_to_response": "external_ids", "language": TMDB_LANGUAGE},
        )

    def tv_details_lang(self, tv_id: int, language: str) -> dict:
        lang = (language or "").strip() or TMDB_LANGUAGE
        key = f"tv_details:{tv_id}:{lang}"
        return self._cached_get(
            key,
            f"/tv/{tv_id}",
            {"append_to_response": "external_ids", "language": lang},
        )

    def tv_episode(self, tv_id: int, season: str, episode: str) -> dict:
        key = f"tv_episode:{tv_id}:{season}:{episode}:{TMDB_LANGUAGE}"
        return self._cached_get(
            key,
            f"/tv/{tv_id}/season/{season}/episode/{episode}",
            {"language": TMDB_LANGUAGE},
        )

    def movie_details(self, movie_id: int) -> dict:
        key = f"movie_details:{movie_id}:{TMDB_LANGUAGE}"
        return self._cached_get(key, f"/movie/{movie_id}", {"language": TMDB_LANGUAGE})

    def movie_details_lang(self, movie_id: int, language: str) -> dict:
        lang = (language or "").strip() or TMDB_LANGUAGE
        key = f"movie_details:{movie_id}:{lang}"
        return self._cached_get(key, f"/movie/{movie_id}", {"language": lang})

    def find_by_imdb(self, imdb_id: str) -> dict:
        normalized = normalize_imdb_id(imdb_id)
        if not normalized:
            return {}
        key = f"find_imdb:{normalized}"
        return self._cached_get(
            key,
            f"/find/{normalized}",
            {"external_source": "imdb_id", "language": "de-DE"},
        )


def year_from_date(date_value: str) -> str:
    if not date_value:
        return ""
    if len(date_value) >= 4 and date_value[:4].isdigit():
        return date_value[:4]
    return ""


def fill_missing(row: dict[str, str], key: str, value: str) -> None:
    if value and not row.get(key, "").strip():
        row[key] = value.strip()


def fill_missing_or_na(row: dict[str, str], key: str, value: str) -> None:
    current = (row.get(key, "") or "").strip()
    if not value:
        return
    if not current or current.lower() in {"n/a", "na", "unknown", "unbekannt"}:
        row[key] = value.strip()


def format_season_episode(value: str) -> str:
    if not value:
        return ""
    if value.strip().lower() in {"none", "null", "n/a", "na", "unbekannt", "unknown"}:
        return ""
    try:
        return f"{int(value):02d}"
    except Exception:
        return value


def normalize_runtime(value: str) -> str:
    if not value:
        return ""
    raw = value.strip().lower()
    if raw in {"none", "null", "n/a", "na", "unbekannt", "unknown"}:
        return ""
    hhmm = re.search(r"\b(\d{1,2}):(\d{2})\b", raw)
    if hhmm:
        total = int(hhmm.group(1)) * 60 + int(hhmm.group(2))
        return str(total) if 20 <= total <= 300 else ""
    match = re.search(r"\d{2,3}", raw)
    if not match:
        return ""
    minutes = int(match.group(0))
    if 20 <= minutes <= 300:
        return str(minutes)
    return ""


def probe_runtime_minutes(file_path: Path) -> str:
    if not file_path.exists() or not file_path.is_file():
        return "n/a"

    ffprobe_cmd = [
        FFPROBE_BIN,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(file_path),
    ]
    try:
        result = subprocess.run(ffprobe_cmd, capture_output=True, text=True, check=False)
        if result.returncode == 0 and result.stdout.strip():
            seconds = float(result.stdout.strip())
            minutes = int(round(seconds / 60.0))
            return str(minutes) if minutes > 0 else "n/a"
    except Exception:
        pass

    ffmpeg_cmd = [FFMPEG_BIN, "-i", str(file_path)]
    try:
        result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True, check=False)
        text = f"{result.stdout}\n{result.stderr}"
        match = re.search(r"Duration:\s*(\d{2}):(\d{2}):(\d{2})", text)
        if match:
            hh = int(match.group(1))
            mm = int(match.group(2))
            ss = int(match.group(3))
            minutes = int(round((hh * 3600 + mm * 60 + ss) / 60.0))
            return str(minutes) if minutes > 0 else "n/a"
    except Exception:
        pass

    return "n/a"


def runtime_value_is_plausible(value: str) -> bool:
    try:
        minutes = int(str(value).strip())
    except Exception:
        return False
    return 20 <= minutes <= 400


def runtime_minutes_value(value: str) -> int:
    try:
        minutes = int(str(value).strip())
    except Exception:
        return 0
    return minutes if 20 <= minutes <= 400 else 0


def probe_duration_seconds(file_path: Path) -> float:
    if not file_path.exists() or not file_path.is_file():
        return 0.0
    cmd = [
        FFPROBE_BIN,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(file_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0 or not result.stdout.strip():
            return 0.0
        value = float(result.stdout.strip())
        return value if value > 0 else 0.0
    except Exception:
        return 0.0


def file_size_human(file_path: Path) -> str:
    try:
        size = file_path.stat().st_size
    except Exception:
        return "n/a"
    gb = float(size) / (1024.0 ** 3)
    return f"{gb:.1f}"


def file_size_gb(file_path: Path) -> float:
    try:
        return float(file_path.stat().st_size) / (1024.0 ** 3)
    except Exception:
        return 0.0


def format_live_gb_text(gb: float) -> str:
    if gb <= 0:
        return "0.00"
    decimals = 2 if gb < 100 else 1
    return f"{gb:.{decimals}f}"


def file_size_gb_int(file_path: Path) -> str:
    try:
        size = file_path.stat().st_size
    except Exception:
        return "n/a"
    return str(int(size / (1024.0 ** 3)))


def is_valid_iso_mkv(file_path: Path) -> bool:
    try:
        return file_path.is_file() and file_path.stat().st_size >= MIN_VALID_ISO_MKV_BYTES
    except Exception:
        return False


def parse_gb_text(value: str) -> float:
    text = (value or "").strip().replace(",", ".")
    try:
        return float(text)
    except Exception:
        return 0.0


def parse_band_gb_text(value: str) -> float:
    text = (value or "").strip().replace(",", ".")
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)", text)
    if not match:
        return 0.0
    try:
        return float(match.group(1))
    except Exception:
        return 0.0


def normalize_match_token(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (text or "").lower())


def normalize_title_match_text(text: str) -> str:
    base = clean_title_noise(text) or pretty_title(text)
    folded = unicodedata.normalize("NFKD", base)
    folded = folded.encode("ascii", "ignore").decode("ascii")
    folded = re.sub(r"[^a-z0-9]+", " ", folded.lower()).strip()
    return re.sub(r"\s+", " ", folded).strip()


def title_match_tokens(text: str) -> set[str]:
    normalized = normalize_title_match_text(text)
    if not normalized:
        return set()
    return {
        token
        for token in normalized.split()
        if token and token not in TMDB_TITLE_STOPWORDS and not token.isdigit()
    }


def titles_look_compatible(left: str, right: str) -> bool:
    left_norm = normalize_title_match_text(left)
    right_norm = normalize_title_match_text(right)
    if not left_norm or not right_norm:
        return False

    left_compact = normalize_match_token(left_norm)
    right_compact = normalize_match_token(right_norm)
    if left_compact and right_compact and (left_compact in right_compact or right_compact in left_compact):
        return True

    left_tokens = title_match_tokens(left)
    right_tokens = title_match_tokens(right)
    if not left_tokens or not right_tokens:
        return False

    overlap = len(left_tokens & right_tokens)
    if overlap <= 0:
        return False
    if len(left_tokens) >= 3 and overlap < 2:
        return False

    coverage_left = overlap / float(len(left_tokens))
    coverage_right = overlap / float(len(right_tokens))
    if len(left_tokens) <= 2 and overlap >= 1 and max(coverage_left, coverage_right) >= 0.5:
        return True
    return coverage_left >= 0.55 or (overlap >= 2 and (coverage_left >= 0.4 or coverage_right >= 0.4))


def title_match_score(query: str, candidate: str) -> int:
    query_norm = normalize_title_match_text(query)
    candidate_norm = normalize_title_match_text(candidate)
    if not query_norm or not candidate_norm:
        return -1000

    score = 0
    query_compact = normalize_match_token(query_norm)
    candidate_compact = normalize_match_token(candidate_norm)
    if query_compact == candidate_compact:
        score += 260
    elif query_compact and candidate_compact and (
        query_compact in candidate_compact or candidate_compact in query_compact
    ):
        score += 170

    query_tokens = title_match_tokens(query)
    candidate_tokens = title_match_tokens(candidate)
    if query_tokens and candidate_tokens:
        overlap_tokens = query_tokens & candidate_tokens
        overlap = len(overlap_tokens)
        if overlap == 0:
            score -= 130
        else:
            score += overlap * 34
            score += int((overlap / float(len(query_tokens))) * 90.0)
            score += int((overlap / float(len(candidate_tokens))) * 55.0)
            long_overlap = sum(1 for token in overlap_tokens if len(token) >= 5)
            score += long_overlap * 14
            if len(query_tokens) >= 3 and overlap < 2:
                score -= 75

    if titles_look_compatible(query, candidate):
        score += 45

    return score


def pick_best_tmdb_search_result(
    query: str,
    results: list[dict[str, Any]] | Any,
    title_keys: tuple[str, ...],
    year_hint: str = "",
    date_keys: tuple[str, ...] = (),
) -> dict | None:
    if not isinstance(results, list) or not results:
        return None

    hint_year = normalize_year(year_hint)
    best_item: dict[str, Any] | None = None
    best_rank: tuple[int, int, int, int, float, int] | None = None

    for idx, item in enumerate(results):
        if not isinstance(item, dict):
            continue

        raw_titles = [str(item.get(key, "") or "").strip() for key in title_keys]
        candidates = [title for title in raw_titles if title]
        if not candidates:
            continue

        compatible = any(titles_look_compatible(query, cand) for cand in candidates)
        title_score = max(title_match_score(query, cand) for cand in candidates)
        if not compatible:
            title_score -= 120

        year_score = 0
        if hint_year:
            best_delta: int | None = None
            for key in date_keys:
                year_val = year_from_date(str(item.get(key, "") or "").strip())
                year_norm = normalize_year(year_val)
                if not year_norm:
                    continue
                try:
                    delta = abs(int(year_norm) - int(hint_year))
                except Exception:
                    continue
                if best_delta is None or delta < best_delta:
                    best_delta = delta
            if best_delta is not None:
                year_score = max(0, 36 - (best_delta * 4))

        vote_count = int(item.get("vote_count", 0) or 0)
        popularity = float(item.get("popularity", 0.0) or 0.0)
        total = title_score + year_score + min(22, vote_count // 100) + min(12, int(popularity // 12))
        rank = (total, 1 if compatible else 0, title_score, year_score, vote_count, -idx)

        if best_rank is None or rank > best_rank:
            best_rank = rank
            best_item = item

    if best_item is None or best_rank is None:
        return None
    if best_rank[1] == 0 and best_rank[0] < 95:
        return None
    if best_rank[2] < 35:
        return None
    return best_item


def same_size_gb(q_value: str, z_value: str, tol: float = 0.05) -> bool:
    q_num = parse_gb_text(q_value)
    z_num = parse_gb_text(z_value)
    return abs(q_num - z_num) <= tol


def parse_gb_text_strict(value: str) -> tuple[bool, float]:
    text = (value or "").strip().replace(",", ".")
    if not text:
        return False, 0.0
    if not re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", text):
        return False, 0.0
    try:
        return True, float(text)
    except Exception:
        return False, 0.0


def savings_percent_text(q_value: str, z_value: str) -> str:
    q_ok, q_num = parse_gb_text_strict(q_value)
    z_ok, z_num = parse_gb_text_strict(z_value)
    if not q_ok or not z_ok or q_num <= 0.0 or z_num < 0.0:
        return "n/a"
    saved_pct = int(round(((q_num - z_num) / q_num) * 100.0))
    if saved_pct < 0:
        saved_pct = 0
    return f"{saved_pct}%"


def set_row_egb_status_from_sizes(row: dict[str, str], mode: str) -> None:
    mode_norm = (mode or "").strip().lower()
    if mode_norm == "c":
        e_text = "copy"
    elif mode_norm == "f":
        e_text = savings_percent_text(row.get("Groesse", ""), row.get("Z-GB", ""))
    else:
        e_text = (row.get("E-GB", "") or "n/a").strip()

    row["E-GB-STATUS"] = e_text
    row["E-GB"] = e_text
    row["E-GB-BAND"] = ""


def parse_speed_float(speed_text: str) -> float:
    text = (speed_text or "").strip().lower().replace(",", ".")
    if not text:
        return 0.0
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*x", text)
    if not match:
        return 0.0
    try:
        return float(match.group(1))
    except Exception:
        return 0.0


def parse_copy_speed_mib(speed_text: str) -> float:
    text = (speed_text or "").strip().lower().replace(",", ".")
    if not text:
        return 0.0
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*(?:mib|mb)/s", text)
    if not match:
        return 0.0
    try:
        value = float(match.group(1))
    except Exception:
        return 0.0
    return value if value > 0 else 0.0


def parse_fps_float(fps_text: str) -> float:
    text = (fps_text or "").strip().lower().replace(",", ".")
    if not text or text in {"n/a", "na", "unknown"}:
        return 0.0
    match = re.search(r"([0-9]+(?:\.[0-9]+)?)", text)
    if not match:
        return 0.0
    try:
        return float(match.group(1))
    except Exception:
        return 0.0


def parse_rate_ratio(value: str) -> float:
    raw = (value or "").strip()
    if not raw or raw in {"0", "0/0", "N/A", "n/a"}:
        return 0.0
    if "/" in raw:
        left, right = raw.split("/", 1)
        try:
            num = float(left)
            den = float(right)
            return num / den if den else 0.0
        except Exception:
            return 0.0
    try:
        return float(raw)
    except Exception:
        return 0.0


def probe_video_fps(file_path: Path) -> float:
    if not file_path.exists() or not file_path.is_file():
        return 0.0
    cmd = [
        FFPROBE_BIN,
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=avg_frame_rate,r_frame_rate",
        "-of",
        "json",
        str(file_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0 or not result.stdout.strip():
            return 0.0
        payload = json.loads(result.stdout)
        streams = payload.get("streams", [])
        if not isinstance(streams, list) or not streams:
            return 0.0
        stream = streams[0] if isinstance(streams[0], dict) else {}
        avg = parse_rate_ratio(str(stream.get("avg_frame_rate", "") or ""))
        r = parse_rate_ratio(str(stream.get("r_frame_rate", "") or ""))
        if avg > 0:
            return avg
        if r > 0:
            return r
    except Exception:
        return 0.0
    return 0.0


def effective_encode_speed(speed_val: float, progress_sec: float, started_ts: float, now_ts: float) -> float:
    elapsed = max(1.0, now_ts - started_ts)
    measured_speed = 0.0
    if progress_sec > 0 and elapsed >= 5.0:
        measured_speed = progress_sec / elapsed

    if speed_val > 0 and measured_speed > 0:
        # Prefer the progress-derived speed so ETA reacts to real output progress,
        # but keep ffmpeg's self-reported value as a stabilizer.
        if measured_speed < (speed_val * 0.25) or measured_speed > (speed_val * 4.0):
            return speed_val
        return (measured_speed * 0.65) + (speed_val * 0.35)
    if speed_val > 0:
        return speed_val
    if measured_speed > 0:
        return measured_speed
    return 0.0


def format_speed_text(speed_val: float) -> str:
    if speed_val <= 0:
        return "n/a"
    return f"{speed_val:.1f}x"


def format_eta_seconds(seconds: float) -> str:
    if seconds <= 0:
        return "00:00"
    total = int(round(seconds))
    mm = total // 60
    ss = total % 60
    return f"{mm:02d}:{ss:02d}"


def parse_eta_seconds_text(value: str) -> float:
    text = (value or "").strip().lower()
    if not text or text in {"n/a", "na", "copy", "copied", "encoded", "unknown"}:
        return 0.0
    parts = text.split(":")
    try:
        if len(parts) == 2:
            mm = int(parts[0])
            ss = int(parts[1])
            if mm < 0 or ss < 0:
                return 0.0
            return float(mm * 60 + ss)
        if len(parts) == 3:
            hh = int(parts[0])
            mm = int(parts[1])
            ss = int(parts[2])
            if hh < 0 or mm < 0 or ss < 0:
                return 0.0
            return float(hh * 3600 + mm * 60 + ss)
    except Exception:
        return 0.0
    return 0.0


def format_total_eta(seconds: float) -> str:
    if seconds <= 0:
        return "n/a"
    total = int(round(seconds))
    hh = total // 3600
    mm = (total % 3600) // 60
    ss = total % 60
    if hh > 0:
        return f"{hh:02d}:{mm:02d}:{ss:02d}"
    return f"{mm:02d}:{ss:02d}"


def parse_progress_out_time_seconds(key: str, value: str, duration_sec: float, previous_sec: float) -> float:
    try:
        raw = float((value or "").strip())
    except Exception:
        return 0.0
    if raw <= 0:
        return 0.0

    key_l = (key or "").strip().lower()
    candidates: list[float] = []
    if key_l == "out_time_us":
        candidates = [raw / 1_000_000.0]
    elif key_l == "out_time_ms":
        # ffmpeg builds differ: out_time_ms may be microseconds or milliseconds.
        candidates = [raw / 1_000_000.0, raw / 1_000.0]
    else:
        return 0.0

    valid = [c for c in candidates if c > 0]
    if not valid:
        return 0.0

    if duration_sec > 0:
        within = [c for c in valid if c <= (duration_sec * 1.5)]
        if within:
            valid = within

    if previous_sec > 0:
        monotonic = [c for c in valid if c >= previous_sec * 0.95]
        if monotonic:
            valid = monotonic

    # Prefer the largest plausible value to keep progress monotonic and responsive.
    return max(valid)


def estimate_eta_text(duration_sec: float, progress_sec: float, speed_val: float, started_ts: float, now_ts: float, q_gb: float, z_gb: float) -> str:
    eta_text = "n/a"
    elapsed = max(1.0, now_ts - started_ts)

    if duration_sec > 0:
        prog = progress_sec
        size_ratio = 0.0
        if q_gb > 0 and z_gb > 0:
            size_ratio = min(1.0, max(0.0, z_gb / q_gb))

        # Some files report inconsistent progress timestamps (e.g. near duration too early).
        # If timeline-progress and byte-progress diverge strongly, fall back to speed-based ETA.
        if prog > 0 and size_ratio > 0:
            timeline_ratio = min(1.0, max(0.0, prog / duration_sec))
            if timeline_ratio >= 0.95 and size_ratio <= 0.80:
                prog = 0.0
            elif timeline_ratio >= 0.85 and size_ratio <= 0.60:
                prog = 0.0

        if prog > (duration_sec * 3.0):
            prog = 0.0

        effective_speed = speed_val
        if effective_speed <= 0 and prog > 0:
            effective_speed = prog / elapsed

        if prog > 0 and effective_speed > 0:
            remaining = max(0.0, duration_sec - prog)
            eta_sec = remaining / effective_speed
            if eta_sec <= (24 * 3600):
                eta_text = format_eta_seconds(eta_sec)
                return eta_text
        elif effective_speed > 0:
            eta_sec = max(0.0, (duration_sec / effective_speed) - elapsed)
            if eta_sec <= (24 * 3600):
                eta_text = format_eta_seconds(eta_sec)
                return eta_text

    if speed_val > 0 and q_gb > 0 and z_gb > 0:
        ratio = min(0.99, max(0.01, z_gb / q_gb))
        eta_sec = elapsed * (1.0 - ratio) / ratio
        if eta_sec <= (24 * 3600):
            eta_text = format_eta_seconds(eta_sec)

    return eta_text


def build_subtitle_match_keys(source_stem: str) -> list[str]:
    tokens = [t for t in re.split(r"[._\-\s]+", (source_stem or "").lower()) if t]
    if not tokens:
        return []

    noise = {
        "2160p", "1080p", "720p", "480p",
        "x264", "x265", "h264", "h265", "hevc", "avc",
        "german", "dl", "web", "webrip", "webdl", "bluray", "internal",
        "repack", "proper", "readnfo", "rerip", "intention",
    }

    keys: set[str] = set()
    full_key = normalize_match_token(" ".join(tokens))
    if len(full_key) >= 6:
        keys.add(full_key)

    # Relax matching by trimming trailing tokens (e.g. 1080p, release tags).
    for cut in range(1, min(4, len(tokens))):
        key = normalize_match_token(" ".join(tokens[:-cut]))
        if len(key) >= 6:
            keys.add(key)

    cleaned = [t for t in tokens if t not in noise]
    if cleaned:
        key = normalize_match_token(" ".join(cleaned))
        if len(key) >= 6:
            keys.add(key)
        for cut in range(1, min(4, len(cleaned))):
            key = normalize_match_token(" ".join(cleaned[:-cut]))
            if len(key) >= 6:
                keys.add(key)

    return sorted(keys, key=len, reverse=True)


def build_video_match_keys(source_video: Path) -> list[str]:
    keys: set[str] = set(build_subtitle_match_keys(source_video.stem))
    for part in (source_video.parent.name, source_video.parent.parent.name):
        for key in build_subtitle_match_keys(part):
            if len(key) >= 6:
                keys.add(key)
    return sorted(keys, key=len, reverse=True)


def ensure_clean_dir(path: Path) -> None:
    def describe_path(target: Path) -> tuple[str, str, str]:
        owner = "unknown"
        group = "unknown"
        mode = "???"
        try:
            stat_info = target.stat()
            owner = str(stat_info.st_uid)
            group = str(stat_info.st_gid)
            mode = oct(stat_info.st_mode & 0o777)
        except Exception:
            pass
        return owner, group, mode

    if path.exists():
        for child in path.iterdir():
            try:
                if child.is_dir():
                    try:
                        os.chmod(child, 0o700)
                    except Exception:
                        pass
                    shutil.rmtree(child)
                else:
                    try:
                        os.chmod(child, 0o600)
                    except Exception:
                        pass
                    child.unlink()
            except PermissionError as exc:
                owner, group, mode = describe_path(child)
                raise RuntimeError(
                    f"Pfad nicht loeschbar: {child} (owner={owner}, group={group}, mode={mode})"
                ) from exc
        return
    try:
        path.mkdir(parents=True, exist_ok=True)
    except PermissionError as exc:
        parent = path.parent if path.parent != path else path
        owner, group, mode = describe_path(parent)
        raise RuntimeError(
            f"Zielpfad nicht anlegbar: {path} (parent={parent}, owner={owner}, group={group}, mode={mode})"
        ) from exc


def clear_post_action_summary() -> None:
    POST_ACTION_SUMMARY_LINES.clear()


def set_post_action_summary(lines: list[str]) -> None:
    POST_ACTION_SUMMARY_LINES.clear()
    for line in lines:
        text = (line or "").strip()
        if text:
            POST_ACTION_SUMMARY_LINES.append(text)


def resolve_target_out_root(start_folder: Path) -> Path:
    out_root = TARGET_OUT_PATH
    if not out_root.is_absolute():
        out_root = start_folder / out_root
    try:
        return out_root.resolve()
    except Exception:
        return out_root


def resolve_target_manual_root(start_folder: Path) -> Path:
    out_root = resolve_target_out_root(start_folder)
    manual_root = out_root.parent / MANUAL_DIR_NAME
    try:
        return manual_root.resolve()
    except Exception:
        return manual_root


def resolve_target_reenqueue_root(start_folder: Path) -> Path:
    reenqueue_root = TARGET_REENQUEUE_PATH
    if not reenqueue_root.is_absolute():
        reenqueue_root = start_folder / reenqueue_root
    try:
        return reenqueue_root.resolve()
    except Exception:
        return reenqueue_root


def target_out_prefix_for_rows(start_folder: Path) -> Path:
    out_root = resolve_target_out_root(start_folder)
    try:
        rel = out_root.relative_to(start_folder)
        if rel.parts:
            return rel
    except Exception:
        pass
    return out_root


def target_manual_prefix_for_rows(start_folder: Path) -> Path:
    manual_root = resolve_target_manual_root(start_folder)
    try:
        rel = manual_root.relative_to(start_folder)
        if rel.parts:
            return rel
    except Exception:
        pass
    return manual_root


def target_out_label(start_folder: Path) -> str:
    prefix = target_out_prefix_for_rows(start_folder)
    if prefix.is_absolute():
        return prefix.name or "__OUT"
    label = str(prefix).strip()
    return label or "__OUT"


def resolve_target_abs(start_folder: Path, target_rel: str) -> Path:
    candidate = Path((target_rel or "").strip())
    if candidate.is_absolute():
        return candidate
    return start_folder / candidate


def map_target_rel_to_nas_path(target_rel: str, nas_root: Path) -> Path | None:
    rel = Path((target_rel or "").strip().replace("\\", "/"))
    if not rel.parts:
        return None
    parts = list(rel.parts)
    if parts and parts[0].lower() == "__out":
        parts = parts[1:]
    if not parts:
        return None
    head = parts[0].lower()
    tail = Path(*parts[1:]) if len(parts) > 1 else Path()
    if head in {"movie", "filme"}:
        return nas_root / "Filme" / tail
    if head in {"serien", "series"}:
        return nas_root / "Serien" / tail
    return nas_root / Path(*parts)


def sync_out_tree_to_nas(
    start_folder: Path,
    nas_root: Path,
    on_progress: Callable[[str], None] | None = None,
    progress_interval_sec: float = 60.0,
) -> tuple[bool, dict[str, int | float | str]]:
    out_root = resolve_target_out_root(start_folder)
    if not out_root.exists() or not out_root.is_dir():
        return False, {"copied": 0, "failed": 0, "source_files": 0, "error": f"Zielpfad fehlt: {out_root}"}
    mount_ok, mount_error, mount_fs_type = ensure_nas_mount_ready(nas_root)
    if not mount_ok:
        return False, {"copied": 0, "failed": 0, "source_files": 0, "error": mount_error}
    if mount_error:
        try:
            processing_log(f"[SYNC-NAS] {mount_error}")
        except Exception:
            pass

    copied = 0
    reused = 0
    failed = 0
    copied_bytes = 0
    reused_bytes = 0
    total_bytes = 0
    failed_examples: list[str] = []
    started_ts = time.time()
    last_emit_ts = 0.0
    last_speed_sample_ts = started_ts
    last_speed_sample_bytes = 0
    recent_speed_mib = 0.0
    last_progress_message = ""
    fatal_error = ""
    try:
        nas_root.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        return False, {"copied": 0, "failed": 0, "source_files": 0, "error": f"NAS-Ziel nicht schreibbar ({exc})"}

    files_to_copy: list[Path] = []
    for source_file in sorted(out_root.rglob("*"), key=lambda p: str(p).lower()):
        if not source_file.is_file():
            continue
        if source_file.name.startswith("._"):
            continue
        files_to_copy.append(source_file)
        try:
            total_bytes += max(0, int(source_file.stat().st_size))
        except Exception:
            pass

    source_files = len(files_to_copy)
    nas_free_bytes = -1
    try:
        nas_usage = shutil.disk_usage(nas_root)
        nas_free_bytes = int(getattr(nas_usage, "free", -1))
    except Exception:
        nas_free_bytes = -1

    if nas_free_bytes >= 0 and total_bytes > 0 and nas_free_bytes < total_bytes:
        free_gb = nas_free_bytes / (1024.0 ** 3)
        needed_gb = total_bytes / (1024.0 ** 3)
        return False, {
            "copied": 0,
            "failed": source_files or 1,
            "source_files": source_files,
            "total_bytes": total_bytes,
            "copied_bytes": 0,
            "elapsed_sec": 0.0,
            "speed_mib": 0.0,
            "failed_examples": [
                f"NAS-Speicher zu knapp: frei={free_gb:.1f} GB, benoetigt={needed_gb:.1f} GB"
            ],
            "error": f"NAS-Speicher zu knapp: frei={free_gb:.1f} GB, benoetigt={needed_gb:.1f} GB",
        }

    effective_chunk_size = min(max(4 * 1024 * 1024, int(COPY_CHUNK_SIZE_BYTES)), 128 * 1024 * 1024)
    fsync_enabled = should_fsync_copy(nas_root / "__sync_nas_probe__.tmp")
    try:
        fs_type = mount_fs_type or filesystem_type_for_path(nas_root) or "unknown"
        if "smb" in fs_type and effective_chunk_size < (128 * 1024 * 1024):
            # SMB fallback copy path benefits from larger buffered writes.
            effective_chunk_size = 128 * 1024 * 1024
        native_fast = "off"
        if sys.platform == "darwin" and "smb" in fs_type:
            native_fast = "cp -X + COPYFILE_DISABLE (mit auto-fallback)"
        processing_log(
            f"[SYNC-NAS] Einstellungen: chunk={effective_chunk_size // (1024 * 1024)} MiB | "
            f"fsync-mode={COPY_FSYNC_MODE} | fsync={'on' if fsync_enabled else 'off'} | fs={fs_type} | native-fast={native_fast}"
        )
    except Exception:
        pass

    def emit_progress(force: bool = False, bytes_override: int | None = None) -> None:
        nonlocal last_emit_ts, last_speed_sample_ts, last_speed_sample_bytes, recent_speed_mib, last_progress_message
        if on_progress is None:
            return
        now = time.time()
        if not force and (now - last_emit_ts) < max(1.0, float(progress_interval_sec)):
            return
        elapsed = max(0.001, now - started_ts)
        copied_bytes_now = copied_bytes if bytes_override is None else max(0, int(bytes_override))
        if copied_bytes_now <= 0:
            return
        avg_speed_mib = (copied_bytes_now / (1024.0 ** 2)) / elapsed if copied_bytes_now > 0 else 0.0
        delta_bytes = max(0, copied_bytes_now - last_speed_sample_bytes)
        delta_sec = max(0.001, now - last_speed_sample_ts)
        inst_speed_mib = (delta_bytes / (1024.0 ** 2)) / delta_sec if delta_bytes > 0 else 0.0
        if inst_speed_mib > 0:
            if recent_speed_mib > 0:
                recent_speed_mib = (recent_speed_mib * 0.55) + (inst_speed_mib * 0.45)
            else:
                recent_speed_mib = inst_speed_mib
        speed_mib = avg_speed_mib
        if recent_speed_mib > 0 and avg_speed_mib > 0:
            speed_mib = max(recent_speed_mib, (recent_speed_mib * 0.75) + (avg_speed_mib * 0.25))
        elif recent_speed_mib > 0:
            speed_mib = recent_speed_mib
        total_gb = total_bytes / (1024.0 ** 3) if total_bytes > 0 else 0.0
        if total_bytes > 0:
            pct = min(100.0, max(0.0, (copied_bytes_now / total_bytes) * 100.0))
        elif source_files > 0:
            pct = min(100.0, max(0.0, (copied / source_files) * 100.0))
        else:
            pct = 100.0
        # Ignore noisy first bytes on SMB mounts to avoid meaningless 0.0 MB/s / huge ETA logs.
        min_emit_bytes = 64 * 1024 * 1024
        if total_bytes > 0:
            min_emit_bytes = min(min_emit_bytes, max(8 * 1024 * 1024, int(total_bytes * 0.005)))
        if not force and copied_bytes_now < min_emit_bytes:
            last_speed_sample_ts = now
            last_speed_sample_bytes = copied_bytes_now
            return
        eta_text = "n/a"
        if total_bytes > 0 and copied_bytes_now >= total_bytes:
            eta_text = "00:00"
        elif speed_mib >= 1.0 and total_bytes > copied_bytes_now:
            remain_bytes = max(0, total_bytes - copied_bytes_now)
            eta_text = format_eta_seconds(remain_bytes / (speed_mib * 1024.0 * 1024.0))
        message = (
            f"{pct:.1f} % von {total_gb:.1f} GB kopiert - "
            f"Speed = {speed_mib:.1f} MB/s | ETA = {eta_text}"
        )
        if message == last_progress_message:
            last_speed_sample_ts = now
            last_speed_sample_bytes = copied_bytes_now
            return
        try:
            on_progress(message)
        except Exception:
            pass
        last_progress_message = message
        last_emit_ts = now
        last_speed_sample_ts = now
        last_speed_sample_bytes = copied_bytes_now

    for source_file in files_to_copy:
        mount_ok, mount_error, _ = ensure_nas_mount_ready(nas_root)
        if not mount_ok:
            fatal_error = mount_error
            if len(failed_examples) < 5:
                failed_examples.append(mount_error)
            break
        try:
            rel = source_file.relative_to(out_root)
        except Exception:
            failed += 1
            continue
        target_path = map_target_rel_to_nas_path(str(rel), nas_root)
        if target_path is None:
            failed += 1
            continue
        try:
            size_bytes = max(0, int(source_file.stat().st_size))
        except Exception:
            size_bytes = 0
        try:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                target_size = max(0, int(target_path.stat().st_size))
            except Exception:
                target_size = -1
            if target_size >= 0 and target_size == size_bytes and size_bytes > 0:
                copied += 1
                reused += 1
                copied_bytes += size_bytes
                reused_bytes += size_bytes
                emit_progress(force=False)
                continue
            current_file_copied = 0

            def handle_copy_progress(current_bytes: int) -> None:
                nonlocal current_file_copied
                current_file_copied = min(size_bytes, max(0, int(current_bytes)))
                emit_progress(force=False, bytes_override=copied_bytes + current_file_copied)

            copy_file_with_optional_progress(
                source_file,
                target_path,
                chunk_size=effective_chunk_size,
                fsync_enabled=fsync_enabled,
                progress_interval_sec=progress_interval_sec,
                on_bytes_copied=handle_copy_progress,
                preserve_metadata=False,
            )
            copied += 1
            copied_bytes += size_bytes
            emit_progress(force=False)
        except Exception as exc:
            failed += 1
            if len(failed_examples) < 5:
                failed_examples.append(f"{rel}: {exc}")
            mount_ok_after_error, mount_error_after_error, _ = ensure_nas_mount_ready(nas_root)
            if not mount_ok_after_error:
                fatal_error = mount_error_after_error
                if len(failed_examples) < 5:
                    failed_examples.append(mount_error_after_error)
                break

    emit_progress(force=True)
    if fatal_error and failed == 0:
        failed = max(1, source_files - copied)
    elapsed_sec = max(0.0, time.time() - started_ts)
    speed_mib = 0.0
    if copied_bytes > 0 and elapsed_sec > 0:
        speed_mib = (copied_bytes / (1024.0 ** 2)) / elapsed_sec
    return (
        failed == 0,
        {
            "copied": copied,
            "reused": reused,
            "failed": failed,
            "source_files": source_files,
            "total_bytes": total_bytes,
            "copied_bytes": copied_bytes,
            "reused_bytes": reused_bytes,
            "elapsed_sec": round(elapsed_sec, 3),
            "speed_mib": round(speed_mib, 3),
            "failed_examples": failed_examples,
            "error": fatal_error or "; ".join(failed_examples[:3]),
        },
    )


def clear_out_tree(start_folder: Path) -> tuple[bool, str]:
    out_root = resolve_target_out_root(start_folder)
    try:
        ensure_clean_dir(out_root)
        return True, f"Ziel geleert: {out_root}"
    except Exception as exc:
        return False, f"Ziel konnte nicht geleert werden ({exc})"


def count_tree_entries(path: Path) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    if path.is_file() or path.is_symlink():
        return 1, 0
    files = 0
    dirs = 1
    for _, dirnames, filenames in os.walk(path):
        dirs += len(dirnames)
        files += len(filenames)
    return files, dirs


def verify_source_targets_exist(
    rows: list[dict[str, str]],
    start_folder: Path,
    nas_root: Path,
) -> tuple[bool, list[str], int]:
    missing: list[str] = []
    checked = 0
    for row in rows:
        source_rel = (row.get("Quellname", "") or "").strip()
        target_rel = (row.get("Zielname", "") or "").strip()
        if not source_rel or not target_rel:
            continue
        checked += 1

        target_path = Path(target_rel)
        if not target_path.is_absolute():
            target_path = start_folder / target_path
        if target_path.exists() and target_path.is_file():
            continue

        target_on_nas = map_target_rel_to_nas_path(target_rel, nas_root)
        if target_on_nas is not None and target_on_nas.exists() and target_on_nas.is_file():
            continue

        missing.append(f"{source_rel} -> {target_rel}")

    return len(missing) == 0, missing, checked


def delete_source_tree_below_start(start_folder: Path) -> tuple[int, int, int]:
    deleted_files = 0
    deleted_dirs = 0
    failed = 0
    try:
        entries = sorted(start_folder.iterdir(), key=lambda p: str(p).lower())
    except Exception:
        return 0, 0, 1

    skip_top_entries: set[str] = {"__OUT", MANUAL_DIR_NAME, REENQUEUE_DIR_NAME}
    out_root = resolve_target_out_root(start_folder)
    try:
        out_rel = out_root.relative_to(start_folder)
        if out_rel.parts:
            skip_top_entries.add(out_rel.parts[0])
    except Exception:
        pass
    manual_root = resolve_target_manual_root(start_folder)
    try:
        manual_rel = manual_root.relative_to(start_folder)
        if manual_rel.parts:
            skip_top_entries.add(manual_rel.parts[0])
    except Exception:
        pass
    reenqueue_root = resolve_target_reenqueue_root(start_folder)
    try:
        reenqueue_rel = reenqueue_root.relative_to(start_folder)
        if reenqueue_rel.parts:
            skip_top_entries.add(reenqueue_rel.parts[0])
    except Exception:
        pass

    for entry in entries:
        if entry.name in skip_top_entries:
            continue
        try:
            f_count, d_count = count_tree_entries(entry)
            if entry.is_dir() and not entry.is_symlink():
                shutil.rmtree(entry)
            else:
                entry.unlink()
            deleted_files += f_count
            deleted_dirs += d_count
        except Exception:
            failed += 1
    return deleted_files, deleted_dirs, failed


def plex_base_urls(plex_ip: str) -> list[str]:
    raw = (plex_ip or "").strip()
    if not raw:
        return []

    values: list[str] = []
    if raw.startswith("http://") or raw.startswith("https://"):
        parsed = urllib.parse.urlsplit(raw)
        scheme = parsed.scheme or "http"
        alt_scheme = "https" if scheme == "http" else "http"
        host = parsed.hostname or ""
        port = parsed.port
        if host:
            if port:
                values.append(f"{scheme}://{host}:{port}")
                values.append(f"{alt_scheme}://{host}:{port}")
            else:
                values.append(f"{scheme}://{host}:32400")
                values.append(f"{alt_scheme}://{host}:32400")
                values.append(f"{scheme}://{host}")
                values.append(f"{alt_scheme}://{host}")
    else:
        host = raw.rstrip(":").strip()
        if not host:
            return []
        if ":" in host and not host.startswith("["):
            values.append(f"http://{host}")
            values.append(f"https://{host}")
        else:
            values.append(f"http://{host}:32400")
            values.append(f"https://{host}:32400")
            values.append(f"http://{host}")
            values.append(f"https://{host}")

    dedup: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.lower().rstrip("/")
        if key in seen:
            continue
        seen.add(key)
        dedup.append(value.rstrip("/"))
    return dedup


def plex_read_sections(base_url: str, token: str) -> tuple[list[dict[str, str]], str]:
    token_q = urllib.parse.quote(token, safe="")
    url = f"{base_url}/library/sections?X-Plex-Token={token_q}"
    req = urllib.request.Request(url, headers={"Accept": "application/xml"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        payload = resp.read()

    root = ET.fromstring(payload.decode("utf-8", "replace"))
    sections: list[dict[str, str]] = []
    for elem in root.findall(".//Directory"):
        key = (elem.attrib.get("key", "") or "").strip()
        title = (elem.attrib.get("title", "") or elem.attrib.get("titleSort", "") or "").strip() or key
        if not key:
            continue
        sections.append({"key": key, "title": title})
    return sections, url


def plex_refresh_all_libraries(plex_ip: str, token: str) -> tuple[bool, dict[str, Any]]:
    if not token:
        return False, {"error": "Plex-Token fehlt"}
    bases = plex_base_urls(plex_ip)
    if not bases:
        return False, {"error": "Plex-IP fehlt"}

    last_error = "keine Verbindung"
    for base in bases:
        try:
            sections, sections_url = plex_read_sections(base, token)
        except urllib.error.HTTPError as exc:
            code = int(getattr(exc, "code", 0) or 0)
            if code in {401, 403}:
                return False, {"error": f"{base}: HTTP {code} Unauthorized (Plex-Token pruefen)"}
            last_error = f"{base}: HTTP {code or 'Fehler'}"
            continue
        except Exception as exc:
            last_error = f"{base}: {exc}"
            continue

        refreshed = 0
        failed = 0
        refreshed_names: list[str] = []
        failed_names: list[str] = []
        token_q = urllib.parse.quote(token, safe="")
        for section in sections:
            key = (section.get("key", "") or "").strip()
            name = (section.get("title", "") or "").strip() or key
            if not key:
                continue
            refresh_url = f"{base}/library/sections/{urllib.parse.quote(key, safe='')}/refresh?X-Plex-Token={token_q}"
            req = urllib.request.Request(refresh_url, method="GET")
            try:
                with urllib.request.urlopen(req, timeout=20) as resp:
                    _ = resp.read()
                refreshed += 1
                refreshed_names.append(name)
            except Exception:
                failed += 1
                failed_names.append(name)

        return (
            failed == 0,
            {
                "base_url": base,
                "sections_url": sections_url,
                "libraries_total": len(sections),
                "refreshed": refreshed,
                "failed": failed,
                "refreshed_names": refreshed_names,
                "failed_names": failed_names,
            },
        )

    return False, {"error": last_error}


def matching_sidecars_for_video(source_video: Path, extensions: set[str]) -> list[Path]:
    source_stem = source_video.stem
    source_keys = build_video_match_keys(source_video)
    source_stem_lower = source_stem.lower()
    matches: list[Path] = []
    candidates: dict[str, Path] = {}
    try:
        children = sorted(source_video.parent.rglob("*"), key=lambda p: str(p).lower())
    except Exception:
        children = []
    for child in children:
        if child.is_file():
            candidates[str(child).lower()] = child

    for child in candidates.values():
        ext = child.suffix.lower()
        if ext not in extensions:
            continue
        subtitle_stem = child.stem
        subtitle_key = normalize_match_token(subtitle_stem)
        subtitle_stem_lower = subtitle_stem.lower()
        if subtitle_key and any(key in subtitle_key or subtitle_key in key for key in source_keys):
            matches.append(child)
            continue
        if (
            subtitle_stem_lower == source_stem_lower
            or subtitle_stem_lower.startswith(source_stem_lower + ".")
            or subtitle_stem_lower.startswith(source_stem_lower + "_")
            or subtitle_stem_lower.startswith(source_stem_lower + "-")
        ):
            matches.append(child)
    return matches


def matching_subtitles_for_video(source_video: Path) -> list[Path]:
    return matching_sidecars_for_video(source_video, SUBTITLE_EXTENSIONS)


def matching_txts_for_video(source_video: Path) -> list[Path]:
    return matching_sidecars_for_video(source_video, {".txt"})


def matching_nfos_for_video(source_video: Path) -> list[Path]:
    source_stem_lower = source_video.stem.lower()
    matches: list[Path] = []
    seen: set[str] = set()
    try:
        children = sorted(source_video.parent.rglob("*"), key=lambda p: str(p).lower())
    except Exception:
        children = []

    for child in children:
        if not child.is_file() or child.suffix.lower() != ".nfo":
            continue
        nfo_stem_lower = child.stem.lower()
        if (
            nfo_stem_lower == source_stem_lower
            or nfo_stem_lower.startswith(source_stem_lower + ".")
            or nfo_stem_lower.startswith(source_stem_lower + "_")
            or nfo_stem_lower.startswith(source_stem_lower + "-")
        ):
            key = str(child).lower()
            if key in seen:
                continue
            seen.add(key)
            matches.append(child)
    return matches


def read_text_best_effort(path: Path) -> str:
    for enc in ("utf-8", "cp1252", "latin-1"):
        try:
            return path.read_text(encoding=enc)
        except Exception:
            continue
    return ""


def strip_tags(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text or "").strip()


def nfo_labeled_value(text: str, labels: list[str]) -> str:
    for label in labels:
        pattern = rf"(?im)^.*?\b{label}\b[^a-z0-9\n]{{0,80}}[:=]\s*(.+?)\s*$"
        match = re.search(pattern, text)
        if match:
            return match.group(1).strip()
    return ""


def parse_nfo_metadata(nfo_file: Path) -> dict[str, str]:
    text = read_text_best_effort(nfo_file)
    if not text:
        return {}

    title = ""
    year = ""
    imdb = ""
    runtime = ""

    title_patterns = [
        r"(?is)<title>\s*(.*?)\s*</title>",
        r"(?is)<movie(?:name)?>\s*(.*?)\s*</movie(?:name)?>",
        r"(?im)^\s*(?:title|movie\s*name)\s*[:=]\s*(.+?)\s*$",
    ]
    for pattern in title_patterns:
        m = re.search(pattern, text)
        if m:
            title = strip_tags(m.group(1))
            break
    if not title:
        title = strip_tags(nfo_labeled_value(text, [r"title", r"movie\s*name", r"name"]))

    # Prefer movie year from IMDb summary lines (e.g. "IMDb ....: 1975 (8,1/10)")
    imdb_year_line = nfo_labeled_value(text, [r"imdb"])
    m = re.search(r"\b((?:19|20)\d{2})\b", imdb_year_line)
    if m:
        year = m.group(1)

    if not year:
        year_patterns = [
            r"(?is)<year>\s*((?:19|20)\d{2})\s*</year>",
            r"(?im)^\s*year\s*[:=]\s*((?:19|20)\d{2})\s*$",
        ]
        for pattern in year_patterns:
            m = re.search(pattern, text)
            if m:
                year = m.group(1)
                break
    if not year:
        year_line = nfo_labeled_value(text, [r"year", r"release\s*year"])
        m = re.search(r"\b((?:19|20)\d{2})\b", year_line)
        if m:
            year = m.group(1)

    imdb_match = re.search(r"\b(tt\d{7,10})\b", text, flags=re.IGNORECASE)
    if imdb_match:
        imdb = imdb_match.group(1)

    runtime_patterns = [
        r"(?is)<runtime>\s*([0-9]{2,3}(?::[0-5]\d)?)\s*(?:min|mins|minutes)?\s*</runtime>",
        r"(?im)^\s*(?:runtime|duration)\s*[:=]\s*([0-9]{2,3}(?::[0-5]\d)?)",
    ]
    for pattern in runtime_patterns:
        m = re.search(pattern, text)
        if m:
            runtime = m.group(1)
            break
    if not runtime:
        runtime_line = nfo_labeled_value(text, [r"runtime", r"duration"])
        m = re.search(r"([0-9]{2,3}(?::[0-5]\d)?)", runtime_line)
        if m:
            runtime = m.group(1)

    return {
        "title": (title or "").strip(),
        "year": (year or "").strip(),
        "imdb": (imdb or "").strip(),
        "runtime": (runtime or "").strip(),
    }


def normalize_resolution_token(token: str) -> str:
    value = (token or "").strip().lower()
    if value in {"4k", "uhd", "2160p"}:
        return "4k"
    if value in {"1080p", "720p", "480p"}:
        return value
    return "unknown"


def resolution_from_text(text: str) -> str:
    if not text:
        return "unknown"
    token_match = re.search(r"(?i)\b(4k|uhd|2160p|1080p|720p|480p)\b", text)
    if token_match:
        return normalize_resolution_token(token_match.group(1))

    dim_match = re.search(r"(?i)\b(\d{3,4})\s*[xX]\s*(\d{3,4})\b", text)
    if not dim_match:
        return "unknown"
    width = int(dim_match.group(1))
    height = int(dim_match.group(2))
    major = max(width, height)
    if major >= 3000:
        return "4k"
    if major >= 1800:
        return "1080p"
    if major >= 700:
        return "720p"
    return "480p"


def nfo_resolution_hint(source_video: Path) -> str:
    nfo_files = matching_nfos_for_video(source_video)
    for nfo_file in nfo_files:
        text = read_text_best_effort(nfo_file)
        if not text:
            continue
        hint = resolution_from_text(text)
        if hint != "unknown":
            return hint
    return "unknown"


def enrich_row_from_sidecar_nfo(row: dict[str, str], source_video: Path) -> None:
    nfo_files = matching_nfos_for_video(source_video)
    if not nfo_files:
        return

    best: dict[str, str] = {}
    best_score = -1
    for nfo_file in nfo_files:
        meta = parse_nfo_metadata(nfo_file)
        score = sum(1 for key in ("title", "year", "imdb", "runtime") if meta.get(key))
        score = score * 100 + len(meta.get("title", ""))
        if score > best_score:
            best_score = score
            best = meta

    if not best:
        return

    title = clean_title_noise(best.get("title", "")) or best.get("title", "")
    if title:
        row["Name des Film/Serie"] = title

    year_val = normalize_year(best.get("year", ""))
    if year_val:
        row["Erscheinungsjahr"] = year_val

    imdb_val = normalize_imdb_id(best.get("imdb", ""))
    if imdb_val:
        row["IMDB-ID"] = imdb_val

    runtime_val = normalize_runtime(best.get("runtime", ""))
    if runtime_val:
        row["Laufzeit"] = runtime_val


def build_gemini_sidecar_context(source_video: Path) -> str:
    sidecars: list[tuple[str, Path]] = []
    for nfo in matching_nfos_for_video(source_video):
        sidecars.append(("NFO", nfo))
    for txt in matching_txts_for_video(source_video):
        sidecars.append(("TXT", txt))
    if not sidecars:
        return ""

    snippets: list[str] = []
    seen: set[str] = set()
    for label, sidecar in sidecars:
        key = str(sidecar).lower()
        if key in seen:
            continue
        seen.add(key)
        raw = read_text_best_effort(sidecar)
        if not raw:
            continue
        compact = re.sub(r"\s+", " ", strip_tags(raw)).strip()
        if not compact:
            continue
        if len(compact) > 420:
            compact = compact[:420].rstrip() + "..."
        snippets.append(f"{label}:{sidecar.name}={compact}")
        if len(snippets) >= 3:
            break
    return " | ".join(snippets)


def apply_editor_override_row(target_row: dict[str, str], editor_row: dict[str, str]) -> None:
    title = clean_title_noise(editor_row.get("Name des Film/Serie", "")) or (editor_row.get("Name des Film/Serie", "") or "").strip()
    if title:
        target_row["Name des Film/Serie"] = title

    year_val = normalize_year(editor_row.get("Erscheinungsjahr", ""))
    if year_val and year_val != "0000":
        target_row["Erscheinungsjahr"] = year_val

    imdb_val = normalize_imdb_id(editor_row.get("IMDB-ID", ""))
    if imdb_val and imdb_val != "tt0000000":
        target_row["IMDB-ID"] = imdb_val

    season_val = format_season_episode(editor_row.get("Staffel", ""))
    episode_val = format_season_episode(editor_row.get("Episode", ""))
    if season_val and episode_val:
        target_row["Staffel"] = season_val
        target_row["Episode"] = episode_val
    else:
        target_row["Staffel"] = ""
        target_row["Episode"] = ""


def build_target_subtitle_path(source_video: Path, subtitle: Path, target_video: Path) -> Path:
    source_stem = source_video.stem
    subtitle_stem = subtitle.stem
    suffix = ""
    matched = re.match(re.escape(source_stem), subtitle_stem, flags=re.IGNORECASE)
    if matched:
        suffix = subtitle_stem[matched.end():]
    else:
        src_tokens = [t for t in re.split(r"[._\-\s]+", source_stem.lower()) if t]
        sub_tokens = [t for t in re.split(r"[._\-\s]+", subtitle_stem.lower()) if t]
        common = 0
        for s_tok, t_tok in zip(src_tokens, sub_tokens):
            if s_tok != t_tok:
                break
            common += 1
        if common > 0 and common < len(sub_tokens):
            tail = sub_tokens[common:]
            tail = [t for t in tail if t not in {"2160p", "1080p", "720p", "480p", "0p"}]
            if tail:
                suffix = "." + ".".join(tail)
        if not suffix and subtitle_stem:
            suffix = "." + subtitle_stem
    if suffix and suffix[0] not in {".", "_", "-"}:
        suffix = "." + suffix
    return target_video.parent / "Subs" / f"{target_video.stem}{suffix}{subtitle.suffix.lower()}"


def should_copy_subtitle_sidecar(subtitle: Path) -> bool:
    return subtitle.suffix.lower() in SUBTITLE_EXTENSIONS


def build_target_nfo_path(source_video: Path, nfo_file: Path, target_video: Path) -> Path:
    source_stem = source_video.stem
    nfo_stem = nfo_file.stem
    suffix = ""
    matched = re.match(re.escape(source_stem), nfo_stem, flags=re.IGNORECASE)
    if matched:
        suffix = nfo_stem[matched.end():]
    elif nfo_stem:
        suffix = "." + nfo_stem
    if suffix and suffix[0] not in {".", "_", "-"}:
        suffix = "." + suffix
    return target_video.with_name(f"{target_video.stem}{suffix}.nfo")


def probe_codec_from_video(file_path: Path) -> str:
    if not file_path.exists() or not file_path.is_file():
        return "codec"
    cmd = [
        FFPROBE_BIN,
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=codec_name",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(file_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            return "codec"
        codec_raw = (result.stdout or "").strip().lower()
        if not codec_raw:
            return "codec"
    except Exception:
        return "codec"

    if codec_raw in {"h264", "x264", "avc"}:
        return "h264"
    if codec_raw in {"h265", "x265", "hevc"}:
        return "h265"
    if codec_raw.startswith("mpeg2"):
        return "mpeg2"
    return re.sub(r"[^a-z0-9]+", "", codec_raw) or "codec"


def preferred_copy_codec(source_video: Path, target_video: Path | None = None) -> str:
    codec = extract_codec(source_video.name)
    if codec != "codec":
        return codec
    try:
        codec = probe_codec_from_video(source_video)
    except Exception:
        codec = "codec"
    if codec != "codec":
        return codec
    if target_video is not None:
        try:
            codec = probe_codec_from_video(target_video)
        except Exception:
            codec = "codec"
    return codec


def apply_codec_placeholder(target_video: Path, codec: str) -> Path:
    if not re.search(r"(?i)\.codec\.", target_video.name):
        return target_video
    if not codec or codec == "codec":
        return target_video
    new_name = re.sub(r"(?i)\.codec\.", f".{codec}.", target_video.name, count=1)
    renamed = target_video.with_name(new_name)
    target_video.rename(renamed)
    return renamed


def normalize_target_codec_name(target_video: Path, codec: str) -> Path:
    codec_norm = re.sub(r"[^a-z0-9]+", "", (codec or "").lower())
    if not codec_norm or codec_norm == "codec":
        return target_video

    name = target_video.name
    if re.search(rf"(?i)\.{re.escape(codec_norm)}\.", name):
        return target_video
    new_name = re.sub(
        r"(?i)\.(codec|x264|h264|avc|x265|h265|hevc|mpeg2)\.",
        f".{codec_norm}.",
        name,
        count=1,
    )
    if new_name == name:
        new_name = re.sub(r"(\.(?:tt\d{7,10}|n-a)\.[^.]+)$", f".{codec_norm}\\1", name, count=1)
    if new_name == name:
        new_name = re.sub(r"(\.\{[^{}]+\}\.[^.]+)$", f".{codec_norm}\\1", name, count=1)
    if new_name == name:
        new_name = f"{target_video.stem}.{codec_norm}{target_video.suffix}"
    if new_name == name:
        return target_video

    renamed = target_video.with_name(new_name)
    target_video.rename(renamed)
    return renamed


def filesystem_type_for_path(path: Path) -> str:
    try:
        resolved = str(path.resolve())
    except Exception:
        resolved = str(path)
    if resolved in COPY_FS_TYPE_CACHE:
        return COPY_FS_TYPE_CACHE[resolved]

    fs_type = ""

    # Reliable cross-platform fallback: parse `mount` output and pick the
    # longest mountpoint prefix for the target path.
    mount_entries: list[tuple[str, str]] = []
    try:
        result_mount = subprocess.run(["mount"], capture_output=True, text=True, check=False)
        if result_mount.returncode == 0:
            for raw_line in (result_mount.stdout or "").splitlines():
                line = raw_line.strip()
                if not line or " on " not in line:
                    continue

                mount_point = ""
                fstype = ""

                # Linux style: "<src> on <mountpoint> type <fstype> (...)"
                m_linux = re.match(r"^.+ on (.+?) type ([^ ]+) \(.+\)$", line)
                if m_linux:
                    mount_point = m_linux.group(1).replace("\\040", " ")
                    fstype = m_linux.group(2).strip().lower()
                else:
                    # macOS/BSD style: "<src> on <mountpoint> (<fstype>, ...)"
                    m_bsd = re.match(r"^.+ on (.+?) \(([^,) ]+).*\)$", line)
                    if m_bsd:
                        mount_point = m_bsd.group(1).replace("\\040", " ")
                        fstype = m_bsd.group(2).strip().lower()

                if mount_point and fstype:
                    mount_entries.append((mount_point, fstype))
    except Exception:
        mount_entries = []

    def fs_priority(fstype: str) -> int:
        f = (fstype or "").strip().lower()
        if not f:
            return 0
        if any(token in f for token in ("smb", "cifs", "nfs", "afp", "webdav", "sshfs")):
            return 3
        if "autofs" in f:
            return 2
        return 1

    if mount_entries:
        best_mount = ""
        best_type = ""
        for mount_point, fstype in mount_entries:
            if resolved == mount_point or resolved.startswith(mount_point.rstrip("/") + "/"):
                if len(mount_point) > len(best_mount):
                    best_mount = mount_point
                    best_type = fstype
                elif len(mount_point) == len(best_mount) and fs_priority(fstype) > fs_priority(best_type):
                    # Prefer concrete network fs entries (e.g. nfs) over autofs for identical mountpoints.
                    best_mount = mount_point
                    best_type = fstype
        fs_type = best_type

    if not fs_type:
        if sys.platform == "darwin":
            cmd = ["stat", "-f", "%T", resolved]
        else:
            cmd = ["stat", "-f", "-c", "%T", resolved]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if result.returncode == 0:
                fs_type = (result.stdout or "").strip().lower()
        except Exception:
            fs_type = ""

    COPY_FS_TYPE_CACHE[resolved] = fs_type
    return fs_type


def refresh_filesystem_type_for_path(path: Path) -> str:
    try:
        resolved = str(path.resolve())
    except Exception:
        resolved = str(path)
    COPY_FS_TYPE_CACHE.pop(resolved, None)
    return filesystem_type_for_path(path)


def is_network_filesystem_type(fs_type: str) -> bool:
    text = (fs_type or "").strip().lower()
    if not text:
        return False
    return any(token in text for token in ("smb", "cifs", "nfs", "afp", "webdav", "sshfs"))


def normalize_nas_host(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if "://" in raw:
        try:
            parsed = urllib.parse.urlparse(raw)
            if parsed.hostname:
                raw = parsed.hostname
        except Exception:
            pass
    raw = raw.strip().strip("/")
    if "@" in raw:
        raw = raw.rsplit("@", 1)[-1]
    if ":" in raw and raw.count(":") == 1:
        host_part, port_part = raw.rsplit(":", 1)
        if port_part.isdigit():
            raw = host_part
    raw = re.sub(r"[^0-9A-Za-z._-]", "", raw)
    return raw.strip()


def try_activate_autofs_mount(nas_root: Path, mount_root: Path) -> tuple[bool, str, str]:
    if sys.platform != "darwin":
        return False, "", ""
    paths = [nas_root, mount_root]
    fs_candidates: list[str] = []
    for path in paths:
        try:
            _ = path.exists()
            _ = path.is_dir()
        except Exception:
            pass
        try:
            fs_now = refresh_filesystem_type_for_path(path) or ""
            if fs_now:
                fs_candidates.append(fs_now)
        except Exception:
            pass
    for _ in range(24):
        fs_nas = refresh_filesystem_type_for_path(nas_root) or ""
        fs_mount = refresh_filesystem_type_for_path(mount_root) or ""
        active_fs = fs_nas or fs_mount
        if is_network_filesystem_type(fs_nas) or is_network_filesystem_type(fs_mount):
            return True, f"NFS-Auto-Mount erfolgreich: {mount_root} ({active_fs or 'network'}).", active_fs
        time.sleep(0.25)
    fs_hint = ""
    if fs_candidates:
        fs_hint = fs_candidates[-1]
    return False, f"NFS-Auto-Mount fehlgeschlagen: {mount_root} (fs={fs_hint or 'unknown'}).", fs_hint


def nfs_export_candidates_for_share(share_name: str) -> list[str]:
    share = (share_name or "").strip().strip("/")
    if not share:
        return []
    candidates: list[str] = []
    explicit_export = (os.environ.get("MANAGEMOVIE_NFS_EXPORT", "") or "").strip()
    if explicit_export:
        if "{share}" in explicit_export:
            try:
                value = explicit_export.format(share=share)
            except Exception:
                value = explicit_export
            candidates.append(value)
        else:
            candidates.append(explicit_export)
    prefix = (os.environ.get("MANAGEMOVIE_NFS_EXPORT_PREFIX", "") or "").strip().rstrip("/")
    if prefix:
        candidates.append(f"{prefix}/{share}")
    for base in ("/mnt/data", "/volume1", "/export", "/exports", "/srv/nfs"):
        candidates.append(f"{base}/{share}")
    candidates.append(f"/{share}")
    out: list[str] = []
    seen: set[str] = set()
    for entry in candidates:
        value = str(entry or "").strip()
        if not value:
            continue
        if not value.startswith("/"):
            value = "/" + value
        value = re.sub(r"/+", "/", value)
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def try_mount_nfs_share(mount_root: Path) -> tuple[bool, str]:
    if sys.platform != "darwin":
        return False, ""
    share_name = (mount_root.name or "").strip()
    if not share_name:
        return False, "NFS-Mount: Share-Name unklar."
    nas_host = normalize_nas_host(NAS_IP_RAW)
    if not nas_host:
        return False, "NFS-Mount: NAS-IP fehlt."
    exports = nfs_export_candidates_for_share(share_name)
    if not exports:
        return False, f"NFS-Mount: Keine Export-Kandidaten fuer Share {share_name}."

    mount_key = f"nfs|{nas_host}|{str(mount_root)}"
    now = time.time()
    last_attempt = float(NAS_MOUNT_LAST_ATTEMPT_TS.get(mount_key, 0.0) or 0.0)
    if (now - last_attempt) < NAS_MOUNT_RETRY_INTERVAL_SEC:
        remaining = max(0.0, NAS_MOUNT_RETRY_INTERVAL_SEC - (now - last_attempt))
        return False, f"NFS-Mount: Cooldown aktiv ({remaining:.0f}s)."
    NAS_MOUNT_LAST_ATTEMPT_TS[mount_key] = now

    try:
        mount_root.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        return False, f"NFS-Mount: Mountpunkt nicht anlegbar ({exc})."

    mount_cmd = "/sbin/mount_nfs"
    if not Path(mount_cmd).exists():
        mount_cmd = "mount_nfs"
    errors: list[str] = []
    for export_path in exports:
        remote = f"{nas_host}:{export_path}"
        try:
            result = subprocess.run(
                [mount_cmd, "-o", "rw,tcp,hard,intr", remote, str(mount_root)],
                capture_output=True,
                text=True,
                check=False,
                timeout=20,
            )
            if result.returncode == 0:
                for _ in range(24):
                    fs_now = refresh_filesystem_type_for_path(mount_root) or ""
                    if "nfs" in fs_now or is_network_filesystem_type(fs_now):
                        return True, f"NFS-Mount erfolgreich: {mount_root} ({fs_now or 'nfs'})."
                    time.sleep(0.25)
                return False, f"NFS-Mount: mount_nfs meldet OK, aber kein Netzwerk-FS auf {mount_root}."
            stderr = (result.stderr or "").strip()
            stdout = (result.stdout or "").strip()
            detail = stderr or stdout or f"Exit-Code {result.returncode}"
            errors.append(f"{remote} -> {detail}")
        except subprocess.TimeoutExpired:
            errors.append(f"{remote} -> Timeout")
        except Exception as exc:
            errors.append(f"{remote} -> {exc}")
    if errors:
        joined = " | ".join(errors[:3])
        return False, f"NFS-Mount fehlgeschlagen: {joined}"
    return False, f"NFS-Mount fehlgeschlagen fuer {mount_root}"


def try_mount_nas_share(mount_root: Path) -> tuple[bool, str]:
    if sys.platform != "darwin":
        return False, ""
    share_name = (mount_root.name or "").strip()
    if not share_name:
        return False, "Auto-Mount: Share-Name unklar."
    nas_host = normalize_nas_host(NAS_IP_RAW)
    if not nas_host:
        return False, "Auto-Mount: NAS-IP fehlt."

    mount_key = f"{nas_host}|{str(mount_root)}"
    now = time.time()
    last_attempt = float(NAS_MOUNT_LAST_ATTEMPT_TS.get(mount_key, 0.0) or 0.0)
    if (now - last_attempt) < NAS_MOUNT_RETRY_INTERVAL_SEC:
        remaining = max(0.0, NAS_MOUNT_RETRY_INTERVAL_SEC - (now - last_attempt))
        return False, f"Auto-Mount: Cooldown aktiv ({remaining:.0f}s)."
    NAS_MOUNT_LAST_ATTEMPT_TS[mount_key] = now

    mkdir_error = ""
    mountpoint_ready = mount_root.exists()
    if not mountpoint_ready:
        try:
            mount_root.mkdir(parents=True, exist_ok=True)
            mountpoint_ready = True
        except Exception as exc:
            mkdir_error = f"Auto-Mount: Mountpunkt nicht anlegbar ({exc})."

    attempts: list[str] = []
    nas_user = (os.environ.get("MANAGEMOVIE_NAS_USER", "") or "").strip()
    if nas_user:
        attempts.append(f"//{nas_user}@{nas_host}/{share_name}")
    attempts.append(f"//{nas_host}/{share_name}")

    mount_cmd = "/sbin/mount_smbfs"
    if not Path(mount_cmd).exists():
        mount_cmd = "mount_smbfs"
    last_error = ""
    if mountpoint_ready:
        for target_spec in attempts:
            try:
                result = subprocess.run(
                    [mount_cmd, target_spec, str(mount_root)],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=15,
                )
                if result.returncode == 0:
                    for _ in range(20):
                        fs_now = refresh_filesystem_type_for_path(mount_root) or ""
                        if is_network_filesystem_type(fs_now):
                            return True, f"Auto-Mount erfolgreich: {mount_root} ({fs_now})."
                        time.sleep(0.25)
                    return False, f"Auto-Mount: mount_smbfs meldet OK, aber kein Netzwerk-FS auf {mount_root}."
                stderr = (result.stderr or "").strip()
                stdout = (result.stdout or "").strip()
                detail = stderr or stdout or f"Exit-Code {result.returncode}"
                last_error = f"Auto-Mount ({target_spec}) fehlgeschlagen: {detail}"
            except subprocess.TimeoutExpired:
                last_error = f"Auto-Mount ({target_spec}) Timeout."
            except Exception as exc:
                last_error = f"Auto-Mount ({target_spec}) Fehler: {exc}"

    try:
        open_url = f"smb://{nas_host}/{share_name}"
        subprocess.run(
            ["/usr/bin/open", open_url],
            capture_output=True,
            text=True,
            check=False,
            timeout=8,
        )
        for _ in range(24):
            fs_now = refresh_filesystem_type_for_path(mount_root) or ""
            if is_network_filesystem_type(fs_now):
                return True, f"Auto-Mount über open erfolgreich: {mount_root} ({fs_now})."
            time.sleep(0.25)
    except Exception:
        pass

    details = [text for text in (last_error, mkdir_error) if text]
    if details:
        return False, " | ".join(details)
    return False, f"Auto-Mount fehlgeschlagen für smb://{nas_host}/{share_name}"


def ensure_nas_mount_ready(nas_root: Path) -> tuple[bool, str, str]:
    fs_type = refresh_filesystem_type_for_path(nas_root) or ""
    if sys.platform != "darwin":
        return True, "", fs_type

    try:
        resolved = nas_root.resolve()
    except Exception:
        resolved = nas_root
    parts = resolved.parts
    if len(parts) < 3 or parts[1] != "Volumes":
        return True, "", fs_type

    mount_root = Path("/", parts[1], parts[2])
    mount_fs_type = refresh_filesystem_type_for_path(mount_root) or ""
    if is_network_filesystem_type(mount_fs_type):
        return True, "", fs_type or mount_fs_type
    autofs_active = "autofs" in (mount_fs_type or "").lower() or "autofs" in (fs_type or "").lower()
    if autofs_active:
        auto_ok, auto_message, auto_fs = try_activate_autofs_mount(nas_root, mount_root)
        if auto_ok:
            return True, auto_message, auto_fs or fs_type or mount_fs_type
        nfs_ok, nfs_message = try_mount_nfs_share(mount_root)
        if nfs_ok:
            mount_fs_type = refresh_filesystem_type_for_path(mount_root) or mount_fs_type
            detail = auto_message or nfs_message
            return True, detail, fs_type or mount_fs_type
        details = [text for text in (auto_message, nfs_message) if text]
        auto_suffix = f" | {' | '.join(details)}" if details else ""
        return (
            False,
            f"NAS-Share nicht gemountet: {mount_root} (fs={mount_fs_type or fs_type or 'autofs'}){auto_suffix}",
            auto_fs or fs_type or mount_fs_type,
        )
    nfs_ok, nfs_message = try_mount_nfs_share(mount_root)
    if nfs_ok:
        mount_fs_type = refresh_filesystem_type_for_path(mount_root) or mount_fs_type
        return True, nfs_message, fs_type or mount_fs_type
    auto_suffix = f" | {nfs_message}" if nfs_message else ""
    if not mount_root.exists():
        return False, f"NAS-Share fehlt: {mount_root}{auto_suffix}", fs_type or mount_fs_type
    return (
        False,
        f"NAS-Share nicht gemountet: {mount_root} (fs={mount_fs_type or 'unknown'}){auto_suffix}",
        fs_type or mount_fs_type,
    )


def should_probe_runtime_for_source(source_video: Path) -> bool:
    global RUNTIME_PROBE_REPORTED

    mode = RUNTIME_PROBE_MODE
    if mode == "always":
        if not RUNTIME_PROBE_REPORTED:
            msg = "[ANALYZE] Laufzeit-Probe aktiv (MANAGEMOVIE_RUNTIME_PROBE=always)."
            processing_log(msg)
            log("INFO", msg)
            RUNTIME_PROBE_REPORTED = True
        return True
    if mode == "never":
        if not RUNTIME_PROBE_REPORTED:
            msg = "[ANALYZE] Laufzeit-Probe deaktiviert (MANAGEMOVIE_RUNTIME_PROBE=never)."
            processing_log(msg)
            log("INFO", msg)
            RUNTIME_PROBE_REPORTED = True
        return False

    fs_type = filesystem_type_for_path(source_video.parent)
    network_tokens = ("nfs", "smb", "cifs", "afp", "sshfs")
    should_probe = not any(token in fs_type for token in network_tokens)
    if not RUNTIME_PROBE_REPORTED:
        reason = "lokales Dateisystem" if should_probe else f"Netz-Dateisystem erkannt ({fs_type or 'unknown'})"
        msg = f"[ANALYZE] Laufzeit-Probe {'aktiv' if should_probe else 'deaktiviert'} (auto): {reason}."
        processing_log(msg)
        log("INFO", msg)
        RUNTIME_PROBE_REPORTED = True
    return should_probe


def should_fsync_copy(target_video: Path) -> bool:
    mode = COPY_FSYNC_MODE
    if mode == "always":
        return True
    if mode == "never":
        return False
    if sys.platform == "darwin":
        return False

    fs_type = filesystem_type_for_path(target_video.parent)
    network_tokens = ("nfs", "smb", "cifs", "afp", "sshfs")
    if any(token in fs_type for token in network_tokens):
        return False
    return True


def should_use_native_copy(source_file: Path, target_file: Path) -> bool:
    if sys.platform not in {"darwin", "linux"}:
        return False
    return source_file.is_file() and target_file.parent.exists()


def _run_native_cp_copy(source_file: Path, target_file: Path, *, preserve_metadata: bool = True) -> None:
    source_text = str(source_file)
    target_text = str(target_file)
    attempts: list[tuple[list[str], dict[str, str] | None]] = []

    if sys.platform == "darwin" and not preserve_metadata:
        fast_cmd = ["/bin/cp", "-f", "-X", source_text, target_text]
        fast_env = os.environ.copy()
        fast_env["COPYFILE_DISABLE"] = "1"
        attempts.append((fast_cmd, fast_env))
        # Fallback for unstable SMB servers/sessions where fast mode can fail on large files.
        attempts.append((["/bin/cp", "-f", source_text, target_text], None))
    else:
        cmd = ["/bin/cp", "-f"]
        if preserve_metadata:
            cmd.append("-p")
        cmd.extend([source_text, target_text])
        attempts.append((cmd, None))

    last_error: Exception | None = None
    for idx, (cmd, run_env) in enumerate(attempts):
        try:
            source_size = 0
            try:
                source_size = max(0, int(source_file.stat().st_size))
            except Exception:
                source_size = 0
            copy_with_stall_guard(cmd, target_file, source_size, run_env=run_env)
            return
        except Exception as exc:
            last_error = exc
            if idx + 1 < len(attempts):
                try:
                    target_file.unlink()
                except Exception:
                    pass
                continue
            raise

    if last_error is not None:
        raise last_error


def estimate_copy_hard_timeout_seconds(source_size_bytes: int) -> float:
    # Conservative timeout budget: tolerate slow SMB links but avoid infinite hangs.
    if source_size_bytes <= 0:
        return 180.0
    size_mib = float(source_size_bytes) / (1024.0 ** 2)
    expected_min_mib_s = 20.0
    estimated = (size_mib / expected_min_mib_s) * 4.0 + 120.0
    return max(180.0, min(4.0 * 3600.0, estimated))


def copy_with_stall_guard(
    cmd: list[str],
    target_file: Path,
    source_size_bytes: int,
    *,
    run_env: dict[str, str] | None = None,
    stall_timeout_sec: float = 90.0,
) -> None:
    hard_timeout_sec = estimate_copy_hard_timeout_seconds(source_size_bytes)
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=run_env,
    )
    started_ts = time.time()
    last_growth_ts = started_ts
    last_size = -1
    while True:
        rc = process.poll()
        if rc is not None:
            if rc == 0:
                return
            raise subprocess.CalledProcessError(rc, cmd)

        now = time.time()
        copied_size = -1
        try:
            copied_size = max(0, int(target_file.stat().st_size))
        except Exception:
            copied_size = -1
        if copied_size > last_size:
            last_size = copied_size
            last_growth_ts = now

        if (now - last_growth_ts) > stall_timeout_sec:
            try:
                process.terminate()
                process.wait(timeout=3.0)
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass
            raise TimeoutError(f"Copy stalled > {int(stall_timeout_sec)}s")

        if (now - started_ts) > hard_timeout_sec:
            try:
                process.terminate()
                process.wait(timeout=3.0)
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass
            raise TimeoutError(f"Copy timeout > {int(hard_timeout_sec)}s")

        time.sleep(0.5)


def copy_file_with_optional_progress(
    source_file: Path,
    target_file: Path,
    *,
    chunk_size: int,
    fsync_enabled: bool,
    progress_interval_sec: float = 1.0,
    on_bytes_copied: Callable[[int], None] | None = None,
    preserve_metadata: bool = True,
) -> None:
    target_file.parent.mkdir(parents=True, exist_ok=True)

    native_copy_error: Exception | None = None
    if should_use_native_copy(source_file, target_file):
        poll_stop = threading.Event()
        poll_thread: threading.Thread | None = None

        def poll_target_size() -> None:
            last_reported = -1
            interval = max(0.2, float(progress_interval_sec))
            while not poll_stop.wait(interval):
                try:
                    copied_now = max(0, int(target_file.stat().st_size))
                except Exception:
                    continue
                if copied_now == last_reported:
                    continue
                last_reported = copied_now
                if on_bytes_copied is not None:
                    try:
                        on_bytes_copied(copied_now)
                    except Exception:
                        pass

        try:
            if on_bytes_copied is not None:
                poll_thread = threading.Thread(
                    target=poll_target_size,
                    daemon=True,
                    name="native-copy-progress",
                )
                poll_thread.start()
            _run_native_cp_copy(source_file, target_file, preserve_metadata=preserve_metadata)
            if fsync_enabled:
                try:
                    fd = os.open(str(target_file), os.O_RDONLY)
                    try:
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                except Exception:
                    pass
            if on_bytes_copied is not None:
                try:
                    on_bytes_copied(max(0, int(target_file.stat().st_size)))
                except Exception:
                    pass
            return
        except Exception as exc:
            native_copy_error = exc
            try:
                target_file.unlink()
            except Exception:
                pass
        finally:
            poll_stop.set()
            if poll_thread is not None:
                poll_thread.join(timeout=max(1.0, float(progress_interval_sec) + 0.5))

    try:
        with source_file.open("rb") as src_handle, target_file.open("wb") as dst_handle:
            while True:
                chunk = src_handle.read(chunk_size)
                if not chunk:
                    break
                dst_handle.write(chunk)
                if on_bytes_copied is not None:
                    try:
                        on_bytes_copied(max(0, int(dst_handle.tell())))
                    except Exception:
                        pass
            dst_handle.flush()
            if fsync_enabled:
                try:
                    os.fsync(dst_handle.fileno())
                except Exception:
                    pass
        if preserve_metadata:
            shutil.copystat(source_file, target_file)
    except Exception:
        if native_copy_error is not None:
            raise native_copy_error
        raise


def report_copy_settings_once(target_video: Path, fsync_enabled: bool) -> None:
    global COPY_SETTINGS_REPORTED
    if COPY_SETTINGS_REPORTED:
        return
    fs_type = filesystem_type_for_path(target_video.parent) or "unknown"
    fsync_state = "on" if fsync_enabled else "off"
    message = (
        f"[COPY] Einstellungen: chunk={COPY_CHUNK_MIB} MiB | "
        f"fsync-mode={COPY_FSYNC_MODE} | fsync={fsync_state} | fs={fs_type} | "
        f"host_cpu={HOST_CPU_COUNT} | host_ram={HOST_MEMORY_GIB:.1f} GiB | ffmpeg_threads={FFMPEG_THREADS}"
    )
    try:
        processing_log(message)
    except Exception:
        pass
    log("INFO", message)
    COPY_SETTINGS_REPORTED = True


def copy_video_with_progress(
    source_video: Path,
    target_video: Path,
    on_progress: Callable[[str, str, str], None] | None = None,
    progress_interval_sec: float = 1.0,
    chunk_size: int | None = None,
) -> None:
    target_video.parent.mkdir(parents=True, exist_ok=True)
    effective_chunk_size = int(chunk_size or COPY_CHUNK_SIZE_BYTES)
    if effective_chunk_size < (1 * 1024 * 1024):
        effective_chunk_size = 1 * 1024 * 1024
    fsync_enabled = should_fsync_copy(target_video)
    report_copy_settings_once(target_video, fsync_enabled)

    try:
        total_bytes = max(0, int(source_video.stat().st_size))
    except Exception:
        total_bytes = 0

    copied_bytes = 0
    start_ts = time.time()
    last_emit_ts = 0.0
    last_sample_ts = start_ts
    last_sample_bytes = 0
    recent_speed_mib = 0.0

    def emit_progress(force: bool = False) -> None:
        nonlocal last_emit_ts, last_sample_ts, last_sample_bytes, recent_speed_mib
        if on_progress is None:
            return

        now = time.time()
        if not force and (now - last_emit_ts) < progress_interval_sec:
            return

        elapsed = max(0.001, now - start_ts)
        z_gb_text = f"{(copied_bytes / (1024.0 ** 3)):.1f}"
        avg_speed_mib = (copied_bytes / (1024.0 ** 2)) / elapsed if copied_bytes > 0 else 0.0
        delta_bytes = max(0, copied_bytes - last_sample_bytes)
        delta_sec = max(0.001, now - last_sample_ts)
        inst_speed_mib = (delta_bytes / (1024.0 ** 2)) / delta_sec if delta_bytes > 0 else 0.0
        if inst_speed_mib > 0:
            if recent_speed_mib > 0:
                recent_speed_mib = (recent_speed_mib * 0.55) + (inst_speed_mib * 0.45)
            else:
                recent_speed_mib = inst_speed_mib
        display_speed_mib = avg_speed_mib
        if recent_speed_mib > 0 and avg_speed_mib > 0:
            display_speed_mib = max(recent_speed_mib, (recent_speed_mib * 0.75) + (avg_speed_mib * 0.25))
        elif recent_speed_mib > 0:
            display_speed_mib = recent_speed_mib

        if copied_bytes <= 0:
            speed_text = "0.0 MiB/s"
            eta_text = "n/a"
        else:
            if total_bytes > 0:
                pct = min(100.0, max(0.0, (copied_bytes / total_bytes) * 100.0))
                speed_text = f"{pct:.0f}% {display_speed_mib:.1f} MiB/s"
                if copied_bytes < total_bytes and display_speed_mib > 0:
                    remain_bytes = max(0, total_bytes - copied_bytes)
                    eta_sec = remain_bytes / (display_speed_mib * 1024.0 * 1024.0)
                    eta_text = format_eta_seconds(eta_sec)
                else:
                    eta_text = "00:00"
            else:
                speed_text = f"{display_speed_mib:.1f} MiB/s"
                eta_text = "n/a"

        try:
            on_progress(z_gb_text, speed_text, eta_text)
        except Exception:
            pass

        last_emit_ts = now
        last_sample_ts = now
        last_sample_bytes = copied_bytes

    emit_progress(force=True)
    def handle_copy_progress(current_bytes: int) -> None:
        nonlocal copied_bytes
        if total_bytes > 0:
            copied_bytes = min(total_bytes, max(0, int(current_bytes)))
        else:
            copied_bytes = max(copied_bytes, max(0, int(current_bytes)))
        emit_progress(force=False)

    copy_file_with_optional_progress(
        source_video,
        target_video,
        chunk_size=effective_chunk_size,
        fsync_enabled=fsync_enabled,
        progress_interval_sec=progress_interval_sec,
        on_bytes_copied=handle_copy_progress,
        preserve_metadata=True,
    )
    if total_bytes > 0:
        copied_bytes = total_bytes
    else:
        try:
            copied_bytes = max(copied_bytes, max(0, int(target_video.stat().st_size)))
        except Exception:
            pass
    emit_progress(force=True)


def copy_row_payload(
    source_video: Path,
    target_video: Path,
    on_progress: Callable[[str, str, str], None] | None = None,
) -> tuple[str, int, int, Path]:
    copy_video_with_progress(source_video, target_video, on_progress=on_progress)
    target_video = apply_codec_placeholder(target_video, preferred_copy_codec(source_video, target_video))
    copied_subs = 0
    for subtitle in matching_subtitles_for_video(source_video):
        if not should_copy_subtitle_sidecar(subtitle):
            continue
        target_sub = build_target_subtitle_path(source_video, subtitle, target_video)
        target_sub.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(subtitle, target_sub)
        copied_subs += 1
    copied_nfos = 0
    for nfo_file in matching_nfos_for_video(source_video):
        target_nfo = build_target_nfo_path(source_video, nfo_file, target_video)
        target_nfo.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(nfo_file, target_nfo)
        copied_nfos += 1
    return file_size_human(target_video), copied_subs, copied_nfos, target_video


def copy_sidecars_payload(source_video: Path, target_video: Path) -> tuple[int, int]:
    copied_subs = 0
    for subtitle in matching_subtitles_for_video(source_video):
        if not should_copy_subtitle_sidecar(subtitle):
            continue
        target_sub = build_target_subtitle_path(source_video, subtitle, target_video)
        target_sub.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(subtitle, target_sub)
        copied_subs += 1
    copied_nfos = 0
    for nfo_file in matching_nfos_for_video(source_video):
        target_nfo = build_target_nfo_path(source_video, nfo_file, target_video)
        target_nfo.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(nfo_file, target_nfo)
        copied_nfos += 1
    return copied_subs, copied_nfos


def season_fallback_key(row: dict[str, str]) -> str:
    if not is_series_row(row):
        return ""
    title = (row.get("Name des Film/Serie", "") or "").strip()
    season = format_season_episode((row.get("Staffel", "") or "").strip())
    if not title or not season:
        return ""
    return f"{normalize_match_token(title)}|S{season}"


def run_ffmpeg_encode_with_monitor(
    source_video: Path,
    target_video: Path,
    q_gb: float,
    duration_sec: float,
    encoder_mode: str,
    on_status: Callable[..., None],
    on_log_60s: Callable[..., None],
) -> tuple[bool, str]:
    _ok, _reason, resolved_mode = resolve_ffmpeg_runtime_encoder_mode(encoder_mode)
    mode_norm = normalize_ffmpeg_encoder_mode(resolved_mode) or "cpu"
    video_args = ffmpeg_video_encoder_args(mode_norm)
    if mode_norm == "apple":
        video_args.extend(ffmpeg_apple_rate_control_args(source_video, duration_sec, q_gb))

    cmd = [
        FFMPEG_BIN,
        "-hide_banner",
        "-nostdin",
        "-y",
        "-progress",
        "pipe:1",
        "-stats_period",
        "1",
        "-i",
        str(source_video),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-map",
        "0:s?",
        *video_args,
        *ffmpeg_audio_encoder_args(),
        "-c:s",
        "copy",
        str(target_video),
    ]

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
    except FileNotFoundError:
        return False, "ffmpeg fehlt im PATH"

    lines: queue.Queue[str] = queue.Queue()

    def reader() -> None:
        assert process.stdout is not None
        for raw in process.stdout:
            lines.put(raw.strip())

    thread = threading.Thread(target=reader, daemon=True)
    thread.start()

    start_ts = time.time()
    next_log_ts = time.time() + FFMPEG_PROGRESS_LOG_INTERVAL_SEC
    last_status_ts = 0.0
    last_speed_text = "n/a"
    last_fps_text = "n/a"
    source_fps = probe_video_fps(source_video)
    out_time_sec = 0.0
    low_speed_since: float | None = None
    abort_reason = ""
    live_estimate_gb = ""
    live_estimate_band = ""
    smoothed_projection: float | None = None

    while process.poll() is None:
        now = time.time()
        while True:
            try:
                line = lines.get_nowait()
            except queue.Empty:
                break
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if key == "speed":
                last_speed_text = value or "n/a"
            elif key == "fps":
                last_fps_text = value or "n/a"
            elif key in {"out_time_ms", "out_time_us"}:
                parsed_sec = parse_progress_out_time_seconds(key, value, duration_sec, out_time_sec)
                if parsed_sec > 0:
                    out_time_sec = max(out_time_sec, parsed_sec)
            elif key == "out_time":
                match = re.match(r"(\d+):(\d+):(\d+(?:\.\d+)?)", value)
                if match:
                    hh = int(match.group(1))
                    mm = int(match.group(2))
                    ss = float(match.group(3))
                    out_time_sec = max(out_time_sec, hh * 3600 + mm * 60 + ss)

        z_gb = file_size_gb(target_video) if target_video.exists() else 0.0
        z_gb_text = format_live_gb_text(z_gb)
        raw_speed_val = parse_speed_float(last_speed_text)
        fps_val = parse_fps_float(last_fps_text)
        speed_val = effective_encode_speed(raw_speed_val, out_time_sec, start_ts, now)
        if speed_val <= 0 and fps_val > 0 and source_fps > 0:
            speed_val = fps_val / source_fps
        display_speed_text = format_speed_text(speed_val)
        display_fps_text = "n/a"
        if fps_val > 0:
            display_fps_text = str(int(round(fps_val)))
        elif speed_val > 0 and source_fps > 0:
            display_fps_text = str(int(round(speed_val * source_fps)))
        eta_text = estimate_eta_text(
            duration_sec=duration_sec,
            progress_sec=out_time_sec,
            speed_val=speed_val,
            started_ts=start_ts,
            now_ts=now,
            q_gb=q_gb,
            z_gb=z_gb,
        )

        projection_total: float | None = None
        projection_ratio = 0.0
        if duration_sec > 0 and out_time_sec >= 60.0 and z_gb > 0.0:
            projection_total = z_gb * (duration_sec / max(1.0, out_time_sec))
            projection_ratio = min(1.0, max(0.0, out_time_sec / max(1.0, duration_sec)))
        elif duration_sec > 0 and speed_val > 0 and z_gb > 0.0:
            elapsed = max(1.0, now - start_ts)
            projected_total_time = duration_sec / speed_val
            if projected_total_time > 0:
                projection_total = z_gb * (projected_total_time / elapsed)
                projection_ratio = min(1.0, max(0.0, elapsed / projected_total_time))
        elif q_gb > 0 and z_gb > 0.0:
            # Last fallback when no timing data is available yet.
            projection_total = max(z_gb, q_gb)
            projection_ratio = min(1.0, max(0.0, z_gb / max(0.1, projection_total)))

        if projection_total is not None and projection_total > 0:
            if smoothed_projection is None:
                smoothed_projection = projection_total
            else:
                smoothed_projection = (smoothed_projection * 0.80) + (projection_total * 0.20)
            uncertainty_ratio = max(0.04, 0.35 * (1.0 - projection_ratio))
            spread = max(0.1, smoothed_projection * uncertainty_ratio)
            display_estimate = smoothed_projection
            if q_gb > 0:
                display_estimate = min(display_estimate, q_gb)
                max_spread = max(0.0, q_gb - display_estimate)
                spread = min(spread, max_spread)
            live_estimate_gb = f"{display_estimate:.1f}"
            live_estimate_band = f"±{spread:.1f}" if spread >= 0.1 else ""

        if now - last_status_ts >= 1.0:
            on_status(z_gb_text, display_speed_text, eta_text, live_estimate_gb, live_estimate_band, display_fps_text)
            last_status_ts = now
        if now >= next_log_ts:
            on_log_60s(z_gb_text, display_speed_text, eta_text, live_estimate_gb, live_estimate_band, display_fps_text)
            next_log_ts += FFMPEG_PROGRESS_LOG_INTERVAL_SEC

        if q_gb > 0 and z_gb > (q_gb + 0.05):
            abort_reason = "Z-GB > Q-GB"
            process.terminate()
            break

        if 0 < speed_val < 1.0:
            if low_speed_since is None:
                low_speed_since = now
            elif now - low_speed_since >= 120.0:
                abort_reason = "Speed < 1.0x fuer >120s"
                process.terminate()
                break
        else:
            low_speed_since = None

        time.sleep(1.0)

    rc = process.wait()
    thread.join(timeout=1.0)

    z_gb = file_size_gb(target_video) if target_video.exists() else 0.0
    z_gb_text = format_live_gb_text(z_gb)
    raw_speed_val = parse_speed_float(last_speed_text)
    fps_val = parse_fps_float(last_fps_text)
    speed_val = effective_encode_speed(raw_speed_val, out_time_sec, start_ts, time.time())
    if speed_val <= 0 and fps_val > 0 and source_fps > 0:
        speed_val = fps_val / source_fps
    display_speed_text = format_speed_text(speed_val)
    display_fps_text = "n/a"
    if fps_val > 0:
        display_fps_text = str(int(round(fps_val)))
    elif speed_val > 0 and source_fps > 0:
        display_fps_text = str(int(round(speed_val * source_fps)))
    eta_text = estimate_eta_text(
        duration_sec=duration_sec,
        progress_sec=out_time_sec,
        speed_val=speed_val,
        started_ts=start_ts,
        now_ts=time.time(),
        q_gb=q_gb,
        z_gb=z_gb,
    )
    on_status(z_gb_text, display_speed_text, eta_text, live_estimate_gb, live_estimate_band, display_fps_text)

    if abort_reason:
        return False, abort_reason
    if rc != 0:
        return False, f"ffmpeg Exit-Code {rc}"
    if not target_video.exists():
        return False, "Zieldatei fehlt nach ffmpeg"
    return True, ""


def _percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    p = max(0.0, min(1.0, p))
    pos = p * (len(values) - 1)
    lo = int(pos)
    hi = min(len(values) - 1, lo + 1)
    frac = pos - lo
    return values[lo] * (1.0 - frac) + values[hi] * frac


def _robust_video_bitrate_bps(samples_bps: list[float]) -> tuple[float, float, float]:
    ordered = sorted(x for x in samples_bps if x > 0)
    if not ordered:
        return 0.0, 0.0, 0.0
    if len(ordered) >= 6:
        cut = max(1, int(len(ordered) * 0.15))
        trimmed = ordered[cut:-cut] if len(ordered) > (2 * cut) else ordered
    elif len(ordered) >= 4:
        trimmed = ordered[1:-1]
    else:
        trimmed = ordered

    mean_trimmed = sum(trimmed) / float(len(trimmed))
    median = _percentile(ordered, 0.50)
    low = _percentile(ordered, 0.20)
    high = _percentile(ordered, 0.80)
    estimate = (mean_trimmed * 0.70) + (median * 0.30)
    if estimate <= 0:
        estimate = mean_trimmed
    return estimate, low, max(high, estimate)


def _probe_duration_audio_sub_bps(file_path: Path) -> tuple[float, float, float]:
    ffprobe_cmd = [
        FFPROBE_BIN,
        "-v",
        "error",
        "-show_entries",
        "format=duration,bit_rate:stream=codec_type,bit_rate",
        "-of",
        "json",
        str(file_path),
    ]
    try:
        result = subprocess.run(ffprobe_cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0 or not result.stdout.strip():
            return 0.0, 0.0, 0.0
        payload = json.loads(result.stdout)
    except Exception:
        return 0.0, 0.0, 0.0

    duration = float(payload.get("format", {}).get("duration", 0) or 0.0)
    if duration <= 0:
        return 0.0, 0.0, 0.0

    audio_bps = 0.0
    subtitle_bps = 0.0
    video_bps = 0.0
    streams = payload.get("streams", [])
    if isinstance(streams, list):
        for stream in streams:
            if not isinstance(stream, dict):
                continue
            codec_type = str(stream.get("codec_type", "") or "").strip().lower()
            bit_rate_raw = stream.get("bit_rate", 0)
            try:
                bit_rate = float(bit_rate_raw or 0)
            except Exception:
                bit_rate = 0.0
            if bit_rate <= 0:
                continue
            if codec_type == "audio":
                audio_bps += bit_rate
            elif codec_type == "subtitle":
                subtitle_bps += bit_rate
            elif codec_type == "video":
                video_bps += bit_rate

    if audio_bps <= 0:
        try:
            total_bps = float(payload.get("format", {}).get("bit_rate", 0) or 0)
        except Exception:
            total_bps = 0.0
        if total_bps > 0 and video_bps > 0 and total_bps > video_bps:
            residual = max(0.0, total_bps - video_bps)
            # Conservative fallback split when per-stream audio bitrate is unavailable.
            audio_bps = residual * 0.95
            subtitle_bps = max(subtitle_bps, residual * 0.05)

    return duration, audio_bps, subtitle_bps


def estimate_target_size_details(file_path: Path, step_log: Callable[[str], None] | None = None, encoder_mode: str = "hardware") -> dict[str, str]:
    result = {
        "estimate_gb": "n/a",
        "low_gb": "n/a",
        "high_gb": "n/a",
        "band_gb": "",
        "band_text": "",
        "display": "n/a",
    }
    if not file_path.exists() or not file_path.is_file():
        return result
    _encoder_ok, _encoder_reason, runtime_encoder_mode = resolve_ffmpeg_runtime_encoder_mode(encoder_mode)

    duration, audio_bps, subtitle_bps = _probe_duration_audio_sub_bps(file_path)
    if duration <= 0:
        return result

    try:
        source_q_gb = float(file_path.stat().st_size) / (1024.0 ** 3)
    except Exception:
        source_q_gb = 0.0

    sample_seconds = min(EST_SAMPLE_SECONDS, max(30.0, duration * 0.10))
    if duration < sample_seconds:
        try:
            size = float(file_path.stat().st_size)
            gb = size / (1024.0 ** 3)
            result["estimate_gb"] = f"{gb:.1f}"
            result["low_gb"] = result["estimate_gb"]
            result["high_gb"] = result["estimate_gb"]
            result["display"] = result["estimate_gb"]
        except Exception:
            pass
        return result

    start_points: list[tuple[float, float]] = []
    max_start = max(0.0, duration - sample_seconds)
    for ratio in EST_SAMPLE_RATIOS:
        start = max(0.0, min(duration * ratio, max_start))
        start_points.append((ratio, start))

    sample_video_bps: list[float] = []
    with tempfile.TemporaryDirectory(prefix="managemovie_est_") as tmp_dir:
        for idx, (ratio, start) in enumerate(start_points, start=1):
            tmp_out = Path(tmp_dir) / f"sample_v_{idx}.mkv"
            if step_log is not None:
                step_log(
                    f"Fenster {idx}/{len(start_points)}: {int(ratio * 100)}% bei t={int(start)}s, Dauer={int(sample_seconds)}s"
                )
            ffmpeg_cmd = [
                FFMPEG_BIN,
                "-hide_banner",
                "-nostdin",
                "-progress",
                "pipe:1",
                "-stats_period",
                "1",
                "-ss",
                str(int(start)),
                "-i",
                str(file_path),
                "-map",
                "0:v:0",
                "-an",
                "-sn",
                "-t",
                str(int(sample_seconds)),
                *ffmpeg_video_encoder_args(runtime_encoder_mode),
                *(
                    ffmpeg_apple_rate_control_args(file_path, duration, source_q_gb)
                    if (normalize_ffmpeg_encoder_mode(runtime_encoder_mode) or "cpu") == "apple"
                    else []
                ),
                "-y",
                str(tmp_out),
            ]

            process = subprocess.Popen(
                ffmpeg_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                bufsize=1,
            )

            lines: queue.Queue[str] = queue.Queue()

            def reader() -> None:
                assert process.stdout is not None
                for raw_line in process.stdout:
                    lines.put((raw_line or "").strip())

            reader_thread = threading.Thread(target=reader, daemon=True)
            reader_thread.start()

            out_time_sec = 0.0
            window_started_ts = time.time()
            last_heartbeat_ts = 0.0
            rc: int | None = None

            while True:
                while True:
                    try:
                        line = lines.get_nowait()
                    except queue.Empty:
                        break
                    if not line or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    parsed_sec = parse_progress_out_time_seconds(key, value, sample_seconds, out_time_sec)
                    if parsed_sec > out_time_sec:
                        out_time_sec = parsed_sec

                now = time.time()
                if step_log is not None and (now - last_heartbeat_ts) >= 10.0:
                    pct = min(100.0, max(0.0, (out_time_sec / max(1.0, sample_seconds)) * 100.0))
                    eta_text = format_eta_seconds(max(0.0, sample_seconds - out_time_sec))
                    run_sec = int(max(0.0, now - window_started_ts))
                    step_log(f"Fenster {idx}: laeuft {pct:.0f}% | ETA {eta_text} | Laufzeit {run_sec}s")
                    last_heartbeat_ts = now

                rc = process.poll()
                if rc is not None:
                    break
                time.sleep(0.5)

            try:
                if process.stdout is not None:
                    process.stdout.close()
            except Exception:
                pass
            reader_thread.join(timeout=1.0)

            if (rc or 0) != 0 or not tmp_out.exists():
                if step_log is not None:
                    step_log(f"Fenster {idx}: ffmpeg fehlgeschlagen")
                continue
            try:
                sample_size = float(tmp_out.stat().st_size)
                sample_bps = (sample_size * 8.0) / max(1.0, sample_seconds)
                sample_video_bps.append(sample_bps)
                if step_log is not None:
                    step_log(f"Fenster {idx}: OK, Video-Sample={sample_size / (1024.0 ** 2):.1f}MB")
            except Exception:
                if step_log is not None:
                    step_log(f"Fenster {idx}: Dateigroesse nicht lesbar")

    if not sample_video_bps:
        return result

    est_video_bps, low_video_bps, high_video_bps = _robust_video_bitrate_bps(sample_video_bps)
    if est_video_bps <= 0:
        return result

    overhead_bytes = 4.0 * 1024.0 * 1024.0
    est_total_bytes = ((est_video_bps * duration) / 8.0) * EST_VIDEO_OVERHEAD_FACTOR
    est_total_bytes += ((audio_bps + subtitle_bps) * duration) / 8.0
    est_total_bytes += overhead_bytes

    low_total_bytes = ((low_video_bps * duration) / 8.0) * EST_VIDEO_OVERHEAD_FACTOR
    low_total_bytes += ((audio_bps + subtitle_bps) * duration) / 8.0
    low_total_bytes += overhead_bytes

    high_total_bytes = ((high_video_bps * duration) / 8.0) * EST_VIDEO_OVERHEAD_FACTOR
    high_total_bytes += ((audio_bps + subtitle_bps) * duration) / 8.0
    high_total_bytes += overhead_bytes

    est_gb = est_total_bytes / (1024.0 ** 3)
    low_gb = max(0.0, low_total_bytes / (1024.0 ** 3))
    high_gb = max(low_gb, high_total_bytes / (1024.0 ** 3))
    spread = max(0.1, (high_gb - low_gb) / 2.0)

    result["estimate_gb"] = f"{est_gb:.1f}"
    result["low_gb"] = f"{low_gb:.1f}"
    result["high_gb"] = f"{high_gb:.1f}"
    result["band_gb"] = f"{spread:.1f}"
    result["band_text"] = f"±{spread:.1f}"
    result["display"] = f"{est_gb:.1f}{result['band_text']}"
    return result


def estimate_target_size_gb(file_path: Path, step_log: Callable[[str], None] | None = None, encoder_mode: str = "hardware") -> str:
    return estimate_target_size_details(file_path, step_log=step_log, encoder_mode=encoder_mode).get("estimate_gb", "n/a")


def normalize_imdb_id(value: str) -> str:
    if not value:
        return ""
    raw = value.strip().lower()
    if raw in {"none", "null", "n/a", "na", "unbekannt", "unknown"}:
        return ""
    foreign_match = re.search(r"\b([a-z]{2})(\d{7,10})\b", raw)
    if foreign_match and foreign_match.group(1) != "tt":
        return ""
    match = re.search(r"(tt\d{7,10})", raw)
    imdb_id = match.group(1) if match else ""
    if not imdb_id:
        numeric = re.search(r"(?<!\d)(\d{7,10})(?!\d)", raw)
        if numeric:
            imdb_id = f"tt{numeric.group(1)}"
    if not imdb_id:
        return ""
    if imdb_id in {"tt1234567", "tt0000000"}:
        return ""
    return imdb_id


def normalize_year(value: str) -> str:
    if not value:
        return ""
    value = value.strip()
    if value.lower() in {"none", "null", "n/a", "na", "unbekannt", "unknown"}:
        return ""
    if value == "0000":
        return "0000"
    if re.fullmatch(r"(18|19|20)\d{2}", value):
        return value
    if re.fullmatch(r"(18|19|20)\d{2}\s*/\s*(18|19|20)\d{2}", value):
        return value.replace(" ", "")
    match = re.search(r"(18|19|20)\d{2}", value)
    return match.group(0) if match else ""


def source_looks_series(source_name: str) -> bool:
    season, episode = extract_season_episode_from_source(source_name)
    return bool(season and episode)


def reconcile_series_title_with_source(row: dict[str, str]) -> None:
    source_name = row.get("Quellname", "")
    if not source_looks_series(source_name):
        return

    guessed_title = clean_title_noise(series_title_from_source(source_name))
    if not guessed_title:
        guess = normalize_title_guess(source_name)
        guessed_title = clean_title_noise(guess.get("title", "")) or guess.get("title", "")
    guessed_title = guessed_title.strip()
    if not guessed_title:
        return

    current_title = (row.get("Name des Film/Serie", "") or "").strip()
    if not current_title:
        row["Name des Film/Serie"] = guessed_title
        return

    guess_key = normalize_match_token(guessed_title)
    current_key = normalize_match_token(current_title)
    if not guess_key or not current_key:
        row["Name des Film/Serie"] = guessed_title
        return

    if guess_key in current_key or current_key in guess_key:
        return

    # Hard mismatch (e.g. wrong title from model): trust source-derived series title,
    # but keep year/imdb so validated metadata is not lost.
    row["Name des Film/Serie"] = guessed_title


def apply_known_series_overrides(row: dict[str, str]) -> None:
    source_name = row.get("Quellname", "") or ""
    title_current = clean_title_noise((row.get("Name des Film/Serie", "") or "").strip())
    title_guess = clean_title_noise(normalize_title_guess(source_name).get("title", ""))
    keys = {
        normalize_match_token(title_current),
        normalize_match_token(title_guess),
    }
    if any(k and ("thelastofus" in k or "lastofus" in k) for k in keys):
        row["Name des Film/Serie"] = "The Last of Us"
        if source_looks_series(source_name) or is_series_row(row):
            row["Erscheinungsjahr"] = "2023"


def apply_row_normalization(row: dict[str, str]) -> None:
    name = (row.get("Name des Film/Serie", "") or "").strip()
    if name.lower() in {"none", "null", "n/a", "na", "unbekannt", "unknown"}:
        name = ""
    row["Name des Film/Serie"] = name
    row["Erscheinungsjahr"] = normalize_year(row.get("Erscheinungsjahr", ""))
    row["Staffel"] = format_season_episode((row.get("Staffel", "") or "").strip())
    row["Episode"] = format_season_episode((row.get("Episode", "") or "").strip())
    row["Laufzeit"] = normalize_runtime(row.get("Laufzeit", ""))
    row["IMDB-ID"] = normalize_imdb_id(row.get("IMDB-ID", ""))
    apply_known_series_overrides(row)


def row_quality_score(row: dict[str, str]) -> float:
    checks = []
    checks.append(bool((row.get("Name des Film/Serie", "") or "").strip()))
    checks.append(bool(normalize_year(row.get("Erscheinungsjahr", ""))))
    checks.append(bool(normalize_runtime(row.get("Laufzeit", ""))))
    checks.append(bool(normalize_imdb_id(row.get("IMDB-ID", ""))))

    if source_looks_series(row.get("Quellname", "")):
        checks.append(bool((row.get("Staffel", "") or "").strip()))
        checks.append(bool((row.get("Episode", "") or "").strip()))

    if not checks:
        return 0.0
    return sum(1 for item in checks if item) / float(len(checks))


def dataset_quality_ratio(rows: list[dict[str, str]]) -> float:
    if not rows:
        return 0.0
    passed = sum(1 for row in rows if row_quality_score(row) >= 0.60)
    return passed / float(len(rows))


def normalized_series_key(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9 ]+", " ", (name or "").lower())
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned


def apply_series_metadata(row: dict[str, str]) -> None:
    series_key = normalized_series_key(row.get("Name des Film/Serie", ""))
    meta = SERIES_METADATA.get(series_key)
    if not meta:
        return

    season = row.get("Staffel", "").strip()
    year_by_season = meta.get("year_by_season", {})
    year_val = year_by_season.get(str(int(season))) if season.isdigit() else ""
    if not year_val:
        year_val = year_by_season.get(season, "")
    fill_missing(row, "Erscheinungsjahr", year_val)
    fill_missing(row, "Laufzeit", meta.get("default_runtime_min", ""))
    fill_missing(row, "IMDB-ID", meta.get("imdb_id", ""))


def year_start_value(value: str) -> int | None:
    norm = normalize_year(value)
    if re.fullmatch(r"(19|20)\d{2}", norm):
        return int(norm)
    if re.fullmatch(r"(19|20)\d{2}/(19|20)\d{2}", norm):
        return int(norm.split("/")[0])
    return None


def series_group_key(row: dict[str, str]) -> str:
    title = series_title_from_source(row.get("Quellname", ""))
    if not title:
        title = (row.get("Name des Film/Serie", "") or "").strip()
    if not title:
        title = normalize_title_guess(row.get("Quellname", "")).get("title", "")
    title = clean_title_noise(title) or title
    return normalized_series_key(title)


def harmonize_series_titles(rows: list[dict[str, str]]) -> None:
    groups: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        source_name = row.get("Quellname", "") or ""
        if not is_series_row(row) and not source_looks_series(source_name):
            continue
        key = series_group_key(row)
        if not key:
            continue
        groups.setdefault(key, []).append(row)

    if not groups:
        return

    changed = 0
    for _, group in groups.items():
        imdb_titles: dict[str, int] = {}
        source_titles: dict[str, int] = {}
        fallback_titles: dict[str, int] = {}

        for row in group:
            imdb_id = normalize_imdb_id(row.get("IMDB-ID", ""))
            cur_title = clean_title_noise((row.get("Name des Film/Serie", "") or "").strip())
            if imdb_id and cur_title:
                imdb_titles[cur_title] = imdb_titles.get(cur_title, 0) + 1

            src_title = series_title_from_source(row.get("Quellname", ""))
            if src_title:
                source_titles[src_title] = source_titles.get(src_title, 0) + 1

            if cur_title:
                fallback_titles[cur_title] = fallback_titles.get(cur_title, 0) + 1

        def _title_rank(item: tuple[str, int]) -> tuple[int, int, int, int]:
            title, count = item
            has_upper = 1 if any(ch.isupper() for ch in title) else 0
            title_case = 1 if title == title.title() else 0
            return (count, has_upper, title_case, len(title))

        canonical = ""
        if imdb_titles:
            canonical = max(imdb_titles.items(), key=_title_rank)[0]
        elif fallback_titles:
            canonical = max(fallback_titles.items(), key=_title_rank)[0]
        elif source_titles:
            canonical = max(source_titles.items(), key=_title_rank)[0]
        elif fallback_titles:
            canonical = max(fallback_titles.items(), key=_title_rank)[0]

        if not canonical:
            continue

        canonical_key = normalize_match_token(canonical)
        for row in group:
            current = (row.get("Name des Film/Serie", "") or "").strip()
            if current != canonical or normalize_match_token(current) != canonical_key:
                row["Name des Film/Serie"] = canonical
                changed += 1

    if changed > 0:
        log("INFO", f"Serientitel-Harmonisierung: {changed} Zeile(n) auf Serientitel gesetzt.")


def harmonize_series_start_year(rows: list[dict[str, str]]) -> None:
    groups: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        source_name = row.get("Quellname", "") or ""
        if not is_series_row(row) and not source_looks_series(source_name):
            continue
        key = series_group_key(row)
        if not key:
            continue
        groups.setdefault(key, []).append(row)

    if not groups:
        return

    lookup_cache: dict[str, tuple[int | None, str]] = {}
    changed = 0
    imdb_aligned = 0

    for _, group in groups.items():
        known_years: list[int] = []
        imdb_counts: dict[str, int] = {}
        source_year_counts: dict[int, int] = {}
        lookup_title = ""

        for row in group:
            year_num = year_start_value(row.get("Erscheinungsjahr", ""))
            if year_num:
                known_years.append(year_num)
            imdb_num = normalize_imdb_id(row.get("IMDB-ID", ""))
            if imdb_num:
                imdb_counts[imdb_num] = imdb_counts.get(imdb_num, 0) + 1
            source_hint = year_start_value(source_year_hint(row.get("Quellname", "")))
            if source_hint:
                source_year_counts[source_hint] = source_year_counts.get(source_hint, 0) + 1
            if not lookup_title:
                raw_title = (row.get("Name des Film/Serie", "") or "").strip()
                if not raw_title:
                    raw_title = normalize_title_guess(row.get("Quellname", "")).get("title", "")
                lookup_title = clean_title_noise(raw_title) or raw_title

        lookup_year: int | None = None
        lookup_imdb = ""
        if lookup_title:
            cache_key = lookup_title.lower()
            if cache_key not in lookup_cache:
                yr, imdb_id = imdb_title_lookup(lookup_title, True)
                lookup_cache[cache_key] = (year_start_value(yr), normalize_imdb_id(imdb_id))
            lookup_year, lookup_imdb = lookup_cache[cache_key]

        source_year = max(source_year_counts.items(), key=lambda item: (item[1], item[0]))[0] if source_year_counts else None
        canonical_year = (
            source_year
            if source_year is not None
            else (lookup_year if lookup_year is not None else (min(known_years) if known_years else None))
        )
        canonical_imdb = ""
        if lookup_imdb:
            canonical_imdb = lookup_imdb
        elif imdb_counts:
            top_imdb, top_count = max(imdb_counts.items(), key=lambda item: item[1])
            if top_count > 1 or len(imdb_counts) == 1:
                canonical_imdb = top_imdb
        if canonical_year is None:
            canonical_text = ""
        else:
            canonical_text = str(canonical_year)
        for row in group:
            if canonical_year is not None:
                prev = year_start_value(row.get("Erscheinungsjahr", ""))
                if prev != canonical_year:
                    row["Erscheinungsjahr"] = canonical_text
                    changed += 1
            if canonical_imdb:
                current_imdb = normalize_imdb_id(row.get("IMDB-ID", ""))
                current_count = imdb_counts.get(current_imdb, 0) if current_imdb else 0
                if current_imdb != canonical_imdb and (
                    not current_imdb
                    or current_count <= 1
                    or bool(lookup_imdb)
                ):
                    row["IMDB-ID"] = canonical_imdb
                    imdb_aligned += 1

    if changed > 0 or imdb_aligned > 0:
        log("INFO", f"Serienstart-Harmonisierung: Jahr angepasst={changed}, IMDb angepasst={imdb_aligned}")


def _imdb_suggestion_lookup(query: str, series_hint: bool, year_hint: str = "", _depth: int = 0) -> tuple[str, str]:
    cleaned = re.sub(r"\s+", " ", (query or "").strip())
    if not cleaned:
        return "", ""
    first = re.sub(r"[^a-z0-9]", "", cleaned.lower())[:1] or "a"
    items: list[dict[str, Any]] = []
    for slug_source in (cleaned, cleaned.lower()):
        slug = urllib.parse.quote(slug_source)
        url = f"https://v2.sg.media-imdb.com/suggestion/{first}/{slug}.json"
        try:
            data = fetch_json(url)
        except Exception:
            continue
        payload = data.get("d", [])
        if isinstance(payload, list) and payload:
            items = [entry for entry in payload if isinstance(entry, dict)]
            if items:
                break
    if not isinstance(items, list) or not items:
        return "", ""

    hint = normalize_year(year_hint)

    def score(entry: dict[str, Any]) -> int:
        label = str(entry.get("l", "")).strip().lower()
        query = cleaned.lower()
        kind = str(entry.get("q", "")).lower()
        entry_year = normalize_year(str(entry.get("y", "")).strip())
        kind_score = 0
        if series_hint and "tv" in kind:
            kind_score = 25
        elif (not series_hint) and ("movie" in kind or kind == "feature"):
            kind_score = 25
        elif "tv" not in kind and "movie" not in kind:
            kind_score = 8
        title_score = 0
        if label == query:
            title_score += 120
        elif query in label or label in query:
            title_score += 80

        q_tokens = set(re.findall(r"[a-z0-9]+", query))
        l_tokens = set(re.findall(r"[a-z0-9]+", label))
        if q_tokens and l_tokens:
            overlap = len(q_tokens & l_tokens) / float(len(q_tokens | l_tokens))
            title_score += int(overlap * 70.0)

        year_score = 0
        if hint and entry_year:
            try:
                delta = abs(int(entry_year) - int(hint))
                year_score = max(0, 35 - delta * 3)
            except Exception:
                year_score = 0
        return title_score + kind_score + year_score

    best = max(items, key=score)
    imdb_id = normalize_imdb_id(str(best.get("id", "")).strip())
    year = normalize_year(str(best.get("y", "")).strip())
    if imdb_id or year:
        return year, imdb_id
    if _depth < 1:
        support_blob = str(best.get("s", "") or "").strip()
        if not support_blob:
            try:
                support_blob = json.dumps(best, ensure_ascii=False)
            except Exception:
                support_blob = str(best)
        support_text = re.sub(r"\s+", " ", support_blob)
        rel_title = ""
        rel_year = ""
        year_match = re.search(r"\((19\d{2}|20\d{2})\)", support_text)
        if year_match:
            rel_year = normalize_year(year_match.group(1))
            title_part = support_text[:year_match.start()].strip(" ,:-")
            if "," in title_part:
                title_part = title_part.split(",")[-1].strip()
            rel_title = clean_title_noise(title_part) or pretty_title(title_part)
        if rel_title:
            nested_year, nested_imdb = _imdb_suggestion_lookup(rel_title, series_hint, year_hint=(rel_year or hint), _depth=_depth + 1)
            if nested_year or nested_imdb:
                return nested_year or rel_year, nested_imdb
            if rel_year:
                return rel_year, ""
    return year, imdb_id


def imdb_related_title_year_from_support_text(text: str) -> tuple[str, str]:
    raw = re.sub(r"\s+", " ", str(text or "").strip())
    if not raw:
        return "", ""
    year_match = re.search(r"\((19\d{2}|20\d{2})\)", raw)
    if not year_match:
        return "", ""
    year = normalize_year(year_match.group(1))
    title_part = raw[:year_match.start()].strip(" ,:-")
    if "," in title_part:
        title_part = title_part.split(",")[-1].strip()
    title = clean_title_noise(title_part) or pretty_title(title_part)
    return title, year


def imdb_support_title_year_lookup(query: str, series_hint: bool, year_hint: str = "") -> tuple[str, str]:
    cleaned = re.sub(r"\s+", " ", (query or "").strip())
    if not cleaned:
        return "", ""
    first = re.sub(r"[^a-z0-9]", "", cleaned.lower())[:1] or "a"
    items: list[dict[str, Any]] = []
    for slug_source in (cleaned, cleaned.lower()):
        slug = urllib.parse.quote(slug_source)
        url = f"https://v2.sg.media-imdb.com/suggestion/{first}/{slug}.json"
        try:
            data = fetch_json(url)
        except Exception:
            continue
        payload = data.get("d", [])
        if isinstance(payload, list) and payload:
            items = [entry for entry in payload if isinstance(entry, dict)]
            if items:
                break
    if not isinstance(items, list) or not items:
        return "", ""

    hint = normalize_year(year_hint)

    def score(entry: dict[str, Any]) -> int:
        label = str(entry.get("l", "")).strip().lower()
        query_norm = cleaned.lower()
        kind = str(entry.get("q", "")).lower()
        entry_year = normalize_year(str(entry.get("y", "")).strip())
        kind_score = 0
        if series_hint and "tv" in kind:
            kind_score = 25
        elif (not series_hint) and ("movie" in kind or kind == "feature"):
            kind_score = 25
        elif "tv" not in kind and "movie" not in kind:
            kind_score = 8
        title_score = 0
        if label == query_norm:
            title_score += 120
        elif query_norm in label or label in query_norm:
            title_score += 80
        q_tokens = set(re.findall(r"[a-z0-9]+", query_norm))
        l_tokens = set(re.findall(r"[a-z0-9]+", label))
        if q_tokens and l_tokens:
            overlap = len(q_tokens & l_tokens) / float(len(q_tokens | l_tokens))
            title_score += int(overlap * 70.0)
        year_score = 0
        if hint and entry_year:
            try:
                delta = abs(int(entry_year) - int(hint))
                year_score = max(0, 35 - delta * 3)
            except Exception:
                year_score = 0
        return title_score + kind_score + year_score

    best = max((entry for entry in items if isinstance(entry, dict)), key=score, default={})
    if not best:
        return "", ""
    return imdb_related_title_year_from_support_text(str(best.get("s", "") or "").strip())


def imdb_title_lookup(title: str, series_hint: bool, year_hint: str = "", _depth: int = 0) -> tuple[str, str]:
    attempts: list[str] = []
    base = re.sub(r"\s+", " ", (title or "").strip())
    if base:
        attempts.append(base)
    cleaned = clean_title_noise(base)
    if cleaned and cleaned.lower() not in {x.lower() for x in attempts}:
        attempts.append(cleaned)
    if re.search(r"(?i)\bkudamm\s*77\b", base or ""):
        attempts.append("Ku'damm 77")
        attempts.append("Kudamm 77")

    seen = set()
    for attempt in attempts:
        key = attempt.lower()
        if key in seen:
            continue
        seen.add(key)
        year, imdb_id = _imdb_suggestion_lookup(attempt, series_hint, year_hint=year_hint)
        if year or imdb_id:
            return year, imdb_id
    return "", ""


def imdb_title_year_from_id(imdb_id: str) -> tuple[str, str]:
    normalized = normalize_imdb_id(imdb_id)
    if not normalized:
        return "", ""
    url = f"https://www.imdb.com/title/{normalized}/"
    try:
        html = fetch_text(
            url,
            headers={
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
            },
            timeout=20,
        )
    except Exception:
        return "", ""

    for match in re.finditer(r'<script[^>]+type="application/ld\+json"[^>]*>\s*(\{.*?\})\s*</script>', html, re.I | re.S):
        payload_raw = match.group(1).strip()
        if not payload_raw:
            continue
        try:
            payload = json.loads(payload_raw)
        except Exception:
            continue
        if not isinstance(payload, dict):
            continue
        title = clean_title_noise(str(payload.get("name", "") or "").strip())
        year = normalize_year(year_from_date(str(payload.get("datePublished", "") or "").strip()))
        if title or year:
            return title, year

    title = ""
    year = ""
    title_match = re.search(r"<title>\s*(.*?)\s*</title>", html, re.I | re.S)
    if title_match:
        title_text = re.sub(r"\s+", " ", title_match.group(1)).strip()
        title_text = re.sub(r"\s*-\s*IMDb\s*$", "", title_text, flags=re.I)
        year_match = re.search(r"\((19\d{2}|20\d{2})\)", title_text)
        if year_match:
            year = year_match.group(1)
            title_text = re.sub(r"\s*\((19\d{2}|20\d{2})\)\s*$", "", title_text).strip()
        title = clean_title_noise(title_text)
    if not year:
        release_match = re.search(r'"releaseYear"\s*:\s*\{\s*"year"\s*:\s*(19\d{2}|20\d{2})', html)
        if release_match:
            year = release_match.group(1)
    return title, normalize_year(year)


def build_imdb_title_candidates(row: dict[str, str]) -> list[str]:
    candidates: list[str] = []
    source_name = str(row.get("Quellname", "") or "").strip()
    raw_title = str(row.get("Name des Film/Serie", "") or "").strip()
    guess_title = normalize_title_guess(source_name).get("title", "")
    stem_title = Path(source_name).stem if source_name else ""

    for raw in (raw_title, guess_title, stem_title):
        cleaned = clean_title_noise(raw) or pretty_title(raw)
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        if not cleaned:
            continue
        candidates.append(cleaned)

        tokens = [t for t in cleaned.split() if t]
        if len(tokens) >= 3:
            for n in range(min(8, len(tokens)), 2, -1):
                candidates.append(" ".join(tokens[:n]))
        # As a final generic fallback, allow strong single-token probes.
        for tok in tokens:
            if len(tok) >= 5:
                candidates.append(tok)

    out: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        cand = re.sub(r"\s+", " ", (item or "").strip(" ._-"))
        if not cand:
            continue
        key = cand.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(cand)
    return out[:32]


def web_backfill_missing_years_imdb(rows: list[dict[str, str]], tmdb: TmdbClient | None = None) -> None:
    missing = [
        r for r in rows
        if (not normalize_year(r.get("Erscheinungsjahr", "")) or r.get("Erscheinungsjahr", "") == "0000")
        or (not normalize_imdb_id(r.get("IMDB-ID", "")))
    ]
    if not missing:
        return

    missing_year_before = sum(
        1 for r in missing if (not normalize_year(r.get("Erscheinungsjahr", "")) or r.get("Erscheinungsjahr", "") == "0000")
    )
    missing_imdb_before = sum(1 for r in missing if not normalize_imdb_id(r.get("IMDB-ID", "")))
    processing_log(
        f"[ANALYZE] Fallback-Metadaten START: Dateien={len(missing)} | Jahr offen={missing_year_before} | IMDb offen={missing_imdb_before}"
    )
    log("INFO", f"[WEB] IMDb-Fallback fuer {len(missing)} Datei(en).")
    cache: dict[str, tuple[str, str]] = {}
    imdb_meta_cache: dict[str, bool] = {}

    def imdb_id_has_metadata(imdb_id: str) -> bool:
        imdb_norm = normalize_imdb_id(imdb_id)
        if not imdb_norm:
            return False
        if imdb_norm in imdb_meta_cache:
            return imdb_meta_cache[imdb_norm]
        title, year = imdb_title_year_from_id(imdb_norm)
        has_meta = bool(title or year)
        imdb_meta_cache[imdb_norm] = has_meta
        return has_meta

    def resolve_title(candidate: str, series_hint: bool, hint: str) -> tuple[str, str]:
        key = f"{candidate.lower()}|{'1' if series_hint else '0'}|{hint}"
        if key in cache:
            return cache[key]

        year = ""
        imdb_id = ""

        if tmdb is not None and candidate:
            try:
                if series_hint:
                    tv = tmdb.search_tv(candidate, hint)
                    if tv:
                        tv_id = int(tv.get("id", 0))
                        if tv_id:
                            details = tmdb.tv_details(tv_id)
                            year = (
                                year_from_date(str(details.get("first_air_date", "")))
                                or year_from_date(str(tv.get("first_air_date", "")))
                            )
                            imdb_id = normalize_imdb_id((details.get("external_ids", {}) or {}).get("imdb_id", ""))
                else:
                    movie = tmdb.search_movie(candidate, hint)
                    if movie:
                        movie_id = int(movie.get("id", 0))
                        if movie_id:
                            details = tmdb.movie_details(movie_id)
                            year = (
                                year_from_date(str(details.get("release_date", "")))
                                or year_from_date(str(movie.get("release_date", "")))
                            )
                            imdb_id = normalize_imdb_id(details.get("imdb_id", ""))
            except Exception:
                year = year or ""
                imdb_id = imdb_id or ""

        if not year or not imdb_id:
            y_fallback, imdb_fallback = imdb_title_lookup(candidate, series_hint, year_hint=hint)
            if not year:
                year = y_fallback
            if not imdb_id:
                imdb_id = imdb_fallback
        if not year:
            support_title, support_year = imdb_support_title_year_lookup(candidate, series_hint, year_hint=hint)
            if support_year:
                year = support_year
            if support_title and not imdb_id:
                _, support_imdb = _imdb_suggestion_lookup(support_title, series_hint, year_hint=(support_year or hint))
                if support_imdb:
                    imdb_id = support_imdb

        cache[key] = (normalize_year(year), normalize_imdb_id(imdb_id))
        return cache[key]

    series_groups: dict[str, list[dict[str, str]]] = {}
    for row in missing:
        source_name = row.get("Quellname", "") or ""
        if not is_series_row(row) and not source_looks_series(source_name):
            continue
        key = series_group_key(row)
        if not key:
            continue
        series_groups.setdefault(key, []).append(row)

    for group in series_groups.values():
        year_hints: dict[str, int] = {}
        candidates_raw: list[str] = []
        for row in group:
            source_name = row.get("Quellname", "") or ""
            title_current = (row.get("Name des Film/Serie", "") or "").strip()
            if title_current:
                candidates_raw.append(title_current)
            source_title = series_title_from_source(source_name)
            if source_title:
                candidates_raw.append(source_title)
            guess_title = normalize_title_guess(source_name).get("title", "")
            if guess_title:
                candidates_raw.append(guess_title)
            hint = normalize_year(source_year_hint(source_name))
            if hint:
                year_hints[hint] = year_hints.get(hint, 0) + 1

        hint = max(year_hints.items(), key=lambda item: (item[1], item[0]))[0] if year_hints else ""
        candidates: list[str] = []
        seen_candidates: set[str] = set()
        for raw in candidates_raw:
            candidate = clean_title_noise(raw) or pretty_title(raw)
            candidate = re.sub(r"\s+", " ", (candidate or "").strip())
            if not candidate:
                continue
            key = candidate.lower()
            if key in seen_candidates:
                continue
            seen_candidates.add(key)
            candidates.append(candidate)

        year = ""
        imdb_id = ""
        for candidate in candidates[:12]:
            y, imdb = resolve_title(candidate, True, hint)
            if y and not year:
                year = y
            if imdb and not imdb_id:
                imdb_id = imdb
            if year and imdb_id:
                break
        if not year and not imdb_id:
            continue
        for row in group:
            current_year = normalize_year(row.get("Erscheinungsjahr", ""))
            year_missing_before = not current_year or current_year == "0000" or row.get("Erscheinungsjahr", "") == "0000"
            if year and year_missing_before:
                row["Erscheinungsjahr"] = year
            current_imdb = normalize_imdb_id(row.get("IMDB-ID", ""))
            if imdb_id and (
                not current_imdb
                or (
                    year_missing_before
                    and current_imdb != imdb_id
                    and not imdb_id_has_metadata(current_imdb)
                )
            ):
                row["IMDB-ID"] = imdb_id

    for idx, row in enumerate(missing, start=1):
        current_year = normalize_year(row.get("Erscheinungsjahr", ""))
        current_imdb = normalize_imdb_id(row.get("IMDB-ID", ""))
        if current_year and current_year != "0000" and current_imdb:
            pass
        else:
            candidates = build_imdb_title_candidates(row)
            if candidates:
                year = ""
                imdb_id = ""
                series_hint = is_series_row(row) or source_looks_series(row.get("Quellname", ""))
                hint = source_year_hint(row.get("Quellname", ""))
                for candidate in candidates:
                    y, imdb = resolve_title(candidate, series_hint, hint)
                    if y and not year:
                        year = y
                    if imdb and not imdb_id:
                        imdb_id = imdb
                    if year and imdb_id:
                        break
                if year and (not normalize_year(row.get("Erscheinungsjahr", "")) or row.get("Erscheinungsjahr", "") == "0000"):
                    row["Erscheinungsjahr"] = year
                year_missing_before = not current_year or current_year == "0000" or row.get("Erscheinungsjahr", "") == "0000"
                current_imdb = normalize_imdb_id(row.get("IMDB-ID", ""))
                if imdb_id and (
                    not current_imdb
                    or (
                        year_missing_before
                        and current_imdb != imdb_id
                        and not imdb_id_has_metadata(current_imdb)
                    )
                ):
                    row["IMDB-ID"] = imdb_id

        if idx % 25 == 0 or idx == len(missing):
            processing_log(f"[ANALYZE] Fallback-Metadaten Fortschritt: {idx}/{len(missing)}")

    if tmdb is not None:
        for row in missing:
            imdb_id = normalize_imdb_id(row.get("IMDB-ID", ""))
            if not imdb_id:
                continue
            title_missing = not clean_title_noise(row.get("Name des Film/Serie", "") or "")
            year_missing = not normalize_year(row.get("Erscheinungsjahr", "")) or row.get("Erscheinungsjahr", "") == "0000"
            if not title_missing and not year_missing:
                continue
            series_hint = is_series_row(row) or source_looks_series(row.get("Quellname", ""))
            tmdb_title, tmdb_year = tmdb_title_year_from_imdb(tmdb, imdb_id, series_hint)
            if title_missing and tmdb_title:
                row["Name des Film/Serie"] = tmdb_title
            if year_missing and tmdb_year:
                row["Erscheinungsjahr"] = tmdb_year

    for row in missing:
        imdb_id = normalize_imdb_id(row.get("IMDB-ID", ""))
        if not imdb_id:
            continue
        title_missing = not clean_title_noise(row.get("Name des Film/Serie", "") or "")
        year_missing = not normalize_year(row.get("Erscheinungsjahr", "")) or row.get("Erscheinungsjahr", "") == "0000"
        if not title_missing and not year_missing:
            continue
        imdb_title, imdb_year = imdb_title_year_from_id(imdb_id)
        if title_missing and imdb_title:
            row["Name des Film/Serie"] = imdb_title
        if year_missing and imdb_year:
            row["Erscheinungsjahr"] = imdb_year

    missing_year_after = sum(
        1 for r in missing if (not normalize_year(r.get("Erscheinungsjahr", "")) or r.get("Erscheinungsjahr", "") == "0000")
    )
    missing_imdb_after = sum(1 for r in missing if not normalize_imdb_id(r.get("IMDB-ID", "")))
    processing_log(
        f"[ANALYZE] Fallback-Metadaten ENDE: Jahr offen={missing_year_after} | IMDb offen={missing_imdb_after}"
    )


def enrich_row_from_tmdb(row: dict[str, str], tmdb: TmdbClient | None) -> None:
    guess = normalize_title_guess(row["Quellname"])

    fill_missing(row, "Name des Film/Serie", guess["title"])
    fill_missing(row, "Erscheinungsjahr", guess["year"])
    fill_missing(row, "Staffel", guess["season"])
    fill_missing(row, "Episode", guess["episode"])

    if tmdb is None:
        row["Staffel"] = format_season_episode(row.get("Staffel", "").strip())
        row["Episode"] = format_season_episode(row.get("Episode", "").strip())
        apply_series_metadata(row)
        if not row.get("Erscheinungsjahr", "").strip():
            row["Erscheinungsjahr"] = guess["year"] or "n/a"
        if not row.get("Laufzeit", "").strip():
            row["Laufzeit"] = "n/a"
        if not row.get("IMDB-ID", "").strip():
            row["IMDB-ID"] = "n/a"
        return

    try:
        if guess["is_series"] == "1":
            tv = tmdb.search_tv(guess["title"])
            if not tv:
                return
            tv_id = int(tv.get("id", 0))
            if not tv_id:
                return

            details = tmdb.tv_details(tv_id)
            try:
                details_en = tmdb.tv_details_lang(tv_id, "en-US")
            except Exception:
                details_en = {}
            fill_missing(row, "Name des Film/Serie", tmdb_preferred_title("tv", details, details_en, tv))
            fill_missing(row, "Erscheinungsjahr", year_from_date(details.get("first_air_date", "")))

            run_times = details.get("episode_run_time", [])
            if run_times:
                fill_missing(row, "Laufzeit", str(run_times[0]))

            ext = details.get("external_ids", {})
            fill_missing(row, "IMDB-ID", ext.get("imdb_id", ""))

            if row.get("Staffel") and row.get("Episode") and not row.get("Laufzeit"):
                episode = tmdb.tv_episode(tv_id, row["Staffel"], row["Episode"])
                runtime = episode.get("runtime")
                if runtime:
                    fill_missing(row, "Laufzeit", str(runtime))
        else:
            movie = tmdb.search_movie(guess["title"], guess["year"])
            if not movie:
                return
            movie_id = int(movie.get("id", 0))
            if not movie_id:
                return

            details = tmdb.movie_details(movie_id)
            try:
                details_en = tmdb.movie_details_lang(movie_id, "en-US")
            except Exception:
                details_en = {}
            fill_missing(row, "Name des Film/Serie", tmdb_preferred_title("movie", details, details_en, movie))
            fill_missing(row, "Erscheinungsjahr", year_from_date(details.get("release_date", "")))

            runtime = details.get("runtime")
            if runtime:
                fill_missing(row, "Laufzeit", str(runtime))

            fill_missing(row, "IMDB-ID", details.get("imdb_id", ""))
    except Exception:
        pass

    # Hard fallback values so requested columns are never empty for non-series fields.
    fill_missing(row, "Name des Film/Serie", guess["title"])
    row["Staffel"] = format_season_episode(row.get("Staffel", "").strip())
    row["Episode"] = format_season_episode(row.get("Episode", "").strip())
    apply_series_metadata(row)
    if not row.get("Erscheinungsjahr", "").strip():
        row["Erscheinungsjahr"] = guess["year"] or "n/a"
    if not row.get("Laufzeit", "").strip():
        row["Laufzeit"] = "n/a"
    if not row.get("IMDB-ID", "").strip():
        row["IMDB-ID"] = "n/a"
    apply_row_normalization(row)


def tmdb_preferred_title(
    kind: str,
    details_de: dict[str, Any] | None,
    details_en: dict[str, Any] | None,
    candidate: dict[str, Any] | None = None,
) -> str:
    details_de = details_de or {}
    details_en = details_en or {}
    candidate = candidate or {}
    if kind == "tv":
        values = [
            details_de.get("name", ""),
            details_en.get("name", ""),
            details_de.get("original_name", ""),
            details_en.get("original_name", ""),
            candidate.get("name", ""),
            candidate.get("original_name", ""),
        ]
    else:
        values = [
            details_de.get("title", ""),
            details_en.get("title", ""),
            details_de.get("original_title", ""),
            details_en.get("original_title", ""),
            candidate.get("title", ""),
            candidate.get("original_title", ""),
        ]
    for raw in values:
        title = clean_title_noise(str(raw or "").strip()) or str(raw or "").strip()
        if title and title.lower() != "unknown":
            return title
    return ""


def tmdb_title_year_from_imdb(tmdb: TmdbClient, imdb_id: str, series_hint: bool) -> tuple[str, str]:
    normalized_imdb = normalize_imdb_id(imdb_id)
    if not normalized_imdb:
        return "", ""
    try:
        result = tmdb.find_by_imdb(normalized_imdb)
    except Exception:
        return "", ""
    if not isinstance(result, dict):
        return "", ""

    tv_results = result.get("tv_results", [])
    movie_results = result.get("movie_results", [])

    kind = ""
    candidate: dict[str, Any] = {}
    if series_hint and isinstance(tv_results, list) and tv_results:
        kind = "tv"
        candidate = tv_results[0]
    elif (not series_hint) and isinstance(movie_results, list) and movie_results:
        kind = "movie"
        candidate = movie_results[0]
    elif isinstance(tv_results, list) and tv_results:
        kind = "tv"
        candidate = tv_results[0]
    elif isinstance(movie_results, list) and movie_results:
        kind = "movie"
        candidate = movie_results[0]
    else:
        return "", ""

    if kind == "tv":
        tv_id = int(candidate.get("id", 0) or 0)
        details: dict[str, Any] = {}
        details_en: dict[str, Any] = {}
        if tv_id > 0:
            try:
                details = tmdb.tv_details(tv_id)
            except Exception:
                details = {}
            try:
                details_en = tmdb.tv_details_lang(tv_id, "en-US")
            except Exception:
                details_en = {}
        title_raw = tmdb_preferred_title("tv", details, details_en, candidate)
        year = normalize_year(
            year_from_date(str(details.get("first_air_date", "") or candidate.get("first_air_date", "")).strip())
        )
        return (clean_title_noise(title_raw) or title_raw), year

    movie_id = int(candidate.get("id", 0) or 0)
    details = {}
    details_en = {}
    if movie_id > 0:
        try:
            details = tmdb.movie_details(movie_id)
        except Exception:
            details = {}
        try:
            details_en = tmdb.movie_details_lang(movie_id, "en-US")
        except Exception:
            details_en = {}
    title_raw = tmdb_preferred_title("movie", details, details_en, candidate)
    year = normalize_year(
        year_from_date(str(details.get("release_date", "") or candidate.get("release_date", "")).strip())
    )
    return (clean_title_noise(title_raw) or title_raw), year


def verify_detected_titles_via_tmdb_imdb(
    rows: list[dict[str, str]],
    tmdb_key: str,
    tmdb_client: TmdbClient | None = None,
) -> None:
    key = (tmdb_key or "").strip()
    if not rows:
        return
    candidates = [
        row
        for row in rows
        if (row.get("MANIFEST-SKIP", "") or "").strip() != "1" and normalize_imdb_id(row.get("IMDB-ID", ""))
    ]
    if not candidates:
        return
    if not key:
        processing_log("[TMDB] IMDb-Verifikation uebersprungen: kein TMDB-Key gesetzt.")
        return
    processing_log(f"[TMDB] IMDb-Verifikation START: Kandidaten={len(candidates)}")
    tmdb = tmdb_client if isinstance(tmdb_client, TmdbClient) else TmdbClient(key)
    db_hits_before = int(getattr(tmdb, "db_hits", 0) or 0)
    db_writes_before = int(getattr(tmdb, "db_writes", 0) or 0)
    cache: dict[tuple[str, str], tuple[str, str]] = {}
    request_count = 0
    checked = 0
    title_updated = 0
    year_filled = 0
    mismatch_skipped = 0
    total = len(candidates)
    for row in candidates:
        imdb_id = normalize_imdb_id(row.get("IMDB-ID", ""))
        if not imdb_id:
            continue
        series_hint = is_series_row(row) or source_looks_series(row.get("Quellname", ""))
        cache_key = (imdb_id, "tv" if series_hint else "movie")
        if cache_key not in cache:
            request_count += 1
            cache[cache_key] = tmdb_title_year_from_imdb(tmdb, imdb_id, series_hint)
        tmdb_title, tmdb_year = cache[cache_key]
        checked += 1

        if tmdb_title and series_hint:
            source_name = row.get("Quellname", "") or ""
            source_title = clean_title_noise(series_title_from_source(source_name))
            if not source_title:
                source_title = clean_title_noise(normalize_title_guess(source_name).get("title", ""))
            if source_title and not titles_look_compatible(source_title, tmdb_title):
                mismatch_skipped += 1
                processing_log(
                    "[TMDB] IMDb-Verifikation: Titel-Update uebersprungen "
                    f"(Mismatch) | Quelle='{source_title}' | TMDB='{tmdb_title}' | IMDb={imdb_id}"
                )
                tmdb_title = ""
                tmdb_year = ""

        if tmdb_title:
            current_title = (row.get("Name des Film/Serie", "") or "").strip()
            if current_title != tmdb_title:
                row["Name des Film/Serie"] = tmdb_title
                title_updated += 1
        current_year = normalize_year(row.get("Erscheinungsjahr", ""))
        if tmdb_year and (not current_year or current_year == "0000"):
            row["Erscheinungsjahr"] = tmdb_year
            year_filled += 1
        if checked % 250 == 0:
            processing_log(
                f"[TMDB] IMDb-Verifikation Fortschritt: {checked}/{total} | Requests={request_count} | Titel={title_updated} | Jahr={year_filled}"
            )
    processing_log(
        "[TMDB] IMDb-Verifikation ENDE: "
        f"geprueft={checked}/{total}, Requests={request_count}, Titel={title_updated}, Jahr={year_filled}, "
        f"Mismatch-Block={mismatch_skipped}, "
        f"Cache-Hit={max(0, int(getattr(tmdb, 'db_hits', 0) or 0) - db_hits_before)}, "
        f"Cache-Write={max(0, int(getattr(tmdb, 'db_writes', 0) or 0) - db_writes_before)}"
    )
    if checked > 0:
        log(
            "INFO",
            "TMDB-IMDb-Verifikation: "
            f"geprueft={checked}/{total}, Requests={request_count}, Titel aktualisiert={title_updated}, "
            f"Jahr ergaenzt={year_filled}, Mismatch geblockt={mismatch_skipped}, "
            f"Cache-Hit={max(0, int(getattr(tmdb, 'db_hits', 0) or 0) - db_hits_before)}, "
            f"Cache-Write={max(0, int(getattr(tmdb, 'db_writes', 0) or 0) - db_writes_before)}",
        )


def parse_gemini_json_rows(gemini_key: str, prompt: str) -> tuple[list[dict[str, Any]], str]:
    global GEMINI_QUOTA_BLOCKED

    if AI_QUERY_DISABLED:
        return [], "Gemini deaktiviert: KI-Abfrage deaktiviert (Advanced)."

    cached_rows = get_cached_gemini_rows(prompt)
    if cached_rows:
        return cached_rows, ""

    if GEMINI_QUOTA_BLOCKED:
        return [], "Gemini uebersprungen: API-Quota im aktuellen Lauf erschoepft."

    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    last_error = ""
    models = list_gemini_generate_models(gemini_key)
    if not models:
        return [], "Keine verfuegbaren Gemini-Modelle gefunden."

    previous_model = read_last_successful_model().strip()
    if previous_model:
        log("INFO", f"[KI] Start-Modell aus letztem Lauf: {previous_model}")
    else:
        log("INFO", "[KI] Start-Modell aus letztem Lauf: keines")
    log("INFO", f"[KI] Modell-Reihenfolge: {', '.join(models[:8])}")

    for model in models:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={gemini_key}"
        for attempt in range(1, 4):
            try:
                log("INFO", f"KI-Request: model={model}, attempt={attempt}")
                processing_log(f"[KI] Request gestartet: model={model}, attempt={attempt}")
                data = post_json_with_heartbeat(
                    url,
                    payload,
                    heartbeat_label=f"model={model}, attempt={attempt}",
                    heartbeat_sec=10,
                )
                candidates = data.get("candidates", [])
                if not candidates:
                    raise RuntimeError("Gemini lieferte keine candidates.")
                parts = candidates[0].get("content", {}).get("parts", [])
                text = "".join(part.get("text", "") for part in parts)
                rows = extract_first_json_array(text)
                if not rows:
                    raise RuntimeError("Gemini lieferte kein gueltiges JSON-Array.")
                write_last_successful_model(model)
                store_gemini_rows_in_cache(prompt, rows, model)
                log("INFO", f"[KI] Antwort OK: model={model}, rows={len(rows)}")
                processing_log(f"[KI] Antwort OK: model={model}, rows={len(rows)}")
                log("INFO", f"Gemini-Modell erfolgreich: {model}")
                return rows, ""
            except urllib.error.HTTPError as exc:
                err_msg = get_http_error_message(exc)
                last_error = f"{model}: HTTP {exc.code}: {err_msg}".strip()
                log("WARN", f"[KI] HTTP-Fehler: model={model}, attempt={attempt}, code={exc.code}")
                processing_log(f"[KI] HTTP-Fehler: model={model}, attempt={attempt}, code={exc.code}")
                if exc.code == 429 and is_quota_exhausted_message(err_msg):
                    GEMINI_QUOTA_BLOCKED = True
                    log("WARN", f"{model}: Quota erschöpft, breche Gemini-Retries sofort ab.")
                    return [], last_error
                if exc.code == 429 and attempt < 3:
                    wait_sec = attempt * 5
                    log("WARN", f"{model}: 429, neuer Versuch in {wait_sec}s (Versuch {attempt}/3).")
                    time.sleep(wait_sec)
                    continue
                if exc.code == 429:
                    log("WARN", f"{model}: 429 bleibt bestehen, wechsle Modell.")
                    break
                log("WARN", f"{model}: HTTP {exc.code}, wechsle Modell.")
                break
            except Exception as exc:
                last_error = f"{model}: {exc}"
                log("WARN", f"[KI] Fehler: model={model}, attempt={attempt}, detail={exc}")
                processing_log(f"[KI] Fehler: model={model}, attempt={attempt}, detail={exc}")
                break
    return [], last_error


def has_valid_year_imdb(row: dict[str, str]) -> bool:
    year_ok = bool(normalize_year(row.get("Erscheinungsjahr", ""))) and row.get("Erscheinungsjahr", "") != "0000"
    imdb_ok = bool(normalize_imdb_id(row.get("IMDB-ID", "")))
    return year_ok and imdb_ok


def year_imdb_ratio(rows: list[dict[str, str]]) -> float:
    if not rows:
        return 0.0
    ok = sum(1 for r in rows if has_valid_year_imdb(r))
    return ok / float(len(rows))


def row_title_key(row: dict[str, str]) -> str:
    title = (row.get("Name des Film/Serie", "") or "").strip()
    if not title:
        title = normalize_title_guess(row.get("Quellname", "")).get("title", "")
    key = re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()
    return key or "unknown"


def gemini_enrich_by_title(gemini_key: str, rows: list[dict[str, str]]) -> None:
    missing = [r for r in rows if not has_valid_year_imdb(r)]
    if not missing:
        return

    unique: dict[str, dict[str, str]] = {}
    for r in missing:
        key = row_title_key(r)
        if key in unique:
            continue
        title = (r.get("Name des Film/Serie", "") or "").strip()
        if not title:
            title = normalize_title_guess(r.get("Quellname", "")).get("title", "")
        unique[key] = {
            "key": key,
            "title": title,
            "season": (r.get("Staffel", "") or "").strip(),
            "episode": (r.get("Episode", "") or "").strip(),
        }

    items_list = [f"{v['key']} | {v['title']} | S{v['season']} E{v['episode']}" for v in unique.values()]
    if not items_list:
        return

    prompt = (
        "Liefere nur ein JSON-Array. Jedes Objekt exakt mit: key,Erscheinungsjahr,IMDB-ID. "
        "Erscheinungsjahr nur YYYY. IMDB-ID im Format tt1234567. "
        "Nutze den Titel als Film/Serienname, bei Serien Staffel/Episode nur als Hinweis. "
        "Wenn unklar, Feld leer.\n\nEingaben:\n" + "\n".join(items_list)
    )
    items, err = parse_gemini_json_rows(gemini_key, prompt)
    if err:
        log("WARN", f"Gemini-Titel-Backfill uebersprungen: {err}")
        return

    by_key: dict[str, tuple[str, str]] = {}
    for item in items:
        key = get_alias_value(item, ["key", "Key", "title_key"]).lower()
        if not key:
            continue
        by_key[key] = (
            normalize_year(get_alias_value(item, ["Erscheinungsjahr", "year", "Year"])),
            normalize_imdb_id(get_alias_value(item, ["IMDB-ID", "imdb", "imdb_id", "IMDb-ID"])),
        )

    for r in missing:
        key = row_title_key(r)
        yr, imdb = by_key.get(key, ("", ""))
        if yr and (not normalize_year(r.get("Erscheinungsjahr", "")) or r.get("Erscheinungsjahr", "") == "0000"):
            r["Erscheinungsjahr"] = yr
        if imdb and not normalize_imdb_id(r.get("IMDB-ID", "")):
            r["IMDB-ID"] = imdb


def gemini_backfill_rows(gemini_key: str, rows: list[dict[str, str]]) -> None:
    weak_rows = [row for row in rows if row_quality_score(row) < 0.60]
    if not weak_rows:
        return

    log("INFO", f"Gemini-Metadaten-Backfill fuer {len(weak_rows)} Datei(en).")

    chunk_size = 12
    for chunk_start in range(0, len(weak_rows), chunk_size):
        chunk = weak_rows[chunk_start:chunk_start + chunk_size]
        lines = []
        for row in chunk:
            lines.append(
                " | ".join(
                    [
                        row.get("Quellname", ""),
                        row.get("Name des Film/Serie", ""),
                        row.get("Erscheinungsjahr", ""),
                        row.get("Staffel", ""),
                        row.get("Episode", ""),
                    ]
                )
            )

        prompt = (
            "Analysiere die folgenden Video-Dateinamen und gib ein JSON-Array zurueck. "
            "Jedes Objekt braucht exakt diese Keys: "
            "Quellname,Name des Film/Serie,Erscheinungsjahr,Staffel,Episode,Laufzeit,IMDB-ID. "
            "Regeln: IMDb-ID immer im Format tt1234567; Laufzeit als Minutenzahl ohne Einheit; "
            "Staffel/Episode nur bei Serien, sonst leer; Jahr ist Serienstartjahr oder Filmjahr. "
            "Nutze bekannte oeffentliche Filmdaten. Keine Erklaerungen, nur JSON.\n\n"
            "Eingabedaten:\n" + "\n".join(lines)
        )

        items, err = parse_gemini_json_rows(gemini_key, prompt)
        if err:
            log("WARN", f"Gemini-Backfill uebersprungen: {err}")
            continue

        by_source = {}
        for item in items:
            src = str(item.get("Quellname", "")).strip()
            if src:
                by_source[src.lower()] = item

        for row in chunk:
            item = by_source.get(row.get("Quellname", "").lower())
            if not item:
                continue
            fill_missing_or_na(row, "Name des Film/Serie", str(item.get("Name des Film/Serie", "")).strip())
            fill_missing_or_na(row, "Erscheinungsjahr", str(item.get("Erscheinungsjahr", "")).strip())
            fill_missing_or_na(row, "Staffel", str(item.get("Staffel", "")).strip())
            fill_missing_or_na(row, "Episode", str(item.get("Episode", "")).strip())
            fill_missing_or_na(row, "Laufzeit", str(item.get("Laufzeit", "")).strip())
            fill_missing_or_na(row, "IMDB-ID", str(item.get("IMDB-ID", "")).strip())
            apply_series_metadata(row)
            apply_row_normalization(row)


def gemini_backfill_missing_years_imdb(gemini_key: str, rows: list[dict[str, str]]) -> None:
    missing = [
        r for r in rows
        if (not normalize_year(r.get("Erscheinungsjahr", "")) or r.get("Erscheinungsjahr", "") == "0000")
        or (not normalize_imdb_id(r.get("IMDB-ID", "")))
    ]
    if not missing:
        return

    log("INFO", f"Gemini-Jahr/IMDb-Backfill fuer {len(missing)} Datei(en).")
    chunk_size = 15
    for i in range(0, len(missing), chunk_size):
        chunk = missing[i:i + chunk_size]
        lines = []
        for r in chunk:
            lines.append(
                " | ".join(
                    [
                        r.get("Quellname", ""),
                        r.get("Name des Film/Serie", ""),
                        r.get("Staffel", ""),
                        r.get("Episode", ""),
                        r.get("IMDB-ID", ""),
                    ]
                )
            )

        prompt = (
            "Liefere nur ein JSON-Array. Jedes Objekt hat exakt: Quellname,Erscheinungsjahr,IMDB-ID. "
            "Erscheinungsjahr muss YYYY sein (bei Serien Startjahr). "
            "IMDB-ID muss tt1234567 Format haben. "
            "Nutze Dateiname, Titel, Staffel/Episode und IMDb-ID als Hinweis. "
            "Wenn unklar, Feld leer lassen.\n\nEingaben:\n" + "\n".join(lines)
        )
        items, err = parse_gemini_json_rows(gemini_key, prompt)
        if err:
            log("WARN", f"Gemini-Jahr/IMDb-Backfill uebersprungen: {err}")
            continue

        by_src: dict[str, tuple[str, str]] = {}
        by_base: dict[str, tuple[str, str]] = {}
        by_title: dict[str, tuple[str, str]] = {}
        for item in items:
            src = get_alias_value(item, ["Quellname", "source", "file", "filename"]).lower()
            if src:
                val = (
                    get_alias_value(item, ["Erscheinungsjahr", "year", "Year"]),
                    get_alias_value(item, ["IMDB-ID", "imdb", "imdb_id", "IMDb-ID"]),
                )
                by_src[src] = val
                by_base[Path(src).name] = val
            title_key = re.sub(
                r"[^a-z0-9]+",
                " ",
                get_alias_value(item, ["Name des Film/Serie", "name", "title", "Titel"]).lower(),
            ).strip()
            if title_key:
                by_title[title_key] = (
                    get_alias_value(item, ["Erscheinungsjahr", "year", "Year"]),
                    get_alias_value(item, ["IMDB-ID", "imdb", "imdb_id", "IMDb-ID"]),
                )

        for r in chunk:
            full_key = r.get("Quellname", "").lower()
            base_key = Path(full_key).name
            title_key = row_title_key(r)
            year_raw, imdb_raw = by_src.get(full_key, by_base.get(base_key, by_title.get(title_key, ("", ""))))
            year_val = normalize_year(year_raw)
            imdb_val = normalize_imdb_id(imdb_raw)
            if year_val:
                r["Erscheinungsjahr"] = year_val
            if imdb_val:
                r["IMDB-ID"] = imdb_val


def parse_gemini_initial_rows(gemini_key: str, source_files: list[Path], start_folder: Path) -> tuple[list[dict[str, str]], str]:
    file_lines = []
    for rel in source_files:
        rel_text = str(rel)
        source_abs = start_folder / rel_text
        sidecar_context = build_gemini_sidecar_context(source_abs) if source_abs.exists() else ""
        if sidecar_context:
            file_lines.append(f"{rel_text} | Kontext: {sidecar_context}")
        else:
            file_lines.append(rel_text)

    prompt = (
        "Erzeuge ein JSON-Array mit Metadaten zu Videodateien. "
        "Jedes Objekt muss exakt diese Keys enthalten: "
        "Quellname,Name des Film/Serie,Erscheinungsjahr,Staffel,Episode,Laufzeit,IMDB-ID. "
        "Regeln: Quellname exakt aus Eingabe uebernehmen; Staffel/Episode bei Serien zweistellig; "
        "bei Filmen Staffel/Episode leer; Laufzeit als Minutenzahl; IMDB-ID im Format tt1234567; "
        "wenn Sidecar-Kontext vorhanden ist (NFO/TXT), nutze ihn als primaere Erkennungsquelle; "
        "wenn unbekannt leer lassen. Keine Erklaerungen, nur JSON.\n\n"
        "Dateien:\n" + "\n".join(file_lines)
    )
    items, err = parse_gemini_json_rows(gemini_key, prompt)
    if err:
        return [], err
    rows = [coerce_row_from_any(item) for item in items]
    return rows, ""


def write_rows_to_db(rows: list[dict[str, str]]) -> str:
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=CSV_HEADERS)
    writer.writeheader()
    for row in rows:
        writer.writerow({header: row.get(header, "") for header in CSV_HEADERS})
    csv_text = csv_buffer.getvalue()
    rows_json = json.dumps(rows, ensure_ascii=False)
    init_mariadb_schema()
    GEMINI_DB_STORE.write_state("runtime.rows_csv", csv_text)
    GEMINI_DB_STORE.write_state("runtime.rows_json", rows_json)
    GEMINI_DB_STORE.write_state("runtime.rows_count", str(len(rows)))
    GEMINI_DB_STORE.write_state("runtime.rows_updated_unix", str(int(time.time())))
    GEMINI_DB_STORE.write_state("runtime.gemini_csv", csv_text)
    GEMINI_DB_STORE.write_state("runtime.gemini_rows_json", rows_json)
    GEMINI_DB_STORE.write_state("runtime.gemini_rows_count", str(len(rows)))
    GEMINI_DB_STORE.write_state("runtime.gemini_rows_updated_unix", str(int(time.time())))
    return "DB:app_state(runtime.rows_csv)"


def pretty_title(raw: str) -> str:
    text = (raw or "").strip()
    text = re.sub(r"[{}]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text if text else "Unknown"


def clean_title_noise(text: str) -> str:
    value = (text or "").strip()
    if not value:
        return ""
    value = re.sub(r"[._-]+", " ", value)
    value = re.sub(r"[{}]+", " ", value)
    value = re.sub(r"\btt\d{7,10}\b", " ", value, flags=re.IGNORECASE)
    noise = {
        "repack", "proper", "readnfo", "rerip", "internal",
        "german", "dl", "web", "webrip", "webdl", "bluray", "avc",
        "x264", "h264", "x265", "h265", "hevc", "uhd",
        "1080p", "2160p", "720p", "480p", "4k", "4sf", "wvf", "wayne",
        "knoedel", "sauerkraut", "rsg", "p73", "gma", "ml", "tvr", "tmsf",
        "sharphd", "inri", "complete", "pal", "dvd9", "multi", "intention",
        "ac3", "dts", "dtshd", "truehd", "uncut", "extended", "edition", "dubbed",
    }
    tokens: list[str] = []
    for token in value.split():
        t = token.strip()
        if not t:
            continue
        if t.lower() in noise:
            continue
        tokens.append(t)

    # Generic scene-group cleanup for release-like tails (e.g. "... -MONUMENT").
    if len(tokens) >= 2:
        last = tokens[-1]
        if re.fullmatch(r"[A-Z0-9]{4,}", last):
            tokens = tokens[:-1]

    value = " ".join(tokens)
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"(?i)\bkudamm\s*77\b", "Ku'damm 77", value)
    value = re.sub(r"(?i)\bwog\s+of\s+war\b", "Fog of War", value)
    value = re.sub(r"(?i)\bfog\s+of\s+war\s+gma\b", "Fog of War", value)
    value = re.sub(r"(?i)\bder\s+weisse\s+hai\b", "Der weisse Hai", value)
    value = re.sub(r"(?i)\bdead\s+like\s+me\b", "Dead Like Me", value)
    value = re.sub(r"(?i)\b(?:the\s+)?last\s+of\s+us\b", "The Last of Us", value)
    if re.fullmatch(r"(?i)dwhse", value):
        value = "Der weisse Hai"
    if value and value == value.lower() and re.fullmatch(r"[a-z0-9]+", value):
        value = value.capitalize()
    return value


def transliterate_german_for_filename(text: str) -> str:
    if not text:
        return ""
    table = str.maketrans(
        {
            "ä": "ae",
            "ö": "oe",
            "ü": "ue",
            "Ä": "Ae",
            "Ö": "Oe",
            "Ü": "Ue",
            "ß": "ss",
        }
    )
    return str(text).translate(table)


def safe_folder_name(text: str) -> str:
    cleaned = re.sub(r'[\\/:*?"<>|]+', "", text or "")
    cleaned = re.sub(r"[{}]+", " ", cleaned)
    cleaned = re.sub(r"\(\s*\)", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned if cleaned else "Unknown"


def dotted_name(text: str) -> str:
    transliterated = transliterate_german_for_filename(text or "")
    normalized = re.sub(r"[^A-Za-z0-9 ]+", " ", transliterated)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized.replace(" ", ".") if normalized else "Unknown"


def extract_resolution(source_name: str) -> str:
    match = re.search(r"(?i)\b(2160p|1080p|720p|480p|4k|uhd)\b", source_name)
    if not match:
        return "unknown"
    return normalize_resolution_token(match.group(1))


def probe_resolution_label(file_path: Path) -> str:
    if not file_path.exists() or not file_path.is_file():
        return "unknown"

    cmd = [
        FFPROBE_BIN,
        "-v",
        "error",
        "-show_entries",
        "stream=codec_type,height,width",
        "-of",
        "json",
        str(file_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if result.returncode != 0 or not result.stdout.strip():
            return "unknown"
        payload = json.loads(result.stdout)
        streams = payload.get("streams", [])
        heights = []
        for stream in streams:
            if str(stream.get("codec_type", "")).lower() != "video":
                continue
            height = stream.get("height")
            if isinstance(height, (int, float)) and height > 0:
                heights.append(int(height))
        if not heights:
            return "unknown"
        h = max(heights)
        if h >= 2000:
            return "4k"
        if h >= 1000:
            return "1080p"
        if h >= 700:
            return "720p"
        return "480p"
    except Exception:
        return "unknown"


def extract_codec(source_name: str) -> str:
    match = re.search(r"(?i)\b(x265|h265|hevc|x264|h264)\b", source_name)
    if not match:
        return "codec"
    token = match.group(1).lower()
    if token in {"x264", "h264"}:
        return "h264"
    if token in {"x265", "h265", "hevc"}:
        return "h265"
    return "codec"


def should_skip_encode_for_h265(source_abs: Path, source_name: str) -> bool:
    if not SKIP_H265_ENCODE_ENABLED:
        return False
    codec = extract_codec(source_name or "")
    if codec == "h265":
        return True
    try:
        return probe_codec_from_video(source_abs) == "h265"
    except Exception:
        return False


def should_skip_encode_for_4k_h265(source_abs: Path, source_name: str) -> bool:
    if not SKIP_4K_H265_ENCODE_ENABLED:
        return False

    resolution = extract_resolution(source_name or "")
    codec = extract_codec(source_name or "")

    if resolution == "unknown":
        try:
            resolution = probe_resolution_label(source_abs)
        except Exception:
            resolution = "unknown"

    if codec == "codec":
        try:
            codec = probe_codec_from_video(source_abs)
        except Exception:
            codec = "codec"

    return resolution == "4k" and codec == "h265"


def year_for_folder(row: dict[str, str]) -> str:
    year = (row.get("Erscheinungsjahr", "") or "").strip()
    if year == "0000":
        return "0000"
    if re.fullmatch(r"(19|20)\d{2}", year):
        return year
    if re.fullmatch(r"(19|20)\d{2}/(19|20)\d{2}", year):
        return year.split("/")[0]
    return "Unbekannt"


def ensure_year_present(row: dict[str, str], source_name: str) -> None:
    current = normalize_year(row.get("Erscheinungsjahr", ""))
    if current:
        row["Erscheinungsjahr"] = current
        return

    guessed = normalize_title_guess(source_name).get("year", "")
    guessed = normalize_year(guessed)
    if guessed:
        row["Erscheinungsjahr"] = guessed
        return

    m = re.search(r"\b(19\d{2}|20\d{2})\b", source_name)
    if m:
        row["Erscheinungsjahr"] = m.group(1)
        return

    row["Erscheinungsjahr"] = "0000"


def source_year_hint(source_name: str) -> str:
    match = re.search(r"\b(19\d{2}|20\d{2})\b", source_name)
    return match.group(1) if match else ""


def prefer_source_year(row: dict[str, str], source_name: str) -> None:
    if is_series_row(row):
        return
    hint = normalize_year(source_year_hint(source_name))
    if not hint:
        return
    current = normalize_year(row.get("Erscheinungsjahr", ""))
    if current != hint:
        row["Erscheinungsjahr"] = hint


def ensure_imdb_present(row: dict[str, str], source_name: str) -> None:
    current = normalize_imdb_id(row.get("IMDB-ID", ""))
    if current:
        row["IMDB-ID"] = current
        return

    m = re.search(r"(tt\d{7,10})", source_name.lower())
    if m:
        row["IMDB-ID"] = m.group(1)
        return

    row["IMDB-ID"] = "tt0000000"


def is_series_row(row: dict[str, str]) -> bool:
    return bool((row.get("Staffel", "") or "").strip() and (row.get("Episode", "") or "").strip())


def force_target_rel_codec(target_rel: str, codec: str = FFMPEG_TARGET_VIDEO_CODEC) -> str:
    rel = (target_rel or "").strip()
    if not rel:
        return rel
    codec_norm = re.sub(r"[^a-z0-9]+", "", (codec or "").lower())
    if not codec_norm:
        return rel

    path = Path(rel)
    name = path.name
    if re.search(rf"(?i)\.{re.escape(codec_norm)}\.", name):
        return rel
    new_name = re.sub(r"(?i)\.(codec|x264|h264|avc|x265|h265|hevc|mpeg2)\.", f".{codec_norm}.", name, count=1)
    if new_name == name:
        new_name = re.sub(r"(\.(?:tt\d{7,10}|n-a)\.[^.]+)$", f".{codec_norm}\\1", name, count=1)
    if new_name == name:
        new_name = re.sub(r"(\.\{[^{}]+\}\.[^.]+)$", f".{codec_norm}\\1", name, count=1)
    if new_name == name:
        new_name = f"{path.stem}.{codec_norm}{path.suffix}"
    if new_name == name:
        return rel
    return str(path.with_name(new_name))


def build_target_rel_path(row: dict[str, str], target_out_prefix: Path) -> str:
    source_name = (row.get("Quellname", "") or "").strip()
    source_file = Path(source_name).name
    ext = Path(source_file).suffix.lower() or ".mkv"
    base_title = (row.get("Name des Film/Serie", "") or "").strip() or Path(source_file).stem
    if str(base_title).strip().lower() == "unknown":
        base_title = Path(source_file).stem or base_title
    title = clean_title_noise(pretty_title(base_title)) or pretty_title(base_title)
    title_folder = safe_folder_name(title)
    title_dotted = dotted_name(title)
    year = year_for_folder(row)
    imdb_id = normalize_imdb_id(row.get("IMDB-ID", "")) or "n-a"
    imdb_token = "{" + imdb_id + "}"
    res = (row.get("Aufloesung", "") or "").strip().lower()
    if not res or res == "unknown":
        res = extract_resolution(source_name)
    codec = extract_codec(source_name)
    if codec == "codec":
        codec = FFMPEG_TARGET_VIDEO_CODEC

    qual_parts: list[str] = []
    if res and res != "unknown":
        qual_parts.append(res)
    if codec:
        qual_parts.append(codec)
    qual = ".".join(qual_parts) if qual_parts else FFMPEG_TARGET_VIDEO_CODEC

    if is_series_row(row):
        season = format_season_episode((row.get("Staffel", "") or "").strip()) or "01"
        episode = format_season_episode((row.get("Episode", "") or "").strip()) or "01"
        file_name = f"{title_dotted}.{year}.S{season}.E{episode}.{qual}.{imdb_token}{ext}"
        return str(target_out_prefix / "Serien" / f"{title_folder} ({year})" / f"S{season}" / file_name)

    file_name = f"{title_dotted}.{year}.{qual}.{imdb_token}{ext}"
    return str(target_out_prefix / "Movie" / f"{title_folder} ({year})" / file_name)


def write_out_plan(rows: list[dict[str, str]], start_folder: Path, out_root: Path) -> Path:
    dirs = {
        str(out_root),
        str(out_root / "Movie"),
        str(out_root / "Serien"),
    }
    files = []

    for row in rows:
        target_rel = (row.get("Zielname", "") or "").strip()
        if not target_rel:
            continue
        target_abs = resolve_target_abs(start_folder, target_rel)
        dirs.add(str(target_abs.parent))
        files.append(str(target_abs))

    out_lines = ["# Virtuelle Zielstruktur (nur Liste, keine Erstellung)", ""]
    out_lines.append("[DIR]")
    for path in sorted(dirs):
        out_lines.append(path)
    out_lines.append("")
    out_lines.append("[FILE]")
    for path in sorted(set(files)):
        out_lines.append(path)

    overwrite_text_file(OUT_PLAN_FILE, "\n".join(out_lines) + "\n")
    return OUT_PLAN_FILE


def build_virtual_out_tree(rows: list[dict[str, str]], target_out_prefix: Path, tree_label: str) -> str:
    prefix_parts = [part for part in target_out_prefix.parts if part not in {"", "."}]
    prefix_parts_lower = [part.lower() for part in prefix_parts]
    rel_paths = []
    for row in rows:
        target_rel = (row.get("Zielname", "") or "").strip()
        if not target_rel:
            continue
        path = Path(target_rel)
        path_parts = list(path.parts)
        path_parts_lower = [part.lower() for part in path_parts]
        if prefix_parts and len(path_parts) >= len(prefix_parts) and path_parts_lower[: len(prefix_parts)] == prefix_parts_lower:
            rest = path_parts[len(prefix_parts):]
            path = Path(*rest) if rest else Path()
        elif path.parts and path.parts[0].lower() == "__out":
            path = Path(*path.parts[1:])
        rel_paths.append(path)
    return build_tree_from_paths(tree_label, sorted(rel_paths, key=lambda p: str(p).lower()))


def write_out_tree(rows: list[dict[str, str]], target_out_prefix: Path, tree_label: str) -> Path:
    tree_text = build_virtual_out_tree(rows, target_out_prefix, tree_label)
    overwrite_text_file(OUT_TREE_FILE, tree_text + ("\n" if tree_text and not tree_text.endswith("\n") else ""))
    return OUT_TREE_FILE


def sanitize_processing_log_file() -> None:
    try:
        lines = PROCESSING_LOG_FILE.read_text(encoding="utf-8").splitlines()
    except Exception:
        return
    while lines and lines[-1].strip() == "":
        lines.pop()
    overwrite_text_file(PROCESSING_LOG_FILE, "\n".join(lines) + ("\n" if lines else ""))


def build_ascii_table(rows: list[dict[str, str]], total_files: int | None = None, current_index: int | None = None, mode: str = "a") -> str:
    headers = ["Nr.", "Quelle -> Ziel", "Jahr", "St/E-M", "IMDB-ID", "Q-GB", "Z-GB", "E-GB", "Lzeit", "Speed"]
    if mode == "f":
        headers.append("FPS")
    headers.append("ETA")
    raw_rows: list[list[str]] = []
    if total_files is None or total_files <= 0:
        total_files = len(rows)

    def yellow(text: str) -> str:
        return f"\033[33m{text}\033[0m"

    def visible_len(text: str) -> int:
        return len(ANSI_RE.sub("", text))

    def pad_ansi(text: str, width: int) -> str:
        delta = width - visible_len(text)
        return text + (" " * delta if delta > 0 else "")

    nr_width = max(2, len(str(total_files if total_files > 0 else max(1, len(rows)))))
    for idx, row in enumerate(rows, start=1):
        source_name = Path((row.get("Quellname", "") or "").strip()).name
        source_short = source_name[:30]
        target_name = Path((row.get("Zielname", "") or "").strip()).name
        source_target = f"{source_short.ljust(30)} --> {target_name}"
        year_display = year_for_folder(row)
        imdb_display = (row.get("IMDB-ID", "") or "").strip()
        if year_display == "0000":
            year_display = f"\033[31m{year_display}\033[0m"
        imdb_lower = imdb_display.lower()
        if imdb_lower == "n/a" or imdb_lower.startswith("tt0000"):
            imdb_display = f"\033[31m{imdb_display}\033[0m"
        season = (row.get("Staffel", "") or "").strip()
        episode = (row.get("Episode", "") or "").strip()
        if season and episode:
            ste = f"S{season}E{episode}"
        else:
            ste = "Movie"

        e_gb_status = (row.get("E-GB-STATUS", "") or "").strip()
        if e_gb_status:
            e_gb_display = e_gb_status
        else:
            e_gb_display = (row.get("E-GB", "") or "").strip()
            e_gb_band = (row.get("E-GB-BAND", "") or "").strip()
            if e_gb_display and e_gb_display not in {"n/a", "copy"} and e_gb_band:
                e_gb_display = f"{e_gb_display}{e_gb_band}"

        speed_display = (row.get("Speed", "") or "").strip()
        fps_display = (row.get("FPS", "") or "").strip()
        eta_display = (row.get("ETA", "") or "").strip()
        if mode == "c":
            if not speed_display:
                speed_display = "n/a"
            if not eta_display:
                eta_display = "n/a"
        elif mode == "a":
            speed_display = speed_display or "n/a"
            eta_display = eta_display or "n/a"
        else:
            speed_display = speed_display or "n/a"
            fps_display = fps_display or "n/a"
            eta_display = eta_display or "n/a"

        row_cells = [
            f"{idx:0{nr_width}d}/{total_files:0{nr_width}d}",
            source_target,
            year_display,
            ste,
            imdb_display,
            (row.get("Groesse", "") or "").strip(),
            (row.get("Z-GB", "") or "").strip(),
            e_gb_display,
            (row.get("Laufzeit (f)", "") or "").strip(),
            speed_display,
        ]
        if mode == "f":
            row_cells.append(fps_display)
        row_cells.append(eta_display)
        raw_rows.append(row_cells)

    widths = [len(h) for h in headers]
    for values in raw_rows:
        for idx, value in enumerate(values):
            widths[idx] = max(widths[idx], visible_len(value))
    widths[0] = max(widths[0], (nr_width * 2) + 1)

    def line(char: str = "-") -> str:
        return "+" + "+".join(char * (w + 2) for w in widths) + "+"

    processed_rows: list[tuple[dict[str, str], float, float]] = []
    processed_rows_ex_manifest: list[tuple[dict[str, str], float, float]] = []
    completed_rows_ex_manifest: list[tuple[dict[str, str], float, float]] = []
    manifest_count = 0
    for row in rows:
        is_manifest_row = (row.get("MANIFEST-SKIP", "") or "").strip() == "1"
        is_completed_row = (row.get("VERARBEITET", "") or "").strip() == "1"
        if is_manifest_row:
            manifest_count += 1
        z_ok, z_num = parse_gb_text_strict(row.get("Z-GB", ""))
        if not z_ok:
            continue
        q_num = parse_gb_text(row.get("Groesse", ""))
        processed_rows.append((row, q_num, z_num))
        if not is_manifest_row:
            processed_rows_ex_manifest.append((row, q_num, z_num))
            if is_completed_row:
                completed_rows_ex_manifest.append((row, q_num, z_num))

    rows_ex_manifest = [row for row in rows if (row.get("MANIFEST-SKIP", "") or "").strip() != "1"]

    total_rows = len(rows)
    processed_count = len(processed_rows)
    completed_count = sum(1 for row in rows if (row.get("VERARBEITET", "") or "").strip() == "1")
    if total_files > 0:
        if current_index is not None and current_index > 0:
            progress_cur = min(current_index, total_files)
        elif mode in {"c", "f"}:
            progress_cur = min(processed_count, total_files)
        else:
            progress_cur = min(total_rows, total_files)
        progress_text = f"{progress_cur:0{nr_width}d}/{total_files:0{nr_width}d}"
    else:
        progress_text = "0/0"

    active_index: int | None = None
    if current_index is not None and 1 <= current_index <= len(rows):
        active_index = current_index
    elif mode in {"c", "f"} and processed_count > 0 and rows:
        active_index = min(processed_count, len(rows))

    active_line = "n/a"
    if active_index is not None:
        active_target = Path((rows[active_index - 1].get("Zielname", "") or "").strip()).name
        active_name = active_target or "n/a"
        if total_files > 0:
            active_nr = f"{active_index:0{nr_width}d}/{total_files:0{nr_width}d}"
        else:
            active_nr = str(active_index)
        active_line = f"{active_nr} {active_name}"

    total_qgb_all = sum(parse_gb_text(row.get("Groesse", "")) for row in rows_ex_manifest)
    total_zgb_all = sum(parse_gb_text(row.get("Z-GB", "")) for row in rows_ex_manifest)
    total_qgb_processed = sum(q for _, q, _ in processed_rows_ex_manifest)
    total_zgb_processed = sum(z for _, _, z in processed_rows_ex_manifest)

    overall_eta_text = "n/a"
    if mode == "f":
        speed_samples: list[float] = []
        runtime_samples_sec: list[float] = []
        for row in rows_ex_manifest:
            spd = parse_speed_float(row.get("Speed", ""))
            if spd > 0:
                speed_samples.append(spd)
            rt_min = runtime_minutes_value(row.get("Laufzeit (f)", ""))
            if rt_min > 0:
                runtime_samples_sec.append(float(rt_min * 60))

        avg_speed = (sum(speed_samples) / len(speed_samples)) if speed_samples else 0.0
        avg_runtime_sec = (sum(runtime_samples_sec) / len(runtime_samples_sec)) if runtime_samples_sec else 45.0 * 60.0

        remaining_total_sec = 0.0
        for row_idx, row in enumerate(rows, start=1):
            if (row.get("MANIFEST-SKIP", "") or "").strip() == "1":
                continue
            if (row.get("VERARBEITET", "") or "").strip() == "1":
                continue

            is_active = active_index is not None and row_idx == active_index
            if is_active:
                eta_active = parse_eta_seconds_text(row.get("ETA", ""))
                if eta_active > 0:
                    remaining_total_sec += eta_active
                    continue

            rt_min = runtime_minutes_value(row.get("Laufzeit (f)", ""))
            runtime_sec = float(rt_min * 60) if rt_min > 0 else avg_runtime_sec
            if avg_speed > 0:
                remaining_total_sec += runtime_sec / avg_speed
            else:
                remaining_total_sec += runtime_sec

        overall_eta_text = format_total_eta(remaining_total_sec)
    elif mode == "c":
        speed_samples_mib: list[float] = []
        for row in rows_ex_manifest:
            if (row.get("VERARBEITET", "") or "").strip() != "1":
                continue
            spd = parse_copy_speed_mib(row.get("Speed", ""))
            if spd > 0:
                speed_samples_mib.append(spd)

        active_speed_mib = 0.0
        if active_index is not None and 1 <= active_index <= len(rows):
            active_speed_mib = parse_copy_speed_mib(rows[active_index - 1].get("Speed", ""))
        avg_speed_mib = (sum(speed_samples_mib) / len(speed_samples_mib)) if speed_samples_mib else 0.0
        speed_mib = active_speed_mib if active_speed_mib > 0 else avg_speed_mib
        remaining_q_gb = max(0.0, total_qgb_all - total_qgb_processed)
        if remaining_q_gb <= 0:
            overall_eta_text = "00:00"
        elif speed_mib > 0:
            overall_eta_text = format_total_eta((remaining_q_gb * 1024.0) / speed_mib)

    out = []
    out.append(f"{'Aktiv:':<12}{active_line}")
    out.append("")
    out.append("Gesamt:")
    if mode in {"c", "f"} and total_files > 0:
        out.append(f"{'Dateien:':<12}{processed_count:0{nr_width}d}/{total_files:0{nr_width}d}")
    else:
        out.append(f"{'Dateien:':<12}{len(rows)}")
    if total_files > 0:
        out.append(f"{'Verlauf:':<12}{manifest_count:0{nr_width}d}/{total_files:0{nr_width}d}")
        out.append(f"{'Erledigt:':<12}{completed_count:0{nr_width}d}/{total_files:0{nr_width}d}")
    else:
        out.append(f"{'Verlauf:':<12}{manifest_count}/0")
        out.append(f"{'Erledigt:':<12}{completed_count}/0")

    if mode == "a":
        q_text = f"{total_qgb_all:.1f}".replace(".", ",")
        out.append(f"{'Q-GB:':<12}{q_text} GB")
        out.append(f"{'Z-GB:':<12}0 GB")
        out.append(f"{'Ersparnis:':<12}0 GB - 0%")
    elif mode == "f":
        q_base = total_qgb_all
        z_base = total_zgb_all
        q_text = f"{q_base:.1f}".replace(".", ",")
        z_text = f"{z_base:.1f}".replace(".", ",")
        out.append(f"{'Q-GB:':<12}{q_text} GB")
        out.append(f"{'Z-GB:':<12}{z_text} GB")

        q_saved_base = sum(q for _, q, _ in completed_rows_ex_manifest)
        z_saved_base = sum(z for _, _, z in completed_rows_ex_manifest)
        saved = max(0.0, q_saved_base - z_saved_base)
        saved_pct = (saved / q_saved_base * 100.0) if q_saved_base > 0 else 0.0
        effective_total = max(0, total_files - manifest_count)
        effective_done = len(completed_rows_ex_manifest)
        out.append(
            f"{'Ersparnis:':<12}{saved:.1f} GB - {saved_pct:.0f}% "
            f"({effective_done:0{nr_width}d}/{effective_total:0{nr_width}d})"
        )
    else:
        q_base = total_qgb_all
        z_base = total_zgb_all
        q_text = f"{q_base:.1f}".replace(".", ",")
        z_text = f"{z_base:.1f}".replace(".", ",")
        out.append(f"{'Q-GB:':<12}{q_text} GB")
        out.append(f"{'Z-GB:':<12}{z_text} GB")

    out.append(f"{'Laufz.:':<12}{elapsed_hhmm()}")
    if mode in {"c", "f"}:
        out.append(f"{'ETA:':<12}{overall_eta_text}")
    if POST_ACTION_SUMMARY_LINES:
        for idx, text in enumerate(POST_ACTION_SUMMARY_LINES):
            label = "Nachlauf:" if idx == 0 else ""
            out.append(f"{label:<12}{text}")
    out.append("")
    out.append(line("-"))
    out.append("| " + " | ".join(headers[i].ljust(widths[i]) for i in range(len(headers))) + " |")
    out.append(line("="))
    for row_idx, values in enumerate(raw_rows, start=1):
        row_line = "| " + " | ".join(pad_ansi(values[i], widths[i]) for i in range(len(headers))) + " |"
        out.append(row_line)
    sep = line("-")
    out.append(sep)
    return "\n".join(out)


def write_status_table(rows: list[dict[str, str]], total_files: int | None = None, current_index: int | None = None, mode: str = "a") -> Path:
    table_text = build_ascii_table(rows, total_files=total_files, current_index=current_index, mode=mode)
    overwrite_text_file(STATUS_TABLE_FILE, table_text + "\n")
    return STATUS_TABLE_FILE


def sh_quote(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def history_off_prefix() -> str:
    # Best effort for bash/zsh sessions opened via Terminal "do script".
    return (
        "export HISTFILE=/dev/null HISTSIZE=0 HISTFILESIZE=0 SAVEHIST=0; "
        "if [ -n \"${BASH_VERSION:-}\" ]; then set +o history; fi; "
        "if [ -n \"${ZSH_VERSION:-}\" ]; then unsetopt SHARE_HISTORY INC_APPEND_HISTORY APPEND_HISTORY 2>/dev/null || true; fi; "
    )


def open_status_terminal(version: str, table_file: Path) -> None:
    status_cmd = (
        history_off_prefix()
        +
        f"cd {sh_quote(str(TARGET_DIR))}; "
        "export DISABLE_AUTO_TITLE=true; "
        f"while [ ! -f {sh_quote(str(STATUS_DONE_FILE))} ]; do "
        "clear; "
        f"if [ -f {sh_quote(str(table_file))} ]; then cat {sh_quote(str(table_file))}; else echo 'STATUS wird vorbereitet...'; fi; "
        "sleep 1; "
        "done; "
        "clear; "
        f"if [ -f {sh_quote(str(table_file))} ]; then cat {sh_quote(str(table_file))}; else echo 'STATUS-Datei nicht gefunden.'; fi"
    )
    escaped_status_cmd = status_cmd.replace("\"", "\\\"")
    script = f'''
set customTitle to "ManageMovie STATUS {version}"
set runCmd to "{escaped_status_cmd}"
tell application "Terminal"
    activate
    try
        repeat with ss in settings sets
            set title displays custom title of ss to true
            set title displays settings name of ss to false
            set title displays device name of ss to false
            set title displays shell path of ss to false
            set title displays file name of ss to false
            set title displays window size of ss to false
        end repeat
    end try

    set statusTab to do script runCmd
    set statusWindow to front window
    try
        set number of columns of statusWindow to 230
        set number of rows of statusWindow to 30
    end try
    set custom title of statusTab to customTitle
    try
        set title displays custom title of statusTab to true
        set title displays device name of statusTab to false
        set title displays shell path of statusTab to false
        set title displays file name of statusTab to false
        set title displays window size of statusTab to false
    end try
end tell
'''
    result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        log("WARN", f"STATUS-Fenster konnte nicht geöffnet werden: {result.stderr.strip()}")


def open_out_tree_terminal(version: str, out_tree_file: Path) -> None:
    out_cmd = (
        history_off_prefix()
        +
        f"cd {sh_quote(str(TARGET_DIR))}; "
        "export DISABLE_AUTO_TITLE=true; "
        f"touch {sh_quote(str(out_tree_file))}; "
        f"tail -n +1 -f {sh_quote(str(out_tree_file))} & "
        "TAIL_PID=$!; "
        f"while [ ! -f {sh_quote(str(OUT_TREE_DONE_FILE))} ]; do sleep 1; done; "
        "sleep 0.2; kill $TAIL_PID >/dev/null 2>&1 || true; wait $TAIL_PID 2>/dev/null || true"
    )
    escaped_out_cmd = out_cmd.replace("\"", "\\\"")
    script = f'''
set customTitle to "ManageMovie {version} VERARBEITUNG"
set runCmd to "{escaped_out_cmd}"
tell application "Terminal"
    activate
    try
        repeat with ss in settings sets
            set title displays custom title of ss to true
            set title displays settings name of ss to false
            set title displays device name of ss to false
            set title displays shell path of ss to false
            set title displays file name of ss to false
            set title displays window size of ss to false
        end repeat
    end try

    set outTab to do script runCmd
    set outWindow to front window
    try
        set number of columns of outWindow to 160
        set number of rows of outWindow to 30
    end try
    set custom title of outTab to customTitle
    try
        set title displays custom title of outTab to true
        set title displays device name of outTab to false
        set title displays shell path of outTab to false
        set title displays file name of outTab to false
        set title displays window size of outTab to false
    end try
end tell
'''
    result = subprocess.run(["osascript", "-e", script], capture_output=True, text=True, check=False)
    if result.returncode != 0:
        log("WARN", f"VERARBEITUNG-Fenster konnte nicht geöffnet werden: {result.stderr.strip()}")


def close_old_ui_windows() -> None:
    # Schließe vor Neustart alle Terminal-Fenster ohne Nachfrage (aktives Fenster bleibt offen).
    terminal_script = r'''
tell application "Terminal"
    try
        set keepWindow to front window
    on error
        set keepWindow to missing value
    end try

    repeat 100 times
        set closedOne to false
        repeat with w in (every window)
            try
                if keepWindow is missing value then
                    close w saving no
                    set closedOne to true
                    exit repeat
                else if (id of w is not id of keepWindow) then
                    close w saving no
                    set closedOne to true
                    exit repeat
                end if
            end try
        end repeat
        if closedOne is false then exit repeat
        delay 0.05
    end repeat
end tell
'''
    subprocess.run(["osascript", "-e", terminal_script], capture_output=True, text=True, check=False)

    # iTerm2 optional: ebenfalls Fenster schließen (falls vorhanden).
    iterm_script = r'''
tell application "iTerm2"
    try
        set keepWindow to current window
    on error
        set keepWindow to missing value
    end try

    repeat 100 times
        set closedOne to false
        repeat with w in windows
            try
                if keepWindow is missing value then
                    close w
                    set closedOne to true
                    exit repeat
                else if (id of w is not id of keepWindow) then
                    close w
                    set closedOne to true
                    exit repeat
                end if
            end try
        end repeat
        if closedOne is false then exit repeat
        delay 0.05
    end repeat
end tell
'''
    subprocess.run(["osascript", "-e", iterm_script], capture_output=True, text=True, check=False)

def keep_only_ui_windows() -> None:
    script = r'''
tell application "Terminal"
    repeat 40 times
        set closedOne to false
        repeat with w in (every window)
            try
                set wname to name of w
                if (wname does not contain "MovieMager STATUS") and (wname does not contain "VERARBEITUG") then
                    close w saving no
                    set closedOne to true
                    exit repeat
                end if
            end try
        end repeat
        if closedOne is false then exit repeat
        delay 0.05
    end repeat
end tell
'''
    subprocess.run(["osascript", "-e", script], capture_output=True, text=True, check=False)


def generate_output_csv(gemini_key: str, tmdb_key: str, tree_content: str, source_files: list[Path], start_folder: Path, mode: str, manifest_entries: dict[str, dict[str, str]] | None = None) -> str | None:
    if OUT_TREE_DONE_FILE.exists():
        OUT_TREE_DONE_FILE.unlink()
    if STATUS_DONE_FILE.exists():
        STATUS_DONE_FILE.unlink()
    clear_post_action_summary()
    processing_log(f"[VERARBEITUNG] Version {VERSION}")
    processing_log(f"[VERARBEITUNG] Dateien: {len(source_files)}")
    total = len(source_files)
    manifest_entries = {}

    copy_root = resolve_target_out_root(start_folder)
    target_out_prefix = target_out_prefix_for_rows(start_folder)
    target_tree_label = target_out_label(start_folder)
    if mode in {"c", "f"}:
        ensure_clean_dir(copy_root)
        if mode == "c":
            log("INFO", f"Copy-Modus: Zielordner geleert: {copy_root}")
        elif mode == "f":
            log("INFO", f"FFMPEG-Modus: Zielordner geleert: {copy_root}")

    processing_log(f"[DB-HISTORY] Pruefung: {len(source_files)} Datei(en)")
    manifest_prefilter, source_files_for_gemini = load_processed_source_history(
        source_files,
        retention_days=PROCESSED_SOURCE_ROW_RETENTION_DAYS,
        cache_label="DB-HISTORY",
    )
    processing_log(
        f"[DB-HISTORY] Ergebnis: Treffer={len(manifest_prefilter)} Offen={len(source_files_for_gemini)}"
    )
    if manifest_prefilter:
        processing_log(
            f"[DB-HISTORY] Vorab-Skip (365 Tage): {len(manifest_prefilter)}/{len(source_files)}"
        )

    processing_log(f"[EDITOR-CACHE] Pruefung: {len(source_files)} Datei(en)")
    editor_override_map, _ = load_cached_source_rows(
        source_files,
        cache_prefix=EDITOR_SOURCE_ROW_CACHE_PREFIX,
        retention_days=EDITOR_SOURCE_ROW_RETENTION_DAYS,
        cache_label="EDITOR-CACHE",
    )
    processing_log(f"[EDITOR-CACHE] Ergebnis: Treffer={len(editor_override_map)}")

    gemini_seed_map: dict[str, dict[str, str]] = {}
    source_files_missing_gemini: list[Path] = []
    gemini_rows: list[dict[str, str]] = []
    if AI_QUERY_DISABLED:
        source_files_missing_gemini = list(source_files_for_gemini)
        processing_log(
            "[KI] KI-Abfrage deaktiviert (Advanced): Source-Cache uebersprungen, "
            "nur Cache-DB (EDITOR-CACHE/DB-RUNTIME/DB-HISTORY)."
        )
    else:
        processing_log(f"[SOURCE-CACHE] Pruefung: {len(source_files_for_gemini)} Datei(en)")
        gemini_seed_map, source_files_missing_gemini = load_cached_source_rows(
            source_files_for_gemini,
            cache_prefix=GEMINI_SOURCE_ROW_CACHE_PREFIX,
            cache_label="SOURCE-CACHE",
        )
        processing_log(
            f"[SOURCE-CACHE] Ergebnis: Treffer={len(gemini_seed_map)} Offen={len(source_files_missing_gemini)}"
        )

    processing_log(f"[DB-RUNTIME] Pruefung: {len(source_files_missing_gemini)} Datei(en)")
    runtime_seed_map = load_runtime_rows_seed(
        source_files_missing_gemini,
        cache_label="DB-RUNTIME",
    )
    runtime_seed_used_keys: set[str] = set()
    if runtime_seed_map:
        for source_key, row in runtime_seed_map.items():
            if source_key in editor_override_map:
                continue
            if source_key not in gemini_seed_map:
                gemini_seed_map[source_key] = row
            runtime_seed_used_keys.add(source_key)
        source_files_missing_gemini = [
            rel
            for rel in source_files_missing_gemini
            if normalize_source_row_name(str(rel)) not in runtime_seed_used_keys
        ]
    processing_log(
        f"[DB-RUNTIME] Ergebnis: Treffer={len(runtime_seed_map)} Offen={len(source_files_missing_gemini)}"
    )

    source_files_missing_gemini_uncached: list[Path] = []
    for rel in source_files_missing_gemini:
        if normalize_source_row_name(str(rel)) in editor_override_map:
            continue
        source_files_missing_gemini_uncached.append(rel)
    if len(source_files_missing_gemini_uncached) != len(source_files_missing_gemini):
        skipped_by_editor = len(source_files_missing_gemini) - len(source_files_missing_gemini_uncached)
        processing_log(f"[EDITOR-CACHE] KI-Abfrage uebersprungen fuer {skipped_by_editor} Datei(en).")
    source_files_missing_gemini = source_files_missing_gemini_uncached

    gemini_error = ""
    if source_files_missing_gemini:
        if AI_QUERY_DISABLED:
            processing_log(
                f"[KI] KI-Abfrage deaktiviert: "
                f"keine Live-KI-Abfrage fuer {len(source_files_missing_gemini)} Datei(en)."
            )
        else:
            if not gemini_key:
                raise RuntimeError(
                    "KI-Key fehlt: settings.gemini_api ist leer "
                    "und MANAGEMOVIE_GEMINI_KEY ist nicht gesetzt."
                )
            gemini_rows, gemini_error = parse_gemini_initial_rows(gemini_key, source_files_missing_gemini, start_folder)
            if gemini_error:
                log("WARN", f"KI Initial-Fallback: {gemini_error}")
            elif gemini_rows:
                store_source_rows_cache(
                    gemini_rows,
                    cache_prefix=GEMINI_SOURCE_ROW_CACHE_PREFIX,
                    cache_label="SOURCE-CACHE",
                )
    elif source_files_for_gemini:
        if AI_QUERY_DISABLED:
            processing_log("[KI] KI-Abfrage deaktiviert: keine Live-KI-Abfrage notwendig (vollstaendig aus Cache-DB).")
        else:
            processing_log("[SOURCE-CACHE] Keine neue Abfrage notwendig: alle Quellen im Source-Cache vorhanden.")
    else:
        processing_log("[DB-HISTORY] Keine KI-Analyse notwendig: alle Dateien sind bereits verarbeitet.")

    gemini_map: dict[str, dict[str, str]] = {}
    for source_key, row in gemini_seed_map.items():
        gemini_map[source_key] = row
    for row in gemini_rows:
        src = row.get("Quellname", "").strip()
        if src:
            gemini_map[normalize_source_row_name(src)] = row

    tmdb = TmdbClient(tmdb_key) if TMDB_ENABLED and bool((tmdb_key or "").strip()) else None
    if tmdb is not None:
        processing_log(
            f"[TMDB] Cache aktiv: MariaDB tmdb_cache, Retention={TMDB_CACHE_RETENTION_DAYS} Tage, Sprache={TMDB_LANGUAGE}"
        )
    final_rows: list[dict[str, str]] = []
    editor_override_hits = 0
    editor_override_keys: set[str] = set()
    analyze_scan_total = len(source_files)
    analyze_scan_started = time.time()
    analyze_mode_selected = str(mode).strip().lower() in {"a", "analyze"}
    if analyze_scan_total > 0:
        processing_log(f"[ANALYZE] Metadaten-Scan START: {analyze_scan_total} Datei(en)")
        if analyze_mode_selected and not ANALYZE_RUNTIME_PROBE:
            processing_log(
                "[ANALYZE] Laufzeit-Probe im Analyze-Modus deaktiviert "
                "(MANAGEMOVIE_ANALYZE_RUNTIME_PROBE=0)."
            )

    row_entries: list[tuple[dict[str, str], Path]] = []
    for idx, rel_path in enumerate(source_files, start=1):
        source_name = str(rel_path)
        source_abs = start_folder / source_name
        source_key = normalize_source_row_name(source_name)
        manifest_seed = manifest_prefilter.get(source_key)

        base_row = {header: "" for header in CSV_HEADERS}
        base_row["Quellname"] = source_name

        if manifest_seed:
            cached_row = manifest_seed.get("row")
            if isinstance(cached_row, dict):
                for key, value in cached_row.items():
                    key_text = str(key or "").strip()
                    if not key_text:
                        continue
                    value_text = str(value or "").strip()
                    if not value_text:
                        continue
                    base_row[key_text] = value_text
            guess = normalize_title_guess(source_name)
            guessed_title = clean_title_noise(guess.get("title", "")) or guess.get("title", "")
            if guessed_title and not (base_row.get("Name des Film/Serie", "") or "").strip():
                base_row["Name des Film/Serie"] = guessed_title
            guessed_year = normalize_year(guess.get("year", ""))
            if guessed_year and not (base_row.get("Erscheinungsjahr", "") or "").strip():
                base_row["Erscheinungsjahr"] = guessed_year
            season_guess = format_season_episode(guess.get("season", ""))
            episode_guess = format_season_episode(guess.get("episode", ""))
            if season_guess and episode_guess and not (base_row.get("Staffel", "") or "").strip() and not (base_row.get("Episode", "") or "").strip():
                base_row["Staffel"] = season_guess
                base_row["Episode"] = episode_guess

            mode_skip = normalize_manifest_mode(manifest_seed.get("mode", ""))
            base_row["MANIFEST-SKIP"] = "1"
            base_row["MANIFEST-MODE"] = mode_skip or "copy"
            base_row["MANIFEST-TARGET"] = (manifest_seed.get("target_rel", "") or "").strip()
            if not (base_row.get("Z-GB", "") or "").strip():
                base_row["Z-GB"] = (manifest_seed.get("z_gb", "") or "n/a").strip() or "n/a"
            base_row["Speed"] = "copied" if base_row["MANIFEST-MODE"] == "copy" else "encoded"
            base_row["ETA"] = "copied" if base_row["MANIFEST-MODE"] == "copy" else "encoded"
            base_row["VERARBEITET"] = "1"
        else:
            seed = gemini_map.get(source_key)
            if seed:
                for header in CSV_HEADERS:
                    value = (seed.get(header, "") or "").strip()
                    if value:
                        base_row[header] = value

            enrich_row_from_sidecar_nfo(base_row, source_abs)
            enrich_row_from_tmdb(base_row, tmdb)

        reconcile_series_title_with_source(base_row)
        apply_known_series_overrides(base_row)
        prefer_source_year(base_row, source_name)
        ensure_year_present(base_row, source_name)
        if manifest_seed:
            ensure_imdb_present(base_row, source_name)

        if manifest_seed and analyze_mode_selected:
            if not (base_row.get("Aufloesung", "") or "").strip():
                res_hint_fast = extract_resolution(source_name)
                if res_hint_fast == "unknown":
                    res_hint_fast = nfo_resolution_hint(source_abs)
                base_row["Aufloesung"] = res_hint_fast
            if not (base_row.get("Groesse", "") or "").strip():
                base_row["Groesse"] = file_size_human(source_abs)
            base_row["Z-GB"] = base_row.get("Z-GB", "") or "n/a"
            base_row["E-GB"] = base_row.get("E-GB", "") or ""
            base_row["E-GB-BAND"] = base_row.get("E-GB-BAND", "") or ""
            base_row["E-GB-STATUS"] = base_row.get("E-GB-STATUS", "") or ""
            runtime_fast = normalize_runtime(base_row.get("Laufzeit (f)", "")) or normalize_runtime(base_row.get("Laufzeit", "")) or "n/a"
            base_row["Laufzeit (f)"] = runtime_fast
            row_entries.append((base_row, source_abs))
            final_rows.append(base_row)
            if idx % 25 == 0 or idx == analyze_scan_total:
                elapsed = format_hh_mm_ss(time.time() - analyze_scan_started)
                processing_log(f"[ANALYZE] Metadaten-Scan Fortschritt: {idx}/{analyze_scan_total} | Laufzeit: {elapsed}")
            continue

        res_hint = extract_resolution(source_name)
        if res_hint == "unknown":
            res_hint = nfo_resolution_hint(source_abs)
        if res_hint == "unknown":
            res_hint = probe_resolution_label(source_abs)
        base_row["Aufloesung"] = res_hint
        base_row["Groesse"] = file_size_human(source_abs)
        base_row["Z-GB"] = base_row.get("Z-GB", "") or "n/a"
        base_row["E-GB"] = ""
        base_row["E-GB-BAND"] = ""
        base_row["E-GB-STATUS"] = ""
        runtime_probe = "n/a"
        should_probe_runtime = (not analyze_mode_selected) or ANALYZE_RUNTIME_PROBE
        if should_probe_runtime and should_probe_runtime_for_source(source_abs):
            runtime_probe = probe_runtime_minutes(source_abs)
        if runtime_probe != "n/a" and not runtime_value_is_plausible(runtime_probe):
            runtime_probe = "n/a"
        if runtime_probe == "n/a":
            runtime_probe = normalize_runtime(base_row.get("Laufzeit", "")) or "n/a"
        base_row["Laufzeit (f)"] = runtime_probe
        row_entries.append((base_row, source_abs))
        final_rows.append(base_row)
        if idx % 25 == 0 or idx == analyze_scan_total:
            elapsed = format_hh_mm_ss(time.time() - analyze_scan_started)
            processing_log(f"[ANALYZE] Metadaten-Scan Fortschritt: {idx}/{analyze_scan_total} | Laufzeit: {elapsed}")

    if analyze_scan_total > 0:
        processing_log(
            f"[ANALYZE] Metadaten-Scan ENDE: {analyze_scan_total}/{analyze_scan_total} | Laufzeit: {format_hh_mm_ss(time.time() - analyze_scan_started)}"
        )

    manifest_missing_meta_rows = [
        row
        for row in final_rows
        if row.get("MANIFEST-SKIP", "") == "1" and not has_valid_year_imdb(row)
    ]
    if manifest_missing_meta_rows:
        processing_log(
            f"[ANALYZE] DB-History-Metadaten-Fallback START: Dateien={len(manifest_missing_meta_rows)}"
        )
        web_backfill_missing_years_imdb(manifest_missing_meta_rows, tmdb=tmdb)
        remaining_manifest_missing = [row for row in manifest_missing_meta_rows if not has_valid_year_imdb(row)]
        processing_log(
            f"[ANALYZE] DB-History-Metadaten-Fallback ENDE: Rest offen={len(remaining_manifest_missing)}"
        )
        for row in remaining_manifest_missing[:8]:
            processing_log(
                f"[ANALYZE] DB-History-Metadaten offen: {Path(row.get('Quellname', '')).name}"
            )

    # Try multiple Gemini enrichment passes until at least 80% rows have both year + IMDb.
    rows_for_enrichment: list[dict[str, str]] = []
    editor_override_missing_for_enrichment = 0
    for r in final_rows:
        if r.get("MANIFEST-SKIP", "") == "1":
            continue
        source_key = normalize_source_row_name(str(r.get("Quellname", "")))
        is_editor_override = source_key in editor_override_map
        if is_editor_override and has_valid_year_imdb(r):
            continue
        if is_editor_override:
            editor_override_missing_for_enrichment += 1
        rows_for_enrichment.append(r)
    if editor_override_missing_for_enrichment > 0:
        processing_log(
            f"[EDITOR-CACHE] Override-Zeilen fuer Enrichment freigegeben: {editor_override_missing_for_enrichment}"
        )
    missing_year_imdb_before = sum(1 for r in rows_for_enrichment if not has_valid_year_imdb(r))
    if rows_for_enrichment:
        if AI_QUERY_DISABLED and missing_year_imdb_before > 0:
            log("INFO", "KI-Enrichment uebersprungen: KI-Abfrage deaktiviert (TMDB-only).")
        elif gemini_key and missing_year_imdb_before > 0:
            log("INFO", f"KI-Enrichment aktiv: fehlende Jahr/IMDb-Zeilen={missing_year_imdb_before}")
            for pass_idx in range(1, 5):
                ratio = year_imdb_ratio(rows_for_enrichment)
                log("INFO", f"Jahr/IMDb-Quote vor Pass {pass_idx}: {ratio:.0%}")
                if ratio >= 1.0:
                    break
                gemini_backfill_missing_years_imdb(gemini_key, rows_for_enrichment)
                gemini_enrich_by_title(gemini_key, rows_for_enrichment)
                # For stubborn rows, request full-row refresh (title/year/imdb/runtime).
                if year_imdb_ratio(rows_for_enrichment) < 1.0:
                    gemini_backfill_rows(gemini_key, rows_for_enrichment)
        elif not gemini_key and missing_year_imdb_before > 0:
            log("INFO", "KI-Enrichment uebersprungen: kein KI-Key gesetzt.")
        else:
            log("INFO", "KI-Enrichment uebersprungen: keine fehlenden Jahr/IMDb-Zeilen.")
        web_backfill_missing_years_imdb(rows_for_enrichment, tmdb=tmdb)
        # Final online sweep after generic fallback.
        if (not AI_QUERY_DISABLED) and gemini_key and year_imdb_ratio(rows_for_enrichment) < 1.0:
            gemini_backfill_missing_years_imdb(gemini_key, rows_for_enrichment)
            gemini_enrich_by_title(gemini_key, rows_for_enrichment)
            web_backfill_missing_years_imdb(rows_for_enrichment, tmdb=tmdb)
        ratio = year_imdb_ratio(rows_for_enrichment)
        log("INFO", f"Jahr/IMDb-Quote nach Enrichment: {ratio:.0%}")
    else:
        log("INFO", "Jahr/IMDb-Quote nach Enrichment: 100% (nur DB-History-Skip).")

    verify_detected_titles_via_tmdb_imdb(final_rows, tmdb_key, tmdb_client=tmdb)
    if tmdb is not None:
        processing_log(
            f"[TMDB] Cache-Summe: Hit={int(getattr(tmdb, 'db_hits', 0) or 0)} | "
            f"Write={int(getattr(tmdb, 'db_writes', 0) or 0)}"
        )
    harmonize_series_titles(final_rows)
    harmonize_series_start_year(final_rows)
    rows_for_edit_original = [dict(row) for row in final_rows]

    # Persist the pre-editor baseline in source cache.
    # Keep richer existing rows if a rerun produced worse metadata.
    store_source_rows_cache(
        rows_for_edit_original,
        cache_prefix=GEMINI_SOURCE_ROW_CACHE_PREFIX,
        cache_label="SOURCE-CACHE",
        overwrite=True,
        prefer_richer_existing=True,
    )

    for row in final_rows:
        if row.get("MANIFEST-SKIP", "") == "1":
            continue
        source_key = normalize_source_row_name(str(row.get("Quellname", "")))
        if not source_key:
            continue
        editor_override = editor_override_map.get(source_key)
        if not editor_override:
            continue
        apply_editor_override_row(row, editor_override)
        editor_override_keys.add(source_key)
        editor_override_hits += 1

    if editor_override_hits > 0:
        processing_log(f"[EDITOR-CACHE] Overrides angewendet: {editor_override_hits}")

    for row in final_rows:
        if row.get("MANIFEST-SKIP", "") == "1":
            manifest_target = (row.get("MANIFEST-TARGET", "") or "").strip()
            if manifest_target:
                row["Zielname"] = manifest_target
            else:
                row["Zielname"] = build_target_rel_path(row, target_out_prefix)
            continue

        source_key = normalize_source_row_name(str(row.get("Quellname", "")))
        is_editor_override = source_key in editor_override_keys
        if not is_editor_override:
            if not str(row.get("Name des Film/Serie", "") or "").strip():
                reconcile_series_title_with_source(row)
            apply_known_series_overrides(row)
            prefer_source_year(row, row.get("Quellname", ""))
        ensure_year_present(row, row.get("Quellname", ""))
        ensure_imdb_present(row, row.get("Quellname", ""))
        row["Zielname"] = build_target_rel_path(row, target_out_prefix)
        if mode == "f":
            row["Zielname"] = force_target_rel_codec(row["Zielname"], FFMPEG_TARGET_VIDEO_CODEC)

    manifest_skip_count = 0
    for row in final_rows:
        if row.get("MANIFEST-SKIP", "") != "1":
            continue
        skip_mode = normalize_manifest_mode(row.get("MANIFEST-MODE", ""))
        if skip_mode not in {"copy", "encode"}:
            row["MANIFEST-MODE"] = "copy"
            skip_mode = "copy"
        row["Speed"] = "copied" if skip_mode == "copy" else "encoded"
        row["ETA"] = "copied" if skip_mode == "copy" else "encoded"
        if mode == "c":
            row["E-GB"] = "copy"
            row["E-GB-BAND"] = ""
            row["E-GB-STATUS"] = "copy"
        elif mode == "f":
            set_row_egb_status_from_sizes(row, mode)
        row["VERARBEITET"] = "1"
        manifest_skip_count += 1

    if manifest_skip_count > 0:
        processing_log(f"[DB-HISTORY] Ueberspringe aus Verlauf: {manifest_skip_count}/{total}")

    # Build full status first with Q-GB values, then fill E-GB/Speed/ETA line by line.
    # During copy the workdir sits on the same volume on many setups, so avoid rewriting
    # the whole status file every second unless we really need to.
    status_refresh_lock = threading.Lock()
    status_refresh_stop = threading.Event()
    status_refresh_interval_sec = 2.5 if mode == "c" else 1.0
    status_refresh_state: dict[str, object] = {
        "current_index": None,
        "last_text": "",
        "last_write_ts": 0.0,
    }

    def status_refresh_set_index(value: int | None) -> None:
        with status_refresh_lock:
            status_refresh_state["current_index"] = value

    def status_refresh_write(*, force: bool = False) -> None:
        with status_refresh_lock:
            current_index = status_refresh_state["current_index"]
            last_text = str(status_refresh_state.get("last_text", "") or "")
            last_write_ts = float(status_refresh_state.get("last_write_ts", 0.0) or 0.0)
        table_text = build_ascii_table(final_rows, total_files=total, current_index=current_index, mode=mode)
        now = time.time()
        if not force:
            if table_text == last_text and (now - last_write_ts) < status_refresh_interval_sec:
                return
            if (now - last_write_ts) < status_refresh_interval_sec:
                return
        overwrite_text_file(STATUS_TABLE_FILE, table_text + "\n")
        with status_refresh_lock:
            status_refresh_state["last_text"] = table_text
            status_refresh_state["last_write_ts"] = now

    def status_refresh_worker() -> None:
        while not status_refresh_stop.wait(max(1.0, status_refresh_interval_sec)):
            try:
                status_refresh_write()
            except Exception:
                pass

    status_refresh_write(force=True)
    status_refresh_thread = threading.Thread(target=status_refresh_worker, daemon=True, name="status-refresh")
    status_refresh_thread.start()

    ffmpeg_encoder_mode = read_ffmpeg_encoder_default()
    if mode in {"a", "c", "f"}:
        should_start, selected_encoder = confirm_processing_start(
            mode,
            total,
            start_folder,
            rows_for_edit=final_rows,
            rows_for_edit_original=rows_for_edit_original,
        )
        if mode == "f":
            ffmpeg_encoder_mode = selected_encoder
            log("INFO", f"FFMPEG-Encoder: {ffmpeg_encoder_mode}")
            encoder_ok, encoder_reason, effective_encoder_mode = resolve_ffmpeg_runtime_encoder_mode(ffmpeg_encoder_mode)
            if not encoder_ok:
                log("ERROR", encoder_reason)
                processing_log(f"[ERROR] Encode-Start abgebrochen: {encoder_reason}")
                status_refresh_set_index(None)
                status_refresh_write(force=True)
                status_refresh_stop.set()
                status_refresh_thread.join(timeout=1.5)
                return None
            if effective_encoder_mode != ffmpeg_encoder_mode:
                log("INFO", f"FFMPEG-Hardwarepfad: {ffmpeg_encoder_mode} -> {effective_encoder_mode}")
                processing_log(f"[INFO] Hardwarepfad angepasst: {ffmpeg_encoder_mode} -> {effective_encoder_mode}")
            ffmpeg_encoder_mode = effective_encoder_mode
        if not should_start:
            action_text = "Analyze" if mode == "a" else ("Copy" if mode == "c" else "Encode")
            log("WARN", f"{action_text}-Start nach Analyse abgebrochen.")
            processing_log(f"[INFO] {action_text}-Start nach Analyse abgebrochen. Keine Verarbeitung gestartet.")
            status_refresh_set_index(None)
            status_refresh_write(force=True)
            status_refresh_stop.set()
            status_refresh_thread.join(timeout=1.5)

            if mode == "a":
                for path in (STATUS_TABLE_FILE, OUT_TREE_FILE, OUT_PLAN_FILE, PROCESSING_LOG_FILE):
                    try:
                        overwrite_text_file(path, "")
                    except Exception:
                        pass
                for marker_file in (OUT_TREE_DONE_FILE, STATUS_DONE_FILE):
                    try:
                        marker_file.unlink()
                    except FileNotFoundError:
                        pass
                    except Exception:
                        pass
                sanitize_processing_log_file()
                return None

            csv_file = write_rows_to_db(final_rows)
            write_status_table(final_rows, mode=mode)
            out_plan = write_out_plan(final_rows, start_folder, copy_root)
            out_tree = write_out_tree(final_rows, target_out_prefix, target_tree_label)
            sanitize_processing_log_file()
            overwrite_text_file(OUT_TREE_DONE_FILE, "done\n")
            overwrite_text_file(STATUS_DONE_FILE, "done\n")
            log("OK", f"OUT-Plan gespeichert unter: {format_path(out_plan)}")
            log("OK", f"OUT-Tree gespeichert unter: {format_path(out_tree)}")
            return csv_file

    requeue_rows = [row for row in final_rows if _is_truthy(row.get("REQUEUE", ""))]
    if requeue_rows:
        requeue_keys = {normalize_source_row_name(str(row.get("Quellname", ""))) for row in requeue_rows}
        final_rows = [
            row
            for row in final_rows
            if normalize_source_row_name(str(row.get("Quellname", ""))) not in requeue_keys
        ]
        row_entries = [
            (row, source_abs)
            for (row, source_abs) in row_entries
            if normalize_source_row_name(str(row.get("Quellname", ""))) not in requeue_keys
        ]
        total = len(final_rows)
        processing_log(f"[RE-QUEUE] Ausgenommen vor Verarbeitung: {len(requeue_rows)} Datei(en).")
        status_refresh_set_index(None)
        status_refresh_write(force=True)

    forced_copy_seasons: set[str] = set()
    progress_log_interval = 25

    if mode == "f" and not PRECHECK_EGB_ENABLED:
        processing_log("[INFO] Pre-Check E-GB deaktiviert: Encode startet ohne E-GB-Vorpruefung.")

    def should_log_file_progress(index: int, total_count: int) -> bool:
        if index <= 1 or index >= total_count:
            return True
        return (index % progress_log_interval) == 0

    def persist_manifest(
        source_abs: Path,
        target_abs: Path,
        processed_mode: str,
        z_gb: str,
        *,
        row_data: dict[str, str] | None = None,
    ) -> None:
        mode_norm = normalize_manifest_mode(processed_mode)
        if mode_norm not in {"copy", "encode"}:
            return
        try:
            source_rel = str(source_abs.relative_to(start_folder))
        except Exception:
            source_rel = str(source_abs)
        try:
            target_rel = str(target_abs.relative_to(start_folder))
        except Exception:
            target_rel = str(target_abs)
        payload_row = dict(row_data or {})
        if target_rel and not (payload_row.get("Zielname", "") or "").strip():
            payload_row["Zielname"] = target_rel
        if z_gb and not (payload_row.get("Z-GB", "") or "").strip():
            payload_row["Z-GB"] = str(z_gb).strip()
        payload_row["MANIFEST-SKIP"] = "1"
        payload_row["MANIFEST-MODE"] = mode_norm
        payload_row["VERARBEITET"] = "1"
        saved = store_processed_source_history_row(
            source_rel,
            payload_row,
            processed_mode=mode_norm,
            target_rel=target_rel,
            z_gb=z_gb,
        )
        if not saved:
            log("WARN", f"DB-History konnte nicht gespeichert werden ({source_rel}).")
        source_key = normalize_source_row_name(source_rel)
        if source_key:
            manifest_prefilter[source_key] = {
                "target_rel": target_rel,
                "mode": mode_norm,
                "z_gb": str(z_gb or "").strip() or "n/a",
                "row": payload_row,
            }
    for idx, (row, source_abs) in enumerate(row_entries, start=1):
        file_started_ts = time.time()
        source_name = row.get("Quellname", "")
        row["Speed"] = row.get("Speed", "") or "n/a"
        row["FPS"] = row.get("FPS", "") or ("n/a" if mode == "f" else "")
        row["ETA"] = row.get("ETA", "") or "n/a"
        row["E-GB-BAND"] = row.get("E-GB-BAND", "") or ""
        row["E-GB-STATUS"] = row.get("E-GB-STATUS", "") or ""
        status_refresh_set_index(idx)
        status_refresh_write(force=True)

        def step_log(message: str, *, force: bool = False, tag_override: str = "") -> None:
            if not force and not should_log_file_progress(idx, total):
                return
            tag = str(tag_override or "").strip().upper() or ("COPY" if mode == "c" else "E-GB")
            processing_log(f"[{tag}] [{idx}/{total}] {Path(source_name).name} | {message}")

        def update_status(z_gb: str, speed: str, eta: str, est_gb: str = "", est_band: str = "", fps: str = "") -> None:
            row["Z-GB"] = z_gb
            row["Speed"] = speed or "n/a"
            if mode == "f":
                row["FPS"] = fps or "n/a"
            row["ETA"] = eta or "n/a"
            if est_gb:
                row["E-GB"] = est_gb
                row["E-GB-BAND"] = est_band or ""
            status_refresh_write()

        def finalize_copy_speed_text(last_progress_speed_text: str) -> str:
            try:
                source_bytes = max(0, int(source_abs.stat().st_size))
            except Exception:
                source_bytes = 0
            file_elapsed = max(0.001, time.time() - file_started_ts)
            copy_speed_mib = (source_bytes / (1024.0 ** 2)) / file_elapsed if source_bytes > 0 else 0.0
            progress_speed_mib = parse_copy_speed_mib(last_progress_speed_text)
            if progress_speed_mib > 0:
                copy_speed_mib = max(copy_speed_mib, progress_speed_mib)
            final_copy_speed_text = f"{copy_speed_mib:.1f} MiB/s"
            row["Speed"] = final_copy_speed_text
            row["ETA"] = format_eta_seconds(max(0.0, time.time() - file_started_ts))
            return final_copy_speed_text

        manifest_mode = normalize_manifest_mode(row.get("MANIFEST-MODE", ""))
        if mode in {"c", "f"} and row.get("MANIFEST-SKIP", "") == "1" and manifest_mode in {"copy", "encode"}:
            row["Speed"] = "copied" if manifest_mode == "copy" else "encoded"
            if mode == "f":
                row["FPS"] = "n/a"
            row["ETA"] = "copied" if manifest_mode == "copy" else "encoded"
            if mode == "c":
                row["E-GB"] = "copy"
                row["E-GB-BAND"] = ""
                row["E-GB-STATUS"] = "copy"
            else:
                set_row_egb_status_from_sizes(row, mode)
            step_log(
                f"Uebersprungen (DB-Verlauf): {row['ETA']} | Ziel={Path((row.get('Zielname', '') or '').strip()).name} | Z-GB={row.get('Z-GB', 'n/a')}",
                tag_override="COPY" if manifest_mode == "copy" else "FFMPEG",
            )
            row["VERARBEITET"] = "1"
            target_rel_skip = (row.get("Zielname", "") or row.get("MANIFEST-TARGET", "")).strip()
            if target_rel_skip:
                target_abs_skip = resolve_target_abs(start_folder, target_rel_skip)
            else:
                target_abs_skip = source_abs
            persist_manifest(source_abs, target_abs_skip, manifest_mode, row.get("Z-GB", "n/a"), row_data=row)
            status_refresh_write(force=True)
            continue

        if mode in {"c", "f"} and _is_truthy(row.get("MANUAL", "")):
            target_rel = (row.get("Zielname", "") or "").strip()
            if not target_rel:
                target_rel = build_manual_target_rel_path(source_name, start_folder)
                row["Zielname"] = target_rel
            target_abs = resolve_target_abs(start_folder, target_rel)
            row["Z-GB"], sub_count, nfo_count, final_target_abs = copy_row_payload(source_abs, target_abs)
            if final_target_abs != target_abs:
                try:
                    row["Zielname"] = str(final_target_abs.relative_to(start_folder))
                except Exception:
                    row["Zielname"] = str(final_target_abs)
                target_abs = final_target_abs
            row["E-GB"] = "copy"
            row["E-GB-BAND"] = ""
            row["E-GB-STATUS"] = "copy"
            row["Speed"] = "manual"
            if mode == "f":
                row["FPS"] = "n/a"
            row["ETA"] = "manual"
            step_log(f"Manual -> {MANUAL_DIR_NAME}: Ziel={target_abs.name}, Subs={sub_count}, NFO={nfo_count}", force=True)
            persist_manifest(source_abs, target_abs, "copy", row.get("Z-GB", "n/a"), row_data=row)
            row["VERARBEITET"] = "1"
            status_refresh_write(force=True)
            continue

        if mode == "c":
            target_rel = (row.get("Zielname", "") or "").strip()
            target_abs = resolve_target_abs(start_folder, target_rel)
            last_progress_speed_text = ""

            def on_copy_progress(z_gb: str, speed: str, eta: str) -> None:
                nonlocal last_progress_speed_text
                last_progress_speed_text = str(speed or "").strip()
                update_status(z_gb, speed, eta)

            row["Z-GB"], sub_count, nfo_count, final_target_abs = copy_row_payload(
                source_abs,
                target_abs,
                on_progress=on_copy_progress,
            )
            if final_target_abs != target_abs:
                try:
                    row["Zielname"] = str(final_target_abs.relative_to(start_folder))
                except Exception:
                    row["Zielname"] = str(final_target_abs)
                target_abs = final_target_abs
            row["E-GB"] = row.get("E-GB", "") or "n/a"
            row["E-GB-BAND"] = ""
            final_copy_speed_text = finalize_copy_speed_text(last_progress_speed_text)
            step_log(
                f"COPY OK | Ziel={target_abs.name} | Q-GB={row.get('Groesse', 'n/a')} | Z-GB={row.get('Z-GB', 'n/a')} | Speed: {final_copy_speed_text}",
                force=True,
                tag_override="COPY",
            )
            persist_manifest(source_abs, target_abs, "copy", row.get("Z-GB", "n/a"), row_data=row)
            status_refresh_write(force=True)
        elif mode == "f":
            target_rel = (row.get("Zielname", "") or "").strip()
            target_abs = resolve_target_abs(start_folder, target_rel)
            step_log(f"Zieldatei: {target_abs.name}")
            season_key = season_fallback_key(row)
            fallback_reason = ""
            q_gb = parse_gb_text(row.get("Groesse", ""))

            if should_skip_encode_for_4k_h265(source_abs, source_name):
                fallback_reason = '4k+h265-Quelle erkannt und "4k/h265 nicht encoden" aktiv'
            elif should_skip_encode_for_h265(source_abs, source_name):
                fallback_reason = 'h265-Quelle erkannt und "h265 nicht encoden" aktiv'
            elif season_key and season_key in forced_copy_seasons:
                fallback_reason = "Staffel-Fallback aktiv"
            elif PRECHECK_EGB_ENABLED:
                estimate = estimate_target_size_details(source_abs, step_log=step_log, encoder_mode=ffmpeg_encoder_mode)
                row["E-GB"] = estimate.get("estimate_gb", "n/a")
                row["E-GB-BAND"] = estimate.get("band_text", "")
                processing_log(
                    f"[FFMPEG] [{idx}/{total}] {Path(source_name).name} | Quelle:{row.get('Groesse', 'n/a')} GB | Hochrechnung:{row.get('E-GB', 'n/a')}{row.get('E-GB-BAND', '')} GB"
                )
                status_refresh_write(force=True)
                e_gb = parse_gb_text(row.get("E-GB", ""))
                e_band = parse_band_gb_text(row.get("E-GB-BAND", ""))
                e_low = max(0.0, e_gb - e_band)
                threshold = q_gb * 0.90
                if q_gb > 0 and e_gb > 0 and e_low > threshold:
                    e_disp = f"{e_gb:.1f}"
                    if e_band > 0:
                        e_disp = f"{e_disp}±{e_band:.1f}"
                    fallback_reason = (
                        f"E-GB {e_disp} GB > 90% von {q_gb:.1f} GB "
                        f"(untere Grenze {e_low:.1f} > {threshold:.1f})"
                    )
            else:
                row["E-GB"] = "n/a"
                row["E-GB-BAND"] = ""

            if fallback_reason:
                if season_key:
                    forced_copy_seasons.add(season_key)
                try:
                    if target_abs.exists():
                        target_abs.unlink()
                except Exception:
                    pass
                last_fallback_progress_speed_text = ""

                def on_fallback_copy_progress(z_gb: str, speed: str, eta: str) -> None:
                    nonlocal last_fallback_progress_speed_text
                    last_fallback_progress_speed_text = str(speed or "").strip()
                    row["Z-GB"] = z_gb
                    row["Speed"] = speed or "n/a"
                    row["FPS"] = "n/a"
                    row["ETA"] = eta or "n/a"
                    status_refresh_write()

                row["Z-GB"], sub_count, nfo_count, final_target_abs = copy_row_payload(
                    source_abs,
                    target_abs,
                    on_progress=on_fallback_copy_progress,
                )
                if final_target_abs != target_abs:
                    try:
                        row["Zielname"] = str(final_target_abs.relative_to(start_folder))
                    except Exception:
                        row["Zielname"] = str(final_target_abs)
                    target_abs = final_target_abs
                row["E-GB"] = row.get("E-GB", "") or "n/a"
                row["E-GB-BAND"] = ""
                final_copy_speed_text = finalize_copy_speed_text(last_fallback_progress_speed_text)
                row["FPS"] = "n/a"
                step_log(
                    f"Fallback -> Copy ({fallback_reason}): Ziel={target_abs.name}, Q-GB={row.get('Groesse', 'n/a')} | Z-GB={row.get('Z-GB', 'n/a')} | Subs={sub_count}, NFO={nfo_count} | Speed: {final_copy_speed_text}",
                    force=True,
                    tag_override="COPY",
                )
                persist_manifest(source_abs, target_abs, "copy", row.get("Z-GB", "n/a"), row_data=row)
                status_refresh_write(force=True)
            else:
                target_abs.parent.mkdir(parents=True, exist_ok=True)
                duration_sec = probe_duration_seconds(source_abs)
                runtime_min = runtime_minutes_value(row.get("Laufzeit (f)", ""))
                if duration_sec <= 0 and runtime_min > 0:
                    # Fallback only when ffprobe duration is unavailable.
                    duration_sec = float(runtime_min * 60)
                row["Z-GB"] = "0.0"
                row["Speed"] = "n/a"
                row["FPS"] = "n/a"
                row["ETA"] = "n/a"
                status_refresh_write(force=True)

                def log_60s(z_gb: str, speed: str, eta: str, est_gb: str, est_band: str, fps: str) -> None:
                    est_text = (est_gb or row.get("E-GB", "n/a") or "n/a").strip()
                    band_text = (est_band or row.get("E-GB-BAND", "") or "").strip()
                    processing_log(
                        f"[FFMPEG] [{idx}/{total}] {Path(source_name).name} | Q-GB: {row['Groesse']} | Z-GB: {z_gb} | E-GB: {est_text}{band_text} | Speed: {speed} | FPS: {fps} | ETA: {eta}"
                    )

                ok, reason = run_ffmpeg_encode_with_monitor(
                    source_abs,
                    target_abs,
                    q_gb=q_gb,
                    duration_sec=duration_sec,
                    encoder_mode=ffmpeg_encoder_mode,
                    on_status=update_status,
                    on_log_60s=log_60s,
                )

                if not ok:
                    if season_key:
                        forced_copy_seasons.add(season_key)
                    try:
                        if target_abs.exists():
                            target_abs.unlink()
                    except Exception:
                        pass
                    last_fallback_progress_speed_text = ""

                    def on_fallback_copy_progress(z_gb: str, speed: str, eta: str) -> None:
                        nonlocal last_fallback_progress_speed_text
                        last_fallback_progress_speed_text = str(speed or "").strip()
                        row["Z-GB"] = z_gb
                        row["Speed"] = speed or "n/a"
                        row["FPS"] = "n/a"
                        row["ETA"] = eta or "n/a"
                        status_refresh_write()

                    row["Z-GB"], sub_count, nfo_count, final_target_abs = copy_row_payload(
                        source_abs,
                        target_abs,
                        on_progress=on_fallback_copy_progress,
                    )
                    if final_target_abs != target_abs:
                        try:
                            row["Zielname"] = str(final_target_abs.relative_to(start_folder))
                        except Exception:
                            row["Zielname"] = str(final_target_abs)
                        target_abs = final_target_abs
                    row["E-GB"] = row.get("E-GB", "") or "n/a"
                    row["E-GB-BAND"] = ""
                    final_copy_speed_text = finalize_copy_speed_text(last_fallback_progress_speed_text)
                    row["FPS"] = "n/a"
                    step_log(
                        f"Fallback -> Copy ({reason}): Ziel={target_abs.name}, Q-GB={row.get('Groesse', 'n/a')} | Z-GB={row.get('Z-GB', 'n/a')} | Subs={sub_count}, NFO={nfo_count} | Speed: {final_copy_speed_text}",
                        force=True,
                        tag_override="COPY",
                    )
                    persist_manifest(source_abs, target_abs, "copy", row.get("Z-GB", "n/a"), row_data=row)
                    status_refresh_write(force=True)
                else:
                    detected_codec = probe_codec_from_video(target_abs)
                    final_target_abs = apply_codec_placeholder(target_abs, detected_codec)
                    final_target_abs = normalize_target_codec_name(final_target_abs, FFMPEG_TARGET_VIDEO_CODEC)
                    if final_target_abs != target_abs:
                        try:
                            row["Zielname"] = str(final_target_abs.relative_to(start_folder))
                        except Exception:
                            row["Zielname"] = str(final_target_abs)
                        target_abs = final_target_abs
                    sub_count, nfo_count = copy_sidecars_payload(source_abs, target_abs)
                    row["Z-GB"] = file_size_human(target_abs)
                    row["FPS"] = row.get("FPS", "") or "n/a"
                    row["ETA"] = "00:00"
                    step_log(f"FFMPEG abgeschlossen: Ziel={target_abs.name}, Subs={sub_count}, NFO={nfo_count}")
                    persist_manifest(source_abs, target_abs, "encode", row.get("Z-GB", "n/a"), row_data=row)
                    status_refresh_write(force=True)
        else:
            row["E-GB"] = row.get("E-GB", "") or "n/a"
            row["Speed"] = row.get("Speed", "") or "n/a"
            row["FPS"] = row.get("FPS", "") or ("n/a" if mode == "f" else "")
            row["ETA"] = row.get("ETA", "") or "n/a"
            status_refresh_write(force=True)

        if mode in {"c", "f"}:
            row["ETA"] = format_eta_seconds(max(0.0, time.time() - file_started_ts))
            set_row_egb_status_from_sizes(row, mode)
            row["VERARBEITET"] = "1"
            status_refresh_write(force=True)

        if should_log_file_progress(idx, total):
            if mode == "f":
                processing_log(
                    f"[FFMPEG] Fortschritt: {idx}/{total} | Q-GB={row['Groesse']} | Z-GB={row.get('Z-GB', 'n/a')} | "
                    f"E-GB={row['E-GB']}{row.get('E-GB-BAND', '')} | Lzeit={row['Laufzeit (f)']} | "
                    f"Speed={row.get('Speed', 'n/a')} | FPS={row.get('FPS', 'n/a')} | ETA={row.get('ETA', 'n/a')}"
                )
            elif mode == "a":
                processing_log(
                    f"[ANALYZE] Fortschritt: {idx}/{total} | Q-GB={row.get('Groesse', 'n/a')} | "
                    f"Lzeit={row.get('Laufzeit (f)', 'n/a')} | IMDB={row.get('IMDB-ID', 'n/a')}"
                )

    status_refresh_set_index(None)
    status_refresh_write(force=True)
    status_refresh_stop.set()
    status_refresh_thread.join(timeout=1.5)

    post_lines: list[str] = []

    def post_step(tag: str, text: str) -> None:
        message = (text or "").strip()
        if not message:
            return
        processing_log(f"[{tag}] {message}")
        post_lines.append(message)
        set_post_action_summary(post_lines)
        write_status_table(final_rows, total_files=total, current_index=None, mode=mode)

    if mode in {"c", "f"}:
        nas_root = TARGET_NFS_PATH
        if not nas_root.is_absolute():
            nas_root = (start_folder / nas_root).resolve()
        verified_targets_ok = False
        verified_missing_targets: list[str] = []
        verified_checked_targets = 0
        sync_ok = not SYNC_NAS_ENABLED

        post_step(
            "NACHLAUF",
            f"Optionen: Sync NAS={'an' if SYNC_NAS_ENABLED else 'aus'} | Sync Plex={'an' if SYNC_PLEX_ENABLED else 'aus'} | Loesche OUT={'an' if DEL_OUT_ENABLED else 'aus'} | Loesche Quelle={'an' if DEL_SOURCE_ENABLED else 'aus'}",
        )

        if SYNC_NAS_ENABLED:
            processing_log("[SYNC-NAS] START")
            ok_sync, sync_stats = sync_out_tree_to_nas(
                start_folder,
                nas_root,
                on_progress=lambda text: processing_log(f"[SYNC-NAS] {text}"),
                progress_interval_sec=60.0,
            )
            copied = int(sync_stats.get("copied", 0) or 0)
            reused = int(sync_stats.get("reused", 0) or 0)
            failed = int(sync_stats.get("failed", 0) or 0)
            total_sync = int(sync_stats.get("source_files", 0) or 0)
            total_sync_bytes = int(sync_stats.get("total_bytes", 0) or 0)
            copied_sync_bytes = int(sync_stats.get("copied_bytes", 0) or 0)
            reused_sync_bytes = int(sync_stats.get("reused_bytes", 0) or 0)
            elapsed_sync_sec = float(sync_stats.get("elapsed_sec", 0.0) or 0.0)
            speed_mib = float(sync_stats.get("speed_mib", 0.0) or 0.0)
            speed_text = f"{speed_mib:.1f} MB/s"
            sync_eta_text = "n/a"
            if total_sync_bytes > 0 and copied_sync_bytes >= total_sync_bytes:
                sync_eta_text = "00:00"
            elif speed_mib > 0 and total_sync_bytes > copied_sync_bytes:
                remain_sync_bytes = max(0, total_sync_bytes - copied_sync_bytes)
                sync_eta_text = format_eta_seconds(remain_sync_bytes / (speed_mib * 1024.0 * 1024.0))
            sync_runtime_text = format_eta_seconds(max(0.0, elapsed_sync_sec))
            if ok_sync:
                sync_ok = True
                post_step(
                    "SYNC-NAS",
                    f"Sync NAS ok: {copied}/{total_sync} Datei(en) nach {nas_root} | Reused={reused} | "
                    f"Reused-GB={reused_sync_bytes / (1024.0 ** 3):.1f} | Speed: {speed_text} | ETA: {sync_eta_text} | Laufzeit: {sync_runtime_text}",
                )
            else:
                sync_ok = False
                error = str(sync_stats.get("error", "") or "").strip()
                detail = f" | Fehler={error}" if error else ""
                post_step(
                    "SYNC-NAS",
                    f"Sync NAS unvollstaendig: kopiert={copied}, reused={reused}, Fehler={failed}{detail} | "
                    f"Reused-GB={reused_sync_bytes / (1024.0 ** 3):.1f} | Speed: {speed_text} | ETA: {sync_eta_text} | Laufzeit: {sync_runtime_text}",
                )
                for item in (sync_stats.get("failed_examples", []) or [])[:5]:
                    processing_log(f"[SYNC-NAS] FEHLT/KOPIERFEHLER: {item}")

        verified_targets_ok, verified_missing_targets, verified_checked_targets = verify_source_targets_exist(
            final_rows,
            start_folder,
            nas_root,
        )

        if SYNC_PLEX_ENABLED:
            if not SYNC_NAS_ENABLED:
                post_step("SYNC-PLEX", "Abbruch: nur mit aktivem Sync NAS erlaubt.")
            elif not sync_ok:
                post_step("SYNC-PLEX", "Abbruch: Sync NAS fehlgeschlagen.")
            else:
                processing_log("[SYNC-PLEX] START")
                ok_plex, plex_stats = plex_refresh_all_libraries(PLEX_IP_RAW, PLEX_TOKEN_RAW)
                if ok_plex:
                    total_libs = int(plex_stats.get("libraries_total", 0) or 0)
                    refreshed = int(plex_stats.get("refreshed", 0) or 0)
                    failed = int(plex_stats.get("failed", 0) or 0)
                    base_url = str(plex_stats.get("base_url", "") or "").strip()
                    post_step(
                        "SYNC-PLEX",
                        f"Plex-Rescan ok: refreshed={refreshed}/{total_libs}, failed={failed}, server={base_url}",
                    )
                    for name in (plex_stats.get("refreshed_names", []) or [])[:8]:
                        processing_log(f"[SYNC-PLEX] refreshed: {name}")
                    extra_refreshed = max(0, len(plex_stats.get("refreshed_names", []) or []) - 8)
                    if extra_refreshed > 0:
                        processing_log(f"[SYNC-PLEX] ... weitere {extra_refreshed} Library(s) refreshed")
                else:
                    error = str(plex_stats.get("error", "") or "").strip() or "unbekannter Fehler"
                    post_step("SYNC-PLEX", f"Plex-Rescan fehlgeschlagen: {error}")
                    failed_names = (plex_stats.get("failed_names", []) or [])[:8]
                    for name in failed_names:
                        processing_log(f"[SYNC-PLEX] failed: {name}")

        if DEL_SOURCE_ENABLED:
            processing_log("[DEL-QUELLE] START")
            if not DEL_SOURCE_CONFIRMED:
                post_step("DEL-QUELLE", "Abbruch: keine Zustimmung im Startfenster.")
            elif SYNC_NAS_ENABLED and not sync_ok:
                post_step("DEL-QUELLE", "Abbruch: Sync NAS fehlgeschlagen, kein verifizierter Spiegel verfuegbar.")
            else:
                if not verified_targets_ok:
                    post_step(
                        "DEL-QUELLE",
                        f"Abbruch: Zielpruefung fehlgeschlagen ({len(verified_missing_targets)}/{verified_checked_targets} fehlen).",
                    )
                    for item in verified_missing_targets[:8]:
                        processing_log(f"[DEL-QUELLE] FEHLT: {item}")
                    if len(verified_missing_targets) > 8:
                        processing_log(f"[DEL-QUELLE] ... weitere {len(verified_missing_targets) - 8} fehlende Datei(en)")
                else:
                    deleted_files, deleted_dirs, failed_count = delete_source_tree_below_start(start_folder)
                    if failed_count == 0:
                        post_step(
                            "DEL-QUELLE",
                            f"Quelle geloescht: Dateien={deleted_files}, Ordner={deleted_dirs} (ohne Zielordner).",
                        )
                    else:
                        post_step(
                            "DEL-QUELLE",
                            f"Teilweise geloescht: Dateien={deleted_files}, Ordner={deleted_dirs}, Fehler={failed_count}.",
                        )

        if DEL_OUT_ENABLED:
            processing_log("[DEL-OUT] START")
            if not SYNC_NAS_ENABLED:
                post_step("DEL-OUT", "Abbruch: nur nach erfolgreichem Sync NAS erlaubt.")
            elif not sync_ok:
                post_step("DEL-OUT", "Abbruch: Sync NAS fehlgeschlagen, kein verifizierter Spiegel verfuegbar.")
            elif not verified_targets_ok:
                post_step(
                    "DEL-OUT",
                    f"Abbruch: Zielpruefung fehlgeschlagen ({len(verified_missing_targets)}/{verified_checked_targets} fehlen).",
                )
            else:
                ok_out, out_msg = clear_out_tree(start_folder)
                if ok_out:
                    post_step("DEL-OUT", out_msg)
                else:
                    post_step("DEL-OUT", f"Fehler: {out_msg}")

    write_status_table(final_rows, total_files=total, current_index=None, mode=mode)

    csv_file = write_rows_to_db(final_rows)
    write_status_table(final_rows, mode=mode)
    out_plan = write_out_plan(final_rows, start_folder, copy_root)
    out_tree = write_out_tree(final_rows, target_out_prefix, target_tree_label)
    sanitize_processing_log_file()
    overwrite_text_file(OUT_TREE_DONE_FILE, "done\n")
    overwrite_text_file(STATUS_DONE_FILE, "done\n")
    log("OK", f"OUT-Plan gespeichert unter: {format_path(out_plan)}")
    log("OK", f"OUT-Tree gespeichert unter: {format_path(out_tree)}")
    return csv_file


def main() -> None:
    global MIRROR_LOG_TO_PROCESSING, SCRIPT_START_TS
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--api-check-only", action="store_true", help="Prueft nur API-Verbindungen und beendet danach.")
    parser.add_argument("--folder", help="Optionaler Startordner (ohne Finder-Auswahl).")
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("-a", "--analyze", action="store_true", help="Analyze-Modus.")
    mode_group.add_argument("-c", "--copy", action="store_true", help="Copy-Modus.")
    mode_group.add_argument("-f", "--ffmpeg", action="store_true", help="FFMPEG-Modus.")
    args = parser.parse_args()
    SCRIPT_START_TS = time.time()
    configure_local_tmpdir()
    terminate_old_managemovie_processes()

    MIRROR_LOG_TO_PROCESSING = False
    log("INFO", f"ManageMovie gestartet (Version {VERSION})")
    ensure_venv()
    ensure_dependencies()
    init_mariadb_schema()
    migrate_legacy_gemini_cache_file()
    purge_legacy_gemini_files()

    gemini_key, tmdb_key = read_secret_keys(SECRET_FILE)
    if AI_QUERY_DISABLED:
        log("INFO", "Advanced: KI-Abfrage deaktiviert. KI-Aufrufe sind komplett ausgeschaltet (TMDB-only).")

    if args.api_check_only:
        if AI_QUERY_DISABLED:
            log("INFO", "KI-Check uebersprungen: KI-Abfrage deaktiviert.")
        else:
            if not gemini_key:
                raise RuntimeError(
                    "KI-Key fehlt: settings.gemini_api ist leer "
                    "und MANAGEMOVIE_GEMINI_KEY ist nicht gesetzt."
                )
            check_gemini_connection_and_tokens(gemini_key)
        if TMDB_ENABLED and tmdb_key:
            check_tmdb_connection(tmdb_key)
        elif TMDB_ENABLED and not tmdb_key:
            log("WARN", "TMDB aktiv, aber kein Key gesetzt. TMDB-Checks uebersprungen.")
        else:
            log("INFO", "TMDB-Abfrage ist deaktiviert.")
        log("OK", "Nur API-Check ausgefuehrt.")
        return

    if TMDB_ENABLED and tmdb_key:
        check_tmdb_connection(tmdb_key)
    elif TMDB_ENABLED and not tmdb_key:
        log("WARN", "TMDB aktiv, aber kein Key gesetzt. TMDB-Anreicherung uebersprungen.")
    else:
        log("INFO", "TMDB-Abfrage ist deaktiviert.")

    if args.analyze:
        mode = "a"
    elif args.copy:
        mode = "c"
    elif args.ffmpeg:
        mode = "f"
    else:
        mode = choose_start_mode()
        if not mode:
            raise RuntimeError("Startparameter-Auswahl abgebrochen.")
    reset_files = cleanup_previous_output(mode)
    overwrite_text_file(OUT_TREE_FILE, "")
    overwrite_text_file(PROCESSING_LOG_FILE, "")
    MIRROR_LOG_TO_PROCESSING = LOGS_ENABLED
    for old_file in reset_files:
        log("INFO", f"Arbeitsdatei zurückgesetzt: {format_path(old_file)}")
    log("INFO", f"Startmodus: -{mode}")

    if args.folder:
        folder = Path(args.folder)
    else:
        folder = choose_folder()

    if not folder.exists() or not folder.is_dir():
        raise RuntimeError(f"Ungültiger Ordner: {folder}")

    log("INFO", f"Gewählter Ordner: {folder}")

    if LOGS_ENABLED:
        write_status_table([], total_files=0, current_index=None, mode=mode)
        if sys.platform == "darwin" and TERMINAL_UI_ENABLED and not is_web_ui_only_mode():
            close_old_ui_windows()
            open_status_terminal(VERSION, STATUS_TABLE_FILE)
            open_out_tree_terminal(VERSION, OUT_TREE_FILE)

    extract_isos_in_tree(folder)
    source_files = collect_video_rel_paths(folder)
    tree_content = build_tree_from_paths(folder.name, source_files)

    manifest_entries: dict[str, dict[str, str]] = {}

    out_path = write_tree_file(tree_content)
    log("OK", f"Tree gespeichert unter: {format_path(out_path)}")
    csv_path = generate_output_csv(gemini_key, tmdb_key, tree_content, source_files, folder, mode, manifest_entries=manifest_entries)
    if csv_path is None:
        log("INFO", "Lauf nach Analyse abgebrochen.")
        return
    log("OK", f"Runtime-CSV in DB gespeichert: {csv_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log("ERROR", str(exc))
        traceback.print_exc()
        sys.exit(1)
