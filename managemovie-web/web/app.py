#!/usr/bin/env python3
from __future__ import annotations
import csv
import errno
import hashlib
import io
import json
import os
import platform
import re
import secrets
import signal
import shlex
import shutil
import socket
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from flask import Flask, Response, jsonify, make_response, redirect, render_template_string, request, url_for
APP_DIR = Path(__file__).resolve().parent
BASE_DIR = APP_DIR.parent
APP_CORE_DIR = BASE_DIR / "app"
if str(APP_CORE_DIR) not in sys.path:
    sys.path.insert(0, str(APP_CORE_DIR))

from mmcore.db_cache import GeminiDbStore
from mmcore.secret_store import (
    STATE_SECRET_KEYS,
    decrypt_state_value,
    encrypt_state_value,
    is_encrypted_state_value,
    state_crypto_configured,
)
from mmcore.web_settings import apply_secret_update, build_public_runtime_settings

def detect_default_folder() -> str:
    env = (os.environ.get("MANAGEMOVIE_DEFAULT_FOLDER", "") or "").strip()
    if env:
        return env

    for candidate in ("/mnt/NFS/GK-Filer", "/mnt/NFS", "/mnt", str(Path.home())):
        try:
            path = Path(candidate).expanduser()
            if path.exists() and path.is_dir():
                return str(path.resolve())
        except Exception:
            continue
    return str(Path.home())


def detect_browse_root() -> Path:
    env = (os.environ.get("MANAGEMOVIE_BROWSE_ROOT", "") or "").strip()
    if env:
        try:
            return Path(env).expanduser().resolve()
        except Exception:
            pass

    return Path("/").resolve()

def detect_data_root() -> Path:
    raw = (os.environ.get("MANAGEMOVIE_DATA_ROOT", "") or "").strip() or "./MovieManager"
    if raw.endswith("MovieMaager"):
        raw = raw[: -len("MovieMaager")] + "MovieManager"
    path = Path(raw).expanduser()
    if path.name == "MovieManager":
        legacy_path = path.with_name("MovieMaager")
        if not path.exists() and legacy_path.exists():
            try:
                legacy_path.rename(path)
            except Exception:
                pass
    try:
        return path.resolve()
    except Exception:
        return path


DATA_DIR = detect_data_root()

CORE_SCRIPT = BASE_DIR / "app" / "managemovie.py"
BIN_DIR = BASE_DIR / "bin"
WORK_DIR = DATA_DIR / "work"
TEMP_DIR = DATA_DIR / "temp"
LOG_DIR = DATA_DIR / "logs"
STATUS_FILE = WORK_DIR / "gemini-status-table.txt"
OUT_TREE_FILE = WORK_DIR / "out_tree.txt"
PROCESSING_LOG_FILE = WORK_DIR / "processing_log.txt"
OUT_PLAN_FILE = WORK_DIR / "out_plan.txt"
CONFIRM_FILE = WORK_DIR / "web-confirm.json"
DEFAULT_FOLDER = detect_default_folder()
BROWSE_ROOT = detect_browse_root()
VERSION_STATE_FILE = DATA_DIR / "VERSION.current"
LEGACY_LAST_FOLDER_STATE_FILE = DATA_DIR / "LAST_FOLDER.current"
LEGACY_LAST_MODE_STATE_FILE = DATA_DIR / "LAST_MODE.current"
LEGACY_LAST_ENCODER_STATE_FILE = DATA_DIR / "LAST_ENCODER.current"
RELEASE_MAJOR = 0
RELEASE_MINOR = 2
VERSION_MIN_PATCH = 0
VERSION_MAX_PATCH = 999
MANAGEMOVIE_TRACK_FILE_ALIASES = (".managemovie.txt", ".managamovie.txt")
MANAGEMOVIE_VIDEO_MANIFEST_SUFFIX = ".managemovie.txt"
DEFAULT_TARGET_NFS_PATH = "/Volumes/Data/Movie/"
DEFAULT_TARGET_OUT_PATH = "__OUT"
REENQUEUE_DIR_NAME = "__RE-ENQUEUE"
DEFAULT_TARGET_REENQUEUE_PATH = REENQUEUE_DIR_NAME
MANUAL_DIR_NAME = "__MANUAL"
DEFAULT_NAS_IP = "192.168.52.4"
DEFAULT_PLEX_IP = "192.168.52.5"
REENQUEUE_SIDECAR_EXTENSIONS = {
    ".srt", ".ass", ".ssa", ".vtt", ".sub", ".idx", ".nfo", ".txt",
}
REENQUEUE_VIDEO_EXTENSIONS = {
    ".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm",
    ".m4v", ".mpg", ".mpeg", ".ts", ".mts", ".m2ts", ".3gp",
}

MARIADB_HOST = (os.environ.get("MANAGEMOVIE_DB_HOST", "127.0.0.1") or "127.0.0.1").strip()
MARIADB_PORT = int((os.environ.get("MANAGEMOVIE_DB_PORT", "3306") or "3306").strip() or "3306")
MARIADB_DB = (os.environ.get("MANAGEMOVIE_DB_NAME", "managemovie") or "managemovie").strip()
MARIADB_USER = (os.environ.get("MANAGEMOVIE_DB_USER", "managemovie") or "managemovie").strip()
MARIADB_PASSWORD = os.environ.get("MANAGEMOVIE_DB_PASSWORD", "")
MARIADB_RETENTION_DAYS = int((os.environ.get("MANAGEMOVIE_DB_RETENTION_DAYS", "365") or "365").strip() or "365")

STATE_KEY_LAST_FOLDER = "web.last_folder"
STATE_KEY_LAST_MODE = "web.last_mode"
STATE_KEY_LAST_ENCODER = "web.last_encoder"
STATE_KEY_TARGET_NFS_PATH = "settings.target_nfs_path"
STATE_KEY_TARGET_OUT_PATH = "settings.target_out_path"
STATE_KEY_TARGET_REENQUEUE_PATH = "settings.target_reenqueue_path"
STATE_KEY_NAS_IP = "settings.nas_ip"
STATE_KEY_PLEX_IP = "settings.plex_ip"
STATE_KEY_PLEX_API = "settings.plex_api"
STATE_KEY_TMDB_API = "settings.tmdb_api"
STATE_KEY_GEMINI_API = "settings.gemini_api"
STATE_KEY_AI_QUERY_DISABLED = "settings.ai_query_disabled"
STATE_KEY_SKIP_H265_ENCODE = "settings.skip_h265_encode"
STATE_KEY_SKIP_4K_H265_ENCODE = "settings.skip_4k_h265_encode"
STATE_KEY_ENCODE_ONCE_4K = "settings.encode_once_4k"
STATE_KEY_ONE_ENCODE_PER_WORKER = "settings.one_encode_per_worker"
STATE_KEY_ALLOW_WORKER_4 = "settings.allow_worker_4"
STATE_KEY_PARALLEL_JOB_LIMIT = "settings.parallel_job_limit"
STATE_KEY_PRECHECK_EGB = "settings.precheck_egb"
STATE_KEY_SPEED_FALLBACK_COPY = "settings.speed_fallback_copy"
STATE_KEY_START_ON_BOOT = "settings.start_on_boot"
STATE_KEY_INITIAL_SETUP_DONE = "settings.initial_setup_done"
STATE_KEY_AUTO_CONFIRM_START_FOLDER = "settings.auto_confirm_start_folder"
STATE_KEY_HISTORY_OFF_START_FOLDER = "settings.history_off_start_folder"
STATE_KEY_MIGRATED_V1 = "settings.migrated_v1"
STATE_KEY_MIGRATED_SECRETS_V2 = "settings.migrated_secret_encryption_v2"
EDITOR_SOURCE_ROW_CACHE_PREFIX = "editor.source.row."
GEMINI_SOURCE_ROW_CACHE_PREFIX = "gemini.source.row."
PROCESSED_SOURCE_ROW_CACHE_PREFIX = "processed.source.row."
EDITOR_SOURCE_ROW_RETENTION_DAYS = 365

STATE_DB_STORE = GeminiDbStore(
    host=MARIADB_HOST,
    port=MARIADB_PORT,
    database=MARIADB_DB,
    user=MARIADB_USER,
    password=MARIADB_PASSWORD,
    retention_days=MARIADB_RETENTION_DAYS,
    connect_timeout_sec=8,
    read_timeout_sec=20,
    write_timeout_sec=20,
)
STATE_DB_READY = False
STATE_DB_FAILED = False
STATE_DB_RETRY_AFTER = 0.0
STATE_DB_RETRY_COOLDOWN_SEC = 20.0

app = Flask(__name__)

DEFAULT_WORKER_SPECS = (
    {"name": "mamow01", "host": "mamow01", "node": "pve01", "ctid": "241", "mount_root": "/mnt/Q-NAS", "rootfs_size_gb": "6", "swap_mb": "4096", "default_encoder": "intel_qsv", "hwaddr": "42:4D:AE:00:F4:01"},
    {"name": "mamow02", "host": "mamow02", "node": "pve02", "ctid": "242", "mount_root": "/mnt/Q-NAS", "rootfs_size_gb": "6", "swap_mb": "4096", "default_encoder": "intel_qsv", "hwaddr": "42:4D:AE:00:F4:02"},
    {"name": "mamow03", "host": "mamow03", "node": "pve03", "ctid": "243", "mount_root": "/mnt/Q-NAS", "rootfs_size_gb": "6", "swap_mb": "4096", "default_encoder": "intel_qsv", "hwaddr": "42:4D:AE:00:F4:03"},
    {"name": "mamow04", "host": "mamow04", "node": "pve04", "ctid": "244", "mount_root": "/mnt/Q-NAS", "rootfs_size_gb": "6", "swap_mb": "4096", "default_encoder": "intel_qsv", "hwaddr": "42:4D:AE:00:F4:04"},
)
WORKER_STATUS_CACHE_TTL_SEC = 10.0
DEFAULT_WORKER_NAS_EXPORT = "192.168.100.201:/mnt/pool2_5x8tb/media3"


def nocache_html_response(html: str):
    response = make_response(html)
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


def detect_site_title() -> str:
    env = (os.environ.get("MANAGEMOVIE_SITE_TITLE", "") or "").strip()
    if env:
        return env
    if platform.system().strip().lower() == "darwin":
        return "ManageMovie Mac"
    return "ManageMovie LXS"


def initial_settings_gate_enabled() -> bool:
    return parse_form_bool(os.environ.get("MANAGEMOVIE_REQUIRE_INITIAL_SETTINGS", "0"))


def configured_basic_auth() -> tuple[str, str] | None:
    user = (os.environ.get("MANAGEMOVIE_WEB_USER", "") or "").strip()
    password = os.environ.get("MANAGEMOVIE_WEB_PASSWORD", "")
    if not user and not password:
        return None
    if not user:
        user = "admin"
    return user, password


def is_request_authorized() -> bool:
    auth_cfg = configured_basic_auth()
    if not auth_cfg:
        return True
    user_expected, pw_expected = auth_cfg
    auth = request.authorization
    if auth is None:
        return False
    if (auth.type or "").lower() != "basic":
        return False
    user_ok = secrets.compare_digest(auth.username or "", user_expected)
    pw_ok = secrets.compare_digest(auth.password or "", pw_expected)
    return user_ok and pw_ok


@app.before_request
def require_basic_auth():
    if is_request_authorized():
        return None
    return Response(
        "Authentication required.\n",
        401,
        {"WWW-Authenticate": 'Basic realm="ManageMovie"'},
    )


@app.after_request
def add_no_cache_headers(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


class JobState:
    def __init__(
        self,
        job_id: str,
        mode: str,
        folder: str,
        encoder: str,
        sync_nas: bool,
        sync_plex: bool,
        del_out: bool,
        del_source: bool,
        started_at: float,
        process: subprocess.Popen,
        log_path: Path,
        worker_name: str = "",
        release_version: str = "-",
        running: bool = True,
        exit_code: int | None = None,
        ended_at: float | None = None,
    ) -> None:
        self.job_id = job_id
        self.mode = mode
        self.folder = folder
        self.encoder = encoder
        self.sync_nas = sync_nas
        self.sync_plex = sync_plex
        self.del_out = del_out
        self.del_source = del_source
        self.started_at = started_at
        self.process = process
        self.log_path = log_path
        self.worker_name = worker_name
        self.release_version = release_version
        self.running = running
        self.exit_code = exit_code
        self.ended_at = ended_at


job_lock = threading.Lock()
current_job: JobState | None = None
dispatch_lock = threading.Lock()
dispatch_master_job: JobState | None = None
dispatch_subjobs: dict[str, dict[str, Any]] = {}
dispatch_pending_rows: list[dict[str, Any]] = []
dispatch_round_robin_cursor_name = ""
dispatch_runtime_rows: list[dict[str, Any]] = []
dispatch_worker_pool: list[dict[str, Any]] = []
restart_lock = threading.Lock()
restart_requested_at = 0.0
update_lock = threading.Lock()
update_requested_at = 0.0
pending_payload_cache_lock = threading.Lock()
pending_payload_cache_mtime_ns = -1
pending_payload_cache_size = -1
pending_payload_cache_payload: dict | None = None
pending_status_override_cache_lock = threading.Lock()
pending_status_override_cache_key = ""
pending_status_override_cache_text = ""
BOOT_START_FLAG_FILE = DATA_DIR / "config" / "start_on_boot.flag"
worker_status_cache_lock = threading.Lock()
worker_status_cache_expires_at = 0.0
worker_status_cache_payload: list[dict[str, Any]] = []
worker_enabled_runtime_overrides: dict[str, bool] = {}
worker_reinit_runtime_states: dict[str, bool] = {}
worker_busy_runtime_states: dict[str, str] = {}
worker_busy_runtime_states: dict[str, str] = {}


def ensure_layout() -> None:
    for path in (DATA_DIR, BIN_DIR, WORK_DIR, TEMP_DIR, LOG_DIR, DATA_DIR / "certs" / "server", DATA_DIR / "certs" / "ca"):
        path.mkdir(parents=True, exist_ok=True)
    init_state_store()
    try:
        write_start_on_boot_flag(read_runtime_settings().get("start_on_boot", "1"))
    except Exception:
        pass


def normalize_simple_text(raw: str | None) -> str:
    return str(raw or "").strip()


def normalize_ipv4(raw: str | None) -> str:
    value = normalize_simple_text(raw)
    if not value:
        return ""
    if not re.fullmatch(r"(?:\d{1,3}\.){3}\d{1,3}", value):
        return ""
    parts = value.split(".")
    try:
        nums = [int(part) for part in parts]
    except Exception:
        return ""
    if any(part < 0 or part > 255 for part in nums):
        return ""
    return value


def simple_host_label() -> str:
    try:
        return platform.node().strip() or "mamo01"
    except Exception:
        return "mamo01"


def master_db_host_for_workers() -> str:
    current = normalize_ipv4(MARIADB_HOST)
    if current and current != "127.0.0.1":
        return current
    detected = ""
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.connect(("8.8.8.8", 80))
        detected = normalize_ipv4(probe.getsockname()[0])
        probe.close()
    except Exception:
        pass
    if detected and detected != "127.0.0.1":
        return detected
    explicit = normalize_ipv4(os.environ.get("MANAGEMOVIE_MASTER_DB_HOST", ""))
    if explicit and explicit != "127.0.0.1":
        return explicit
    return ""


def worker_enabled_state_key(name: str) -> str:
    normalized = re.sub(r"[^a-z0-9_-]+", "", str(name or "").strip().lower())
    return f"worker.enabled.{normalized or 'unknown'}"


def read_worker_specs() -> list[dict[str, str]]:
    raw = (os.environ.get("MANAGEMOVIE_WORKER_SPECS", "") or "").strip()
    specs: list[dict[str, str]] = []
    if raw:
        for chunk in raw.split(";"):
            parts = [str(part or "").strip() for part in chunk.split("|")]
            if not parts or not parts[0]:
                continue
            name = parts[0]
            specs.append(
                {
                    "name": name,
                    "host": parts[1] if len(parts) > 1 and parts[1] else name,
                    "node": parts[2] if len(parts) > 2 else "",
                    "ctid": parts[3] if len(parts) > 3 else "",
                    "mount_root": parts[4] if len(parts) > 4 and parts[4] else "/mnt/Q-NAS",
                    "rootfs_size_gb": parts[5] if len(parts) > 5 and parts[5] else "6",
                    "swap_mb": parts[6] if len(parts) > 6 and parts[6] else "4096",
                    "default_encoder": parts[7] if len(parts) > 7 and parts[7] else "intel_qsv",
                }
            )
    if not specs:
        specs = [dict(item) for item in DEFAULT_WORKER_SPECS]
    return specs


def read_worker_spec_map() -> dict[str, dict[str, str]]:
    return {item["name"]: item for item in read_worker_specs()}


def read_worker_enabled_states() -> dict[str, bool]:
    specs = read_worker_specs()
    defaults = {worker_enabled_state_key(item["name"]): "1" for item in specs}
    values = read_state_values(defaults)
    result = {
        item["name"]: parse_form_bool(values.get(worker_enabled_state_key(item["name"]), "1"))
        for item in specs
    }
    for key, value in worker_enabled_runtime_overrides.items():
        result[key] = bool(value)
    return result


def write_worker_enabled_state(name: str, enabled: bool) -> bool:
    worker_enabled_runtime_overrides[str(name or "").strip()] = bool(enabled)
    return write_state_value(worker_enabled_state_key(name), "1" if enabled else "0") or True


def set_worker_reinit_running(name: str, running: bool) -> None:
    worker_reinit_runtime_states[str(name or "").strip()] = bool(running)
    invalidate_worker_state_cache()


def set_worker_busy_action(name: str, action: str | None) -> None:
    key = str(name or "").strip()
    action_text = str(action or "").strip().lower()
    if not key:
        return
    if action_text:
        worker_busy_runtime_states[key] = action_text
    else:
        worker_busy_runtime_states.pop(key, None)
    invalidate_worker_state_cache()


def set_worker_busy_action(name: str, action: str | None) -> None:
    key = str(name or "").strip()
    action_text = str(action or "").strip().lower()
    if not key:
        return
    if action_text:
        worker_busy_runtime_states[key] = action_text
    else:
        worker_busy_runtime_states.pop(key, None)
    invalidate_worker_state_cache()


def run_worker_ssh(worker_host: str, remote_cmd: str, timeout: int = 10) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            f"ConnectTimeout={max(2, int(timeout))}",
            worker_host,
            remote_cmd,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=max(3, int(timeout) + 2),
    )


def run_proxmox_ssh(node: str, remote_cmd: str, timeout: int = 20) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            f"ConnectTimeout={max(2, int(timeout))}",
            f"root@{node}",
            remote_cmd,
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=max(5, int(timeout) + 3),
    )


def read_master_public_ssh_key() -> str:
    for path in (Path("/root/.ssh/id_ed25519.pub"), Path("/root/.ssh/id_rsa.pub")):
        try:
            value = path.read_text(encoding="utf-8").strip()
        except Exception:
            value = ""
        if value:
            return value
    return ""


def worker_nas_export() -> str:
    return (os.environ.get("MANAGEMOVIE_WORKER_NAS_EXPORT", DEFAULT_WORKER_NAS_EXPORT) or DEFAULT_WORKER_NAS_EXPORT).strip()


def apply_worker_profile(spec: dict[str, str]) -> tuple[bool, str]:
    worker_name = str(spec.get("name", "") or "").strip() or "worker"
    node = str(spec.get("node", "") or "").strip()
    ctid = str(spec.get("ctid", "") or "").strip()
    mount_root = str(spec.get("mount_root", "") or "").strip() or "/mnt/Q-NAS"
    master_pub = read_master_public_ssh_key()
    if not node or not ctid:
        return False, "Node/CT-ID fehlt"
    if not master_pub:
        return False, "SSH-Public-Key von mamo01 fehlt"
    export_path = worker_nas_export()
    q_pub = shlex.quote(master_pub)
    q_export = shlex.quote(export_path)
    q_mount = shlex.quote(mount_root)
    q_mount_alt = shlex.quote("/mnt/Movie")
    master_db_host = master_db_host_for_workers()
    script = f"""
set -e
mkdir -p /root/.ssh
chmod 700 /root/.ssh
touch /root/.ssh/authorized_keys
tr '\\r' '\\n' < /root/.ssh/authorized_keys \\
  | sed 's/nssh-ed25519 /\\nssh-ed25519 /g; s/nssh-rsa /\\nssh-rsa /g; s/nssh-ecdsa-/\\nssh-ecdsa-/g' \\
  | awk 'NF && !seen[$0]++' > /root/.ssh/authorized_keys.tmp
grep -qxF {q_pub} /root/.ssh/authorized_keys.tmp || printf '%s\\n' {q_pub} >> /root/.ssh/authorized_keys.tmp
mv /root/.ssh/authorized_keys.tmp /root/.ssh/authorized_keys
mkdir -p {q_mount}
python3 - <<'PY'
from pathlib import Path
target = {mount_root!r}
export = {export_path!r}
line = f"{{export}} {{target}} nfs nfsvers=4.2,proto=tcp,port=2049,_netdev,noatime,rsize=1048576,wsize=1048576 0 0"
fstab = Path('/etc/fstab')
existing = fstab.read_text(encoding='utf-8').splitlines() if fstab.exists() else []
filtered = []
for raw in existing:
    stripped = raw.strip()
    if stripped and not stripped.startswith('#') and (' /mnt/Movie ' in f" {{stripped}} " or ' /mnt/Q-NAS ' in f" {{stripped}} "):
        continue
    filtered.append(raw)
filtered.append(line)
fstab.write_text("\\n".join(filtered).rstrip() + "\\n", encoding='utf-8')
PY
python3 - <<'PY'
from pathlib import Path
path = Path('/opt/managemovie/.env.local')
key = 'MANAGEMOVIE_MASTER_DB_HOST'
value = {master_db_host!r}
lines = path.read_text(encoding='utf-8').splitlines() if path.exists() else []
updated = []
seen = False
for raw in lines:
    if raw.startswith(key + '='):
        updated.append(f"{{key}}={{value}}")
        seen = True
    else:
        updated.append(raw)
if value and not seen:
    updated.append(f"{{key}}={{value}}")
path.write_text("\\n".join(updated).rstrip() + "\\n", encoding='utf-8')
PY
chmod 600 /root/.ssh/authorized_keys
umount {q_mount} >/dev/null 2>&1 || true
umount {q_mount_alt} >/dev/null 2>&1 || true
mount -t nfs -o vers=4.2,proto=tcp,port=2049,_netdev,noatime,rsize=1048576,wsize=1048576 {q_export} {q_mount} >/dev/null 2>&1 || mount -a >/dev/null 2>&1 || true
systemctl disable --now managemovie-web >/dev/null 2>&1 || true
systemctl disable --now mariadb >/dev/null 2>&1 || true
systemctl disable --now postfix >/dev/null 2>&1 || true
systemctl enable --now ssh >/dev/null 2>&1 || true
"""
    result = run_proxmox_ssh(
        node,
        f"pct exec {ctid} -- sh -lc {shlex.quote(script)}",
        timeout=120,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip().splitlines()
        return False, detail[-1] if detail else "unbekannt"
    return True, "ok"


def ensure_worker_mount_ready(spec: dict[str, str], timeout: int = 90) -> tuple[bool, str]:
    worker_host = str(spec.get("host", "") or "").strip() or str(spec.get("name", "") or "").strip()
    mount_root = str(spec.get("mount_root", "") or "").strip() or "/mnt/Q-NAS"
    export_path = worker_nas_export()
    if not worker_host:
        return False, "Worker-Host fehlt"
    q_mount = shlex.quote(mount_root)
    q_export = shlex.quote(export_path)
    mount_cmd = (
        f"mkdir -p {q_mount} >/dev/null 2>&1 || true; "
        f"mountpoint -q {q_mount} || "
        f"(mount -t nfs -o vers=4.2,proto=tcp,port=2049,_netdev,noatime,rsize=1048576,wsize=1048576 {q_export} {q_mount} >/dev/null 2>&1 || mount -a >/dev/null 2>&1 || true); "
        f"mountpoint -q {q_mount}"
    )
    deadline = time.time() + max(10, int(timeout))
    last_error = ""
    while time.time() < deadline:
        probe = run_worker_ssh(worker_host, "true", timeout=8)
        if probe.returncode != 0:
            last_error = (probe.stderr or probe.stdout or "").strip() or "SSH noch nicht erreichbar"
            time.sleep(2.0)
            continue
        result = run_worker_ssh(worker_host, mount_cmd, timeout=15)
        if result.returncode == 0:
            return True, "ok"
        last_error = (result.stderr or result.stdout or "").strip() or "NFS-Mount nicht aktiv"
        time.sleep(2.0)
    return False, last_error or "NFS-Mount nicht aktiv"


def worker_hwaddr(spec: dict[str, str]) -> str:
    explicit = str(spec.get("hwaddr", "") or "").strip()
    if explicit:
        return explicit
    ctid_raw = str(spec.get("ctid", "") or "").strip()
    try:
        suffix = int(ctid_raw) - 240
    except Exception:
        suffix = 0
    if suffix < 0 or suffix > 255:
        suffix = 0
    return f"42:4D:AE:00:F4:{suffix:02X}"


def wait_for_ct_running(node: str, ctid: str, timeout: int = 90) -> bool:
    deadline = time.time() + max(10, int(timeout))
    while time.time() < deadline:
        probe = run_proxmox_ssh(
            node,
            f"pct status {ctid} 2>/dev/null | grep -q 'status: running'",
            timeout=8,
        )
        if probe.returncode == 0:
            return True
        time.sleep(2.0)
    return False


def normalize_target_out_path(raw: str | None) -> str:
    value = normalize_simple_text(raw)
    if not value:
        return DEFAULT_TARGET_OUT_PATH

    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        try:
            return str(candidate.resolve())
        except Exception:
            return str(candidate)

    parts: list[str] = []
    for part in candidate.parts:
        p = str(part or "").strip()
        if not p or p == ".":
            continue
        if p == "..":
            return DEFAULT_TARGET_OUT_PATH
        parts.append(p)
    if not parts:
        return DEFAULT_TARGET_OUT_PATH
    return str(Path(*parts))


def default_target_reenqueue_path_for_out(target_out_value: str | None) -> str:
    out_value = normalize_target_out_path(target_out_value)
    out_path = Path(out_value).expanduser()
    if out_path.is_absolute():
        candidate = out_path.parent / REENQUEUE_DIR_NAME
        try:
            return str(candidate.resolve())
        except Exception:
            return str(candidate)
    parent = out_path.parent
    if not str(parent) or str(parent) == ".":
        return REENQUEUE_DIR_NAME
    return str(parent / REENQUEUE_DIR_NAME)


def normalize_target_reenqueue_path(raw: str | None, target_out_value: str | None = None) -> str:
    value = normalize_simple_text(raw)
    if not value:
        return default_target_reenqueue_path_for_out(target_out_value)

    candidate = Path(value).expanduser()
    if candidate.is_absolute():
        try:
            return str(candidate.resolve())
        except Exception:
            return str(candidate)

    parts: list[str] = []
    for part in candidate.parts:
        p = str(part or "").strip()
        if not p or p == ".":
            continue
        if p == "..":
            return default_target_reenqueue_path_for_out(target_out_value)
        parts.append(p)
    if not parts:
        return default_target_reenqueue_path_for_out(target_out_value)
    return str(Path(*parts))


def display_target_out_path(start_folder: str, target_out_value: str) -> str:
    folder = normalize_start_folder(start_folder)
    out_value = normalize_target_out_path(target_out_value)
    out_path = Path(out_value).expanduser()
    if out_path.is_absolute():
        try:
            return str(out_path.resolve())
        except Exception:
            return str(out_path)
    if not folder:
        return out_value
    try:
        return str((Path(folder) / out_path).resolve())
    except Exception:
        return str(Path(folder) / out_path)


def display_target_reenqueue_path(start_folder: str, target_reenqueue_value: str, target_out_value: str) -> str:
    folder = normalize_start_folder(start_folder)
    reenqueue_value = normalize_target_reenqueue_path(target_reenqueue_value, target_out_value)
    reenqueue_path = Path(reenqueue_value).expanduser()
    if reenqueue_path.is_absolute():
        try:
            return str(reenqueue_path.resolve())
        except Exception:
            return str(reenqueue_path)
    if not folder:
        return reenqueue_value
    try:
        return str((Path(folder) / reenqueue_path).resolve())
    except Exception:
        return str(Path(folder) / reenqueue_path)


def init_state_store() -> bool:
    global STATE_DB_READY, STATE_DB_FAILED, STATE_DB_RETRY_AFTER
    if STATE_DB_READY:
        return True
    now = time.time()
    if STATE_DB_FAILED and now < STATE_DB_RETRY_AFTER:
        return False
    try:
        STATE_DB_STORE.init_schema()
        STATE_DB_READY = True
        STATE_DB_FAILED = False
        STATE_DB_RETRY_AFTER = 0.0
        migrate_legacy_state_once()
        migrate_secret_state_encryption_once()
        return True
    except Exception as exc:
        STATE_DB_FAILED = True
        STATE_DB_RETRY_AFTER = now + STATE_DB_RETRY_COOLDOWN_SEC
        print(
            "[WARN] MariaDB-State nicht verfuegbar: "
            f"{exc} (naechster Retry in {int(STATE_DB_RETRY_COOLDOWN_SEC)}s)"
        )
        return False


def read_state_value(key: str, default: str = "") -> str:
    values = read_state_values({key: default})
    return values.get(key, default)


def read_state_values(defaults: dict[str, str]) -> dict[str, str]:
    normalized_defaults: dict[str, str] = {}
    for key, default in defaults.items():
        state_key = normalize_simple_text(key)
        if not state_key:
            continue
        normalized_defaults[state_key] = normalize_simple_text(default)
    if not normalized_defaults:
        return {}
    if not init_state_store():
        return dict(normalized_defaults)
    try:
        raw_map = STATE_DB_STORE.read_state_many(list(normalized_defaults.keys()))
    except Exception:
        return dict(normalized_defaults)

    values: dict[str, str] = {}
    for key, default in normalized_defaults.items():
        raw = normalize_simple_text(raw_map.get(key, ""))
        if not raw:
            values[key] = default
            continue
        try:
            value = normalize_simple_text(decrypt_state_value(key, raw))
        except Exception:
            values[key] = default
            continue
        values[key] = value or default
    return values


def write_state_value(key: str, value: str) -> bool:
    normalized = normalize_simple_text(value)
    if not init_state_store():
        return False
    try:
        to_store = encrypt_state_value(key, normalized)
        STATE_DB_STORE.write_state(key, to_store)
        return True
    except Exception:
        return False


def migrate_legacy_state_once() -> None:
    if read_state_value(STATE_KEY_MIGRATED_V1, "") == "1":
        return

    legacy_folder = ""
    try:
        legacy_folder = normalize_simple_text(LEGACY_LAST_FOLDER_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    if legacy_folder:
        normalized = normalize_start_folder(legacy_folder)
        if normalized:
            write_state_value(STATE_KEY_LAST_FOLDER, normalized)

    legacy_mode = ""
    try:
        legacy_mode = normalize_simple_text(LEGACY_LAST_MODE_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    normalized_mode = normalize_mode(legacy_mode)
    if normalized_mode:
        write_state_value(STATE_KEY_LAST_MODE, normalized_mode)

    legacy_encoder = ""
    try:
        legacy_encoder = normalize_simple_text(LEGACY_LAST_ENCODER_STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    normalized_encoder = normalize_encoder_mode(legacy_encoder)
    if normalized_encoder:
        write_state_value(STATE_KEY_LAST_ENCODER, normalized_encoder)

    if not read_state_value(STATE_KEY_TARGET_NFS_PATH, ""):
        write_state_value(STATE_KEY_TARGET_NFS_PATH, DEFAULT_TARGET_NFS_PATH)
    if not read_state_value(STATE_KEY_TARGET_OUT_PATH, ""):
        write_state_value(STATE_KEY_TARGET_OUT_PATH, DEFAULT_TARGET_OUT_PATH)
    if not read_state_value(STATE_KEY_TARGET_REENQUEUE_PATH, ""):
        out_value = read_state_value(STATE_KEY_TARGET_OUT_PATH, DEFAULT_TARGET_OUT_PATH)
        write_state_value(
            STATE_KEY_TARGET_REENQUEUE_PATH,
            default_target_reenqueue_path_for_out(out_value),
        )
    if not read_state_value(STATE_KEY_NAS_IP, ""):
        write_state_value(STATE_KEY_NAS_IP, DEFAULT_NAS_IP)
    if not read_state_value(STATE_KEY_PLEX_IP, ""):
        write_state_value(STATE_KEY_PLEX_IP, DEFAULT_PLEX_IP)
    if not initial_settings_gate_enabled() and not read_state_value(STATE_KEY_INITIAL_SETUP_DONE, ""):
        write_state_value(STATE_KEY_INITIAL_SETUP_DONE, "1")

    write_state_value(STATE_KEY_MIGRATED_V1, "1")


def migrate_secret_state_encryption_once() -> None:
    if read_state_value(STATE_KEY_MIGRATED_SECRETS_V2, "") == "1":
        return
    if not state_crypto_configured():
        return

    for key in STATE_SECRET_KEYS:
        try:
            raw = normalize_simple_text(STATE_DB_STORE.read_state(key))
        except Exception:
            continue
        if not raw or is_encrypted_state_value(raw):
            continue
        try:
            STATE_DB_STORE.write_state(key, encrypt_state_value(key, raw))
        except Exception:
            continue

    write_state_value(STATE_KEY_MIGRATED_SECRETS_V2, "1")


def read_runtime_settings() -> dict[str, str]:
    default_start_on_boot = "0" if platform.system().strip().lower() == "darwin" else "1"
    parallel_job_limit = "2"
    state_values = read_state_values(
        {
            STATE_KEY_TARGET_NFS_PATH: DEFAULT_TARGET_NFS_PATH,
            STATE_KEY_TARGET_OUT_PATH: DEFAULT_TARGET_OUT_PATH,
            STATE_KEY_TARGET_REENQUEUE_PATH: "",
            STATE_KEY_NAS_IP: DEFAULT_NAS_IP,
            STATE_KEY_PLEX_IP: DEFAULT_PLEX_IP,
            STATE_KEY_PLEX_API: "",
            STATE_KEY_TMDB_API: "",
            STATE_KEY_GEMINI_API: "",
            STATE_KEY_AI_QUERY_DISABLED: "1",
            STATE_KEY_SKIP_H265_ENCODE: "0",
            STATE_KEY_SKIP_4K_H265_ENCODE: "0",
            STATE_KEY_ENCODE_ONCE_4K: "1",
            STATE_KEY_ONE_ENCODE_PER_WORKER: "0",
            STATE_KEY_ALLOW_WORKER_4: "0",
            STATE_KEY_PARALLEL_JOB_LIMIT: "2",
            STATE_KEY_PRECHECK_EGB: "1",
            STATE_KEY_SPEED_FALLBACK_COPY: "1",
            STATE_KEY_START_ON_BOOT: default_start_on_boot,
            STATE_KEY_INITIAL_SETUP_DONE: "0" if initial_settings_gate_enabled() else "1",
        }
    )
    try:
        parallel_job_limit = str(max(1, min(4, int((state_values.get(STATE_KEY_PARALLEL_JOB_LIMIT, "2") or "2").strip() or "2"))))
    except Exception:
        parallel_job_limit = "2"
    target_out = state_values.get(STATE_KEY_TARGET_OUT_PATH, DEFAULT_TARGET_OUT_PATH)
    initial_setup_done = "1"
    if initial_settings_gate_enabled():
        initial_setup_done = "1" if parse_form_bool(state_values.get(STATE_KEY_INITIAL_SETUP_DONE, "0")) else "0"
    return {
        "target_nfs_path": state_values.get(STATE_KEY_TARGET_NFS_PATH, DEFAULT_TARGET_NFS_PATH),
        "target_out_path": target_out,
        "target_reenqueue_path": normalize_target_reenqueue_path(
            state_values.get(STATE_KEY_TARGET_REENQUEUE_PATH, ""),
            target_out,
        ),
        "nas_ip": state_values.get(STATE_KEY_NAS_IP, DEFAULT_NAS_IP),
        "plex_ip": state_values.get(STATE_KEY_PLEX_IP, DEFAULT_PLEX_IP),
        "plex_api": state_values.get(STATE_KEY_PLEX_API, ""),
        "tmdb_api": state_values.get(STATE_KEY_TMDB_API, ""),
        "gemini_api": state_values.get(STATE_KEY_GEMINI_API, ""),
        "ai_query_disabled": "1" if parse_form_bool(state_values.get(STATE_KEY_AI_QUERY_DISABLED, "1")) else "0",
        "skip_4k_h265_encode": "1" if parse_form_bool(state_values.get(STATE_KEY_SKIP_4K_H265_ENCODE, "0")) else "0",
        "encode_once_4k": "1" if parse_form_bool(state_values.get(STATE_KEY_ENCODE_ONCE_4K, "1")) else "0",
        "one_encode_per_worker": "1" if parse_form_bool(state_values.get(STATE_KEY_ONE_ENCODE_PER_WORKER, "0")) else "0",
        "allow_worker_4": "1" if parse_form_bool(state_values.get(STATE_KEY_ALLOW_WORKER_4, "0")) else "0",
        "parallel_job_limit": parallel_job_limit,
        "precheck_egb": "1" if parse_form_bool(state_values.get(STATE_KEY_PRECHECK_EGB, "1")) else "0",
        "speed_fallback_copy": "1" if parse_form_bool(state_values.get(STATE_KEY_SPEED_FALLBACK_COPY, "1")) else "0",
        "start_on_boot": "1" if parse_form_bool(state_values.get(STATE_KEY_START_ON_BOOT, default_start_on_boot)) else "0",
        "initial_setup_done": initial_setup_done,
        "initial_setup_required": "1" if initial_settings_gate_enabled() else "0",
    }


def write_start_on_boot_flag(enabled: str) -> bool:
    try:
        BOOT_START_FLAG_FILE.parent.mkdir(parents=True, exist_ok=True)
        BOOT_START_FLAG_FILE.write_text("1\n" if parse_form_bool(enabled) else "0\n", encoding="utf-8")
        return True
    except Exception:
        return False


def sync_start_on_boot_runtime(enabled: str) -> None:
    enabled_bool = parse_form_bool(enabled)
    write_start_on_boot_flag("1" if enabled_bool else "0")
    if platform.system().strip().lower() != "linux":
        return
    if os.geteuid() != 0:
        return
    service_name = (os.environ.get("MANAGEMOVIE_SYSTEMD_SERVICE_NAME", "managemovie-web.service") or "managemovie-web.service").strip()
    action = "enable" if enabled_bool else "disable"
    try:
        subprocess.run(["systemctl", action, service_name], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass


def read_public_runtime_settings() -> dict[str, str | bool]:
    payload = build_public_runtime_settings(
        read_runtime_settings(),
        mode=read_last_mode(),
        encoder=read_last_encoder(),
    )
    payload["encoder"] = coerce_encoder_for_ui(payload.get("encoder", ""))
    return payload


def initial_setup_missing_secret_labels(
    plex_api: str,
    tmdb_api: str,
    gemini_api: str,
) -> list[str]:
    missing: list[str] = []
    if not str(plex_api or "").strip():
        missing.append("Plex API")
    if not str(tmdb_api or "").strip():
        missing.append("TMDB API")
    if not str(gemini_api or "").strip():
        missing.append("Gemini API")
    return missing


def update_runtime_settings(payload: dict | None) -> tuple[bool, str, dict[str, str]]:
    current = read_runtime_settings()
    source = payload if isinstance(payload, dict) else {}

    target_nfs_path = normalize_simple_text(source.get("target_nfs_path", current["target_nfs_path"])) or DEFAULT_TARGET_NFS_PATH
    target_out_path = normalize_target_out_path(source.get("target_out_path", current.get("target_out_path", DEFAULT_TARGET_OUT_PATH)))
    target_reenqueue_path = normalize_target_reenqueue_path(
        source.get("target_reenqueue_path", current.get("target_reenqueue_path", DEFAULT_TARGET_REENQUEUE_PATH)),
        target_out_path,
    )
    nas_ip = normalize_ipv4(source.get("nas_ip", current["nas_ip"])) or DEFAULT_NAS_IP
    plex_ip = normalize_ipv4(source.get("plex_ip", current["plex_ip"])) or DEFAULT_PLEX_IP
    plex_api = apply_secret_update(source, "plex_api", current["plex_api"])
    tmdb_api = apply_secret_update(source, "tmdb_api", current["tmdb_api"])
    gemini_api = apply_secret_update(source, "gemini_api", current["gemini_api"])
    ai_query_disabled = "1" if parse_form_bool(source.get("ai_query_disabled", current.get("ai_query_disabled", "1"))) else "0"
    skip_4k_h265_encode = "1" if parse_form_bool(source.get("skip_4k_h265_encode", current.get("skip_4k_h265_encode", "0"))) else "0"
    encode_once_4k = "1" if parse_form_bool(source.get("encode_once_4k", current.get("encode_once_4k", "1"))) else "0"
    one_encode_per_worker = "1" if parse_form_bool(source.get("one_encode_per_worker", current.get("one_encode_per_worker", "0"))) else "0"
    allow_worker_4 = "1" if parse_form_bool(source.get("allow_worker_4", current.get("allow_worker_4", "0"))) else "0"
    try:
        parallel_job_limit = str(max(1, min(4, int(str(source.get("parallel_job_limit", current.get("parallel_job_limit", "2")) or "2").strip() or "2"))))
    except Exception:
        parallel_job_limit = current.get("parallel_job_limit", "2") or "2"
    precheck_egb = "1" if parse_form_bool(source.get("precheck_egb", current.get("precheck_egb", "1"))) else "0"
    speed_fallback_copy = "1" if parse_form_bool(source.get("speed_fallback_copy", current.get("speed_fallback_copy", "1"))) else "0"
    start_on_boot = "1" if parse_form_bool(source.get("start_on_boot", current.get("start_on_boot", "1"))) else "0"
    encoder = coerce_encoder_for_ui(source.get("encoder", read_last_encoder()))

    if normalize_simple_text(source.get("nas_ip", "")) and not normalize_ipv4(source.get("nas_ip")):
        return False, "Ungueltige NAS-IP", current
    if normalize_simple_text(source.get("plex_ip", "")) and not normalize_ipv4(source.get("plex_ip")):
        return False, "Ungueltige Plex-IP", current
    if initial_settings_gate_enabled() and not parse_form_bool(current.get("initial_setup_done", "0")):
        missing_secret_labels = initial_setup_missing_secret_labels(plex_api, tmdb_api, gemini_api)
        if missing_secret_labels:
            return False, (
                "Erststart: Bitte zuerst API-Keys eintragen und speichern "
                f"({', '.join(missing_secret_labels)})."
            ), current

    failed_writes: list[str] = []

    def persist_field(label: str, writer: Callable[[], bool]) -> None:
        try:
            if writer():
                return
        except Exception:
            pass
        failed_writes.append(label)

    persist_field("target_nfs_path", lambda: write_state_value(STATE_KEY_TARGET_NFS_PATH, target_nfs_path))
    persist_field("target_out_path", lambda: write_state_value(STATE_KEY_TARGET_OUT_PATH, target_out_path))
    persist_field(
        "target_reenqueue_path",
        lambda: write_state_value(STATE_KEY_TARGET_REENQUEUE_PATH, target_reenqueue_path),
    )
    persist_field("nas_ip", lambda: write_state_value(STATE_KEY_NAS_IP, nas_ip))
    persist_field("plex_ip", lambda: write_state_value(STATE_KEY_PLEX_IP, plex_ip))
    persist_field("ai_query_disabled", lambda: write_state_value(STATE_KEY_AI_QUERY_DISABLED, ai_query_disabled))
    persist_field("skip_h265_encode", lambda: write_state_value(STATE_KEY_SKIP_H265_ENCODE, "0"))
    persist_field("skip_4k_h265_encode", lambda: write_state_value(STATE_KEY_SKIP_4K_H265_ENCODE, skip_4k_h265_encode))
    persist_field("encode_once_4k", lambda: write_state_value(STATE_KEY_ENCODE_ONCE_4K, encode_once_4k))
    persist_field("one_encode_per_worker", lambda: write_state_value(STATE_KEY_ONE_ENCODE_PER_WORKER, one_encode_per_worker))
    persist_field("allow_worker_4", lambda: write_state_value(STATE_KEY_ALLOW_WORKER_4, allow_worker_4))
    persist_field("parallel_job_limit", lambda: write_state_value(STATE_KEY_PARALLEL_JOB_LIMIT, parallel_job_limit))
    persist_field("precheck_egb", lambda: write_state_value(STATE_KEY_PRECHECK_EGB, precheck_egb))
    persist_field("speed_fallback_copy", lambda: write_state_value(STATE_KEY_SPEED_FALLBACK_COPY, speed_fallback_copy))
    persist_field("start_on_boot", lambda: write_state_value(STATE_KEY_START_ON_BOOT, start_on_boot))
    persist_field("initial_setup_done", lambda: write_state_value(STATE_KEY_INITIAL_SETUP_DONE, "1"))
    persist_field("encoder", lambda: write_last_encoder(encoder))

    # Keep API keys unchanged unless explicitly provided by the UI payload.
    if "plex_api" in source:
        persist_field("plex_api", lambda: write_state_value(STATE_KEY_PLEX_API, plex_api))
    if "tmdb_api" in source:
        persist_field("tmdb_api", lambda: write_state_value(STATE_KEY_TMDB_API, tmdb_api))
    if "gemini_api" in source:
        persist_field("gemini_api", lambda: write_state_value(STATE_KEY_GEMINI_API, gemini_api))

    if failed_writes:
        details = ", ".join(failed_writes[:6])
        if len(failed_writes) > 6:
            details = f"{details}, +{len(failed_writes) - 6} weitere"
        return False, f"MariaDB/Encryption nicht verfuegbar: {details}", current

    sync_start_on_boot_runtime(start_on_boot)

    updated = read_runtime_settings()
    updated["encoder"] = coerce_encoder_for_ui(read_last_encoder())
    return True, "", updated


def empty_cache_db_summary(error: str = "") -> dict[str, Any]:
    return {
        "ok": False if error else True,
        "error": str(error or "").strip(),
        "source_file_cache_rows": 0,
        "gemini_source_rows": 0,
        "editor_source_rows": 0,
        "processed_source_rows": 0,
        "runtime_gemini_rows": 0,
        "runtime_rows": 0,
        "tmdb_state_v1_rows": 0,
        "tmdb_state_v2_rows": 0,
        "settings_rows": 0,
        "app_state_cache_rows": 0,
        "tmdb_cache_rows": 0,
        "tmdb_cache_history_rows": 0,
        "total_cache_rows": 0,
    }


def read_cache_db_summary() -> dict[str, Any]:
    if not init_state_store():
        return empty_cache_db_summary("MariaDB-State nicht verfuegbar")
    try:
        stats = STATE_DB_STORE.read_cache_db_stats()
    except Exception as exc:
        return empty_cache_db_summary(f"Cache-DB-Statistik fehlgeschlagen: {exc}")

    payload = empty_cache_db_summary()
    payload.update({k: int(v or 0) for k, v in stats.items() if k in payload})
    payload["ok"] = True
    payload["error"] = ""
    return payload


def normalize_start_folder(raw_folder: str | None) -> str:
    candidate = (raw_folder or "").strip()
    if not candidate:
        return ""
    try:
        resolved = Path(candidate).expanduser().resolve()
    except Exception:
        return ""
    if not resolved.exists() or not resolved.is_dir():
        return ""
    return str(resolved)


def read_last_started_folder() -> str:
    raw = read_state_value(STATE_KEY_LAST_FOLDER, "")
    normalized = normalize_start_folder(raw)
    return normalized or DEFAULT_FOLDER


def write_last_started_folder(folder: str) -> bool:
    normalized = normalize_start_folder(folder)
    if not normalized:
        return False
    return write_state_value(STATE_KEY_LAST_FOLDER, normalized)


def read_auto_confirm_start_folder() -> str:
    return normalize_start_folder(read_state_value(STATE_KEY_AUTO_CONFIRM_START_FOLDER, ""))


def write_auto_confirm_start_folder(folder: str) -> bool:
    normalized = normalize_start_folder(folder)
    if not normalized:
        return False
    return write_state_value(STATE_KEY_AUTO_CONFIRM_START_FOLDER, normalized)


def read_history_off_start_folder() -> str:
    return normalize_start_folder(read_state_value(STATE_KEY_HISTORY_OFF_START_FOLDER, ""))


def write_history_off_start_folder(folder: str) -> bool:
    normalized = normalize_start_folder(folder)
    if not normalized:
        return False
    return write_state_value(STATE_KEY_HISTORY_OFF_START_FOLDER, normalized)


def folder_history_off(folder: str) -> bool:
    normalized = normalize_start_folder(folder)
    if not normalized:
        return False
    return normalized == read_history_off_start_folder()


def folder_is_auto_confirmed(folder: str) -> bool:
    normalized = normalize_start_folder(folder)
    if not normalized:
        return False
    return normalized == read_auto_confirm_start_folder()


def normalize_mode(raw_mode: str | None) -> str:
    mode = (raw_mode or "").strip().lower()
    if mode in {"analyze", "copy", "ffmpeg"}:
        return mode
    return ""


def parse_form_bool(raw_value: str | None) -> bool:
    return (str(raw_value or "").strip().lower() in {"1", "true", "yes", "y", "on"})


def read_last_mode() -> str:
    raw = read_state_value(STATE_KEY_LAST_MODE, "")
    return normalize_mode(raw) or "analyze"


def write_last_mode(mode: str) -> bool:
    normalized = normalize_mode(mode)
    if not normalized:
        return False
    return write_state_value(STATE_KEY_LAST_MODE, normalized)


def read_last_encoder() -> str:
    raw = read_state_value(STATE_KEY_LAST_ENCODER, "")
    return normalize_encoder_mode(raw) or "cpu"


def write_last_encoder(encoder: str) -> bool:
    normalized = normalize_encoder_mode(encoder)
    if not normalized:
        return False
    return write_state_value(STATE_KEY_LAST_ENCODER, normalized)


def truncate_text_file(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
    except PermissionError:
        try:
            path.unlink()
        except FileNotFoundError:
            return
        except Exception:
            return
        try:
            path.write_text("", encoding="utf-8")
        except Exception:
            pass
    except Exception:
        pass


def clear_log_windows_data() -> None:
    for path in (STATUS_FILE, OUT_TREE_FILE, OUT_PLAN_FILE, PROCESSING_LOG_FILE):
        truncate_text_file(path)


def clear_confirmation_file() -> None:
    global pending_payload_cache_mtime_ns, pending_payload_cache_size, pending_payload_cache_payload
    global pending_status_override_cache_key, pending_status_override_cache_text
    try:
        CONFIRM_FILE.unlink()
    except FileNotFoundError:
        pass
    except Exception:
        pass
    with pending_payload_cache_lock:
        pending_payload_cache_mtime_ns = -1
        pending_payload_cache_size = -1
        pending_payload_cache_payload = None
    with pending_status_override_cache_lock:
        pending_status_override_cache_key = ""
        pending_status_override_cache_text = ""


def normalize_pending_mode(raw_mode: str | None) -> str:
    raw = str(raw_mode or "").strip().lower()
    if raw in {"a", "analyze"}:
        return "analyze"
    if raw in {"c", "copy"}:
        return "copy"
    if raw in {"f", "ffmpeg"}:
        return "ffmpeg"
    return ""


def read_pending_confirmation_payload() -> dict | None:
    global pending_payload_cache_mtime_ns, pending_payload_cache_size, pending_payload_cache_payload
    if not CONFIRM_FILE.exists():
        with pending_payload_cache_lock:
            pending_payload_cache_mtime_ns = -1
            pending_payload_cache_size = -1
            pending_payload_cache_payload = None
        return None
    try:
        stat = CONFIRM_FILE.stat()
    except Exception:
        return None
    with pending_payload_cache_lock:
        if (
            pending_payload_cache_payload is not None
            and pending_payload_cache_mtime_ns == int(stat.st_mtime_ns)
            and pending_payload_cache_size == int(stat.st_size)
        ):
            payload = pending_payload_cache_payload
        else:
            payload = None
    if payload is None:
        try:
            payload = json.loads(CONFIRM_FILE.read_text(encoding="utf-8"))
        except Exception:
            return None
        with pending_payload_cache_lock:
            pending_payload_cache_mtime_ns = int(stat.st_mtime_ns)
            pending_payload_cache_size = int(stat.st_size)
            pending_payload_cache_payload = payload if isinstance(payload, dict) else None
    try:
        if not isinstance(payload, dict):
            return None
    except Exception:
        return None

    state = str(payload.get("state", "")).strip().lower()
    if state != "pending":
        return None

    mode = normalize_pending_mode(payload.get("mode", ""))
    if not mode:
        return None

    token = str(payload.get("token", "")).strip()
    if not token:
        return None

    try:
        file_count = int(payload.get("file_count", 0) or 0)
    except Exception:
        file_count = 0
    try:
        created_at = int(payload.get("created_at", 0) or 0)
    except Exception:
        created_at = 0

    payload["_mode"] = mode
    payload["_file_count"] = file_count
    payload["_created_at"] = created_at
    payload["_start_folder"] = str(payload.get("start_folder", "")).strip()
    payload["_token"] = token
    return payload


def write_confirmation_payload(payload: dict) -> bool:
    global pending_payload_cache_mtime_ns, pending_payload_cache_size, pending_payload_cache_payload
    global pending_status_override_cache_key, pending_status_override_cache_text
    if not isinstance(payload, dict):
        return False
    try:
        CONFIRM_FILE.parent.mkdir(parents=True, exist_ok=True)
        serialized = json.dumps(payload, ensure_ascii=False)
        CONFIRM_FILE.write_text(serialized, encoding="utf-8")
        try:
            stat = CONFIRM_FILE.stat()
            mtime_ns = int(stat.st_mtime_ns)
            size = int(stat.st_size)
        except Exception:
            mtime_ns = -1
            size = len(serialized.encode("utf-8"))
        with pending_payload_cache_lock:
            pending_payload_cache_mtime_ns = mtime_ns
            pending_payload_cache_size = size
            pending_payload_cache_payload = payload
        with pending_status_override_cache_lock:
            pending_status_override_cache_key = ""
            pending_status_override_cache_text = ""
        return True
    except Exception:
        return False


def clone_json_like(value: Any) -> Any:
    try:
        return json.loads(json.dumps(value, ensure_ascii=False))
    except Exception:
        return value


def read_pending_confirmation() -> dict | None:
    payload = read_pending_confirmation_payload()
    return summarize_pending_confirmation(payload)


def summarize_pending_confirmation(payload: dict | None) -> dict | None:
    if not payload:
        return None
    editor_rows = payload.get("editor_rows", [])
    editor_count = len(editor_rows) if isinstance(editor_rows, list) else 0
    return {
        "token": payload["_token"],
        "mode": payload["_mode"],
        "file_count": payload["_file_count"],
        "start_folder": payload["_start_folder"],
        "editor_count": editor_count,
        "created_at": int(payload.get("_created_at", 0) or 0),
    }


def maybe_auto_dispatch_pending_confirmation() -> tuple[bool, str]:
    pending_payload = read_pending_confirmation_payload()
    if not pending_payload:
        return False, ""
    pending_mode = str((pending_payload or {}).get("mode", "") or "").strip().lower()
    if pending_mode != "f":
        return False, ""
    start_folder = str(pending_payload.get("start_folder", "") or "").strip()
    if not folder_is_auto_confirmed(start_folder):
        return False, ""
    token = str(pending_payload.get("_token", "") or pending_payload.get("token", "") or "").strip()
    if not token:
        return False, ""
    rows = collect_editor_rows_from_payload(pending_payload)
    with job_lock:
        job = current_job
        sync_nas = bool(job.sync_nas) if job else False
        sync_plex = bool(job.sync_plex) if job else False
        del_out = bool(job.del_out) if job else False
        del_source = bool(job.del_source) if job else False
    encoder = coerce_encoder_for_ui(read_last_encoder())
    ok_cancel, error_cancel = write_confirmation_decision(token, "dispatch", encoder)
    if not ok_cancel:
        return False, error_cancel
    ok_dispatch, error_dispatch = start_ffmpeg_dispatch_from_rows(
        folder=start_folder,
        encoder=encoder,
        rows=rows,
        sync_nas=sync_nas,
        sync_plex=sync_plex,
        del_out=del_out,
        del_source=del_source,
    )
    if not ok_dispatch:
        return False, error_dispatch
    append_processing_log(f"[dispatch] Auto-Freigabe aktiv: {start_folder}")
    return True, error_dispatch


def get_pending_confirmation_for_token(token: str) -> tuple[dict | None, str]:
    payload = read_pending_confirmation_payload()
    if not payload:
        return None, "Keine aktive Freigabe"
    pending_token = str(payload.get("token", "")).strip()
    if not pending_token:
        return None, "Freigabe-Token fehlt"
    if token and token != pending_token:
        return None, "Freigabe-Token passt nicht"
    return payload, ""


def collect_editor_rows_from_payload(payload: dict) -> list[dict[str, Any]]:
    rows_any = payload.get("editor_rows")
    if not isinstance(rows_any, list):
        rows_any = payload.get("editor_rows_original", [])
    if not isinstance(rows_any, list):
        return []
    start_folder = str(payload.get("_start_folder", "") or payload.get("start_folder", "")).strip()
    if not start_folder:
        return []
    # Fast path for editor/status rendering: avoid expensive target rebuilds on each poll.
    rows = normalize_editor_rows_payload(rows_any, start_folder, rebuild_targets=False)
    out: list[dict[str, Any]] = []
    for idx, row in enumerate(rows, start=1):
        item = dict(row)
        item["nr"] = idx
        item["is_series"] = editor_is_series_from_row(item)
        out.append(item)
    return out


def format_editor_ratio(idx: int, total: int) -> str:
    t = max(1, int(total or 1))
    p = max(1, int(idx or 1))
    width = max(2, len(str(t)))
    return f"{p:0{width}d}/{t:0{width}d}"


def editor_status_row_st_em(row: dict[str, Any]) -> str:
    season = normalize_editor_season_episode(row.get("season", ""))
    episode = normalize_editor_season_episode(row.get("episode", ""))
    if season and episode:
        return f"S{season}E{episode}"
    return "Movie"


def editor_status_row_source_name(row: dict[str, Any]) -> str:
    source_name = Path(str(row.get("source_name", "") or "").replace("\\", "/")).name
    return source_name or "-"


def editor_status_row_target_name(row: dict[str, Any]) -> str:
    target_name = Path(str(row.get("target_name", "") or "").replace("\\", "/")).name
    return target_name or "-"


def build_status_table_override_from_editor_rows(rows: list[dict[str, Any]]) -> str:
    headers = [
        "Nr.",
        "Quelle",
        "Ziel",
        "Jahr",
        "St/E-M",
        "IMDB-ID",
        "Q-GB",
        "Z-GB",
        "E-GB",
        "Speed",
        "FPS",
        "ETA",
    ]
    lines = [f"| {' | '.join(headers)} |"]
    total = max(1, len(rows))
    for idx, row in enumerate(rows, start=1):
        q_gb = str(row.get("q_gb", "") or "").strip() or "n/a"
        z_gb = str(row.get("z_gb", "") or "").strip() or "n/a"
        e_gb = str(row.get("e_gb", "") or "").strip() or "n/a"
        speed = str(row.get("speed", "") or "").strip() or "n/a"
        fps = str(row.get("fps", "") or "").strip() or "n/a"
        eta = str(row.get("eta", "") or "").strip() or "n/a"
        year = normalize_editor_year(row.get("year", "")) or "0000"
        imdb = normalize_editor_imdb_id(row.get("imdb_id", "")) or "tt0000000"
        lines.append(
            "| "
            + " | ".join(
                [
                    format_editor_ratio(idx, total),
                    editor_status_row_source_name(row),
                    editor_status_row_target_name(row),
                    year,
                    editor_status_row_st_em(row),
                    imdb,
                    q_gb,
                    z_gb,
                    e_gb,
                    speed,
                    fps,
                    eta,
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def build_status_table_override_from_pending_payload(payload: dict | None) -> str:
    if not payload:
        return ""
    token = str(payload.get("_token", "") or payload.get("token", "")).strip()
    updated_at = str(payload.get("updated_at", "") or "").strip()
    rows_any = payload.get("editor_rows", [])
    row_count = len(rows_any) if isinstance(rows_any, list) else 0
    cache_key = f"{token}|{updated_at}|{row_count}"
    if cache_key:
        with pending_status_override_cache_lock:
            global pending_status_override_cache_key, pending_status_override_cache_text
            if cache_key == pending_status_override_cache_key:
                return pending_status_override_cache_text
    try:
        rows = collect_editor_rows_from_payload(payload)
    except Exception:
        return ""
    if not rows:
        return ""
    table_text = build_status_table_override_from_editor_rows(rows)
    if cache_key:
        with pending_status_override_cache_lock:
            pending_status_override_cache_key = cache_key
            pending_status_override_cache_text = table_text
    return table_text


def write_confirmation_decision(token: str, state: str, encoder: str) -> tuple[bool, str]:
    if state not in {"start", "cancel", "dispatch"}:
        return False, "Ungueltige Entscheidung"
    payload = read_pending_confirmation_payload()
    if not payload:
        return False, "Keine aktive Freigabe"

    current_token = str(payload.get("token", "")).strip()
    if current_token and token != current_token:
        return False, "Freigabe-Token passt nicht"

    current_state = str(payload.get("state", "")).strip().lower()
    if current_state and current_state != "pending":
        return False, "Freigabe bereits entschieden"

    payload["state"] = state
    payload["updated_at"] = int(time.time())
    if state == "start" and encoder:
        payload["encoder"] = encoder
        write_last_encoder(encoder)

    if not write_confirmation_payload(payload):
        return False, "Freigabe konnte nicht gespeichert werden"

    return True, ""


def append_processing_log_with_host(host_label: str, message: str, default_level: str = "INFO") -> None:
    text = (message or "").strip()
    if not text:
        return
    level = str(default_level or "INFO").strip().upper() or "INFO"
    payload = text
    match = re.match(r"^\[(\d{2}:\d{2}:\d{2})\]\s+\[([A-Z]+)\]\s+(.*)$", text, re.IGNORECASE)
    if match:
        level = str(match.group(2) or level).strip().upper() or level
        payload = str(match.group(3) or "").strip()
    else:
        match = re.match(r"^\[([A-Z]+)\]\s+(.*)$", text, re.IGNORECASE)
        if match:
            level = str(match.group(1) or level).strip().upper() or level
            payload = str(match.group(2) or "").strip()
    host = str(host_label or "").strip() or simple_host_label()
    try:
        PROCESSING_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        with PROCESSING_LOG_FILE.open("a", encoding="utf-8") as out_handle:
            ts = time.strftime("%H:%M:%S")
            out_handle.write(f"[{host}] [{ts}] [{level}] {payload or text}\n")
    except Exception:
        pass


def append_processing_log(message: str) -> None:
    append_processing_log_with_host(simple_host_label(), message, "INFO")


def reset_processing_log() -> None:
    try:
        PROCESSING_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        PROCESSING_LOG_FILE.write_text("", encoding="utf-8")
    except Exception:
        pass


def append_runner_log_info(message: str) -> None:
    text = str(message or "").strip()
    if not text:
        return
    try:
        path_text = latest_runner_log_path()
        if not path_text:
            return
        path = Path(path_text)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as out_handle:
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            out_handle.write(f"[info] {ts} {text}\n")
    except Exception:
        pass


def log_manual_abort_event(reason: str) -> None:
    normalized = str(reason or "").strip().lower() or "manual"
    append_processing_log(f"ABBRUCH ausgefuehrt ({normalized})")
    append_runner_log_info(f"abort reason={normalized}")


def clean_manifest_files(start_folder: str) -> tuple[bool, dict, str]:
    normalized = normalize_start_folder(start_folder)
    if not normalized:
        return False, {}, "Startordner ungueltig"

    root = Path(normalized)
    manifest_map: dict[str, Path] = {}
    for alias in MANAGEMOVIE_TRACK_FILE_ALIASES:
        try:
            for file_path in root.rglob(alias):
                manifest_map[str(file_path)] = file_path
        except Exception:
            continue

    manifest_files = sorted(manifest_map.values(), key=lambda p: str(p).lower())
    sidecar_map: dict[str, Path] = {}
    track_aliases_lower = {alias.lower() for alias in MANAGEMOVIE_TRACK_FILE_ALIASES}
    try:
        for file_path in root.rglob(f"*{MANAGEMOVIE_VIDEO_MANIFEST_SUFFIX}"):
            if not file_path.is_file():
                continue
            name_lower = file_path.name.lower()
            if name_lower in track_aliases_lower:
                continue
            sidecar_map[str(file_path)] = file_path
    except Exception:
        pass
    sidecar_files = sorted(sidecar_map.values(), key=lambda p: str(p).lower())

    deleted = 0
    deleted_track = 0
    deleted_sidecar = 0
    failed = 0
    for manifest_file in manifest_files:
        try:
            manifest_file.unlink()
            deleted += 1
            deleted_track += 1
        except Exception:
            failed += 1
    for sidecar_file in sidecar_files:
        try:
            sidecar_file.unlink()
            deleted += 1
            deleted_sidecar += 1
        except Exception:
            failed += 1

    append_processing_log(
        f"Clean Manifest: Ordner={normalized} gelöscht={deleted} "
        f"(track={deleted_track}, sidecar={deleted_sidecar}) fehler={failed}"
    )
    return True, {
        "start_folder": normalized,
        "deleted": deleted,
        "deleted_track": deleted_track,
        "deleted_sidecar": deleted_sidecar,
        "failed": failed,
    }, ""


def video_manifest_sidecar_path(video_path: Path) -> Path:
    return video_path.with_name(video_path.name + MANAGEMOVIE_VIDEO_MANIFEST_SUFFIX)


def split_manifest_parts(raw_line: str) -> list[str]:
    raw = str(raw_line or "")
    if "\t" in raw:
        return [part.strip() for part in raw.split("\t")]
    return [part.strip() for part in raw.split("|")]


def _safe_read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _safe_write_text(path: Path, text: str) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        return True
    except Exception:
        return False


def remove_track_manifest_entry_for_target(target_video: Path) -> tuple[int, int]:
    removed_entries = 0
    touched_files = 0
    target_name_key = target_video.name.lower()
    if not target_name_key:
        return 0, 0

    for alias in MANAGEMOVIE_TRACK_FILE_ALIASES:
        manifest_path = target_video.parent / alias
        if not manifest_path.exists() or not manifest_path.is_file():
            continue
        existing = _safe_read_text(manifest_path)
        if not existing:
            continue
        lines_out: list[str] = []
        removed_here = 0
        for line in existing.splitlines():
            parts = split_manifest_parts(line.strip())
            name = Path(parts[0]).name.lower() if parts else ""
            if name and name == target_name_key:
                removed_here += 1
                continue
            if line.strip():
                lines_out.append(line.rstrip())
        if removed_here <= 0:
            continue
        removed_entries += removed_here
        touched_files += 1
        if lines_out:
            _safe_write_text(manifest_path, "\n".join(lines_out) + "\n")
        else:
            try:
                manifest_path.unlink()
            except Exception:
                pass
    return removed_entries, touched_files


def source_name_matches_filter(source_name: str, source_filter_set: set[str] | None) -> bool:
    if source_filter_set is None:
        return True
    source_key = normalize_source_row_name_for_gemini(source_name)
    source_base_key = normalize_source_row_name_for_gemini(Path(source_name).name)
    return source_key in source_filter_set or source_base_key in source_filter_set


def reset_editor_done_state_for_sources(
    rows_any: Any,
    *,
    source_filter_set: set[str] | None = None,
) -> tuple[list[dict[str, Any]], int, list[str]]:
    if not isinstance(rows_any, list):
        return [], 0, []

    updated_rows: list[dict[str, Any]] = []
    reset_rows = 0
    affected_sources: list[str] = []
    affected_seen: set[str] = set()
    reset_keys = (
        "speed",
        "eta",
        "z_gb",
        "e_gb",
        "lzeit",
        "Speed",
        "ETA",
        "Z-GB",
        "E-GB",
        "Lzeit",
        "MANIFEST-SKIP",
        "MANIFEST-MODE",
        "MANIFEST-TARGET",
        "MANIFEST-SOURCE",
        "MANIFEST-ZGB",
    )
    for item in rows_any:
        if not isinstance(item, dict):
            continue
        row = dict(item)
        source_name = str(row.get("source_name", row.get("Quellname", "")) or "").strip()
        if source_name and source_name_matches_filter(source_name, source_filter_set):
            changed = False
            for key in reset_keys:
                if key in row and str(row.get(key, "") or "").strip():
                    row[key] = ""
                    changed = True
            if changed:
                reset_rows += 1
            source_key = normalize_source_row_name_for_gemini(source_name)
            if source_key and source_key not in affected_seen:
                affected_seen.add(source_key)
                affected_sources.append(source_name)
        updated_rows.append(row)
    return updated_rows, reset_rows, affected_sources


def clean_manifest_for_editor_rows(
    start_folder: str,
    rows: list[dict[str, Any]],
    source_filter_set: set[str] | None = None,
) -> tuple[bool, dict, str]:
    normalized = normalize_start_folder(start_folder)
    if not normalized:
        return False, {}, "Startordner ungueltig"

    start_root = Path(normalized)
    source_sidecars_deleted = 0
    target_sidecars_deleted = 0
    track_entries_deleted = 0
    track_files_touched = 0
    failed = 0
    affected_rows = 0

    for row in rows:
        source_name = str(row.get("source_name", "") or "").strip()
        if not source_name:
            continue
        if not source_name_matches_filter(source_name, source_filter_set):
            continue
        affected_rows += 1

        source_video = start_root / Path(source_name.lstrip("./"))
        source_manifest = video_manifest_sidecar_path(source_video)
        if source_manifest.exists():
            try:
                source_manifest.unlink()
                source_sidecars_deleted += 1
            except Exception:
                failed += 1

        target_name = str(row.get("target_name", "") or "").strip()
        if not target_name:
            continue
        target_path = Path(target_name)
        if not target_path.is_absolute():
            target_path = start_root / target_path
        target_manifest = video_manifest_sidecar_path(target_path)
        if target_manifest.exists():
            try:
                target_manifest.unlink()
                target_sidecars_deleted += 1
            except Exception:
                failed += 1
        removed_entries, touched_files = remove_track_manifest_entry_for_target(target_path)
        track_entries_deleted += int(removed_entries)
        track_files_touched += int(touched_files)

    append_processing_log(
        "Clean Manifest (Editor): "
        f"Ordner={normalized} Zeilen={affected_rows} "
        f"source_sidecars={source_sidecars_deleted} target_sidecars={target_sidecars_deleted} "
        f"track_entries={track_entries_deleted} track_files={track_files_touched} fehler={failed}"
    )
    return (
        True,
        {
            "start_folder": normalized,
            "rows": affected_rows,
            "source_sidecars_deleted": source_sidecars_deleted,
            "target_sidecars_deleted": target_sidecars_deleted,
            "track_entries_deleted": track_entries_deleted,
            "track_files_touched": track_files_touched,
            "failed": failed,
        },
        "",
    )

def _normalize_sidecar_match_token(value: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^\w]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _matching_sidecars_for_source(source_video: Path) -> list[Path]:
    if not source_video.exists() or not source_video.is_file():
        return []
    parent = source_video.parent
    source_stem = source_video.stem
    source_stem_lower = source_stem.lower()
    source_key = _normalize_sidecar_match_token(source_stem)
    matches: list[Path] = []
    try:
        children = sorted(parent.iterdir(), key=lambda p: p.name.lower())
    except Exception:
        return []
    for child in children:
        if child == source_video or not child.is_file():
            continue
        ext = child.suffix.lower()
        if ext not in REENQUEUE_SIDECAR_EXTENSIONS:
            continue
        stem_lower = child.stem.lower()
        if (
            stem_lower == source_stem_lower
            or stem_lower.startswith(source_stem_lower + ".")
            or stem_lower.startswith(source_stem_lower + "_")
            or stem_lower.startswith(source_stem_lower + "-")
        ):
            matches.append(child)
            continue
        child_key = _normalize_sidecar_match_token(child.stem)
        if source_key and child_key and (source_key in child_key or child_key in source_key):
            matches.append(child)
    return matches


def _is_series_source_name(source_name: str) -> bool:
    text = str(source_name or "").strip().replace("\\", "/")
    if not text:
        return False
    return bool(re.search(r"(?i)(?:^|[ ./_\\-])s\d{1,2}[ ._\\-]*e\d{1,2}(?:$|[ ./_\\-])", text))


def _collect_video_files_in_tree(root: Path) -> list[Path]:
    files: list[Path] = []
    try:
        for file_path in root.rglob("*"):
            if not file_path.is_file():
                continue
            if file_path.name.startswith("._"):
                continue
            if file_path.suffix.lower() in REENQUEUE_VIDEO_EXTENSIONS:
                files.append(file_path)
    except Exception:
        return []
    return files


def _unique_reenqueue_target_path(target: Path) -> Path:
    if not target.exists():
        return target
    stem = target.stem
    suffix = target.suffix
    parent = target.parent
    for idx in range(1, 1000):
        candidate = parent / f"{stem}.requeue{idx}{suffix}"
        if not candidate.exists():
            return candidate
    return parent / f"{stem}.requeue{int(time.time())}{suffix}"


def _safe_move_file(source: Path, destination: Path) -> Path:
    destination = _unique_reenqueue_target_path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        source.replace(destination)
    except OSError as exc:
        if exc.errno == errno.EXDEV:
            raise RuntimeError(
                "Verschieben ueber Dateisystemgrenzen ist fuer RE-QUEUE nicht erlaubt; "
                "Quelle und Ziel muessen auf demselben Volume liegen."
            ) from exc
        raise
    return destination


def _cleanup_empty_parents(start_folder: Path, parent_dir: Path) -> int:
    removed = 0
    try:
        start_resolved = start_folder.resolve()
    except Exception:
        start_resolved = start_folder
    current = parent_dir
    while True:
        try:
            current_resolved = current.resolve()
        except Exception:
            current_resolved = current
        if current_resolved == start_resolved:
            break
        if start_resolved not in current_resolved.parents:
            break
        try:
            next(current.iterdir())
            break
        except StopIteration:
            pass
        except Exception:
            break
        try:
            current.rmdir()
            removed += 1
        except Exception:
            break
        current = current.parent
    return removed


def move_source_to_reenqueue(
    start_folder: str,
    source_name: str,
) -> tuple[bool, dict[str, Any], str]:
    normalized_start = normalize_start_folder(start_folder)
    if not normalized_start:
        return False, {}, "Startordner ungueltig"
    source_text = str(source_name or "").strip().replace("\\", "/")
    if not source_text:
        return False, {}, "Quelle fehlt"

    start_path = Path(normalized_start)
    source_path = Path(source_text)
    if not source_path.is_absolute():
        source_path = start_path / source_path
    try:
        source_abs = source_path.resolve()
    except Exception:
        source_abs = source_path
    if not source_abs.exists() or not source_abs.is_file():
        return False, {}, f"Quelldatei fehlt: {source_text}"

    reenqueue_root = resolve_reenqueue_root_for_start(normalized_start)
    try:
        reenqueue_root.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        return False, {}, f"RE-ENQUEUE-Ziel nicht erreichbar: {exc}"

    sidecars = _matching_sidecars_for_source(source_abs)
    files_to_move = [source_abs] + [path for path in sidecars if path != source_abs]
    moved_files = 0
    moved_sidecars = 0
    touched_parents: set[Path] = set()
    start_resolved = start_path.resolve()
    reenqueue_resolved = reenqueue_root.resolve()

    # Fuer Filme (kein SxxExx) die komplette Unterordnerstruktur des Filmordners verschieben.
    # Serienfolgen bleiben dateibasiert, damit nicht versehentlich ganze Staffeln verschoben werden.
    if not _is_series_source_name(source_text):
        movie_dir = source_abs.parent
        try:
            movie_dir_resolved = movie_dir.resolve()
        except Exception:
            movie_dir_resolved = movie_dir
        if movie_dir_resolved != start_resolved and start_resolved in movie_dir_resolved.parents:
            video_files = _collect_video_files_in_tree(movie_dir_resolved)
            top_level_videos = [p for p in video_files if p.parent == movie_dir_resolved]
            if len(top_level_videos) == 1 and top_level_videos[0] == source_abs:
                try:
                    rel_dir = movie_dir_resolved.relative_to(start_resolved)
                except Exception:
                    rel_dir = Path(movie_dir_resolved.name)
                target_dir = reenqueue_resolved / rel_dir
                try:
                    all_files = [p for p in movie_dir_resolved.rglob("*") if p.is_file()]
                except Exception:
                    all_files = []
                moved_files = len(all_files)
                moved_sidecars = len([p for p in all_files if p.suffix.lower() in REENQUEUE_SIDECAR_EXTENSIONS])
                try:
                    _safe_move_file(movie_dir_resolved, target_dir)
                except Exception as exc:
                    return False, {"moved_files": moved_files, "moved_sidecars": moved_sidecars}, f"Verschieben fehlgeschlagen: {exc}"
                removed_dirs = _cleanup_empty_parents(start_resolved, movie_dir_resolved.parent)
                result = {
                    "start_folder": normalized_start,
                    "source_name": source_text,
                    "moved_files": moved_files,
                    "moved_sidecars": moved_sidecars,
                    "removed_dirs": removed_dirs,
                    "target_root": str(reenqueue_resolved),
                    "moved_container_dir": str(movie_dir_resolved),
                }
                append_processing_log(
                    f"RE-QUEUE: Quelle={Path(source_text).name} Filmordner verschoben ({movie_dir_resolved}) -> {reenqueue_resolved}"
                )
                return True, result, ""

    for source_file in files_to_move:
        try:
            rel = source_file.resolve().relative_to(start_resolved)
        except Exception:
            rel = Path(source_file.name)
        target_file = reenqueue_resolved / rel
        try:
            _safe_move_file(source_file, target_file)
            moved_files += 1
            if source_file != source_abs:
                moved_sidecars += 1
            touched_parents.add(source_file.parent)
        except Exception as exc:
            return False, {"moved_files": moved_files, "moved_sidecars": moved_sidecars}, f"Verschieben fehlgeschlagen: {exc}"

    removed_dirs = 0
    for parent in sorted(touched_parents, key=lambda p: len(p.parts), reverse=True):
        removed_dirs += _cleanup_empty_parents(start_resolved, parent)

    result = {
        "start_folder": normalized_start,
        "source_name": source_text,
        "moved_files": moved_files,
        "moved_sidecars": moved_sidecars,
        "removed_dirs": removed_dirs,
        "target_root": str(reenqueue_resolved),
    }
    append_processing_log(
        f"RE-QUEUE: Quelle={Path(source_text).name} Dateien={moved_files} Sidecars={moved_sidecars} Ziel={reenqueue_resolved}"
    )
    return True, result, ""


def _safe_restore_path(source: Path, destination: Path) -> None:
    if destination.exists():
        raise RuntimeError(f"Ziel existiert bereits: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        source.replace(destination)
    except OSError as exc:
        if exc.errno == errno.EXDEV:
            raise RuntimeError(
                "Rueckverschieben ueber Dateisystemgrenzen ist fuer RE-QUEUE nicht erlaubt; "
                "Quelle und Ziel muessen auf demselben Volume liegen."
            ) from exc
        raise


def _find_reenqueue_source_candidate(reenqueue_root: Path, source_rel: Path) -> Path | None:
    direct = reenqueue_root / source_rel
    if direct.exists() and direct.is_file():
        return direct
    name = source_rel.name
    if not name:
        return None
    matches: list[Path] = []
    try:
        for candidate in reenqueue_root.rglob(name):
            if candidate.is_file():
                matches.append(candidate)
    except Exception:
        return None
    if not matches:
        return None
    matches.sort(
        key=lambda p: (
            p.name.lower() == name.lower(),
            p.suffix.lower() == source_rel.suffix.lower(),
            p.stat().st_mtime if p.exists() else 0.0,
        ),
        reverse=True,
    )
    return matches[0]


def move_source_from_reenqueue(
    start_folder: str,
    source_name: str,
) -> tuple[bool, dict[str, Any], str]:
    normalized_start = normalize_start_folder(start_folder)
    if not normalized_start:
        return False, {}, "Startordner ungueltig"
    source_text = str(source_name or "").strip().replace("\\", "/")
    if not source_text:
        return False, {}, "Quelle fehlt"

    start_path = Path(normalized_start)
    source_rel = Path(source_text.lstrip("./"))
    source_abs = source_rel if source_rel.is_absolute() else (start_path / source_rel)
    try:
        start_resolved = start_path.resolve()
    except Exception:
        start_resolved = start_path
    try:
        source_abs = source_abs.resolve()
    except Exception:
        source_abs = source_abs
    try:
        source_rel_norm = source_abs.relative_to(start_resolved)
    except Exception:
        source_rel_norm = Path(source_text.lstrip("./"))

    reenqueue_root = resolve_reenqueue_root_for_start(normalized_start)
    try:
        reenqueue_resolved = reenqueue_root.resolve()
    except Exception:
        reenqueue_resolved = reenqueue_root

    source_candidate = _find_reenqueue_source_candidate(reenqueue_resolved, source_rel_norm)
    if source_abs.exists() and source_abs.is_file():
        return True, {"moved_back_files": 0, "moved_back_sidecars": 0, "restored_container_dir": False}, ""
    if source_candidate is None:
        return False, {}, f"RE-ENQUEUE-Quelle fehlt: {source_text}"

    moved_back_files = 0
    moved_back_sidecars = 0
    removed_dirs = 0
    restored_container_dir = False
    cleanup_parent = source_candidate.parent

    source_parent = source_abs.parent
    if not source_parent.exists() and source_candidate.parent != reenqueue_resolved:
        old_parent = source_candidate.parent.parent
        try:
            _safe_restore_path(source_candidate.parent, source_parent)
        except Exception as exc:
            return False, {}, f"Rueckverschieben fehlgeschlagen: {exc}"
        restored_container_dir = True
        cleanup_parent = old_parent
        try:
            all_files = [p for p in source_parent.rglob("*") if p.is_file()]
        except Exception:
            all_files = []
        moved_back_files = len(all_files)
        moved_back_sidecars = len([p for p in all_files if p.suffix.lower() in REENQUEUE_SIDECAR_EXTENSIONS])
    else:
        try:
            _safe_restore_path(source_candidate, source_abs)
        except Exception as exc:
            return False, {}, f"Rueckverschieben fehlgeschlagen: {exc}"
        moved_back_files += 1
        for sidecar in _matching_sidecars_for_source(source_candidate):
            if sidecar == source_candidate:
                continue
            target_sidecar = source_parent / sidecar.name
            if target_sidecar.exists():
                continue
            try:
                _safe_restore_path(sidecar, target_sidecar)
                moved_back_files += 1
                moved_back_sidecars += 1
            except Exception:
                continue

    removed_dirs += _cleanup_empty_parents(reenqueue_resolved, cleanup_parent)

    append_processing_log(
        f"RE-QUEUE-UNDO: Quelle={Path(source_text).name} Dateien={moved_back_files} Sidecars={moved_back_sidecars} Quelle={start_resolved}"
    )
    return (
        True,
        {
            "start_folder": normalized_start,
            "source_name": source_text,
            "moved_back_files": moved_back_files,
            "moved_back_sidecars": moved_back_sidecars,
            "removed_dirs": removed_dirs,
            "restored_container_dir": restored_container_dir,
        },
        "",
    )


def normalize_editor_year(raw: str | None) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    match = re.search(r"(19|20)\d{2}", text)
    if not match:
        return ""
    return match.group(0)


def normalize_editor_imdb_id(raw: str | None) -> str:
    text = str(raw or "").strip().lower()
    if not text:
        return ""
    match = re.search(r"(tt\d{7,10})", text)
    if match:
        return match.group(1)
    numeric = re.search(r"(?<!\d)(\d{7,10})(?!\d)", text)
    if numeric:
        return f"tt{numeric.group(1)}"
    return ""


def normalize_editor_season_episode(raw: str | None) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    match = re.search(r"\d{1,2}", text)
    if not match:
        return ""
    try:
        value = int(match.group(0))
    except Exception:
        return ""
    if value < 0:
        return ""
    return f"{value:02d}"


def editor_clean_title(raw: str | None) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    text = re.sub(r"[._-]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def editor_safe_folder_name(raw: str | None) -> str:
    text = editor_clean_title(raw)
    text = re.sub(r'[\\/:*?"<>|]+', "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or "Unknown"


def editor_dotted_name(raw: str | None) -> str:
    text = editor_clean_title(raw)
    text = re.sub(r"[^A-Za-z0-9 ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.replace(" ", ".") if text else "Unknown"


def editor_is_series_from_row(row: dict[str, Any]) -> bool:
    season = normalize_editor_season_episode(row.get("season", ""))
    episode = normalize_editor_season_episode(row.get("episode", ""))
    if season and episode:
        return True
    source_name = str(row.get("source_name", "") or "")
    return bool(
        re.search(r"(?i)(?:^|[ ./_\\-])s\d{1,2}[ ._\\-]*e\d{1,2}(?:[ ._\\-]*e\d{1,2})*(?:$|[ ./_\\-])", source_name)
    )


def resolve_target_out_root_for_start(start_folder: str, settings: dict[str, str] | None = None) -> Path:
    runtime_settings = settings if isinstance(settings, dict) else read_runtime_settings()
    out_value = normalize_target_out_path(runtime_settings.get("target_out_path", DEFAULT_TARGET_OUT_PATH))
    out_path = Path(out_value).expanduser()
    base = Path(start_folder)
    if not out_path.is_absolute():
        out_path = base / out_path
    try:
        return out_path.resolve()
    except Exception:
        return out_path


def target_out_prefix_for_start(start_folder: str, settings: dict[str, str] | None = None) -> Path:
    out_root = resolve_target_out_root_for_start(start_folder, settings=settings)
    base = Path(start_folder)
    try:
        rel = out_root.relative_to(base)
        if rel.parts:
            return rel
    except Exception:
        pass
    return out_root


def resolve_reenqueue_root_for_start(start_folder: str, settings: dict[str, str] | None = None) -> Path:
    runtime_settings = settings if isinstance(settings, dict) else read_runtime_settings()
    out_value = runtime_settings.get("target_out_path", DEFAULT_TARGET_OUT_PATH)
    reenqueue_value = normalize_target_reenqueue_path(
        runtime_settings.get("target_reenqueue_path", DEFAULT_TARGET_REENQUEUE_PATH),
        out_value,
    )
    reenqueue_root = Path(reenqueue_value).expanduser()
    base = Path(start_folder)
    if not reenqueue_root.is_absolute():
        reenqueue_root = base / reenqueue_root
    try:
        return reenqueue_root.resolve()
    except Exception:
        return reenqueue_root


def target_reenqueue_prefix_for_start(start_folder: str, settings: dict[str, str] | None = None) -> Path:
    reenqueue_root = resolve_reenqueue_root_for_start(start_folder, settings=settings)
    base = Path(start_folder)
    try:
        rel = reenqueue_root.relative_to(base)
        if rel.parts:
            return rel
    except Exception:
        pass
    return reenqueue_root


def resolve_manual_root_for_start(start_folder: str) -> Path:
    # Legacy compatibility: MANUAL maps to current RE-ENQUEUE target.
    return resolve_reenqueue_root_for_start(start_folder)


def target_manual_prefix_for_start(start_folder: str, settings: dict[str, str] | None = None) -> Path:
    # Legacy compatibility for old payloads.
    return target_reenqueue_prefix_for_start(start_folder, settings=settings)


def build_manual_target_for_source(
    source_name: str,
    start_folder: str,
    *,
    reenqueue_prefix: Path | None = None,
    settings: dict[str, str] | None = None,
) -> str:
    source = Path(str(source_name or "").strip().replace("\\", "/").lstrip("./"))
    if source.is_absolute():
        source = Path(source.name)
    prefix = reenqueue_prefix if reenqueue_prefix is not None else target_reenqueue_prefix_for_start(start_folder, settings=settings)
    return str(prefix / source)


def parse_target_hints_into_row(target_name: str, row: dict[str, Any]) -> None:
    target = str(target_name or "").strip()
    if not target:
        return
    file_name = Path(target).name
    stem = Path(file_name).stem

    year = normalize_editor_year(file_name)
    imdb = normalize_editor_imdb_id(file_name)
    se_match = re.search(r"(?i)s(\d{1,2})[ ._-]*e(\d{1,2})", file_name)

    if year:
        row["year"] = year
    if imdb:
        row["imdb_id"] = imdb
    if se_match:
        row["season"] = normalize_editor_season_episode(se_match.group(1))
        row["episode"] = normalize_editor_season_episode(se_match.group(2))

    cleaned = re.sub(r"(?i)\.s\d{1,2}\.e\d{1,2}.*$", "", stem)
    cleaned = re.sub(r"(?i)\.(19|20)\d{2}.*$", "", cleaned)
    cleaned = cleaned.replace(".", " ").strip()
    cleaned = editor_clean_title(cleaned)
    if cleaned:
        row["title"] = cleaned


def build_target_name_from_row(
    row: dict[str, Any],
    start_folder: str,
    *,
    out_prefix: Path | None = None,
    settings: dict[str, str] | None = None,
) -> str:
    source_name = str(row.get("source_name", "") or "").strip()
    source_ext = Path(source_name).suffix.lower() or ".mkv"
    title = editor_clean_title(row.get("title", "")) or Path(source_name).stem or "Unknown"
    year = normalize_editor_year(row.get("year", "")) or "0000"
    imdb_id = normalize_editor_imdb_id(row.get("imdb_id", "")) or "tt0000000"
    season = normalize_editor_season_episode(row.get("season", ""))
    episode = normalize_editor_season_episode(row.get("episode", ""))

    title_folder = editor_safe_folder_name(title)
    title_dotted = editor_dotted_name(title)
    if out_prefix is None:
        out_prefix = target_out_prefix_for_start(start_folder, settings=settings)

    if season and episode:
        file_name = f"{title_dotted}.{year}.S{season}.E{episode}.h264.{{{imdb_id}}}{source_ext}"
        return str(out_prefix / "Serien" / f"{title_folder} ({year})" / f"S{season}" / file_name)

    file_name = f"{title_dotted}.{year}.h264.{{{imdb_id}}}{source_ext}"
    return str(out_prefix / "Movie" / f"{title_folder} ({year})" / file_name)


def normalize_editor_rows_payload(rows: Any, start_folder: str, *, rebuild_targets: bool = True) -> list[dict[str, Any]]:
    if not isinstance(rows, list):
        return []
    runtime_settings: dict[str, str] | None = None
    out_prefix: Path | None = None
    reenqueue_prefix: Path | None = None

    def ensure_prefixes() -> tuple[Path, Path]:
        nonlocal runtime_settings, out_prefix, reenqueue_prefix
        if out_prefix is None or reenqueue_prefix is None:
            if runtime_settings is None:
                runtime_settings = read_runtime_settings()
            out_prefix = target_out_prefix_for_start(start_folder, settings=runtime_settings)
            reenqueue_prefix = target_reenqueue_prefix_for_start(start_folder, settings=runtime_settings)
        return out_prefix, reenqueue_prefix

    if rebuild_targets:
        ensure_prefixes()
    normalized: list[dict[str, Any]] = []
    for idx, item in enumerate(rows, start=1):
        if not isinstance(item, dict):
            continue
        source_name = str(item.get("source_name", item.get("Quellname", "")) or "").strip()
        if not source_name:
            continue

        row: dict[str, Any] = {
            "nr": idx,
            "source_name": source_name,
            "target_name": str(item.get("target_name", item.get("Zielname", "")) or "").strip(),
            "title": editor_clean_title(item.get("title", item.get("Name des Film/Serie", ""))),
            "year": normalize_editor_year(item.get("year", item.get("Erscheinungsjahr", ""))),
            "season": normalize_editor_season_episode(item.get("season", item.get("Staffel", ""))),
            "episode": normalize_editor_season_episode(item.get("episode", item.get("Episode", ""))),
            "imdb_id": normalize_editor_imdb_id(item.get("imdb_id", item.get("IMDB-ID", ""))),
            "q_gb": str(item.get("q_gb", item.get("Q-GB", item.get("Groesse", ""))) or "").strip(),
            "z_gb": str(item.get("z_gb", item.get("Z-GB", "")) or "").strip(),
            "e_gb": str(item.get("e_gb", item.get("E-GB", "")) or "").strip(),
            "lzeit": str(item.get("lzeit", item.get("Lzeit", item.get("Laufzeit", item.get("Laufzeit (f)", "")))) or "").strip(),
            "speed": str(item.get("speed", item.get("Speed", "")) or "").strip(),
            "eta": str(item.get("eta", item.get("ETA", "")) or "").strip(),
            "manual": parse_form_bool(str(item.get("manual", "0"))),
        }
        if row["target_name"] and rebuild_targets:
            parse_target_hints_into_row(row["target_name"], row)
        if row["manual"]:
            if rebuild_targets or not row["target_name"]:
                _, manual_prefix = ensure_prefixes()
                row["target_name"] = build_manual_target_for_source(
                    source_name,
                    start_folder,
                    reenqueue_prefix=manual_prefix,
                )
        else:
            if rebuild_targets or not row["target_name"]:
                out_target_prefix, _ = ensure_prefixes()
                row["target_name"] = build_target_name_from_row(
                    row,
                    start_folder,
                    out_prefix=out_target_prefix,
                )
        normalized.append(row)
    return normalized


def reanalyze_editor_rows(rows: list[dict[str, Any]], start_folder: str) -> list[dict[str, Any]]:
    normalized = normalize_editor_rows_payload(rows, start_folder)
    if not normalized:
        return []

    # Harmonize grouped series rows after edits so season blocks stay consistent.
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in normalized:
        if parse_form_bool(str(row.get("manual", "0"))):
            continue
        if not editor_is_series_from_row(row):
            continue
        title_key = editor_clean_title(row.get("title", "")).lower()
        season_key = normalize_editor_season_episode(row.get("season", ""))
        if not title_key or not season_key:
            continue
        grouped.setdefault((title_key, season_key), []).append(row)

    for group_rows in grouped.values():
        canonical_year = ""
        canonical_imdb = ""
        canonical_title = ""
        for row in group_rows:
            year = normalize_editor_year(row.get("year", ""))
            imdb = normalize_editor_imdb_id(row.get("imdb_id", ""))
            title = editor_clean_title(row.get("title", ""))
            if not canonical_year and year:
                canonical_year = year
            if not canonical_imdb and imdb:
                canonical_imdb = imdb
            if not canonical_title and title:
                canonical_title = title
        for row in group_rows:
            if canonical_title:
                row["title"] = canonical_title
            if canonical_year:
                row["year"] = canonical_year
            if canonical_imdb:
                row["imdb_id"] = canonical_imdb
            row["target_name"] = build_target_name_from_row(row, start_folder)

    for idx, row in enumerate(normalized, start=1):
        row["nr"] = idx
    return normalized


def persist_editor_rows_to_db(rows: list[dict[str, Any]]) -> None:
    csv_headers = [
        "Quellname",
        "Name des Film/Serie",
        "Erscheinungsjahr",
        "Staffel",
        "Episode",
        "Laufzeit",
        "IMDB-ID",
    ]
    runtime_rows: list[dict[str, str]] = []
    for row in rows:
        runtime_rows.append(
            {
                "Quellname": str(row.get("source_name", "") or "").strip(),
                "Name des Film/Serie": str(row.get("title", "") or "").strip(),
                "Erscheinungsjahr": str(row.get("year", "") or "").strip() or "0000",
                "Staffel": str(row.get("season", "") or "").strip(),
                "Episode": str(row.get("episode", "") or "").strip(),
                "Laufzeit": "",
                "IMDB-ID": str(row.get("imdb_id", "") or "").strip() or "tt0000000",
                "Zielname": str(row.get("target_name", "") or "").strip(),
                "MANUAL": "1" if parse_form_bool(str(row.get("manual", "0"))) else "",
            }
        )

    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=csv_headers)
    writer.writeheader()
    for row in runtime_rows:
        writer.writerow({header: row.get(header, "") for header in csv_headers})

    if not init_state_store():
        raise RuntimeError("MariaDB-State nicht verfuegbar")
    STATE_DB_STORE.write_state_many(
        [
            ("runtime.gemini_csv", csv_buffer.getvalue()),
            ("runtime.gemini_rows_json", json.dumps(runtime_rows, ensure_ascii=False)),
            ("runtime.gemini_rows_count", str(len(runtime_rows))),
            ("runtime.gemini_rows_updated_unix", str(int(time.time()))),
        ]
    )


def editor_source_row_cache_key(source_name: str) -> str:
    normalized = normalize_source_row_name_for_gemini(source_name)
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()
    return f"{EDITOR_SOURCE_ROW_CACHE_PREFIX}{digest}"


def normalize_source_row_name_for_gemini(source_name: str) -> str:
    normalized = str(source_name or "").strip().replace("\\", "/")
    normalized = re.sub(r"/{2,}", "/", normalized)
    normalized = re.sub(r"^(?:\./)+", "", normalized)
    if normalized.startswith("/"):
        normalized = normalized[1:]
    return normalized.lower()


def gemini_source_row_cache_key(source_name: str) -> str:
    normalized = normalize_source_row_name_for_gemini(source_name)
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()
    return f"{GEMINI_SOURCE_ROW_CACHE_PREFIX}{digest}"


def processed_source_row_cache_key(source_name: str) -> str:
    normalized = normalize_source_row_name_for_gemini(source_name)
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()
    return f"{PROCESSED_SOURCE_ROW_CACHE_PREFIX}{digest}"


def read_cached_source_row_payloads(cache_keys: list[str]) -> dict[str, dict[str, str]]:
    cleaned_keys: list[str] = []
    seen: set[str] = set()
    for cache_key in cache_keys:
        key = str(cache_key or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        cleaned_keys.append(key)
    if not cleaned_keys:
        return {}
    try:
        raw_map = STATE_DB_STORE.read_state_many(cleaned_keys)
    except Exception:
        return {}

    payload_map: dict[str, dict[str, str]] = {}
    for key in cleaned_keys:
        raw = str(raw_map.get(key, "") or "").strip()
        if not raw:
            continue
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        payload = parsed
        if isinstance(parsed, dict) and isinstance(parsed.get("row"), dict):
            payload = parsed.get("row") or {}
        if not isinstance(payload, dict):
            continue
        payload_map[key] = {str(k): str(v or "").strip() for k, v in payload.items()}
    return payload_map


def clear_editor_override_cache_rows(source_names: list[str]) -> int:
    if not source_names:
        return 0
    if not init_state_store():
        raise RuntimeError("MariaDB-State nicht verfuegbar")
    keys_to_clear: list[str] = []
    seen: set[str] = set()
    for source_name_raw in source_names:
        source_name = str(source_name_raw or "").strip()
        if not source_name:
            continue
        candidates = [source_name]
        source_base = Path(source_name).name
        if source_base and source_base != source_name:
            candidates.append(source_base)
        for candidate in candidates:
            source_key = normalize_source_row_name_for_gemini(candidate)
            if not source_key or source_key in seen:
                continue
            seen.add(source_key)
            keys_to_clear.append(editor_source_row_cache_key(candidate))
    if not keys_to_clear:
        return 0
    try:
        STATE_DB_STORE.delete_state_many(keys_to_clear)
    except Exception:
        # Fallback for older/partial DB setups: clear value instead of delete.
        return int(STATE_DB_STORE.write_state_many([(key, "") for key in keys_to_clear]))
    return len(keys_to_clear)


def clear_processed_history_cache_rows(source_names: list[str]) -> int:
    if not source_names:
        return 0
    if not init_state_store():
        raise RuntimeError("MariaDB-State nicht verfuegbar")
    keys_to_clear: list[str] = []
    seen: set[str] = set()
    for source_name_raw in source_names:
        source_name = str(source_name_raw or "").strip()
        if not source_name:
            continue
        candidates = [source_name]
        source_base = Path(source_name).name
        if source_base and source_base != source_name:
            candidates.append(source_base)
        for candidate in candidates:
            source_key = normalize_source_row_name_for_gemini(candidate)
            if not source_key or source_key in seen:
                continue
            seen.add(source_key)
            keys_to_clear.append(processed_source_row_cache_key(candidate))
    if not keys_to_clear:
        return 0
    try:
        STATE_DB_STORE.delete_state_many(keys_to_clear)
    except Exception:
        return int(STATE_DB_STORE.write_state_many([(key, "") for key in keys_to_clear]))
    return len(keys_to_clear)


def rebuild_editor_row_from_gemini_cache(
    current_row: dict[str, Any],
    gemini_payload: dict[str, str],
    start_folder: str,
) -> dict[str, Any]:
    row = dict(current_row)
    row["title"] = editor_clean_title(gemini_payload.get("Name des Film/Serie", ""))
    row["year"] = normalize_editor_year(gemini_payload.get("Erscheinungsjahr", ""))
    row["season"] = normalize_editor_season_episode(gemini_payload.get("Staffel", ""))
    row["episode"] = normalize_editor_season_episode(gemini_payload.get("Episode", ""))
    row["imdb_id"] = normalize_editor_imdb_id(gemini_payload.get("IMDB-ID", ""))
    row["manual"] = False
    row["target_name"] = build_target_name_from_row(row, start_folder)
    return row


def rebuild_editor_row_from_source_guess(current_row: dict[str, Any], start_folder: str) -> dict[str, Any]:
    row = dict(current_row)
    source_name = str(row.get("source_name", "") or "").strip()
    source_file = Path(source_name).name
    source_stem = Path(source_file).stem
    row["title"] = editor_clean_title(source_stem)
    row["year"] = normalize_editor_year(source_file)
    row["imdb_id"] = normalize_editor_imdb_id(source_file)
    se_match = re.search(r"(?i)s(\d{1,2})[ ._-]*e(\d{1,2})", source_file)
    if se_match:
        row["season"] = normalize_editor_season_episode(se_match.group(1))
        row["episode"] = normalize_editor_season_episode(se_match.group(2))
    else:
        row["season"] = ""
        row["episode"] = ""
    row["manual"] = False
    row["target_name"] = build_target_name_from_row(row, start_folder)
    return row


def build_editor_override_row_payload(row: dict[str, Any]) -> dict[str, str]:
    source_name = str(row.get("source_name", row.get("Quellname", "")) or "").strip()
    season = normalize_editor_season_episode(row.get("season", row.get("Staffel", "")))
    episode = normalize_editor_season_episode(row.get("episode", row.get("Episode", "")))
    is_series = bool(season and episode)
    return {
        "Quellname": source_name,
        "Name des Film/Serie": editor_clean_title(row.get("title", row.get("Name des Film/Serie", ""))),
        "Erscheinungsjahr": normalize_editor_year(row.get("year", row.get("Erscheinungsjahr", ""))),
        "Staffel": season if is_series else "",
        "Episode": episode if is_series else "",
        "Laufzeit": "",
        "IMDB-ID": normalize_editor_imdb_id(row.get("imdb_id", row.get("IMDB-ID", ""))),
    }


def editor_override_signature(row: dict[str, Any]) -> tuple[str, str, str, str, str]:
    payload = build_editor_override_row_payload(row)
    return (
        payload.get("Name des Film/Serie", ""),
        payload.get("Erscheinungsjahr", ""),
        payload.get("Staffel", ""),
        payload.get("Episode", ""),
        payload.get("IMDB-ID", ""),
    )


def collect_changed_editor_rows(
    rows: list[dict[str, Any]],
    original_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    original_by_source: dict[str, dict[str, Any]] = {}
    for item in original_rows:
        source_name = str(item.get("source_name", item.get("Quellname", "")) or "").strip()
        if source_name:
            original_by_source[source_name.lower()] = item

    changed: list[dict[str, Any]] = []
    for row in rows:
        source_name = str(row.get("source_name", row.get("Quellname", "")) or "").strip()
        if not source_name:
            continue
        before = original_by_source.get(source_name.lower())
        if before is None or editor_override_signature(row) != editor_override_signature(before):
            changed.append(row)
    return changed


def persist_editor_override_cache_rows(rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    if not init_state_store():
        raise RuntimeError("MariaDB-State nicht verfuegbar")

    saved_unix = int(time.time())
    payload_by_key: dict[str, str] = {}
    for row in rows:
        payload_row = build_editor_override_row_payload(row)
        source_name = payload_row.get("Quellname", "").strip()
        if not source_name:
            continue
        if not any((payload_row.get(k, "") or "").strip() for k in payload_row if k != "Quellname"):
            continue
        payload = {
            "saved_unix": saved_unix,
            "retention_days": EDITOR_SOURCE_ROW_RETENTION_DAYS,
            "source_name": source_name,
            "row": payload_row,
        }
        payload_by_key[editor_source_row_cache_key(source_name)] = json.dumps(payload, ensure_ascii=False)
    if not payload_by_key:
        return 0
    return int(STATE_DB_STORE.write_state_many(list(payload_by_key.items())))


def extract_folder_from_log(log_path: str) -> str:
    if not log_path:
        return ""
    path = Path(log_path)
    if not path.exists() or not path.is_file():
        return ""
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return ""
    for line in lines[:40]:
        if "[info]" not in line or "folder=" not in line:
            continue
        m = re.search(r"\bfolder=(.+)$", line)
        if not m:
            continue
        folder = normalize_start_folder(m.group(1).strip())
        if folder:
            return folder
    return ""


def resolve_default_folder(requested_folder: str | None) -> str:
    requested = normalize_start_folder(requested_folder)
    if requested:
        write_last_started_folder(requested)
        return requested

    live = detect_running_job_from_ps()
    if live:
        live_folder = normalize_start_folder(live.get("folder", ""))
        if live_folder:
            write_last_started_folder(live_folder)
            return live_folder

    last_log_folder = extract_folder_from_log(latest_runner_log_path())
    if last_log_folder:
        write_last_started_folder(last_log_folder)
        return last_log_folder

    return read_last_started_folder()


def normalize_browse_path(raw_path: str | None) -> Path:
    root = BROWSE_ROOT.resolve()
    fallback = read_last_started_folder()
    candidate = Path(raw_path or fallback or str(root)).expanduser()
    if not candidate.is_absolute():
        candidate = Path(fallback or str(root))

    fallback_path = Path(fallback).expanduser() if fallback else root
    try:
        resolved = candidate.resolve()
    except Exception:
        try:
            resolved = fallback_path.resolve()
        except Exception:
            resolved = root

    if not resolved.exists() or not resolved.is_dir():
        try:
            resolved = fallback_path.resolve()
        except Exception:
            resolved = root
    if not resolved.exists() or not resolved.is_dir():
        resolved = root
    return resolved


def build_browse_crumbs(current: Path, root: Path) -> list[dict[str, str]]:
    crumbs: list[dict[str, str]] = [{"name": root.name or str(root), "path": str(root)}]
    if current == root:
        return crumbs

    walk = root
    try:
        parts = current.relative_to(root).parts
    except Exception:
        return crumbs
    for part in parts:
        walk = walk / part
        crumbs.append({"name": part, "path": str(walk)})
    return crumbs


def normalize_browse_root(path: Path) -> Path:
    root = Path("/").resolve()
    try:
        resolved = path.resolve()
    except Exception:
        resolved = root
    return resolved if resolved.exists() and resolved.is_dir() else root


def normalize_browse_target(raw_target: str | None) -> str:
    target = normalize_simple_text(raw_target).lower()
    if target == "settings_target_nfs":
        return "settings_target_nfs"
    if target == "settings_target_out":
        return "settings_target_out"
    if target == "settings_target_reenqueue":
        return "settings_target_reenqueue"
    return "start_folder"


def list_child_dirs(path: Path) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    try:
        for child in path.iterdir():
            try:
                if not child.is_dir():
                    continue
                if child.name.startswith("."):
                    continue
                resolved = child.resolve()
                entries.append({"name": child.name, "path": str(resolved)})
            except Exception:
                continue
    except Exception:
        return []
    entries.sort(key=lambda item: item["name"].lower())
    return entries

def tail_file(path: Path, lines: int = 160, max_chars: int = 32000) -> str:
    if not path.exists():
        return ""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return f"[Fehler beim Lesen: {exc}]"
    chunk = "\n".join(text.splitlines()[-lines:])
    if len(chunk) > max_chars:
        return chunk[-max_chars:]
    return chunk


def read_file_full(path: Path, max_chars: int = 1200000) -> str:
    if not path.exists():
        return ""
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return f"[Fehler beim Lesen: {exc}]"
    if max_chars > 0 and len(text) > max_chars:
        return text[-max_chars:]
    return text


RUNNER_ERROR_MARKERS = ("ERROR", "FEHLER", "TRACEBACK", "EXCEPTION", "FATAL")


def extract_runner_error_lines(runner_log: str, max_lines: int = 12) -> list[str]:
    hits: list[str] = []
    seen: set[str] = set()
    for raw in str(runner_log or "").splitlines():
        line = str(raw or "").strip()
        if not line:
            continue
        upper = line.upper()
        if not any(marker in upper for marker in RUNNER_ERROR_MARKERS):
            continue
        if line in seen:
            continue
        seen.add(line)
        hits.append(line)
    if max_lines > 0 and len(hits) > max_lines:
        return hits[-max_lines:]
    return hits


def runner_error_summary(path: Path | None, rc: int | None = None) -> str:
    if path:
        try:
            runner_tail = tail_file(path, lines=220, max_chars=160000)
        except Exception:
            runner_tail = ""
        hits = extract_runner_error_lines(runner_tail, max_lines=4)
        if hits:
            return " | ".join(hits[-2:])
    if rc is None:
        return "Fehler ohne Detail"
    return f"Exit {int(rc)}"


def build_processing_log_overlay(job_data: dict[str, Any], runner_log: str) -> str:
    lines = [str(line or "").rstrip() for line in str(runner_log or "").splitlines()]
    lines = [line for line in lines if line]
    if not lines and not job_data:
        return ""

    host = simple_host_label()
    started_at = job_data.get("started_at")
    try:
        ts_text = time.strftime("%H:%M:%S", time.localtime(float(started_at))) if started_at else "--:--:--"
    except Exception:
        ts_text = "--:--:--"
    worker_name = str(job_data.get("worker_name", "") or "").strip() or "lokal"
    mode = str(job_data.get("mode", "") or "-").strip() or "-"
    folder_raw = str(job_data.get("folder", "") or "-").strip() or "-"
    folder_name = Path(folder_raw).name if folder_raw not in {"", "-"} else folder_raw
    header_lines = [
        f"[{host}] [{ts_text}] [JOB] mode={mode} worker={worker_name} folder={folder_name}",
    ]

    for candidate in lines[:6]:
        if candidate not in header_lines:
            header_lines.append(candidate)

    error_lines = extract_runner_error_lines(runner_log, max_lines=8)
    if error_lines:
        header_lines.append("")
        header_lines.append(f"[{host}] [{ts_text}] [ERROR]")
        header_lines.extend(error_lines)

    return "\n".join(header_lines).strip()




def format_release_version(patch: int) -> str:
    try:
        value = int(patch)
    except Exception:
        value = VERSION_MIN_PATCH
    if value < VERSION_MIN_PATCH:
        value = VERSION_MIN_PATCH
    if value > VERSION_MAX_PATCH:
        value = VERSION_MAX_PATCH
    return f"{RELEASE_MAJOR}.{RELEASE_MINOR}.{value}"


def release_range_text() -> str:
    return f"{format_release_version(VERSION_MIN_PATCH)} bis {format_release_version(VERSION_MAX_PATCH)}"


def parse_release_patch(version: str) -> int:
    raw = (version or "").strip()
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", raw)
    if not match:
        return -1
    try:
        major = int(match.group(1))
        minor = int(match.group(2))
        value = int(match.group(3))
    except Exception:
        return -1
    if major != RELEASE_MAJOR or minor != RELEASE_MINOR:
        return -1
    if value < VERSION_MIN_PATCH or value > VERSION_MAX_PATCH:
        return -1
    return value


def read_core_release_version() -> str:
    try:
        text = CORE_SCRIPT.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""
    match = re.search(r'^VERSION\s*=\s*"(\d+\.\d+\.\d+)"\s*$', text, flags=re.MULTILINE)
    if not match:
        return ""
    patch = parse_release_patch(match.group(1))
    if patch < VERSION_MIN_PATCH:
        return ""
    return format_release_version(patch)


def read_last_release_patch() -> int:
    try:
        raw = VERSION_STATE_FILE.read_text(encoding="utf-8").strip()
        value = int(raw)
    except Exception:
        return 0
    if value < 0:
        return 0
    if value > VERSION_MAX_PATCH:
        return VERSION_MAX_PATCH
    return value


def write_last_release_patch(patch: int) -> None:
    try:
        VERSION_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        VERSION_STATE_FILE.write_text(str(int(patch)) + "\n", encoding="utf-8")
    except Exception:
        pass


def reserve_next_release_version() -> str:
    current = current_release_version()
    patch = parse_release_patch(current)
    if patch >= VERSION_MIN_PATCH:
        write_last_release_patch(patch)
        return format_release_version(patch)
    return format_release_version(VERSION_MIN_PATCH)


def current_release_version() -> str:
    core = read_core_release_version()
    if core:
        patch = parse_release_patch(core)
        if patch >= VERSION_MIN_PATCH:
            write_last_release_patch(patch)
            return format_release_version(patch)
        return core
    last = read_last_release_patch()
    if last < VERSION_MIN_PATCH:
        return format_release_version(VERSION_MIN_PATCH)
    return format_release_version(last)

def extract_release_from_log(log_path: str) -> str:
    if not log_path:
        return "-"
    path = Path(log_path)
    if not path.exists() or not path.is_file():
        return "-"
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return "-"
    for line in reversed(lines[-120:]):
        token = "release="
        idx = line.find(token)
        if idx < 0:
            continue
        val = line[idx + len(token):].strip().split()[0]
        patch = parse_release_patch(val)
        if patch >= VERSION_MIN_PATCH:
            return format_release_version(patch)
    return "-"

def pid_is_alive(pid: int) -> bool:
    try:
        if pid <= 0:
            return False
        try:
            stat = subprocess.run(
                ["ps", "-o", "stat=", "-p", str(pid)],
                capture_output=True,
                text=True,
                check=False,
            )
            state = (stat.stdout or "").strip().upper()
            if state.startswith("Z"):
                return False
        except Exception:
            pass
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def terminate_pid_or_group(pid: int, *, grace_seconds: float = 10.0) -> bool:
    if pid <= 0:
        return False

    pgid = 0
    try:
        pgid = int(os.getpgid(pid) or 0)
    except Exception:
        pgid = 0

    own_pgid = 0
    try:
        own_pgid = int(os.getpgrp() or 0)
    except Exception:
        own_pgid = 0

    def send(sig: int) -> bool:
        sent = False
        if pgid > 0 and pgid != own_pgid:
            try:
                os.killpg(pgid, sig)
                sent = True
            except Exception:
                sent = False
        if sent:
            return True
        try:
            os.kill(pid, sig)
            return True
        except Exception:
            return False

    if not send(signal.SIGTERM):
        return not pid_is_alive(pid)

    deadline = time.time() + max(1.0, float(grace_seconds))
    while time.time() < deadline:
        if not pid_is_alive(pid):
            return True
        time.sleep(0.2)

    send(signal.SIGKILL)
    hard_deadline = time.time() + 3.0
    while time.time() < hard_deadline:
        if not pid_is_alive(pid):
            return True
        time.sleep(0.1)
    return not pid_is_alive(pid)


def latest_runner_log_path() -> str:
    try:
        logs = sorted(LOG_DIR.glob("managemovie-*.log"), key=lambda p: p.stat().st_mtime, reverse=True)
    except Exception:
        return ""
    return str(logs[0]) if logs else ""


def detect_running_job_from_ps() -> dict | None:
    try:
        result = subprocess.run(["ps", "-eo", "pid=,args="], capture_output=True, text=True, check=False)
    except Exception:
        return None

    core = str(CORE_SCRIPT)
    for raw in (result.stdout or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            continue

        try:
            pid = int(parts[0])
        except Exception:
            continue

        cmdline = parts[1]
        if core not in cmdline:
            continue
        if "--analyze" not in cmdline and "--copy" not in cmdline and "--ffmpeg" not in cmdline:
            continue

        try:
            argv = shlex.split(cmdline)
        except Exception:
            argv = cmdline.split()

        mode = "analyze"
        if "--copy" in argv:
            mode = "copy"
        elif "--ffmpeg" in argv:
            mode = "ffmpeg"

        folder = "-"
        if "--folder" in argv:
            idx = argv.index("--folder")
            if idx + 1 < len(argv):
                folder = argv[idx + 1]

        return {"pid": pid, "mode": mode, "folder": folder}

    return None


def fallback_job_data() -> dict:
    live = detect_running_job_from_ps()
    if live:
        return {
            "exists": True,
            "job_id": f"live-{live['pid']}",
            "mode": live.get("mode", "unknown"),
            "folder": live.get("folder", "-"),
            "encoder": "-",
            "sync_nas": False,
            "sync_plex": False,
            "del_out": False,
            "del_source": False,
            "started_at": None,
            "ended_at": None,
            "running": True,
            "exit_code": None,
            "log_path": latest_runner_log_path(),
            "worker_name": "",
            "release_version": extract_release_from_log(latest_runner_log_path()) if extract_release_from_log(latest_runner_log_path()) != "-" else current_release_version(),
        }

    latest_log = latest_runner_log_path()
    if latest_log:
        return {
            "exists": True,
            "job_id": "last-run",
            "mode": "unknown",
            "folder": extract_folder_from_log(latest_log) or "-",
            "encoder": "-",
            "sync_nas": False,
            "sync_plex": False,
            "del_out": False,
            "del_source": False,
            "started_at": None,
            "ended_at": None,
            "running": False,
            "exit_code": None,
            "log_path": latest_log,
            "worker_name": "",
            "release_version": extract_release_from_log(latest_log),
        }

    return {"exists": False, "running": False}


def build_command(mode: str, folder: str) -> list[str]:
    mode_map = {
        "analyze": "--analyze",
        "copy": "--copy",
        "ffmpeg": "--ffmpeg",
    }
    flag = mode_map.get(mode)
    if not flag:
        raise ValueError(f"Unbekannter Modus: {mode}")
    runner_python = (sys.executable or "").strip() or "python3"
    return [runner_python, str(CORE_SCRIPT), flag, "--folder", folder]


def ffmpeg_remote_host() -> str:
    return (os.environ.get("MANAGEMOVIE_FFMPEG_REMOTE_HOST", "") or "").strip()


def ffmpeg_remote_path_map() -> tuple[str, str] | None:
    raw = (os.environ.get("MANAGEMOVIE_FFMPEG_REMOTE_PATH_MAP", "") or "").strip()
    if not raw:
        return None
    separator = "=>" if "=>" in raw else "="
    if separator not in raw:
        return None
    source, target = raw.split(separator, 1)
    source = source.strip().rstrip("/")
    target = target.strip().rstrip("/")
    if not source or not target:
        return None
    return source, target


def map_folder_for_ffmpeg_worker(folder: str, worker: dict[str, str] | None = None) -> str:
    mapped = ffmpeg_remote_path_map()
    if not mapped:
        mount_root = str((worker or {}).get("mount_root", "") or "").strip()
        if mount_root:
            source = str(Path(read_runtime_settings().get("target_nfs_path", DEFAULT_TARGET_NFS_PATH)).expanduser()).rstrip("/")
            target = mount_root.rstrip("/")
            resolved = str(Path(folder).expanduser())
            if resolved == source:
                return target
            prefix = source + os.sep
            if resolved.startswith(prefix):
                suffix = resolved[len(prefix):]
                return str(Path(target) / suffix) if suffix else target
        return folder
    source, target = mapped
    resolved = str(Path(folder).expanduser())
    if resolved == source:
        return target
    prefix = source + os.sep
    if resolved.startswith(prefix):
        suffix = resolved[len(prefix):]
        return str(Path(target) / suffix) if suffix else target
    return folder


def detect_worker_state(spec: dict[str, str], enabled: bool) -> dict[str, Any]:
    worker_name = str(spec.get("name", "") or "").strip() or "worker"
    worker_host = str(spec.get("host", "") or "").strip() or worker_name
    reinit_running = bool(worker_reinit_runtime_states.get(worker_name, False))
    busy_action = str(worker_busy_runtime_states.get(worker_name, "") or "").strip().lower()
    disconnected = {
        "name": worker_name,
        "host": worker_host,
        "node": str(spec.get("node", "") or "").strip(),
        "ctid": str(spec.get("ctid", "") or "").strip(),
        "installed_version": "",
        "enabled": bool(enabled),
        "connected": False,
        "accepting": bool(enabled),
        "state": "disconnected",
        "label": "N/A",
        "active_job": "",
        "pid": "",
        "reinit_running": reinit_running,
        "busy_action": busy_action,
    }
    try:
        probe = run_worker_ssh(
            worker_host,
            "python3 - <<'PY'\nimport os, re, subprocess\nfrom pathlib import Path\nhost=os.uname().nodename\nversion=''\nversion_path=Path('/opt/managemovie/managemovie-web/app/managemovie.py')\nif version_path.is_file():\n    for line in version_path.read_text(encoding='utf-8', errors='ignore').splitlines():\n        m=re.match(r'^VERSION = \"([0-9]+\\.[0-9]+\\.[0-9]+)\"$', line.strip())\n        if m:\n            version=m.group(1)\n            break\nrows=subprocess.run([\"ps\",\"-eo\",\"pid=,stat=,comm=,args=\"], capture_output=True, text=True).stdout.splitlines()\npy=[]\nff=[]\nfor raw in rows:\n    parts=raw.strip().split(None, 3)\n    if len(parts) < 4:\n        continue\n    pid, stat, comm, args = parts\n    if comm == 'python' and 'managemovie.py --ffmpeg' in args:\n        py.append(f'{pid} {stat} {args}')\n    if comm == 'ffmpeg' and '-progress pipe:1' in args:\n        ff.append(f'{pid} {stat} {args}')\nprint(host)\nprint('__VER__')\nprint(version)\nprint('__PY__')\nprint('\\n'.join(py))\nprint('__FF__')\nprint('\\n'.join(ff))\nPY",
            timeout=6,
        )
    except Exception:
        return disconnected
    if probe.returncode != 0:
        return disconnected
    lines = (probe.stdout or "").splitlines()
    host_name = str(lines[0] if lines else worker_host).strip() or worker_host
    py_lines: list[str] = []
    ff_lines: list[str] = []
    installed_version = ""
    section = ""
    for raw_line in lines[1:]:
        line = str(raw_line or "").strip()
        if line == "__VER__":
            section = "ver"
            continue
        if line == "__PY__":
            section = "py"
            continue
        if line == "__FF__":
            section = "ff"
            continue
        if not line:
            continue
        if section == "ver":
            installed_version = line
            continue
        if section == "ff":
            ff_lines.append(line)
        else:
            py_lines.append(line)
    proc_lines = ff_lines if ff_lines else py_lines
    proc_line = proc_lines[0] if proc_lines else ""
    state = "idle"
    label = "idle"
    active_job = ""
    pid = ""
    active_count = 0
    status_table_text = ""
    active_source = ""
    active_target = ""
    active_speed = ""
    active_fps = ""
    active_eta = ""
    active_z_gb = ""
    active_e_gb = ""
    active_lzeit = ""
    if proc_line:
        active_count = max(len(py_lines), len(ff_lines))
        parts = proc_line.split(None, 2)
        if len(parts) >= 2:
            pid = parts[0]
            active_job = parts[2] if len(parts) > 2 else ""
            if any(" T" in f" {line} " or (len(line.split(None, 2)) >= 2 and "T" in line.split(None, 2)[1]) for line in proc_lines):
                state = "paused"
                label = "pausiert"
            else:
                state = "encoding"
                label = "encoding"
    elif not enabled:
        state = "paused"
        label = "pausiert"
    try:
        status_table_text, live_snapshot = _read_worker_live_status(worker_host)
        if live_snapshot:
            active_source = str(live_snapshot.get("source", "") or "").strip()
            active_target = str(live_snapshot.get("target", "") or "").strip()
            active_speed = str(live_snapshot.get("speed", "") or "").strip()
            active_fps = str(live_snapshot.get("fps", "") or "").strip()
            active_eta = str(live_snapshot.get("eta", "") or "").strip()
            active_z_gb = str(live_snapshot.get("z_gb", "") or "").strip()
            active_e_gb = str(live_snapshot.get("e_gb", "") or "").strip()
            active_lzeit = str(live_snapshot.get("lzeit", "") or "").strip()
            if active_source:
                active_job = active_source
    except Exception:
        pass
    if active_count <= 0 and state != "encoding":
        active_job = ""
        active_source = ""
        active_target = ""
        active_speed = ""
        active_fps = ""
        active_eta = ""
        active_z_gb = ""
        active_e_gb = ""
        active_lzeit = ""
        status_table_text = ""
    return {
        "name": worker_name,
        "host": host_name,
        "node": str(spec.get("node", "") or "").strip(),
        "ctid": str(spec.get("ctid", "") or "").strip(),
        "installed_version": installed_version,
        "enabled": bool(enabled),
        "connected": True,
        "accepting": bool(enabled),
        "state": state,
        "label": label,
        "active_job": active_job,
        "active_source": active_source,
        "active_target": active_target,
        "active_speed": active_speed,
        "active_fps": active_fps,
        "active_eta": active_eta,
        "active_z_gb": active_z_gb,
        "active_e_gb": active_e_gb,
        "active_lzeit": active_lzeit,
        "status_table_text": status_table_text,
        "pid": pid,
        "active_count": active_count,
        "reinit_running": reinit_running,
        "busy_action": busy_action,
    }


def _parse_worker_job_metrics_from_text(text: str) -> dict[str, str]:
    best = {"speed": "", "fps": "", "z_gb": "", "e_gb": "", "eta": ""}
    best_rank = (-1, -1, -1, -1)
    for raw_line in str(text or "").splitlines():
        line = str(raw_line or "").strip()
        if not line:
            continue
        speed = ""
        fps = ""
        z_gb = ""
        e_gb = ""
        eta = ""
        speed_match = re.search(
            r"(?:\bSpeed\s*[:=]\s*|\bspeed=)([0-9]+(?:[.,][0-9]+)?\s*(?:MB/s|MiB/s|x)?)",
            line,
            re.IGNORECASE,
        )
        if speed_match and speed_match.group(1):
            raw_speed = str(speed_match.group(1)).strip().replace("MiB/s", "MB/s")
            if re.fullmatch(r"[0-9]+(?:[.,][0-9]+)?", raw_speed):
                raw_speed = f"{raw_speed}x"
            speed = raw_speed
        fps_match = re.search(r"(?:\bFPS\s*[:=]\s*)([0-9]+(?:[.,][0-9]+)?|n/?a)", line, re.IGNORECASE)
        if fps_match and fps_match.group(1):
            fps = str(fps_match.group(1)).strip().replace(",", ".")
        z_match = re.search(r"(?:\bZ-GB\s*[:=]\s*)([0-9]+(?:[.,][0-9]+)?)", line, re.IGNORECASE)
        if z_match and z_match.group(1):
            z_gb = str(z_match.group(1)).strip().replace(",", ".")
        e_match = re.search(r"(?:\bE-GB\s*[:=]\s*)([0-9]+(?:[.,][0-9]+)?(?:±[0-9]+(?:[.,][0-9]+)?)?|[0-9]+%|copy|n/?a)", line, re.IGNORECASE)
        if e_match and e_match.group(1):
            e_gb = str(e_match.group(1)).strip().replace(",", ".")
        eta_match = re.search(r"(?:\bETA\s*[:=]\s*)([0-9]{2}:[0-9]{2}(?::[0-9]{2})?|[0-9]+:[0-9]{2})", line, re.IGNORECASE)
        if eta_match and eta_match.group(1):
            eta = str(eta_match.group(1)).strip()
        if not (speed or fps or z_gb or e_gb or eta):
            continue
        rank = (
            1 if speed.endswith("x") else 0,
            1 if fps and fps.lower() != "n/a" else 0,
            1 if (z_gb or e_gb) else 0,
            1 if "[FFMPEG]" in line.upper() else 0,
        )
        if rank >= best_rank:
            best_rank = rank
            best = {
                "speed": speed,
                "fps": fps,
                "z_gb": z_gb,
                "e_gb": e_gb,
                "eta": eta,
            }
    return best


def _worker_metrics_look_encode(metrics: dict[str, Any] | None) -> bool:
    data = dict(metrics or {})
    speed = str(data.get("speed", "") or "").strip().lower()
    fps = str(data.get("fps", "") or "").strip().lower()
    z_gb = str(data.get("z_gb", "") or "").strip()
    e_gb = str(data.get("e_gb", "") or "").strip()
    return bool(
        speed.endswith("x")
        or (fps not in {"", "n/a"} and fps)
        or z_gb
        or e_gb
    )


def _merge_worker_metric_sources(
    *,
    running: bool,
    status_metrics: dict[str, Any] | None,
    job_metrics: dict[str, Any] | None,
) -> dict[str, str]:
    status_data = {
        "speed": str((status_metrics or {}).get("speed", "") or "").strip(),
        "fps": str((status_metrics or {}).get("fps", "") or "").strip(),
        "z_gb": str((status_metrics or {}).get("z_gb", "") or "").strip(),
        "e_gb": str((status_metrics or {}).get("e_gb", "") or "").strip(),
        "eta": str((status_metrics or {}).get("eta", "") or "").strip(),
        "lzeit": str((status_metrics or {}).get("lzeit", "") or "").strip(),
    }
    job_data = {
        "speed": str((job_metrics or {}).get("speed", "") or "").strip(),
        "fps": str((job_metrics or {}).get("fps", "") or "").strip(),
        "z_gb": str((job_metrics or {}).get("z_gb", "") or "").strip(),
        "e_gb": str((job_metrics or {}).get("e_gb", "") or "").strip(),
        "eta": str((job_metrics or {}).get("eta", "") or "").strip(),
        "lzeit": str((job_metrics or {}).get("lzeit", "") or "").strip(),
    }
    status_encode = _worker_metrics_look_encode(status_data)
    job_encode = _worker_metrics_look_encode(job_data)
    if running:
        primary = status_data if any(status_data.values()) else job_data
        secondary = job_data if primary is status_data else status_data
    else:
        if job_encode:
            primary, secondary = job_data, status_data
        elif status_encode:
            primary, secondary = status_data, job_data
        elif any(job_data.values()):
            primary, secondary = job_data, status_data
        else:
            primary, secondary = status_data, job_data
    merged: dict[str, str] = {}
    for key in ("speed", "fps", "z_gb", "e_gb", "eta", "lzeit"):
        merged[key] = str(primary.get(key, "") or secondary.get(key, "") or "").strip()
    return merged


def _fallback_live_e_gb(q_gb_text: Any, e_gb_text: Any, *, running: bool) -> str:
    q_text = str(q_gb_text or "").strip()
    e_text = str(e_gb_text or "").strip()
    if e_text and e_text.lower() != "n/a":
        return e_text
    if running and q_text:
        return q_text
    return e_text


WORKER_REMOTE_STATUS_TABLE = "/opt/managemovie/MovieManager/work/gemini-status-table.txt"
WORKER_METRIC_CACHE: dict[str, dict[str, str]] = {}


def _worker_metric_cache_key(worker_name: str, source_name: str) -> str:
    return f"{str(worker_name or '').strip().lower()}|{str(source_name or '').strip().lower()}"


def _norm_status_header_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(text or "").lower())


def _split_worker_status_source_target(raw: str) -> tuple[str, str]:
    text = str(raw or "").strip()
    if not text:
        return "", ""
    match = re.match(r"^(.*?)\s*-+>\s*(.*?)$", text)
    if match:
        return str(match.group(1) or "").strip(), str(match.group(2) or "").strip()
    match = re.match(r"^(.*?)\s*->\s*(.*?)$", text)
    if match:
        return str(match.group(1) or "").strip(), str(match.group(2) or "").strip()
    return text, ""


def _parse_worker_status_table_snapshot(text: str) -> dict[str, str]:
    lines = [str(line or "").rstrip() for line in str(text or "").splitlines() if str(line or "").strip()]
    headers: list[str] = []
    row_values: list[str] = []
    for line in lines:
        stripped = line.strip()
        if not stripped.startswith("|"):
            continue
        if re.match(r"^\+[=\-+]+\+$", stripped):
            continue
        cells = [cell.strip() for cell in stripped.strip("|").split("|")]
        if not headers:
            headers = cells
            continue
        if cells and any(cell for cell in cells):
            row_values = cells
            break
    if not headers or not row_values or len(row_values) != len(headers):
        return {}
    data = {_norm_status_header_key(h): str(v or "").strip() for h, v in zip(headers, row_values)}
    source_text, target_text = _split_worker_status_source_target(data.get("quelleziel", ""))
    if not source_text:
        source_text = str(data.get("quelle", "") or "").strip()
    if not target_text:
        target_text = str(data.get("ziel", "") or "").strip()
    return {
        "source": source_text,
        "target": target_text,
        "year": str(data.get("jahr", "") or "").strip(),
        "st_em": str(data.get("stem", "") or "").strip(),
        "imdb_id": str(data.get("imdbid", "") or "").strip(),
        "q_gb": str(data.get("qgb", "") or "").strip(),
        "lzeit": str(data.get("lzeit", "") or data.get("laufzeit", "") or "").strip(),
        "speed": str(data.get("speed", "") or "").strip(),
        "fps": str(data.get("fps", "") or "").strip(),
        "eta": str(data.get("eta", "") or "").strip(),
        "z_gb": str(data.get("zgb", "") or "").strip(),
        "e_gb": str(data.get("egb", "") or "").strip(),
    }


def _format_duration_hms(total_seconds: Any) -> str:
    try:
        seconds = max(0, int(float(total_seconds or 0)))
    except Exception:
        return ""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _parse_status_eta_minutes(text: str) -> int:
    raw = str(text or "").strip()
    if not raw:
        return -1
    m = re.match(r"^(\d+):(\d+)$", raw)
    if not m:
        return -1
    try:
        return int(m.group(1)) * 60 + int(m.group(2))
    except Exception:
        return -1


def _build_live_status_rows_from_workers(worker_states: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for worker in worker_states:
        if int(worker.get("active_count", 0) or 0) <= 0:
            continue
        snapshot = _parse_worker_status_table_snapshot(str(worker.get("status_table_text", "") or ""))
        source_name = str(snapshot.get("source", "") or worker.get("active_source", "") or worker.get("active_job", "") or "").strip()
        target_name = str(snapshot.get("target", "") or worker.get("active_target", "") or "").strip()
        if not source_name and not target_name:
            continue
        st_em_text = str(snapshot.get("st_em", "") or "").strip()
        season = ""
        episode = ""
        m = re.match(r"^s(\d+)e(\d+)$", st_em_text, re.IGNORECASE)
        if m:
            season = str(m.group(1)).zfill(2)
            episode = str(m.group(2)).zfill(2)
        rows.append(
            {
                "source_name": source_name,
                "target_name": target_name,
                "year": str(snapshot.get("year", "") or "").strip(),
                "season": season,
                "episode": episode,
                "imdb_id": str(snapshot.get("imdb_id", "") or "").strip(),
                "q_gb": str(snapshot.get("q_gb", "") or "").strip(),
                "z_gb": str(snapshot.get("z_gb", "") or "").strip(),
                "e_gb": str(snapshot.get("e_gb", "") or "").strip(),
                "lzeit": str(snapshot.get("lzeit", "") or "").strip(),
                "speed": str(snapshot.get("speed", "") or worker.get("active_speed", "") or "").strip(),
                "fps": str(snapshot.get("fps", "") or worker.get("active_fps", "") or "").strip(),
                "eta": str(snapshot.get("eta", "") or worker.get("active_eta", "") or "").strip(),
                "worker_name": str(worker.get("name", "") or "").strip(),
            }
        )
    rows.sort(key=lambda row: str(row.get("worker_name", "") or ""))
    return rows


def build_live_worker_status_table_text(worker_states: list[dict[str, Any]]) -> str:
    rows = _build_live_status_rows_from_workers(worker_states)
    if not rows:
        return ""

    def _to_float(value: Any) -> float:
        text = str(value or "").strip().replace(",", ".")
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return 0.0
        try:
            return float(match.group(0))
        except Exception:
            return 0.0

    total = len(rows)
    q_total = sum(_to_float(row.get("q_gb", "")) for row in rows)
    z_total = sum(_to_float(row.get("z_gb", "")) for row in rows)
    active_target = Path(str(rows[0].get("target_name", "") or "")).name if total == 1 else "parallel"
    eta_values = [_parse_status_eta_minutes(row.get("eta", "")) for row in rows]
    eta_values = [value for value in eta_values if value >= 0]
    eta_text = "-"
    if len(rows) == 1 and str(rows[0].get("eta", "") or "").strip():
        eta_text = str(rows[0].get("eta", "") or "").strip()
    elif eta_values:
        eta_text = f"{max(eta_values) // 60:02d}:{max(eta_values) % 60:02d}"

    meta_lines = [
        f"Aktiv:      {total:02d}/{total:02d} {active_target}".rstrip(),
        "",
        "Gesamt:",
        f"Dateien:    {total:02d}/{total:02d}",
        f"Verlauf:    {total:02d}/{total:02d}",
        f"Erledigt:   00/{total:02d}",
        f"Q-GB:       {q_total:.1f} GB".replace(".", ","),
        f"Z-GB:       {z_total:.1f} GB".replace(".", ","),
        f"ETA:        {eta_text}",
        "",
    ]
    return "\n".join(meta_lines) + build_status_table_override_from_editor_rows(rows)


def _status_text_covers_active_workers(status_text: str, worker_states: list[dict[str, Any]]) -> bool:
    hay = str(status_text or "").lower()
    active_workers = [worker for worker in worker_states if int(worker.get("active_count", 0) or 0) > 0]
    if not active_workers:
        return True
    for worker in active_workers:
        target = Path(str(worker.get("active_target", "") or "")).name.strip().lower()
        source = Path(str(worker.get("active_source", "") or worker.get("active_job", "") or "")).name.strip().lower()
        if target and target in hay:
            continue
        if source and source in hay:
            continue
        return False
    return True


def _status_snapshot_matches_item(source_name: str, target_name: str, snapshot: dict[str, str]) -> bool:
    source_file = Path(str(source_name or "")).name.strip().lower()
    source_stem = Path(str(source_name or "")).stem.strip().lower()
    target_file = Path(str(target_name or "")).name.strip().lower()
    target_stem = Path(str(target_name or "")).stem.strip().lower()
    source_hay = str(snapshot.get("source", "") or "").strip().lower()
    target_hay = str(snapshot.get("target", "") or "").strip().lower()
    hay = " ".join([source_hay, target_hay]).strip()
    if not source_hay and not target_hay:
        return False
    if target_file and target_hay and target_file in target_hay:
        return True
    if target_stem and target_hay and target_stem in target_hay:
        return True
    candidates = [source_file, source_stem]
    for candidate in candidates:
        if candidate and candidate in hay:
            return True
    return False


def _read_worker_status_metrics(item: dict[str, Any]) -> tuple[dict[str, str], dict[str, str] | None]:
    worker_host = str(item.get("worker_host", "") or "").strip()
    worker_name = str(item.get("worker_name", "") or "").strip()
    source_name = str(item.get("source_name", "") or "").strip()
    target_name = str(item.get("target_name", "") or "").strip()
    cache_key = _worker_metric_cache_key(worker_name, source_name)
    cached = dict(WORKER_METRIC_CACHE.get(cache_key, {}))
    if not worker_host or not source_name:
        return cached, None
    result = run_worker_ssh(
        worker_host,
        f"sed -n '1,120p' {shlex.quote(WORKER_REMOTE_STATUS_TABLE)} 2>/dev/null || true",
        timeout=6,
    )
    snapshot = _parse_worker_status_table_snapshot(result.stdout or "")
    if snapshot and _status_snapshot_matches_item(source_name, target_name, snapshot):
        if snapshot.get("speed"):
            cached["speed"] = str(snapshot.get("speed") or "").strip()
        if snapshot.get("fps"):
            cached["fps"] = str(snapshot.get("fps") or "").strip()
        if snapshot.get("z_gb"):
            cached["z_gb"] = str(snapshot.get("z_gb") or "").strip()
        if snapshot.get("e_gb"):
            cached["e_gb"] = str(snapshot.get("e_gb") or "").strip()
        if snapshot.get("eta"):
            cached["eta"] = str(snapshot.get("eta") or "").strip()
        if snapshot.get("lzeit"):
            cached["lzeit"] = str(snapshot.get("lzeit") or "").strip()
        WORKER_METRIC_CACHE[cache_key] = dict(cached)
        return cached, snapshot
    return cached, None


def _read_worker_status_table_text(worker_host: str) -> str:
    host = str(worker_host or "").strip()
    if not host:
        return ""
    result = run_worker_ssh(
        host,
        f"sed -n '1,160p' {shlex.quote(WORKER_REMOTE_STATUS_TABLE)} 2>/dev/null || true",
        timeout=6,
    )
    return str(result.stdout or "").strip()


def _read_worker_live_status(worker_host: str) -> tuple[str, dict[str, str]]:
    text = _read_worker_status_table_text(worker_host)
    snapshot = _parse_worker_status_table_snapshot(text)
    return text, snapshot


def _format_gb_two_decimals(size_bytes: int) -> str:
    try:
        value = max(0, int(size_bytes)) / (1024.0 ** 3)
    except Exception:
        return ""
    return f"{value:.2f}"


def _read_remote_target_size_gb(worker_host: str, remote_target_path: str) -> str:
    host = str(worker_host or "").strip()
    target = str(remote_target_path or "").strip()
    if not host or not target:
        return ""
    result = run_worker_ssh(
        host,
        f"if [ -f {shlex.quote(target)} ]; then stat -c%s {shlex.quote(target)} 2>/dev/null || wc -c < {shlex.quote(target)} 2>/dev/null; fi",
        timeout=4,
    )
    raw = str(result.stdout or "").strip()
    match = re.search(r"\d+", raw)
    if not match:
        return ""
    try:
        size_bytes = int(match.group(0))
    except Exception:
        return ""
    if size_bytes <= 0:
        return ""
    return _format_gb_two_decimals(size_bytes)


def _looks_encode_speed(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text.endswith("x")


def _looks_non_encode_speed(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text.endswith("mb/s") or text.endswith("mib/s") or text in {"copied", "encoded"}


def _looks_encode_fps(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return bool(text) and text not in {"n/a", "na", "-"}


def _looks_percent_metric(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return text.endswith("%")


def _prefer_live_completed_metrics(base: dict[str, str], preferred: dict[str, str], q_gb: Any = "") -> dict[str, str]:
    out = dict(base or {})
    pref = {key: str((preferred or {}).get(key, "") or "").strip() for key in ("speed", "fps", "z_gb", "e_gb", "eta", "lzeit")}
    if _looks_non_encode_speed(out.get("speed", "")) and _looks_encode_speed(pref.get("speed", "")):
        out["speed"] = pref["speed"]
    if not _looks_encode_fps(out.get("fps", "")) and _looks_encode_fps(pref.get("fps", "")):
        out["fps"] = pref["fps"]
    if (_looks_percent_metric(out.get("e_gb", "")) or not str(out.get("e_gb", "") or "").strip()) and str(pref.get("e_gb", "") or "").strip():
        out["e_gb"] = pref["e_gb"]
    if (not str(out.get("eta", "") or "").strip() or str(out.get("eta", "") or "").strip().lower() in {"n/a", "-", "na"}) and str(pref.get("eta", "") or "").strip():
        out["eta"] = pref["eta"]
    if (not str(out.get("lzeit", "") or "").strip() or str(out.get("lzeit", "") or "").strip().lower() in {"n/a", "-", "na"}) and str(pref.get("lzeit", "") or "").strip():
        out["lzeit"] = pref["lzeit"]
    z_text = str(out.get("z_gb", "") or "").strip()
    z_num = _to_float(z_text)
    pref_z = str(pref.get("z_gb", "") or "").strip()
    pref_z_num = _to_float(pref_z)
    if pref_z_num > z_num:
        out["z_gb"] = pref_z
    q_num = _to_float(q_gb)
    z_final = _to_float(out.get("z_gb", ""))
    if q_num > 0 and z_final >= 0:
        saved_pct = max(0.0, ((q_num - z_final) / q_num) * 100.0)
        out["e_gb"] = f"{int(round(saved_pct))}%"
    return out


def _read_worker_job_metrics(item: dict[str, Any]) -> dict[str, str]:
    worker_host = str(item.get("worker_host", "") or "").strip()
    worker_name = str(item.get("worker_name", "") or "").strip()
    source_name = str(item.get("source_name", "") or "").strip()
    remote_log_path = str(item.get("remote_log_path", "") or "").strip()
    remote_target_path = str(item.get("remote_target_path", "") or "").strip()
    local_log_path = Path(str(item.get("log_path", "") or "").strip() or ".")
    running = bool(item.get("running"))
    live_metrics = {
        key: str((item.get("last_live_metrics", {}) or {}).get(key, "") or "").strip()
        for key in ("speed", "fps", "z_gb", "e_gb", "eta", "lzeit")
    }
    cache_key = _worker_metric_cache_key(worker_name, source_name)
    cached_metrics = dict(WORKER_METRIC_CACHE.get(cache_key, {"speed": "", "fps": "", "z_gb": "", "e_gb": "", "eta": ""}))
    for key, value in live_metrics.items():
        if value and not str(cached_metrics.get(key, "") or "").strip():
            cached_metrics[key] = value
    status_metrics, _ = _read_worker_status_metrics(item)
    if (
        status_metrics.get("speed")
        or status_metrics.get("fps")
        or status_metrics.get("z_gb")
        or status_metrics.get("e_gb")
        or status_metrics.get("eta")
    ):
        if running:
            merged = _merge_worker_metric_sources(
                running=True,
                status_metrics=status_metrics,
                job_metrics=cached_metrics,
            )
            WORKER_METRIC_CACHE[cache_key] = dict(merged)
            return merged
    if running and worker_host and remote_log_path:
        result = run_worker_ssh(
            worker_host,
            f"tail -n 80 {shlex.quote(remote_log_path)} 2>/dev/null || true",
            timeout=4,
        )
        metrics = _parse_worker_job_metrics_from_text(result.stdout or "")
        if (
            metrics.get("speed")
            or metrics.get("fps")
            or metrics.get("z_gb")
            or metrics.get("e_gb")
            or metrics.get("eta")
        ):
                WORKER_METRIC_CACHE[cache_key] = dict(metrics)
                return metrics
    try:
        if local_log_path.exists():
            metrics = _parse_worker_job_metrics_from_text(tail_file(local_log_path, lines=120, max_chars=120000))
            if (
                metrics.get("speed")
                or metrics.get("fps")
                or metrics.get("z_gb")
                or metrics.get("e_gb")
                or metrics.get("eta")
            ):
                parsed_speed = str(metrics.get("speed", "") or "").strip().lower()
                parsed_fps = str(metrics.get("fps", "") or "").strip().lower()
                cached_speed = str(cached_metrics.get("speed", "") or "").strip().lower()
                cached_fps = str(cached_metrics.get("fps", "") or "").strip().lower()
                live_speed = str(live_metrics.get("speed", "") or "").strip().lower()
                live_fps = str(live_metrics.get("fps", "") or "").strip().lower()
                parsed_looks_non_encode = (
                    parsed_speed in {"copied", "encoded"}
                    or parsed_speed.endswith("mb/s")
                    or parsed_speed.endswith("mib/s")
                    or parsed_fps in {"", "n/a"}
                )
                cached_looks_encode = (
                    cached_speed.endswith("x")
                    or (cached_fps not in {"", "n/a"} and bool(cached_fps))
                )
                live_looks_encode = (
                    live_speed.endswith("x")
                    or (live_fps not in {"", "n/a"} and bool(live_fps))
                )
                if not running and parsed_looks_non_encode and (cached_looks_encode or live_looks_encode):
                    preferred = cached_metrics if cached_looks_encode else live_metrics
                    merged = _prefer_live_completed_metrics(dict(metrics), preferred, item.get("q_gb", ""))
                    WORKER_METRIC_CACHE[cache_key] = dict(merged)
                    return merged
                merged = _merge_worker_metric_sources(
                    running=running,
                    status_metrics=status_metrics,
                    job_metrics=metrics,
                )
                if not running and int(item.get("exit_code", 1) or 1) == 0:
                    merged = _prefer_live_completed_metrics(merged, cached_metrics if cached_looks_encode else live_metrics, item.get("q_gb", ""))
                WORKER_METRIC_CACHE[cache_key] = dict(merged)
                return merged
    except Exception:
        pass
    merged = _merge_worker_metric_sources(
        running=running,
        status_metrics=status_metrics,
        job_metrics=cached_metrics,
    )
    if not running and int(item.get("exit_code", 1) or 1) == 0:
        merged = _prefer_live_completed_metrics(merged, live_metrics, item.get("q_gb", ""))
    WORKER_METRIC_CACHE[cache_key] = dict(merged)
    return merged


def _dispatch_row_match_keys(*values: str) -> list[str]:
    keys: list[str] = []
    for raw_value in values:
        raw = str(raw_value or "").replace("\\", "/").strip()
        if not raw:
            continue
        path = Path(raw)
        candidates = [
            raw,
            path.name,
            path.stem,
            normalize_simple_text(raw),
            normalize_simple_text(path.name),
            normalize_simple_text(path.stem),
        ]
        for candidate in candidates:
            key = str(candidate or "").strip().lower()
            if key and key not in keys:
                keys.append(key)
    return keys


def _dispatch_item_precedence_key(item: dict[str, Any]) -> tuple[int, int, float]:
    running = 1 if bool(item.get("running")) else 0
    success = 1 if int(item.get("exit_code", 1) or 1) == 0 else 0
    started_at = float(item.get("started_at", 0.0) or 0.0)
    return (running, success, started_at)


def _build_dispatch_status_index() -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    live_worker_states = {
        str(worker.get("name", "") or "").strip(): dict(worker)
        for worker in read_worker_states()
        if str(worker.get("name", "") or "").strip()
    }
    with dispatch_lock:
        items = [dict(item) for item in dispatch_subjobs.values()]
    items.sort(key=lambda item: float(item.get("started_at", 0.0) or 0.0))
    for item in items:
        worker_name = str(item.get("worker_name", "") or "").strip()
        source_name = str(item.get("source_name", "") or "").strip()
        if not worker_name or not source_name:
            continue
        running = bool(item.get("running"))
        status_metrics, snapshot = _read_worker_status_metrics(item)
        job_metrics = _read_worker_job_metrics(item)
        metrics = _merge_worker_metric_sources(
            running=running,
            status_metrics=status_metrics,
            job_metrics=job_metrics,
        )
        entry = {
            "worker_name": worker_name,
            "source_name": source_name,
            "target_name": str((snapshot or {}).get("target", "") or item.get("target_name", "") or "").strip(),
            "running": bool(item.get("running")),
            "exit_code": item.get("exit_code"),
            "ended_at": float(item.get("ended_at", 0.0) or 0.0),
            "speed": str(metrics.get("speed", "") or "").strip(),
            "fps": str(metrics.get("fps", "") or "").strip(),
            "eta": str((snapshot or {}).get("eta", "") or metrics.get("eta", "") or "").strip(),
            "z_gb": str((snapshot or {}).get("z_gb", "") or metrics.get("z_gb", "") or "").strip(),
            "e_gb": str((snapshot or {}).get("e_gb", "") or metrics.get("e_gb", "") or "").strip(),
            "lzeit": str((snapshot or {}).get("lzeit", "") or "").strip(),
            "started_at": float(item.get("started_at", 0.0) or 0.0),
            "q_gb": str(item.get("q_gb", "") or "").strip(),
        }
        live_worker = live_worker_states.get(worker_name) or {}
        live_source = str(live_worker.get("active_source", "") or live_worker.get("active_job", "") or "").strip()
        live_target = str(live_worker.get("active_target", "") or "").strip()
        live_matches = False
        if live_source or live_target:
            item_target_name = str(item.get("target_name", "") or "").strip()
            for key in _dispatch_row_match_keys(source_name, entry.get("target_name", ""), item_target_name):
                for live_key in _dispatch_row_match_keys(live_source, live_target):
                    if key and live_key and key == live_key:
                        live_matches = True
                        break
                if live_matches:
                    break
        if live_matches or (bool(live_worker.get("active_count", 0)) and not entry.get("speed")):
            entry["speed"] = str(entry.get("speed") or live_worker.get("active_speed", "") or "").strip()
            entry["fps"] = str(entry.get("fps") or live_worker.get("active_fps", "") or "").strip()
            entry["eta"] = str(entry.get("eta") or live_worker.get("active_eta", "") or "").strip()
            entry["target_name"] = str(entry.get("target_name") or live_target or "").strip()
        entry["e_gb"] = _fallback_live_e_gb(entry.get("q_gb", ""), entry.get("e_gb", ""), running=bool(entry.get("running")))
        for key in _dispatch_row_match_keys(source_name, entry.get("target_name", "")):
            prev = grouped.get(key)
            if not prev or _dispatch_item_precedence_key(entry) > _dispatch_item_precedence_key(prev):
                grouped[key] = dict(entry)
    return grouped


def annotate_runtime_rows_with_dispatch_status(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return []
    status_index = _build_dispatch_status_index()
    if not status_index:
        return [dict(row) for row in rows]
    annotated: list[dict[str, Any]] = []
    for row in rows:
        out = dict(row)
        out["completed"] = False
        match: dict[str, Any] | None = None
        for key in _dispatch_row_match_keys(out.get("source_name", ""), out.get("target_name", "")):
            candidate = status_index.get(key)
            if candidate:
                match = candidate
                break
        if match:
            out["worker_name"] = str(match.get("worker_name", "") or "").strip()
            if match.get("target_name"):
                out["target_name"] = str(match.get("target_name", "") or "").strip()
            if match.get("z_gb"):
                out["z_gb"] = str(match.get("z_gb", "") or "").strip()
            out["e_gb"] = _fallback_live_e_gb(out.get("q_gb", ""), match.get("e_gb", ""), running=bool(match.get("running")))
            if match.get("lzeit"):
                out["lzeit"] = str(match.get("lzeit", "") or "").strip()
            if match.get("running"):
                if match.get("speed"):
                    out["speed"] = str(match.get("speed", "") or "").strip()
                if match.get("fps"):
                    out["fps"] = str(match.get("fps", "") or "").strip()
                if match.get("eta"):
                    out["eta"] = str(match.get("eta", "") or "").strip()
            else:
                exit_code = match.get("exit_code")
                duration_text = _format_duration_hms(
                    max(0.0, float(match.get("ended_at", 0.0) or 0.0) - float(match.get("started_at", 0.0) or 0.0))
                )
                if exit_code in (0, "0"):
                    out["completed"] = True
                    if match.get("speed"):
                        out["speed"] = str(match.get("speed", "") or "").strip()
                    if match.get("fps"):
                        out["fps"] = str(match.get("fps", "") or "").strip()
                    if duration_text:
                        out["eta"] = duration_text
                    elif match.get("lzeit"):
                        out["eta"] = str(match.get("lzeit", "") or "").strip()
                elif exit_code not in (None, "", "-"):
                    out["speed"] = "error"
                    out["eta"] = duration_text or "error"
        annotated.append(out)
    return annotated


def build_worker_job_map() -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    live_worker_states = {
        str(worker.get("name", "") or "").strip(): dict(worker)
        for worker in read_worker_states()
        if str(worker.get("name", "") or "").strip()
    }
    with dispatch_lock:
        items = [dict(item) for item in dispatch_subjobs.values()]
    items.sort(key=lambda item: float(item.get("started_at", 0.0) or 0.0))
    for item in items:
        worker_name = str(item.get("worker_name", "") or "").strip()
        source_name = str(item.get("source_name", "") or "").strip()
        if not worker_name or not source_name:
            continue
        status_metrics, snapshot = _read_worker_status_metrics(item)
        job_metrics = _read_worker_job_metrics(item)
        metrics = _merge_worker_metric_sources(
            running=bool(item.get("running")),
            status_metrics=status_metrics,
            job_metrics=job_metrics,
        )
        live_worker = live_worker_states.get(worker_name) or {}
        live_source = str(live_worker.get("active_source", "") or live_worker.get("active_job", "") or "").strip()
        live_target = str(live_worker.get("active_target", "") or "").strip()
        live_matches = False
        if live_source or live_target:
            for key in _dispatch_row_match_keys(source_name, str((snapshot or {}).get("target", "") or item.get("target_name", "") or "").strip()):
                for live_key in _dispatch_row_match_keys(live_source, live_target):
                    if key and live_key and key == live_key:
                        live_matches = True
                        break
                if live_matches:
                    break
        speed_text = str(metrics.get("speed", "") or "").strip()
        fps_text = str(metrics.get("fps", "") or "").strip()
        eta_text = str((snapshot or {}).get("eta", "") or metrics.get("eta", "") or "").strip()
        z_gb_text = str((snapshot or {}).get("z_gb", "") or metrics.get("z_gb", "") or "").strip()
        e_gb_text = str((snapshot or {}).get("e_gb", "") or metrics.get("e_gb", "") or "").strip()
        if live_matches or (running and not speed_text and bool(live_worker.get("active_count", 0))):
            speed_text = speed_text or str(live_worker.get("active_speed", "") or "").strip()
            fps_text = fps_text or str(live_worker.get("active_fps", "") or "").strip()
            eta_text = eta_text or str(live_worker.get("active_eta", "") or "").strip()
            z_gb_text = z_gb_text or str(live_worker.get("active_z_gb", "") or "").strip()
            e_gb_text = e_gb_text or str(live_worker.get("active_e_gb", "") or "").strip()
        if running and (_to_float(z_gb_text) <= 0.0) and remote_target_path:
            remote_z = _read_remote_target_size_gb(worker_host, remote_target_path)
            if _to_float(remote_z) > 0.0:
                z_gb_text = remote_z
        e_gb_text = _fallback_live_e_gb(item.get("q_gb", ""), e_gb_text, running=bool(item.get("running")))
        grouped.setdefault(worker_name, []).append(
            {
                "source_name": source_name,
                "file_name": Path(source_name).name,
                "target_name": str((snapshot or {}).get("target", "") or item.get("target_name", "") or live_target or "").strip(),
                "running": bool(item.get("running")),
                "exit_code": item.get("exit_code"),
                "speed": speed_text,
                "fps": fps_text,
                "eta": eta_text,
                "z_gb": z_gb_text,
                "e_gb": e_gb_text,
                "lzeit": str((snapshot or {}).get("lzeit", "") or "").strip(),
                "started_at": float(item.get("started_at", 0.0) or 0.0),
                "ended_at": float(item.get("ended_at", 0.0) or 0.0),
            }
        )
    return grouped


def build_live_worker_job_map(worker_states: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for worker in worker_states:
        if not bool(worker.get("connected")):
            continue
        if int(worker.get("active_count", 0) or 0) <= 0:
            continue
        worker_name = str(worker.get("name", "") or "").strip()
        source_name = str(worker.get("active_source", "") or worker.get("active_job", "") or "").strip()
        if not worker_name or not source_name:
            continue
        grouped.setdefault(worker_name, []).append(
            {
                "source_name": source_name,
                "file_name": Path(source_name).name,
                "target_name": str(worker.get("active_target", "") or "").strip(),
                "running": worker.get("state") in {"encoding", "paused"},
                "exit_code": None,
                "speed": str(worker.get("active_speed", "") or "").strip(),
                "fps": str(worker.get("active_fps", "") or "").strip(),
                "eta": str(worker.get("active_eta", "") or "").strip(),
                "z_gb": str(worker.get("active_z_gb", "") or "").strip(),
                "e_gb": str(worker.get("active_e_gb", "") or "").strip(),
                "lzeit": str(worker.get("active_lzeit", "") or "").strip(),
                "started_at": time.time(),
            }
        )
    return grouped


def build_live_processing_log_from_workers(worker_states: list[dict[str, Any]]) -> str:
    lines: list[str] = []
    for worker in worker_states:
        if not bool(worker.get("connected")):
            continue
        if int(worker.get("active_count", 0) or 0) <= 0:
            continue
        worker_name = str(worker.get("name", "") or "").strip() or "worker"
        source_name = Path(str(worker.get("active_source", "") or worker.get("active_job", "") or "")).name
        speed = str(worker.get("active_speed", "") or "").strip()
        fps = str(worker.get("active_fps", "") or "").strip()
        eta = str(worker.get("active_eta", "") or "").strip()
        lines.append(f"[{worker_name}] [--:--:--] [INFO] Live-Worker aktiv")
        parts = [f"[FFMPEG] {source_name or '-'}"]
        if speed:
            parts.append(f"Speed: {speed}")
        if fps:
            parts.append(f"FPS: {fps}")
        if eta:
            parts.append(f"ETA: {eta}")
        lines.append(f"[{worker_name}] [--:--:--] [INFO] " + " | ".join(parts))
    return "\n".join(lines).strip()


def read_worker_states(force: bool = False) -> list[dict[str, Any]]:
    global worker_status_cache_expires_at, worker_status_cache_payload
    now = time.time()
    with worker_status_cache_lock:
        if not force and worker_status_cache_payload and now < worker_status_cache_expires_at:
            return [dict(item) for item in worker_status_cache_payload]
    specs = read_worker_specs()
    enabled_states = read_worker_enabled_states()
    payload = [detect_worker_state(spec, enabled_states.get(spec["name"], True)) for spec in specs]
    max_active = per_worker_parallel_limit()
    for item in payload:
        item["max_active"] = max_active
    with worker_status_cache_lock:
        worker_status_cache_payload = [dict(item) for item in payload]
        worker_status_cache_expires_at = now + WORKER_STATUS_CACHE_TTL_SEC
    return payload


def per_worker_parallel_limit() -> int:
    runtime_settings = read_runtime_settings()
    if parse_form_bool(runtime_settings.get("one_encode_per_worker", "0")):
        return 1
    try:
        value = int(str(runtime_settings.get("parallel_job_limit", "2") or "2").strip() or "2")
    except Exception:
        value = 2
    return max(1, min(4, value))


def choose_ffmpeg_worker() -> dict[str, Any]:
    workers = read_worker_states(force=True)
    max_active = per_worker_parallel_limit()
    for worker in workers:
        if not worker.get("connected"):
            continue
        if not worker.get("enabled"):
            continue
        if worker.get("state") == "paused":
            continue
        if int(worker.get("active_count", 0) or 0) >= max_active:
            continue
        return worker
    fallback = ffmpeg_remote_host()
    if fallback:
        return {"name": fallback, "host": fallback, "mount_root": "/mnt/Q-NAS"}
    raise ValueError("Kein verfügbarer Worker für ffmpeg.")


def invalidate_worker_state_cache() -> None:
    global worker_status_cache_expires_at, worker_status_cache_payload
    with worker_status_cache_lock:
        worker_status_cache_expires_at = 0.0
        worker_status_cache_payload = []


def _run_worker_reinit(spec: dict[str, str]) -> None:
    worker_name = str(spec.get("name", "") or "").strip() or "worker"
    set_worker_reinit_running(worker_name, True)
    set_worker_busy_action(worker_name, "init")
    node = str(spec.get("node", "") or "").strip()
    ctid = str(spec.get("ctid", "") or "").strip()
    rootfs_size_gb = str(spec.get("rootfs_size_gb", "") or "").strip() or "6"
    swap_mb = str(spec.get("swap_mb", "") or "").strip() or "4096"
    hwaddr = worker_hwaddr(spec)
    try:
        if not node or not ctid:
            append_processing_log(f"[worker:{worker_name}] Init nicht möglich: Node/CT-ID fehlt")
            return
        reset_processing_log()
        append_processing_log(f"[worker:{worker_name}] Init gestartet")
        latest = run_proxmox_ssh(
            "pve01",
            "latest=$(ls -1t /mnt/pve/nfs/dump/vzdump-lxc-240-*.tar.zst 2>/dev/null | head -n1); "
            "if [ -z \"$latest\" ]; then exit 1; fi; "
            "age=$(( $(date +%s) - $(stat -c %Y \"$latest\") )); "
            "if [ \"$age\" -le 86400 ]; then printf '%s\t%s\n' \"$latest\" \"$age\"; else exit 1; fi",
            timeout=20,
        )
        latest_payload = (latest.stdout or "").strip()
        latest_path = ""
        if latest.returncode == 0 and latest_payload:
            latest_parts = latest_payload.split("\t", 1)
            latest_path = latest_parts[0].strip()
            backup_age_sec = int(latest_parts[1].strip()) if len(latest_parts) > 1 and latest_parts[1].strip().isdigit() else 0
            backup_age_min = max(0, backup_age_sec // 60)
            append_processing_log(
                f"[worker:{worker_name}] Init nutzt vorhandenes Backup: {Path(latest_path).name} | Alter={backup_age_min} min"
            )
        else:
            append_processing_log(f"[worker:{worker_name}] Kein frisches Backup gefunden, erstelle neues Backup")
            backup = run_proxmox_ssh(
                "pve01",
                "vzdump 240 --storage nfs --mode snapshot --compress zstd --stdout 0",
                timeout=1800,
            )
            if backup.returncode != 0:
                detail = (backup.stderr or backup.stdout or "").strip().splitlines()
                reason = detail[-1] if detail else "unbekannt"
                append_processing_log(f"[worker:{worker_name}] Init-Backup fehlgeschlagen: {reason}")
                return
            latest = run_proxmox_ssh("pve01", "ls -1t /mnt/pve/nfs/dump/vzdump-lxc-240-*.tar.zst | head -n1")
            latest_path = (latest.stdout or "").strip()
            if latest.returncode != 0 or not latest_path:
                append_processing_log(f"[worker:{worker_name}] Init-Backup nicht gefunden")
                return
        restore_cmd = (
            f"pct shutdown {ctid} --forceStop 1 --timeout 20 >/dev/null 2>&1 || "
            f"pct stop {ctid} --skiplock 1 >/dev/null 2>&1 || true; "
            f"pct unlock {ctid} >/dev/null 2>&1 || true; "
            f"pct destroy {ctid} --destroy-unreferenced-disks 1 >/dev/null 2>&1 || true; "
            f"rm -rf /var/lib/lxc/{ctid} >/dev/null 2>&1 || true; "
            f"pct restore {ctid} {shlex.quote(latest_path)} --rootfs local-zfs:{rootfs_size_gb}; "
            f"pct set {ctid} -hostname {worker_name} -onboot 1 -swap {swap_mb} "
            f"-net0 name=eth0,bridge=vmbr1,hwaddr={hwaddr},ip=dhcp,ip6=auto,mtu=9000,type=veth "
            f"-dev0 /dev/dri/renderD128,gid=992 -dev1 /dev/dri/card0,gid=44; "
            f"pct start {ctid}"
        )
        try:
            restore = run_proxmox_ssh(node, restore_cmd, timeout=1800)
        except subprocess.TimeoutExpired:
            append_processing_log(f"[worker:{worker_name}] Init-Restore Timeout")
            return
        except Exception as err:
            append_processing_log(f"[worker:{worker_name}] Init-Restore Ausnahme: {err}")
            return
        if restore.returncode != 0:
            detail = (restore.stderr or restore.stdout or "").strip().splitlines()
            reason = detail[-1] if detail else "unbekannt"
            append_processing_log(f"[worker:{worker_name}] Init-Restore fehlgeschlagen: {reason}")
            return
        if not wait_for_ct_running(node, ctid, timeout=90):
            append_processing_log(f"[worker:{worker_name}] Init-Start fehlgeschlagen")
            return
        profile_ok, profile_message = apply_worker_profile(spec)
        if not profile_ok:
            append_processing_log(f"[worker:{worker_name}] Init-Profil fehlgeschlagen: {profile_message}")
            return
        mount_ok, mount_message = ensure_worker_mount_ready(spec, timeout=90)
        if not mount_ok:
            append_processing_log(f"[worker:{worker_name}] Init-Mount fehlgeschlagen: {mount_message}")
            return
        write_worker_enabled_state(worker_name, True)
        append_processing_log(f"[worker:{worker_name}] Init abgeschlossen")
        invalidate_worker_state_cache()
    finally:
        set_worker_busy_action(worker_name, None)
        set_worker_reinit_running(worker_name, False)


def run_worker_action(worker_name: str, action: str) -> tuple[bool, str]:
    name = str(worker_name or "").strip()
    verb = str(action or "").strip().lower()
    spec = read_worker_spec_map().get(name)
    if not spec:
        return False, f"Worker {name} ist nicht konfiguriert."

    host = str(spec.get("host", "") or "").strip() or name
    node = str(spec.get("node", "") or "").strip()
    ctid = str(spec.get("ctid", "") or "").strip()

    if verb == "init":
        if worker_reinit_runtime_states.get(name, False):
            invalidate_worker_state_cache()
            return True, f"{name} Init läuft bereits."
        threading.Thread(target=_run_worker_reinit, args=(spec,), daemon=True, name=f"worker-init-{name}").start()
        invalidate_worker_state_cache()
        return True, f"{name} Init gestartet."

    if verb == "kill":
        set_worker_busy_action(name, "kill")
        try:
            write_worker_enabled_state(name, False)
            requeued = requeue_dispatch_rows_for_worker(name)
            try:
                cleanup_remote_worker_runtime(spec)
                append_processing_log(f"[worker:{name}] Kill: Encode abgebrochen und Worker bereinigt")
            except Exception as err:
                append_processing_log(f"[worker:{name}] Kill-Cleanup fehlgeschlagen: {err}")
            try:
                destroy_worker_container(spec)
                append_processing_log(f"[worker:{name}] Kill: CT gelöscht")
            except Exception as err:
                append_processing_log(f"[worker:{name}] Kill fehlgeschlagen: {err}")
                invalidate_worker_state_cache()
                return False, f"{name} Kill fehlgeschlagen."
            invalidate_worker_state_cache()
            if requeued > 0:
                append_processing_log(f"[worker:{name}] {requeued} Job(s) an Master-Queue zurückgegeben")
            return True, f"{name} gelöscht. {requeued} Job(s) neu eingeplant."
        finally:
            set_worker_busy_action(name, None)

    if verb == "pause":
        write_worker_enabled_state(name, False)
        invalidate_worker_state_cache()
        append_processing_log(f"[worker:{name}] pausiert")
        return True, f"{name} pausiert."

    return False, f"Unbekannte Worker-Aktion: {verb}"


def cleanup_remote_worker_runtime(spec: dict[str, str]) -> None:
    host = str(spec.get("host", "") or "").strip() or str(spec.get("name", "") or "").strip()
    if not host:
        return
    remote_cmd = (
        "pkill -TERM -f '[m]anagemovie.py --ffmpeg' >/dev/null 2>&1 || true; "
        "pkill -TERM -x ffmpeg >/dev/null 2>&1 || true; "
        "sleep 2; "
        "pkill -KILL -f '[m]anagemovie.py --ffmpeg' >/dev/null 2>&1 || true; "
        "pkill -KILL -x ffmpeg >/dev/null 2>&1 || true; "
        "rm -f /opt/managemovie/MovieManager/work/web-confirm.json >/dev/null 2>&1 || true; "
        "find /opt/managemovie/MovieManager/temp -mindepth 1 -maxdepth 1 -exec rm -rf {} + >/dev/null 2>&1 || true; "
        "find /opt/managemovie/.managemovie-tmp -mindepth 1 -maxdepth 1 -exec rm -rf {} + >/dev/null 2>&1 || true"
    )
    run_worker_ssh(host, remote_cmd, timeout=25)


def destroy_worker_container(spec: dict[str, str]) -> None:
    node = str(spec.get("node", "") or "").strip()
    ctid = str(spec.get("ctid", "") or "").strip()
    if not node or not ctid:
        return
    run_proxmox_ssh(
        node,
        f"pct shutdown {ctid} --forceStop 1 --timeout 20 >/dev/null 2>&1 || "
        f"pct stop {ctid} --skiplock 1 >/dev/null 2>&1 || true; "
        f"pct unlock {ctid} >/dev/null 2>&1 || true; "
        f"pct destroy {ctid} --destroy-unreferenced-disks 1 >/dev/null 2>&1 || true",
        timeout=180,
    )


def collect_abort_worker_specs(dispatch_items: list[dict[str, Any]], current: JobState | None) -> list[dict[str, str]]:
    spec_map = read_worker_spec_map()
    names: list[str] = []
    for item in dispatch_items:
        worker_name = str(item.get("worker_name", "") or "").strip()
        if worker_name:
            names.append(worker_name)
    if current and current.mode == "ffmpeg":
        worker_name = str(current.worker_name or "").strip()
        if worker_name:
            names.append(worker_name)
    try:
        for worker in read_worker_states(force=True):
            if str(worker.get("state", "") or "").strip() in {"encoding", "paused"}:
                worker_name = str(worker.get("name", "") or "").strip()
                if worker_name:
                    names.append(worker_name)
    except Exception:
        pass
    ordered: list[dict[str, str]] = []
    seen: set[str] = set()
    for name in names:
        if not name or name in seen:
            continue
        spec = spec_map.get(name)
        if not spec:
            continue
        seen.add(name)
        ordered.append(spec)
    return ordered


def abort_remote_worker_group(worker_specs: list[dict[str, str]]) -> None:
    for spec in worker_specs:
        worker_name = str(spec.get("name", "") or "").strip() or "worker"
        try:
            cleanup_remote_worker_runtime(spec)
            append_processing_log(f"[worker:{worker_name}] Remote-Encode gestoppt und bereinigt")
        except Exception as err:
            append_processing_log(f"[worker:{worker_name}] Cleanup fehlgeschlagen: {err}")
    invalidate_worker_state_cache()


def requeue_dispatch_rows_for_worker(worker_name: str) -> int:
    normalized = str(worker_name or "").strip()
    if not normalized:
        return 0
    requeue_rows: list[dict[str, Any]] = []
    with dispatch_lock:
        global dispatch_pending_rows
        for item in dispatch_subjobs.values():
            if not item.get("running"):
                continue
            if str(item.get("worker_name", "") or "").strip() != normalized:
                continue
            source_name = str(item.get("source_name", "") or "").strip()
            row_payload = item.get("row")
            if isinstance(row_payload, dict):
                restored_row = dict(row_payload)
                if source_name and not str(restored_row.get("source_name", "") or "").strip():
                    restored_row["source_name"] = source_name
                requeue_rows.append(restored_row)
            elif source_name:
                requeue_rows.append({"source_name": source_name})
            item["running"] = False
            item["ended_at"] = time.time()
            if item.get("exit_code") is None:
                item["exit_code"] = 137
        if requeue_rows:
            existing = {normalize_simple_text(item.get("source_name", "")).lower() for item in dispatch_pending_rows}
            merged = [row for row in requeue_rows if normalize_simple_text(row.get("source_name", "")).lower() not in existing]
            if merged:
                dispatch_pending_rows = merged + list(dispatch_pending_rows)
    return len(requeue_rows)


def build_remote_ffmpeg_exec(folder: str, env: dict[str, str], worker: dict[str, Any]) -> tuple[str, list[str]]:
    remote_host = str(worker.get("host", "") or "").strip()
    if not remote_host:
        raise ValueError("FFmpeg-Worker-Host ist nicht konfiguriert.")

    remote_python = (os.environ.get("MANAGEMOVIE_FFMPEG_REMOTE_PYTHON", "") or "").strip()
    if not remote_python:
        remote_python = "/opt/managemovie/.venv/bin/python"

    remote_core_script = (os.environ.get("MANAGEMOVIE_FFMPEG_REMOTE_CORE_SCRIPT", "") or "").strip()
    if not remote_core_script:
        remote_core_script = "/opt/managemovie/managemovie-web/app/managemovie.py"

    remote_folder = map_folder_for_ffmpeg_worker(folder, worker)
    effective_env = dict(env)
    requested_encoder = normalize_encoder_mode(effective_env.get("MANAGEMOVIE_AUTOSTART_ENCODER", ""))
    worker_default_encoder = normalize_encoder_mode(str(worker.get("default_encoder", "") or "").strip()) or "intel_qsv"
    effective_env["MANAGEMOVIE_AUTOSTART_ENCODER"] = worker_default_encoder if requested_encoder in {"", "cpu"} else requested_encoder
    env_keys = [
        "PATH",
        "HOME",
        "PYTHONUNBUFFERED",
        "MANAGEMOVIE_FFMPEG_BIN",
        "MANAGEMOVIE_FFPROBE_BIN",
        "MANAGEMOVIE_WORKDIR",
        "TMPDIR",
        "TMP",
        "TEMP",
        "MANAGEMOVIE_WEB_UI_ONLY",
        "MANAGEMOVIE_TERMINAL_UI",
        "MANAGEMOVIE_AUTOSTART",
        "MANAGEMOVIE_AUTOSTART_ENCODER",
        "MANAGEMOVIE_WEB_CONFIRM_FILE",
        "MANAGEMOVIE_NO_PAUSE",
        "MANAGEMOVIE_EXIT_PAUSE_SEC",
        "MANAGEMOVIE_LOG_TO_STDOUT",
        "MANAGEMOVIE_TARGET_NFS_PATH",
        "MANAGEMOVIE_TARGET_OUT_PATH",
        "MANAGEMOVIE_TARGET_REENQUEUE_PATH",
        "MANAGEMOVIE_NAS_IP",
        "MANAGEMOVIE_PLEX_IP",
        "MANAGEMOVIE_SYNC_NAS",
        "MANAGEMOVIE_SYNC_PLEX",
        "MANAGEMOVIE_DEL_OUT",
        "MANAGEMOVIE_DEL_SOURCE",
        "MANAGEMOVIE_DEL_SOURCE_CONFIRMED",
        "MANAGEMOVIE_DISABLE_AI_QUERY",
        "MANAGEMOVIE_SKIP_H265_ENCODE",
        "MANAGEMOVIE_SKIP_4K_H265_ENCODE",
        "MANAGEMOVIE_ENCODE_ONCE_4K",
        "MANAGEMOVIE_PARALLEL_JOB_LIMIT",
        "MANAGEMOVIE_SOURCE_FILTER",
        "MANAGEMOVIE_PRECHECK_EGB",
        "MANAGEMOVIE_SPEED_FALLBACK_COPY",
        "MANAGEMOVIE_SKIP_SELF_TERMINATE",
        "MANAGEMOVIE_PLEX_API",
        "MANAGEMOVIE_TMDB_KEY",
        "MANAGEMOVIE_GEMINI_KEY",
        "MANAGEMOVIE_SKIP_DB_INIT",
        "MANAGEMOVIE_SKIP_OUT_CLEAN",
    ]
    remote_env = [f"{key}={effective_env[key]}" for key in env_keys if effective_env.get(key)]
    remote_env.append("MANAGEMOVIE_SKIP_DB_INIT=1")
    remote_db_host = master_db_host_for_workers()
    if remote_db_host:
        remote_env.append(f"MANAGEMOVIE_DB_HOST={remote_db_host}")
        remote_env.append(f"MANAGEMOVIE_DB_PORT={MARIADB_PORT}")
        remote_env.append(f"MANAGEMOVIE_DB_NAME={MARIADB_DB}")
        remote_env.append(f"MANAGEMOVIE_DB_USER={MARIADB_USER}")
        remote_env.append(f"MANAGEMOVIE_DB_PASSWORD={MARIADB_PASSWORD}")
        remote_env.append(f"MANAGEMOVIE_DB_RETENTION_DAYS={MARIADB_RETENTION_DAYS}")
    remote_cmd = [
        "env",
        *remote_env,
        remote_python,
        remote_core_script,
        "--ffmpeg",
        "--folder",
        remote_folder,
    ]
    return remote_host, remote_cmd


def build_remote_ffmpeg_command(folder: str, env: dict[str, str], worker: dict[str, Any]) -> list[str]:
    remote_host, remote_cmd = build_remote_ffmpeg_exec(folder, env, worker)
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=10",
        remote_host,
        shlex.join(remote_cmd),
    ]


def normalize_encoder_mode(raw_value: str | None) -> str:
    raw = (raw_value or "").strip().lower()
    hardware_default = "apple" if sys.platform == "darwin" else "intel_qsv"
    aliases = {
        "software": "cpu",
        "sw": "cpu",
        "x265": "cpu",
        "libx265": "cpu",
        "intel": "intel_qsv",
        "qsv": "intel_qsv",
        "quicksync": "intel_qsv",
        "quick sync": "intel_qsv",
        "hevc_qsv": "intel_qsv",
        "apple": "apple",
        "videotoolbox": "apple",
        "vt": "apple",
        "hevc_videotoolbox": "apple",
        "hardware": hardware_default,
        "hw": hardware_default,
        "gpu": hardware_default,
    }
    mapped = aliases.get(raw, raw)
    if mapped in {"cpu", "intel_qsv", "apple"}:
        return mapped
    return ""


def available_encoder_options() -> list[tuple[str, str]]:
    if sys.platform == "darwin":
        return [
            ("cpu", "Software"),
            ("apple", "Apple"),
        ]
    return [
        ("cpu", "Software"),
        ("intel_qsv", "Intel QuickSync"),
    ]


def coerce_encoder_for_ui(raw_value: str | None) -> str:
    normalized = normalize_encoder_mode(raw_value) or "cpu"
    allowed = {value for value, _label in available_encoder_options()}
    if normalized in allowed:
        return normalized
    return "cpu" if "cpu" in allowed else next(iter(allowed), "cpu")


def build_env(
    mode: str,
    encoder: str,
    *,
    sync_nas: bool = False,
    sync_plex: bool = False,
    del_out: bool = False,
    del_source: bool = False,
    del_source_confirmed: bool = False,
) -> dict[str, str]:
    env = os.environ.copy()
    runtime_settings = read_runtime_settings()

    path_parts: list[str] = [str(BIN_DIR)]
    for candidate in ("/opt/homebrew/bin", "/usr/local/bin", "/usr/bin", "/bin", "/usr/sbin", "/sbin"):
        if candidate not in path_parts:
            path_parts.append(candidate)
    for part in (env.get("PATH", "") or "").split(":"):
        p = part.strip()
        if p and p not in path_parts:
            path_parts.append(p)
    env["PATH"] = ":".join(path_parts)

    ffmpeg_bin = shutil.which("ffmpeg", path=env["PATH"])
    ffprobe_bin = shutil.which("ffprobe", path=env["PATH"])
    if ffmpeg_bin:
        env["MANAGEMOVIE_FFMPEG_BIN"] = ffmpeg_bin
    if ffprobe_bin:
        env["MANAGEMOVIE_FFPROBE_BIN"] = ffprobe_bin

    env["MANAGEMOVIE_WORKDIR"] = str(WORK_DIR)
    env["TMPDIR"] = str(TEMP_DIR)
    env["TMP"] = str(TEMP_DIR)
    env["TEMP"] = str(TEMP_DIR)
    env["MANAGEMOVIE_WEB_UI_ONLY"] = "1"
    env["MANAGEMOVIE_TERMINAL_UI"] = "0"
    env["MANAGEMOVIE_AUTOSTART"] = "1"
    env["MANAGEMOVIE_AUTOSTART_ENCODER"] = encoder or "cpu"
    # Web-initiated copy/encode must always go through browser confirmation.
    # Ignore inherited MANAGEMOVIE_SKIP_CONFIRM from service environments.
    env["MANAGEMOVIE_WEB_CONFIRM_FILE"] = str(CONFIRM_FILE)
    env["MANAGEMOVIE_NO_PAUSE"] = "1"
    env["MANAGEMOVIE_EXIT_PAUSE_SEC"] = "0"
    env["MANAGEMOVIE_LOG_TO_STDOUT"] = "1"
    env["PYTHONUNBUFFERED"] = "1"

    env["MANAGEMOVIE_TARGET_NFS_PATH"] = runtime_settings.get("target_nfs_path", DEFAULT_TARGET_NFS_PATH)
    env["MANAGEMOVIE_TARGET_OUT_PATH"] = runtime_settings.get("target_out_path", DEFAULT_TARGET_OUT_PATH)
    env["MANAGEMOVIE_TARGET_REENQUEUE_PATH"] = runtime_settings.get(
        "target_reenqueue_path",
        default_target_reenqueue_path_for_out(runtime_settings.get("target_out_path", DEFAULT_TARGET_OUT_PATH)),
    )
    env["MANAGEMOVIE_NAS_IP"] = runtime_settings.get("nas_ip", DEFAULT_NAS_IP)
    env["MANAGEMOVIE_PLEX_IP"] = runtime_settings.get("plex_ip", DEFAULT_PLEX_IP)
    env["MANAGEMOVIE_SYNC_NAS"] = "1" if sync_nas else "0"
    env["MANAGEMOVIE_SYNC_PLEX"] = "1" if sync_plex else "0"
    env["MANAGEMOVIE_DEL_OUT"] = "1" if del_out else "0"
    env["MANAGEMOVIE_DEL_SOURCE"] = "1" if del_source else "0"
    env["MANAGEMOVIE_DEL_SOURCE_CONFIRMED"] = "1" if del_source_confirmed else "0"

    plex_api = runtime_settings.get("plex_api", "")
    tmdb_api = runtime_settings.get("tmdb_api", "")
    gemini_api = runtime_settings.get("gemini_api", "")
    ai_query_disabled = parse_form_bool(runtime_settings.get("ai_query_disabled", "1"))
    skip_4k_h265_encode = parse_form_bool(runtime_settings.get("skip_4k_h265_encode", "0"))
    precheck_egb = parse_form_bool(runtime_settings.get("precheck_egb", "1"))
    speed_fallback_copy = parse_form_bool(runtime_settings.get("speed_fallback_copy", "1"))
    env["MANAGEMOVIE_DISABLE_AI_QUERY"] = "1" if ai_query_disabled else "0"
    env["MANAGEMOVIE_SKIP_H265_ENCODE"] = "0"
    env["MANAGEMOVIE_SKIP_4K_H265_ENCODE"] = "1" if skip_4k_h265_encode else "0"
    env["MANAGEMOVIE_ENCODE_ONCE_4K"] = "1" if parse_form_bool(runtime_settings.get("encode_once_4k", "1")) else "0"
    env["MANAGEMOVIE_PARALLEL_JOB_LIMIT"] = str(runtime_settings.get("parallel_job_limit", "2") or "2")
    env["MANAGEMOVIE_PRECHECK_EGB"] = "1" if precheck_egb else "0"
    env["MANAGEMOVIE_SPEED_FALLBACK_COPY"] = "1" if speed_fallback_copy else "0"
    history_off_folder = read_history_off_start_folder()
    if history_off_folder:
        env["MANAGEMOVIE_HISTORY_OFF_START_FOLDER"] = history_off_folder
    if plex_api:
        env["MANAGEMOVIE_PLEX_API"] = plex_api
    if tmdb_api:
        env["MANAGEMOVIE_TMDB_KEY"] = tmdb_api
    if gemini_api:
        env["MANAGEMOVIE_GEMINI_KEY"] = gemini_api

    return env


def job_to_dict(job: JobState | None) -> dict:
    if not job:
        return {
            "exists": False,
            "running": False,
        }
    return {
        "exists": True,
        "job_id": job.job_id,
        "mode": job.mode,
        "folder": job.folder,
        "encoder": job.encoder,
        "sync_nas": bool(job.sync_nas),
        "sync_plex": bool(job.sync_plex),
        "del_out": bool(job.del_out),
        "del_source": bool(job.del_source),
        "started_at": job.started_at,
        "ended_at": job.ended_at,
        "running": job.running,
        "exit_code": job.exit_code,
        "log_path": str(job.log_path),
        "worker_name": job.worker_name,
        "release_version": job.release_version,
    }


def active_dispatch_subjob_count() -> int:
    with dispatch_lock:
        return sum(1 for item in dispatch_subjobs.values() if item.get("running"))


def dispatch_job_data() -> dict | None:
    with dispatch_lock:
        job = dispatch_master_job
        if not job:
            return None
        payload = job_to_dict(job)
        running_items = [item for item in dispatch_subjobs.values() if item.get("running")]
        worker_names = sorted({str(item.get("worker_name", "") or "").strip() for item in running_items if str(item.get("worker_name", "") or "").strip()})
        if worker_names:
            payload["worker_name"] = ", ".join(worker_names)
        if running_items:
            payload["running"] = True
            payload["ended_at"] = None
        return payload


def spawn_monitor(job_id: str, process: subprocess.Popen, log_handle) -> None:
    def monitor() -> None:
        rc = process.wait()
        try:
            log_handle.flush()
        except Exception:
            pass
        try:
            log_handle.close()
        except Exception:
            pass

        worker_name = ""
        mode = "-"
        folder = "-"
        log_path: Path | None = None
        with job_lock:
            global current_job
            if current_job and current_job.job_id == job_id:
                worker_name = str(current_job.worker_name or "").strip()
                mode = str(current_job.mode or "-").strip() or "-"
                folder = str(current_job.folder or "-").strip() or "-"
                log_path = current_job.log_path
                current_job.running = False
                current_job.exit_code = rc
                current_job.ended_at = time.time()

        folder_name = Path(folder).name if folder not in {"", "-"} else folder
        worker_label = worker_name or "lokal"
        if rc == 0:
            append_processing_log(f"Job OK: mode={mode} worker={worker_label} folder={folder_name}")
        else:
            append_processing_log(
                f"Job FEHLER: mode={mode} worker={worker_label} folder={folder_name} | {runner_error_summary(log_path, rc)}"
            )

    thread = threading.Thread(target=monitor, daemon=True, name=f"monitor-{job_id}")
    thread.start()


def build_remote_ffmpeg_subjob_command(
    folder: str,
    env: dict[str, str],
    worker: dict[str, Any],
    source_filter: str,
) -> list[str]:
    sub_env = dict(env)
    sub_env["MANAGEMOVIE_SOURCE_FILTER"] = str(source_filter or "").strip()
    sub_env["MANAGEMOVIE_SKIP_OUT_CLEAN"] = "1"
    sub_env["MANAGEMOVIE_SKIP_SELF_TERMINATE"] = "1"
    sub_env["MANAGEMOVIE_FFMPEG_LOG_INTERVAL_SEC"] = "5"
    sub_env.pop("MANAGEMOVIE_WEB_CONFIRM_FILE", None)
    return build_remote_ffmpeg_command(folder, sub_env, worker)


def build_remote_ffmpeg_subjob_launcher(
    folder: str,
    env: dict[str, str],
    worker: dict[str, Any],
    source_filter: str,
    job_id: str,
) -> tuple[list[str], str, str, str, str]:
    sub_env = dict(env)
    sub_env["MANAGEMOVIE_SOURCE_FILTER"] = str(source_filter or "").strip()
    sub_env["MANAGEMOVIE_SKIP_OUT_CLEAN"] = "1"
    sub_env["MANAGEMOVIE_SKIP_SELF_TERMINATE"] = "1"
    sub_env["MANAGEMOVIE_FFMPEG_LOG_INTERVAL_SEC"] = "5"
    sub_env.pop("MANAGEMOVIE_WEB_CONFIRM_FILE", None)
    remote_host, remote_cmd = build_remote_ffmpeg_exec(folder, sub_env, worker)
    remote_status_dir = "/opt/managemovie/MovieManager/work/dispatch-status"
    remote_log_path = f"/opt/managemovie/MovieManager/logs/dispatch-{job_id}.log"
    remote_rc_path = f"{remote_status_dir}/{job_id}.rc"
    remote_pid_path = f"{remote_status_dir}/{job_id}.pid"
    inner_cmd = (
        f"{shlex.join(remote_cmd)}; "
        f"rc=$?; "
        f"printf '%s\\n' \"$rc\" > {shlex.quote(remote_rc_path)}"
    )
    launch_body = (
        f"mkdir -p {shlex.quote(remote_status_dir)} {shlex.quote(str(Path(remote_log_path).parent))}; "
        f"rm -f {shlex.quote(remote_rc_path)} {shlex.quote(remote_pid_path)}; "
        f"nohup sh -lc {shlex.quote(inner_cmd)} > {shlex.quote(remote_log_path)} 2>&1 < /dev/null & "
        "pid=$!; "
        f"printf '%s\\n' \"$pid\" > {shlex.quote(remote_pid_path)}; "
        f"printf 'PID:%s\\nLOG:%s\\nRC:%s\\n' \"$pid\" {shlex.quote(remote_log_path)} {shlex.quote(remote_rc_path)}"
    )
    return (
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=accept-new",
            "-o",
            "ConnectTimeout=10",
            remote_host,
            shlex.join(["sh", "-lc", launch_body]),
        ],
        remote_host,
        remote_log_path,
        remote_rc_path,
        remote_pid_path,
    )


def append_remote_dispatch_log_snapshot(item: dict[str, Any]) -> None:
    worker_host = str(item.get("worker_host", "") or "").strip()
    remote_log_path = str(item.get("remote_log_path", "") or "").strip()
    local_log_path = Path(str(item.get("log_path", "") or "").strip() or ".")
    if not worker_host or not remote_log_path:
        return
    result = run_worker_ssh(worker_host, f"tail -n 120 {shlex.quote(remote_log_path)} 2>/dev/null || true", timeout=8)
    snapshot = (result.stdout or "").strip()
    if not snapshot:
        return
    try:
        with local_log_path.open("a", encoding="utf-8") as handle:
            handle.write("\n[remote-log]\n")
            handle.write(snapshot)
            handle.write("\n")
    except Exception:
        pass


def read_remote_dispatch_log_lines(worker_host: str, remote_log_path: str, max_lines: int = 160) -> list[str]:
    host = str(worker_host or "").strip()
    path = str(remote_log_path or "").strip()
    if not host or not path:
        return []
    result = run_worker_ssh(
        host,
        f"tail -n {max(20, min(max_lines, 400))} {shlex.quote(path)} 2>/dev/null || true",
        timeout=6,
    )
    return [str(line or "").rstrip() for line in (result.stdout or "").splitlines() if str(line or "").strip()]


def diff_remote_dispatch_log_lines(previous_lines: list[str], current_lines: list[str]) -> list[str]:
    prev = list(previous_lines or [])
    curr = list(current_lines or [])
    if not curr:
        return []
    if not prev:
        return curr
    max_overlap = min(len(prev), len(curr))
    for overlap in range(max_overlap, 0, -1):
        if prev[-overlap:] == curr[:overlap]:
            return curr[overlap:]
    if curr == prev:
        return []
    return curr


def choose_ffmpeg_worker_for_dispatch() -> dict[str, Any] | None:
    global dispatch_round_robin_cursor_name
    live_workers = read_worker_states(force=True)
    live_map = {
        str(worker.get("name", "") or "").strip(): dict(worker)
        for worker in live_workers
        if str(worker.get("name", "") or "").strip()
    }
    enabled_states = read_worker_enabled_states()
    with dispatch_lock:
        pool_workers = [dict(worker) for worker in dispatch_worker_pool]
    using_pool = bool(pool_workers)
    workers = pool_workers or live_workers
    max_active = per_worker_parallel_limit()
    running_counts: dict[str, int] = {}
    with dispatch_lock:
        for item in dispatch_subjobs.values():
            if not item.get("running"):
                continue
            name = str(item.get("worker_name", "") or "").strip()
            if not name:
                continue
            running_counts[name] = int(running_counts.get(name, 0) or 0) + 1
        last_cursor = str(dispatch_round_robin_cursor_name or "").strip()
    eligible: list[dict[str, Any]] = []
    for worker in workers:
        name = str(worker.get("name", "") or "").strip()
        if not name:
            continue
        live_worker = live_map.get(name)
        effective_worker = dict(worker)
        if live_worker:
            effective_worker.update(live_worker)
        if worker_reinit_runtime_states.get(name, False):
            continue
        if not enabled_states.get(name, True):
            continue
        if not using_pool and live_worker and not live_worker.get("connected"):
            continue
        if str(effective_worker.get("state", "") or "").strip() == "paused":
            continue
        effective_count = max(
            int((live_worker or effective_worker).get("active_count", 0) or 0) if (live_worker and live_worker.get("connected")) else int(running_counts.get(name, 0) or 0),
            int(running_counts.get(name, 0) or 0),
        )
        if effective_count >= max_active:
            continue
        eligible.append(effective_worker)
    if not eligible:
        return None
    ordered_names = [str(worker.get("name", "") or "").strip() for worker in eligible]
    start_index = 0
    if last_cursor and last_cursor in ordered_names:
        start_index = (ordered_names.index(last_cursor) + 1) % len(eligible)
    chosen = eligible[start_index]
    with dispatch_lock:
        dispatch_round_robin_cursor_name = str(chosen.get("name", "") or "").strip()
    return chosen


def warm_dispatch_worker_pool(timeout_seconds: float = 12.0) -> list[dict[str, Any]]:
    specs = read_worker_specs()
    enabled_states = read_worker_enabled_states()
    expected_names = [
        str(spec.get("name", "") or "").strip()
        for spec in specs
        if str(spec.get("name", "") or "").strip() and enabled_states.get(str(spec.get("name", "") or "").strip(), True)
    ]
    if not expected_names:
        return read_worker_states(force=True)
    deadline = time.time() + max(1.0, float(timeout_seconds or 0.0))
    last_workers: list[dict[str, Any]] = []
    while True:
        workers = read_worker_states(force=True)
        last_workers = [dict(worker) for worker in workers]
        ready_names = {
            str(worker.get("name", "") or "").strip()
            for worker in workers
            if str(worker.get("name", "") or "").strip()
            and bool(worker.get("connected"))
            and bool(worker.get("enabled"))
            and not bool(worker.get("reinit_running"))
            and str(worker.get("state", "") or "").strip() != "paused"
        }
        if all(name in ready_names for name in expected_names):
            return last_workers
        if time.time() >= deadline:
            return last_workers
        time.sleep(1.0)


def resolve_target_out_root_for_dispatch(folder_path: Path) -> Path:
    runtime_settings = read_runtime_settings()
    out_value = normalize_target_out_path(runtime_settings.get("target_out_path", DEFAULT_TARGET_OUT_PATH))
    out_root = Path(out_value).expanduser()
    if not out_root.is_absolute():
        out_root = folder_path / out_root
    try:
        return out_root.resolve()
    except Exception:
        return out_root


def clean_target_out_root_for_dispatch(folder_path: Path) -> tuple[bool, str]:
    out_root = resolve_target_out_root_for_dispatch(folder_path)
    try:
        out_root.mkdir(parents=True, exist_ok=True)
        for child in list(out_root.iterdir()):
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
        return True, str(out_root)
    except Exception as exc:
        return False, f"{out_root}: {exc}"


def launch_remote_ffmpeg_subjob(
    *,
    folder_path: Path,
    source_name: str,
    encoder: str,
    sync_nas: bool,
    sync_plex: bool,
    del_out: bool,
    del_source: bool,
    release_version: str,
    worker: dict[str, Any],
    row: dict[str, Any] | None = None,
) -> dict[str, Any]:
    env = build_env(
        "ffmpeg",
        encoder,
        sync_nas=sync_nas,
        sync_plex=sync_plex,
        del_out=del_out,
        del_source=del_source,
        del_source_confirmed=False,
    )
    worker_name = str(worker.get("name", "") or "").strip()
    job_id = time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8]
    log_path = LOG_DIR / f"managemovie-{job_id}.log"
    remote_folder = map_folder_for_ffmpeg_worker(str(folder_path), worker)
    remote_target_path = ""
    raw_target_name = str((row or {}).get("target_name", "") or "").strip()
    if raw_target_name:
        target_path = Path(raw_target_name)
        remote_target_path = str((Path(remote_folder) / target_path).as_posix()) if not target_path.is_absolute() else str(target_path)
    cmd, worker_host, remote_log_path, remote_rc_path, remote_pid_path = build_remote_ffmpeg_subjob_launcher(
        str(folder_path), env, worker, source_name, job_id
    )
    log_handle = log_path.open("a", encoding="utf-8")
    log_handle.write(f"$ {' '.join(cmd)}\n")
    log_handle.write(f"[info] started={time.strftime('%Y-%m-%d %H:%M:%S')} mode=ffmpeg folder={folder_path}\n")
    log_handle.write(f"[worker] ffmpeg-remote host={worker_name} folder={remote_folder} source={source_name}\n")
    log_handle.write(f"[version] release={release_version} range={format_release_version(VERSION_MIN_PATCH)}..{format_release_version(VERSION_MAX_PATCH)}\n")
    log_handle.flush()
    launch_result = subprocess.run(
        cmd,
        cwd=str(BASE_DIR / "app"),
        capture_output=True,
        text=True,
        env=env,
        timeout=20,
        check=False,
    )
    if launch_result.stdout:
        log_handle.write(launch_result.stdout)
    if launch_result.stderr:
        log_handle.write(launch_result.stderr)
    log_handle.flush()
    if launch_result.returncode != 0:
        log_handle.close()
        raise RuntimeError(f"Remote-Launcher fehlgeschlagen ({launch_result.returncode})")
    remote_pid = ""
    for line in (launch_result.stdout or "").splitlines():
        text = str(line or "").strip()
        if text.startswith("PID:"):
            remote_pid = text.split(":", 1)[1].strip()
            break
    if not remote_pid:
        log_handle.close()
        raise RuntimeError("Remote-Launcher lieferte keine PID")
    log_handle.close()
    subjob = {
        "job_id": job_id,
        "source_name": source_name,
        "target_name": str((row or {}).get("target_name", "") or "").strip(),
        "remote_target_path": remote_target_path,
        "worker_name": worker_name,
        "worker_host": worker_host,
        "remote_pid": remote_pid,
        "remote_log_path": remote_log_path,
        "remote_rc_path": remote_rc_path,
        "remote_pid_path": remote_pid_path,
        "log_path": str(log_path),
        "started_at": time.time(),
        "running": True,
        "exit_code": None,
        "row": dict(row or {}),
    }
    with dispatch_lock:
        dispatch_subjobs[job_id] = subjob
    append_processing_log(f"[dispatch] {Path(source_name).name} -> {worker_name} gestartet")
    return subjob


def spawn_dispatch_subjob_monitor(job_id: str) -> None:
    def monitor() -> None:
        with dispatch_lock:
            item = dispatch_subjobs.get(job_id)
        if not item:
            return
        worker_host = str(item.get("worker_host", "") or "").strip()
        remote_pid = str(item.get("remote_pid", "") or "").strip()
        remote_rc_path = str(item.get("remote_rc_path", "") or "").strip()
        remote_log_path = str(item.get("remote_log_path", "") or "").strip()
        worker_name = str(item.get("worker_name", "") or "").strip() or "-"
        rc: int | None = None
        launch_grace_until = time.time() + 30.0
        last_forwarded_lines: list[str] = []
        last_status_signature = ""
        source_name = Path(str(item.get("source_name", "") or "")).name
        while True:
            if not worker_host or not remote_pid or not remote_rc_path:
                rc = 255
                break
            current_lines = read_remote_dispatch_log_lines(worker_host, remote_log_path, max_lines=160)
            new_lines = diff_remote_dispatch_log_lines(last_forwarded_lines, current_lines)
            for line in new_lines:
                append_processing_log_with_host(worker_name, line, "INFO")
            if current_lines:
                last_forwarded_lines = current_lines
            _, snapshot = _read_worker_status_metrics(item)
            if snapshot:
                with dispatch_lock:
                    current = dispatch_subjobs.get(job_id)
                    if current is not None:
                        current["last_live_metrics"] = {
                            "speed": str(snapshot.get("speed", "") or "").strip(),
                            "fps": str(snapshot.get("fps", "") or "").strip(),
                            "z_gb": str(snapshot.get("z_gb", "") or "").strip(),
                            "e_gb": str(snapshot.get("e_gb", "") or "").strip(),
                            "eta": str(snapshot.get("eta", "") or "").strip(),
                            "lzeit": str(snapshot.get("lzeit", "") or "").strip(),
                        }
                signature = "|".join(
                    [
                        str(snapshot.get("target", "") or "").strip(),
                        str(snapshot.get("z_gb", "") or "").strip(),
                        str(snapshot.get("speed", "") or "").strip(),
                        str(snapshot.get("fps", "") or "").strip(),
                        str(snapshot.get("eta", "") or "").strip(),
                    ]
                )
                if signature and signature != last_status_signature:
                    last_status_signature = signature
                    parts = [
                        f"[FFMPEG] {source_name}",
                    ]
                    if snapshot.get("z_gb"):
                        parts.append(f"Z-GB: {snapshot['z_gb']}")
                    if snapshot.get("speed"):
                        parts.append(f"Speed: {snapshot['speed']}")
                    if snapshot.get("fps"):
                        parts.append(f"FPS: {snapshot['fps']}")
                    if snapshot.get("eta"):
                        parts.append(f"ETA: {snapshot['eta']}")
                    append_processing_log_with_host(worker_name, " | ".join(parts), "INFO")
            probe_cmd = (
                f"if [ -f {shlex.quote(remote_rc_path)} ]; then "
                f"printf 'RC:%s\\n' \"$(cat {shlex.quote(remote_rc_path)} 2>/dev/null)\"; "
                f"elif kill -0 {shlex.quote(remote_pid)} 2>/dev/null; then "
                "echo RUNNING; "
                "else echo UNKNOWN; fi"
            )
            probe = run_worker_ssh(worker_host, probe_cmd, timeout=6)
            output = (probe.stdout or "").strip()
            if probe.returncode == 0 and output.startswith("RC:"):
                try:
                    rc = int(output.split(":", 1)[1].strip())
                except Exception:
                    rc = 1
                break
            if probe.returncode == 0 and "RUNNING" in output:
                time.sleep(3.0)
                continue
            if time.time() < launch_grace_until:
                time.sleep(2.0)
                continue
            rc = 255
            break
        append_remote_dispatch_log_snapshot(item)
        with dispatch_lock:
            current = dispatch_subjobs.get(job_id)
            if current:
                current["running"] = False
                current["exit_code"] = rc
                current["ended_at"] = time.time()
        worker_name = str(item.get("worker_name", "") or "").strip() or "-"
        if rc == 0:
            append_processing_log(f"[dispatch] {source_name} auf {worker_name}: OK")
        else:
            append_processing_log(
                f"[dispatch] {source_name} auf {worker_name}: FEHLER | {runner_error_summary(item.get('log_path'), rc)}"
            )
        invalidate_worker_state_cache()

    threading.Thread(target=monitor, daemon=True, name=f"dispatch-monitor-{job_id}").start()


def start_ffmpeg_dispatch_from_rows(
    *,
    folder: str,
    encoder: str,
    rows: list[dict[str, Any]],
    sync_nas: bool,
    sync_plex: bool,
    del_out: bool,
    del_source: bool,
) -> tuple[bool, str]:
    folder_path = Path(folder).expanduser()
    try:
        folder_path = folder_path.resolve()
    except Exception:
        pass
    queue_rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        full_row = dict(row or {})
        source_name = str(full_row.get("source_name", full_row.get("Quellname", "")) or "").strip()
        if not source_name:
            continue
        full_row["source_name"] = source_name
        key = normalize_simple_text(source_name).lower()
        if key in seen:
            continue
        seen.add(key)
        queue_rows.append(full_row)
    if not queue_rows:
        return False, "Keine Encode-Quellen für den Dispatcher."

    release_version = reserve_next_release_version()
    master_job_id = time.strftime("%Y%m%d-%H%M%S") + "-dispatch-" + uuid.uuid4().hex[:6]
    master_log_path = LOG_DIR / f"managemovie-{master_job_id}.log"
    master_log_path.touch(exist_ok=True)
    warmed_workers = warm_dispatch_worker_pool()
    ready_workers = [
        worker
        for worker in warmed_workers
        if bool(worker.get("connected"))
        and bool(worker.get("enabled"))
        and not bool(worker.get("reinit_running"))
        and str(worker.get("state", "") or "").strip() != "paused"
    ]
    if ready_workers:
        append_processing_log(
            f"[dispatch] Worker-Pool bereit: {', '.join(str(worker.get('name', '') or '').strip() for worker in ready_workers if str(worker.get('name', '') or '').strip())}"
        )
    else:
        append_processing_log("[dispatch] Worker-Pool ohne bestätigte Ready-Worker, starte mit Live-Auswahl")
    ok_clean, clean_message = clean_target_out_root_for_dispatch(folder_path)
    if not ok_clean:
        append_processing_log(f"[dispatch] Zielbereinigung fehlgeschlagen: {clean_message}")
        return False, f"Zielbereinigung fehlgeschlagen: {clean_message}"
    append_processing_log(f"[dispatch] Zielordner geleert: {clean_message}")
    master_job = JobState(
        job_id=master_job_id,
        mode="ffmpeg",
        folder=str(folder_path),
        encoder=encoder,
        sync_nas=bool(sync_nas),
        sync_plex=bool(sync_plex),
        del_out=bool(del_out),
        del_source=bool(del_source),
        started_at=time.time(),
        process=subprocess.Popen(["/bin/sh", "-c", "sleep 0"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL),
        log_path=master_log_path,
        worker_name="",
        release_version=release_version,
    )

    with dispatch_lock:
        global dispatch_master_job, dispatch_subjobs, dispatch_pending_rows, dispatch_round_robin_cursor_name, dispatch_runtime_rows, dispatch_worker_pool
        dispatch_master_job = master_job
        dispatch_subjobs = {}
        dispatch_pending_rows = list(queue_rows)
        dispatch_round_robin_cursor_name = ""
        dispatch_runtime_rows = [dict(row) for row in queue_rows]
        dispatch_worker_pool = [dict(worker) for worker in ready_workers] if ready_workers else [dict(worker) for worker in warmed_workers]

    def runner() -> None:
        global dispatch_master_job, dispatch_pending_rows, dispatch_runtime_rows, dispatch_worker_pool
        while True:
            with dispatch_lock:
                current_master = dispatch_master_job
                continue_dispatch = bool(
                    current_master
                    and current_master.job_id == master_job_id
                    and current_master.running
                )
                row = dispatch_pending_rows.pop(0) if continue_dispatch and dispatch_pending_rows else None
            if not continue_dispatch:
                break
            if row is None:
                break
            worker = choose_ffmpeg_worker_for_dispatch()
            if worker is None:
                with dispatch_lock:
                    dispatch_pending_rows.insert(0, row)
                time.sleep(2.0)
                continue
            worker_name = str(worker.get("name", "") or "").strip()
            try:
                subjob = launch_remote_ffmpeg_subjob(
                    folder_path=folder_path,
                    source_name=str(row.get("source_name", "") or "").strip(),
                    encoder=encoder,
                    sync_nas=sync_nas,
                    sync_plex=sync_plex,
                    del_out=del_out,
                    del_source=del_source,
                    release_version=release_version,
                    worker=worker,
                    row=row,
                )
                spawn_dispatch_subjob_monitor(str(subjob.get("job_id", "")))
            except Exception as exc:
                with dispatch_lock:
                    dispatch_pending_rows.insert(0, dict(row))
                append_processing_log(f"[dispatch] {Path(str(row.get('source_name', '') or '')).name} auf {worker_name}: FEHLER | {exc}")
                time.sleep(2.0)
            invalidate_worker_state_cache()
            time.sleep(1.0)

        while True:
            with dispatch_lock:
                current_master = dispatch_master_job
                continue_dispatch = bool(
                    current_master
                    and current_master.job_id == master_job_id
                    and current_master.running
                )
                pending_results = [
                    item
                    for item in dispatch_subjobs.values()
                    if item.get("running") or item.get("exit_code") is None
                ]
            if not continue_dispatch:
                break
            if not pending_results:
                break
            time.sleep(2.0)

        with dispatch_lock:
            if dispatch_master_job and dispatch_master_job.job_id == master_job_id:
                exit_codes = [item.get("exit_code") for item in dispatch_subjobs.values()]
                dispatch_master_job.running = False
                dispatch_master_job.exit_code = (
                    0
                    if exit_codes and all(code in (0, "0") for code in exit_codes)
                    else 1
                )
                dispatch_master_job.ended_at = time.time()
                dispatch_pending_rows = []
                dispatch_worker_pool = []

    threading.Thread(target=runner, daemon=True, name=f"dispatch-runner-{master_job_id}").start()
    append_processing_log(f"[dispatch] Encode-Dispatcher gestartet: {len(queue_rows)} Quelle(n), max {per_worker_parallel_limit()} pro Worker")
    return True, f"Encode-Dispatcher gestartet: {len(queue_rows)}"


def read_runtime_rows_for_dispatch(start_folder: str) -> list[dict[str, Any]]:
    if not init_state_store():
        return []
    try:
        raw_runtime = str(STATE_DB_STORE.read_state("runtime.gemini_rows_json") or "").strip()
    except Exception:
        return []
    if not raw_runtime:
        return []
    try:
        parsed_runtime = json.loads(raw_runtime)
    except Exception:
        return []
    return normalize_editor_rows_payload(parsed_runtime, start_folder) if isinstance(parsed_runtime, list) else []


def build_dispatch_status_table_text(start_folder: str) -> str:
    folder_hint = str(start_folder or "").strip()
    if not folder_hint or folder_hint in {"", "-"}:
        return ""
    with dispatch_lock:
        runtime_rows = [dict(row) for row in dispatch_runtime_rows]
    if not runtime_rows:
        runtime_rows = read_runtime_rows_for_dispatch(folder_hint)
    if not runtime_rows:
        return ""
    annotated_rows = annotate_runtime_rows_with_dispatch_status(runtime_rows)
    if not annotated_rows:
        return ""
    table_text = build_status_table_override_from_editor_rows(annotated_rows)
    total = len(annotated_rows)
    if total <= 0:
        return table_text

    def _to_float(value: Any) -> float:
        text = str(value or "").strip().replace(",", ".")
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return 0.0
        try:
            return float(match.group(0))
        except Exception:
            return 0.0

    running_rows = [row for row in annotated_rows if not bool(row.get("completed")) and str(row.get("eta", "") or "").strip().lower() not in {"", "encoded", "copied", "manual", "error"}]
    completed_rows = [row for row in annotated_rows if bool(row.get("completed"))]
    active_row = None
    active_index = 0
    for candidate in running_rows:
        try:
            idx = annotated_rows.index(candidate) + 1
        except Exception:
            idx = 0
        if idx >= active_index:
            active_index = idx
            active_row = candidate
    active_target = Path(str(active_row.get("target_name", "") or "")).name if active_row else ""
    active_ratio = f"{active_index:02d}/{total:02d}" if active_index else f"00/{total:02d}"
    q_total = sum(_to_float(row.get("q_gb", "")) for row in annotated_rows)
    z_done = sum(_to_float(row.get("z_gb", "")) for row in completed_rows + running_rows)
    active_eta = str((active_row or {}).get("eta", "") or "").strip() or "-"
    meta_lines = [
        f"Aktiv:      {active_ratio} {active_target}".rstrip(),
        "",
        "Gesamt:",
        f"Dateien:    {total:02d}/{total:02d}",
        f"Verlauf:    {len(running_rows):02d}/{total:02d}",
        f"Erledigt:   {len(completed_rows):02d}/{total:02d}",
        f"Q-GB:       {q_total:.1f} GB".replace(".", ","),
        f"Z-GB:       {z_done:.1f} GB".replace(".", ","),
        f"ETA:        {active_eta}",
        "",
    ]
    return "\n".join(meta_lines) + table_text


def start_job(
    mode: str,
    folder: str,
    encoder: str,
    *,
    sync_nas: bool = False,
    sync_plex: bool = False,
    del_out: bool = False,
    del_source: bool = False,
    del_source_confirmed: bool = False,
) -> tuple[bool, str]:
    runtime_settings = read_runtime_settings()
    initial_setup_required = parse_form_bool(runtime_settings.get("initial_setup_required", "0"))
    initial_setup_done = parse_form_bool(runtime_settings.get("initial_setup_done", "0"))
    if mode in {"a", "analyze", "c", "copy", "f", "ffmpeg", "encode"} and initial_setup_required and not initial_setup_done:
        return False, "Erststart: Zuerst Einstellungen und API-Keys speichern. Analyze, Copy und Encode sind bis dahin gesperrt."

    folder_path = Path(folder).expanduser()
    if not folder_path.exists() or not folder_path.is_dir():
        return False, f"Ordner existiert nicht: {folder_path}"

    try:
        folder_path = folder_path.resolve()
    except Exception:
        pass

    write_last_started_folder(str(folder_path))
    clear_confirmation_file()

    with job_lock:
        global current_job
        with dispatch_lock:
            if dispatch_master_job and dispatch_master_job.running:
                return False, "Es läuft bereits ein Encode-Dispatcher."
        if current_job and current_job.running:
            return False, "Es läuft bereits ein Job."

        job_id = time.strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:8]
        release_version = reserve_next_release_version()
        log_path = LOG_DIR / f"managemovie-{job_id}.log"
        env = build_env(
            mode,
            encoder,
            sync_nas=sync_nas,
            sync_plex=sync_plex,
            del_out=del_out,
            del_source=del_source,
            del_source_confirmed=del_source_confirmed,
        )
        worker_name = ""
        remote_ffmpeg = False
        selected_worker: dict[str, Any] | None = None
        if remote_ffmpeg:
            selected_worker = choose_ffmpeg_worker()
            worker_name = str(selected_worker.get("name", "") or "").strip()
            cmd = build_remote_ffmpeg_command(str(folder_path), env, selected_worker)
        else:
            cmd = build_command(mode, str(folder_path))

        log_handle = log_path.open("a", encoding="utf-8")
        log_handle.write(f"$ {' '.join(cmd)}\n")
        log_handle.write(f"[info] started={time.strftime('%Y-%m-%d %H:%M:%S')} mode={mode} folder={folder_path}\n")
        if remote_ffmpeg:
            log_handle.write(
                f"[worker] ffmpeg-remote host={worker_name} folder={map_folder_for_ffmpeg_worker(str(folder_path), selected_worker)}\n"
            )
        log_handle.write(f"[version] release={release_version} range={format_release_version(VERSION_MIN_PATCH)}..{format_release_version(VERSION_MAX_PATCH)}\n")
        log_handle.flush()

        append_processing_log(f"Release {release_version} | Range {release_range_text()}")

        process = subprocess.Popen(
            cmd,
            cwd=str(BASE_DIR / "app"),
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            env=env,
            start_new_session=True,
        )

        current_job = JobState(
            job_id=job_id,
            mode=mode,
            folder=str(folder_path),
            encoder=encoder,
            sync_nas=bool(sync_nas),
            sync_plex=bool(sync_plex),
            del_out=bool(del_out),
            del_source=bool(del_source),
            started_at=time.time(),
            process=process,
            log_path=log_path,
            worker_name=worker_name,
            release_version=release_version,
        )
        spawn_monitor(job_id, process, log_handle)

    return True, f"Job gestartet: {job_id}"


def stop_job(reason: str = "system") -> tuple[bool, str]:
    normalized_reason = str(reason or "").strip().lower() or "system"
    worker_specs_for_abort: list[dict[str, str]] = []
    with dispatch_lock:
        active_dispatch = bool(dispatch_master_job and dispatch_master_job.running)
        dispatch_items = list(dispatch_subjobs.values())
        if active_dispatch and dispatch_master_job:
            worker_specs_for_abort = collect_abort_worker_specs(dispatch_items, None)
            dispatch_master_job.running = False
            dispatch_master_job.ended_at = time.time()
            dispatch_master_job.exit_code = 130
            for item in dispatch_items:
                item["running"] = False
                item["ended_at"] = time.time()
                if item.get("exit_code") is None:
                    item["exit_code"] = 130
    if active_dispatch:
        if worker_specs_for_abort:
            abort_remote_worker_group(worker_specs_for_abort)
        clear_confirmation_file()
        invalidate_worker_state_cache()
        if normalized_reason == "manual":
            log_manual_abort_event(normalized_reason)
        return True, "Dispatcher gestoppt."

    with job_lock:
        global current_job
        if current_job and current_job.running:
            process = current_job.process
            worker_specs_for_abort = collect_abort_worker_specs([], current_job)
        else:
            process = None

    if process is not None:
        pid = int(getattr(process, "pid", 0) or 0)
        if pid > 0:
            terminate_pid_or_group(pid, grace_seconds=10.0)
        else:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        if worker_specs_for_abort:
            abort_remote_worker_group(worker_specs_for_abort)
        if current_job:
            current_job.running = False
            current_job.ended_at = time.time()
            current_job.exit_code = 130
        clear_confirmation_file()
        if normalized_reason == "manual":
            log_manual_abort_event(normalized_reason)
        return True, "Job gestoppt."

    live = detect_running_job_from_ps()
    if not live:
        return False, "Es läuft kein Job."

    pid = int(live.get("pid", 0) or 0)
    if pid <= 0:
        return False, "Es läuft kein Job."

    if not terminate_pid_or_group(pid, grace_seconds=10.0):
        return False, "Job konnte nicht gestoppt werden."

    clear_confirmation_file()
    if normalized_reason == "manual":
        log_manual_abort_event(normalized_reason)
    return True, "Job gestoppt."


def schedule_full_restart() -> tuple[bool, str]:
    global restart_requested_at
    with restart_lock:
        now = time.time()
        if restart_requested_at > 0 and (now - restart_requested_at) < 20:
            return False, "Restart läuft bereits."
        restart_requested_at = now

    try:
        stop_job()
    except Exception:
        pass

    project_root = BASE_DIR.parent
    stop_script = project_root / "stop.sh"
    start_script = project_root / "start.sh"
    setup_mariadb_script = project_root / "setup_mariadb.sh"
    restart_log = LOG_DIR / "system-restart.log"
    restart_log.parent.mkdir(parents=True, exist_ok=True)

    q_project_root = shlex.quote(str(project_root))
    q_stop = shlex.quote(str(stop_script))
    q_start = shlex.quote(str(start_script))
    q_setup_mariadb = shlex.quote(str(setup_mariadb_script))
    q_log = shlex.quote(str(restart_log))
    web_port = int(os.environ.get("MANAGEMOVIE_WEB_PORT", "8126") or 8126)

    shell_script = f"""
set +e
sleep 1
cd {q_project_root}
echo "[`date '+%Y-%m-%d %H:%M:%S'`] restart requested" >> {q_log}
echo "[restart] stop app via stop.sh" >> {q_log}
{q_stop} >> {q_log} 2>&1 || true
echo "[restart] hard-kill all app processes" >> {q_log}
pkill -f '[m]anagemovie-web/app/run_managemovie.sh' >/dev/null 2>&1 || true
pkill -f '[m]anagemovie-web/app/managemovie.py' >/dev/null 2>&1 || true
pkill -f '[m]anagemovie-web/web/app.py' >/dev/null 2>&1 || true
pkill -f '[m]anagemovie-web/start_web.sh' >/dev/null 2>&1 || true
if command -v lsof >/dev/null 2>&1; then
  for pid in $(lsof -ti tcp:{web_port} 2>/dev/null); do
    kill -9 "$pid" >/dev/null 2>&1 || true
  done
elif command -v fuser >/dev/null 2>&1; then
  for pid in $(fuser -n tcp {web_port} 2>/dev/null); do
    kill -9 "$pid" >/dev/null 2>&1 || true
  done
fi
echo "[restart] stop db services (best effort)" >> {q_log}
(
  systemctl stop mariadb ||
  systemctl stop mysql ||
  service mariadb stop ||
  service mysql stop ||
  brew services stop mariadb ||
  brew services stop mysql ||
  true
) >> {q_log} 2>&1
echo "[restart] hard-kill db processes" >> {q_log}
pkill -9 -f '[m]ariadbd' >/dev/null 2>&1 || true
pkill -9 -f '[m]ysqld_safe' >/dev/null 2>&1 || true
pkill -9 -f '[m]ysqld' >/dev/null 2>&1 || true
sleep 1
echo "[restart] start db services (best effort)" >> {q_log}
(
  systemctl start mariadb ||
  systemctl start mysql ||
  service mariadb start ||
  service mysql start ||
  brew services start mariadb ||
  brew services start mysql ||
  true
) >> {q_log} 2>&1
echo "[restart] setup_mariadb.sh" >> {q_log}
{q_setup_mariadb} >> {q_log} 2>&1 || true
echo "[restart] start.sh" >> {q_log}
nohup {q_start} >> {q_log} 2>&1 </dev/null &
echo "[restart] done (port={web_port})" >> {q_log}
"""

    subprocess.Popen(
        ["bash", "-lc", shell_script],
        cwd=str(project_root),
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return True, "Restart wird ausgefuehrt."


def schedule_system_update() -> tuple[bool, str]:
    global update_requested_at
    with update_lock:
        now = time.time()
        if update_requested_at > 0 and (now - update_requested_at) < 20:
            return False, "Update läuft bereits."
        update_requested_at = now

    try:
        stop_job()
    except Exception:
        pass

    project_root = BASE_DIR.parent
    update_script = project_root / "update_ManageMovie.sh"
    update_log = LOG_DIR / "system-update.log"
    update_log.parent.mkdir(parents=True, exist_ok=True)

    q_project_root = shlex.quote(str(project_root))
    q_update = shlex.quote(str(update_script))
    q_log = shlex.quote(str(update_log))

    shell_script = f"""#!/usr/bin/env bash
set +e
sleep 1
cd {q_project_root}
: > {q_log}
echo "[`date '+%Y-%m-%d %H:%M:%S'`] update requested" >> {q_log}
echo "[update-status] running" >> {q_log}
if [ -f {q_update} ] && [ ! -x {q_update} ]; then
  chmod +x {q_update} >> {q_log} 2>&1 || true
fi
if [ ! -x {q_update} ]; then
  echo "[update] update_ManageMovie.sh fehlt oder ist nicht ausführbar: {q_update}" >> {q_log}
  echo "[update-status] done rc=1" >> {q_log}
  exit 1
fi
{q_update} >> {q_log} 2>&1
rc=$?
echo "[update-status] done rc=$rc" >> {q_log}
exit $rc
"""

    launcher_path = LOG_DIR / "system-update-launcher.sh"
    try:
        launcher_path.write_text(shell_script, encoding="utf-8")
        launcher_path.chmod(0o755)
    except Exception as exc:
        return False, f"Update-Launcher konnte nicht geschrieben werden: {exc}"

    if shutil.which("systemd-run"):
        launch_cmd = [
            "systemd-run",
            "--unit",
            "managemovie-self-update",
            "--collect",
            "--quiet",
            str(launcher_path),
        ]
    else:
        launch_cmd = [str(launcher_path)]

    subprocess.Popen(
        launch_cmd,
        cwd=str(project_root),
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return True, "Update wird ausgefuehrt."


@app.route("/")
def index():
    ensure_layout()
    message = request.args.get("msg", "")
    selected_folder = resolve_default_folder(request.args.get("folder", ""))
    runtime_settings = read_runtime_settings()
    selected_target_nfs = normalize_start_folder(request.args.get("settings_target_nfs", ""))
    selected_target_out = normalize_start_folder(request.args.get("settings_target_out", ""))
    selected_target_reenqueue = normalize_start_folder(request.args.get("settings_target_reenqueue", ""))
    out_setting_raw = selected_target_out or runtime_settings.get("target_out_path", DEFAULT_TARGET_OUT_PATH)
    reenqueue_setting_raw = selected_target_reenqueue or runtime_settings.get(
        "target_reenqueue_path",
        default_target_reenqueue_path_for_out(out_setting_raw),
    )
    response = make_response(
        render_template_string(
            TEMPLATE,
            message=message,
            default_folder=selected_folder,
            default_mode=read_last_mode(),
            default_encoder=coerce_encoder_for_ui(read_last_encoder()),
            encoder_options=available_encoder_options(),
            encoder_values=[value for value, _label in available_encoder_options()],
            settings_target_nfs_path=selected_target_nfs or runtime_settings.get("target_nfs_path", DEFAULT_TARGET_NFS_PATH),
            settings_target_nfs_selected=selected_target_nfs,
            settings_target_out_path=display_target_out_path(selected_folder, out_setting_raw),
            settings_target_out_selected=selected_target_out,
            settings_target_reenqueue_path=display_target_reenqueue_path(selected_folder, reenqueue_setting_raw, out_setting_raw),
            settings_target_reenqueue_selected=selected_target_reenqueue,
            settings_nas_ip=runtime_settings.get("nas_ip", DEFAULT_NAS_IP),
            settings_plex_ip=runtime_settings.get("plex_ip", DEFAULT_PLEX_IP),
            settings_plex_api="",
            settings_tmdb_api="",
            settings_gemini_api="",
            settings_ai_query_disabled=parse_form_bool(runtime_settings.get("ai_query_disabled", "1")),
            settings_start_on_boot=parse_form_bool(runtime_settings.get("start_on_boot", "1")),
            settings_skip_4k_h265_encode=parse_form_bool(runtime_settings.get("skip_4k_h265_encode", "0")),
            settings_encode_once_4k=parse_form_bool(runtime_settings.get("encode_once_4k", "1")),
            settings_one_encode_per_worker=parse_form_bool(runtime_settings.get("one_encode_per_worker", "0")),
            settings_allow_worker_4=parse_form_bool(runtime_settings.get("allow_worker_4", "0")),
            settings_parallel_job_limit=str(runtime_settings.get("parallel_job_limit", "2") or "2"),
            settings_precheck_egb=parse_form_bool(runtime_settings.get("precheck_egb", "1")),
            settings_speed_fallback_copy=parse_form_bool(runtime_settings.get("speed_fallback_copy", "1")),
            worker_names=[item.get("name", "") for item in read_worker_specs()],
            work_dir=str(WORK_DIR),
            temp_dir=str(TEMP_DIR),
            core_script=str(CORE_SCRIPT),
            version_range=release_range_text(),
            version_current=current_release_version(),
            site_title=detect_site_title(),
        )
    )
    return response


@app.route("/start", methods=["POST"])
def start():
    mode = normalize_mode(request.form.get("mode", "")) or read_last_mode()
    folder = (request.form.get("folder", "") or "").strip()
    if not folder:
        folder = read_last_started_folder()
    encoder = coerce_encoder_for_ui(request.form.get("encoder", "")) or coerce_encoder_for_ui(read_last_encoder())
    sync_nas = parse_form_bool(request.form.get("sync_nas", "0"))
    sync_plex = parse_form_bool(request.form.get("sync_plex", "0"))
    del_out = parse_form_bool(request.form.get("del_out", "0"))
    del_source = parse_form_bool(request.form.get("del_source", "0"))
    del_source_confirmed = parse_form_bool(request.form.get("del_source_confirmed", "0"))
    if del_out and not sync_nas:
        del_out = False
    if mode == "analyze":
        sync_nas = False
        sync_plex = False
        del_out = False
        del_source = False
        del_source_confirmed = False
    write_last_encoder(encoder)
    ok, msg = start_job(
        mode,
        folder,
        encoder,
        sync_nas=sync_nas,
        sync_plex=sync_plex,
        del_out=del_out,
        del_source=del_source,
        del_source_confirmed=del_source_confirmed,
    )
    if ok:
        write_last_started_folder(folder)
        write_last_mode(mode)
        write_last_encoder(encoder)
    if ok:
        return redirect(url_for("index", folder=folder))
    return redirect(url_for("index", folder=folder, msg=f"Fehler: {msg}"))


@app.route("/stop", methods=["POST"])
def stop():
    ok, msg = stop_job(reason="manual")
    if ok:
        return redirect(url_for("index"))
    return redirect(url_for("index", msg=f"Fehler: {msg}"))


@app.route("/api/stop", methods=["POST"])
def api_stop():
    ok, msg = stop_job(reason="manual")
    if not ok:
        return jsonify({"ok": False, "error": msg}), 409
    return jsonify({"ok": True, "message": msg})


@app.route("/api/system/restart", methods=["POST"])
def api_system_restart():
    ok, msg = schedule_full_restart()
    if not ok:
        return jsonify({"ok": False, "error": msg}), 409
    return jsonify({"ok": True, "message": msg})


@app.route("/api/system/update", methods=["POST"])
def api_system_update():
    ok, msg = schedule_system_update()
    if not ok:
        return jsonify({"ok": False, "error": msg}), 409
    return jsonify({"ok": True, "message": msg})


@app.route("/api/system/update-status")
def api_system_update_status():
    update_log = LOG_DIR / "system-update.log"
    log_text = tail_file(update_log, lines=220, max_chars=64000)
    log_exists = update_log.exists()
    log_size = 0
    log_mtime = 0.0
    if log_exists:
        try:
            stat = update_log.stat()
            log_size = int(stat.st_size)
            log_mtime = float(stat.st_mtime)
        except Exception:
            log_size = 0
            log_mtime = 0.0

    running = False
    done = False
    success = False
    return_code = None
    if log_text:
        markers = re.findall(r"\[update-status\]\s+(running|done(?:\s+rc=(\d+))?)", log_text, flags=re.IGNORECASE)
        if markers:
            last_marker = markers[-1][0].strip().lower()
            if last_marker.startswith("done"):
                done = True
                rc_raw = markers[-1][1].strip() if len(markers[-1]) > 1 else ""
                try:
                    return_code = int(rc_raw)
                except Exception:
                    return_code = None
                success = return_code == 0
            else:
                running = True
    if log_exists and not done and log_mtime > 0:
        # Nach einem App-Neustart ist der Speicherzustand weg. Frischer Log ohne done-Marker bleibt laufend.
        if (time.time() - log_mtime) < 900:
            running = True

    return jsonify(
        {
            "ok": True,
            "running": running,
            "done": done,
            "success": success,
            "return_code": return_code,
            "log_exists": log_exists,
            "log_size": log_size,
            "log_mtime": log_mtime,
            "log": log_text,
        }
    )


@app.route("/settings/mode", methods=["POST"])
def settings_mode():
    payload = request.get_json(silent=True) or {}
    mode = normalize_mode(payload.get("mode", ""))
    if not mode:
        mode = normalize_mode(request.form.get("mode", ""))
    if not mode:
        return jsonify({"ok": False, "error": "Ungueltiger Modus"}), 400
    if not write_last_mode(mode):
        return jsonify({"ok": False, "error": "MariaDB nicht verfuegbar"}), 503
    return jsonify({"ok": True, "mode": mode})


@app.route("/settings/encoder", methods=["POST"])
def settings_encoder():
    payload = request.get_json(silent=True) or {}
    encoder = coerce_encoder_for_ui(payload.get("encoder", ""))
    if not encoder:
        encoder = coerce_encoder_for_ui(request.form.get("encoder", ""))
    if not encoder:
        return jsonify({"ok": False, "error": "Ungueltiger Encoder"}), 400
    if not write_last_encoder(encoder):
        return jsonify({"ok": False, "error": "MariaDB nicht verfuegbar"}), 503
    return jsonify({"ok": True, "encoder": encoder})


@app.route("/api/settings", methods=["GET"])
def api_settings_get():
    return jsonify(
        {
            "ok": True,
            "settings": read_public_runtime_settings(),
            "cache_db": read_cache_db_summary(),
        }
    )


@app.route("/api/settings", methods=["POST"])
def api_settings_set():
    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        payload = request.form.to_dict(flat=True)
    ok, error, updated = update_runtime_settings(payload)
    if not ok:
        return jsonify({"ok": False, "error": error}), 400
    return jsonify(
        {
            "ok": True,
            "settings": {
                **build_public_runtime_settings(
                updated,
                mode=read_last_mode(),
                encoder=read_last_encoder(),
                ),
                "encoder": coerce_encoder_for_ui(read_last_encoder()),
            },
            "cache_db": read_cache_db_summary(),
        }
    )


@app.route("/api/settings/cache/reset", methods=["POST"])
def api_settings_cache_reset():
    if not init_state_store():
        return jsonify({"ok": False, "error": "MariaDB-State nicht verfuegbar"}), 503
    try:
        cleared = STATE_DB_STORE.reset_cache_db_entries()
        # Reset also clears persisted UI/runtime artefacts, otherwise stale rows
        # from work-files can appear immediately after reset.
        clear_log_windows_data()
        clear_confirmation_file()
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Cache-DB-Reset fehlgeschlagen: {exc}"}), 500

    return jsonify(
        {
            "ok": True,
            "cleared": {
                "app_state_cache_rows": int(cleared.get("app_state_cache_rows", 0) or 0),
                "tmdb_cache_rows": int(cleared.get("tmdb_cache_rows", 0) or 0),
                "tmdb_cache_history_rows": int(cleared.get("tmdb_cache_history_rows", 0) or 0),
                "legacy_gemini_cache_rows": int(cleared.get("legacy_gemini_cache_rows", 0) or 0),
                "legacy_gemini_cache_history_rows": int(cleared.get("legacy_gemini_cache_history_rows", 0) or 0),
                "app_state_non_settings_remaining": int(cleared.get("app_state_non_settings_remaining", 0) or 0),
                "total_cache_rows": int(cleared.get("total_cache_rows", 0) or 0),
            },
            "cache_db": read_cache_db_summary(),
        }
    )


@app.route("/api/workers/<worker_name>/action", methods=["POST"])
def api_worker_action(worker_name: str):
    payload = request.get_json(silent=True) or {}
    action = str(payload.get("action", "") or "").strip().lower()
    ok, message = run_worker_action(worker_name, action)
    return jsonify({"ok": ok, "message": message, "workers": read_worker_states(force=True)}), (200 if ok else 400)


@app.route("/logs/clear", methods=["POST"])
def logs_clear():
    clear_log_windows_data()
    return jsonify({"ok": True})


@app.route("/api/confirm", methods=["POST"])
def api_confirm():
    payload = request.get_json(silent=True) or {}
    state = str(payload.get("state", payload.get("decision", "")) or "").strip().lower()
    token = str(payload.get("token", "") or "").strip()
    encoder = normalize_encoder_mode(payload.get("encoder", ""))

    pending_payload = read_pending_confirmation_payload()
    pending_mode = str((pending_payload or {}).get("mode", "") or "").strip().lower()
    if state == "start" and not pending_payload:
        with job_lock:
            global current_job
            job = current_job
            if job and job.running and str(job.mode or "").strip().lower() == "ffmpeg":
                sync_nas = bool(job.sync_nas)
                sync_plex = bool(job.sync_plex)
                del_out = bool(job.del_out)
                del_source = bool(job.del_source)
                start_folder = str(job.folder or "").strip()
                process = job.process
                current_job = None
            else:
                job = None
                start_folder = ""
                process = None
                sync_nas = False
                sync_plex = False
                del_out = False
                del_source = False
        if job and start_folder:
            rows = read_runtime_rows_for_dispatch(start_folder)
            pid = int(getattr(process, "pid", 0) or 0)
            if pid > 0:
                terminate_pid_or_group(pid, grace_seconds=3.0)
            clear_confirmation_file()
            append_processing_log("[dispatch] Freigabe-Fallback aktiv: Runtime-Zeilen aus DB verwendet")
            ok_dispatch, error_dispatch = start_ffmpeg_dispatch_from_rows(
                folder=start_folder,
                encoder=encoder or coerce_encoder_for_ui(read_last_encoder()),
                rows=rows,
                sync_nas=sync_nas,
                sync_plex=sync_plex,
                del_out=del_out,
                del_source=del_source,
            )
            if not ok_dispatch:
                return jsonify({"ok": False, "error": error_dispatch}), 409
            return jsonify({"ok": True, "message": error_dispatch, "fallback": True})
    if state == "start" and pending_payload and pending_mode == "f":
        start_folder = str(pending_payload.get("start_folder", "") or "").strip()
        rows = collect_editor_rows_from_payload(pending_payload)
        with job_lock:
            job = current_job
            sync_nas = bool(job.sync_nas) if job else False
            sync_plex = bool(job.sync_plex) if job else False
            del_out = bool(job.del_out) if job else False
            del_source = bool(job.del_source) if job else False
        ok_cancel, error_cancel = write_confirmation_decision(token, "dispatch", encoder)
        if not ok_cancel:
            return jsonify({"ok": False, "error": error_cancel}), 409
        ok_dispatch, error_dispatch = start_ffmpeg_dispatch_from_rows(
            folder=start_folder,
            encoder=encoder or coerce_encoder_for_ui(read_last_encoder()),
            rows=rows,
            sync_nas=sync_nas,
            sync_plex=sync_plex,
            del_out=del_out,
            del_source=del_source,
        )
        if not ok_dispatch:
            return jsonify({"ok": False, "error": error_dispatch}), 409
        return jsonify({"ok": True, "message": error_dispatch})

    ok, error = write_confirmation_decision(token, state, encoder)
    if not ok:
        return jsonify({"ok": False, "error": error}), 409
    return jsonify({"ok": True})


@app.route("/api/confirm/clean", methods=["POST"])
def api_confirm_clean():
    payload = request.get_json(silent=True) or {}
    token = str(payload.get("token", "") or "").strip()

    pending = read_pending_confirmation()
    if not pending:
        return jsonify({"ok": False, "error": "Keine aktive Freigabe"}), 409

    pending_token = str(pending.get("token", "") or "").strip()
    if pending_token and token != pending_token:
        return jsonify({"ok": False, "error": "Freigabe-Token passt nicht"}), 409

    start_folder = str(payload.get("start_folder", "") or pending.get("start_folder", "")).strip()
    ok, result, error = clean_manifest_files(start_folder)
    if not ok:
        return jsonify({"ok": False, "error": error}), 400

    return jsonify({"ok": True, **result})


@app.route("/api/confirm/editor", methods=["GET"])
def api_confirm_editor_get():
    token = str(request.args.get("token", "") or "").strip()
    pending, error = get_pending_confirmation_for_token(token)
    if not pending:
        return jsonify({"ok": False, "error": error}), 409

    start_folder = str(pending.get("_start_folder", "") or pending.get("start_folder", "")).strip()
    rows = collect_editor_rows_from_payload(pending)
    return jsonify(
        {
            "ok": True,
            "token": str(pending.get("_token", "") or pending.get("token", "")).strip(),
            "mode": str(pending.get("_mode", "") or pending.get("mode", "")).strip(),
            "start_folder": start_folder,
            "target_out_prefix": str(target_out_prefix_for_start(start_folder)),
            "target_reenqueue_prefix": str(target_reenqueue_prefix_for_start(start_folder)),
            "target_manual_prefix": str(target_manual_prefix_for_start(start_folder)),
            "rows": rows,
        }
    )


@app.route("/api/confirm/editor/save", methods=["POST"])
def api_confirm_editor_save():
    payload = request.get_json(silent=True) or {}
    token = str(payload.get("token", "") or "").strip()
    pending, error = get_pending_confirmation_for_token(token)
    if not pending:
        return jsonify({"ok": False, "error": error}), 409

    start_folder = str(pending.get("_start_folder", "") or pending.get("start_folder", "")).strip()
    rows_input = payload.get("rows", [])
    rows = normalize_editor_rows_payload(rows_input, start_folder)
    rows = reanalyze_editor_rows(rows, start_folder)
    original_rows_any = pending.get("editor_rows_original", [])
    if not isinstance(original_rows_any, list):
        original_rows_any = []
    original_rows = normalize_editor_rows_payload(original_rows_any, start_folder)
    changed_rows = collect_changed_editor_rows(rows, original_rows)

    try:
        persist_editor_rows_to_db(rows)
        changed_cached = persist_editor_override_cache_rows(changed_rows)
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Editor-Save fehlgeschlagen: {exc}"}), 500

    pending["editor_rows"] = clone_json_like(rows)
    pending["file_count"] = len(rows)
    pending["updated_at"] = int(time.time())
    if not write_confirmation_payload(pending):
        return jsonify({"ok": False, "error": "Freigabe konnte nicht aktualisiert werden"}), 500

    rows_out = collect_editor_rows_from_payload(pending)
    return jsonify(
        {
            "ok": True,
            "rows": rows_out,
            "saved": len(rows_out),
            "editor_changed": len(changed_rows),
            "editor_cached": int(changed_cached),
        }
    )


@app.route("/api/confirm/editor/requeue", methods=["POST"])
def api_confirm_editor_requeue():
    payload = request.get_json(silent=True) or {}
    token = str(payload.get("token", "") or "").strip()
    pending, error = get_pending_confirmation_for_token(token)
    if not pending:
        return jsonify({"ok": False, "error": error}), 409

    start_folder = str(pending.get("_start_folder", "") or pending.get("start_folder", "")).strip()
    current_rows_any = pending.get("editor_rows", [])
    if not isinstance(current_rows_any, list):
        current_rows_any = []
    current_rows = normalize_editor_rows_payload(current_rows_any, start_folder)

    source_filter_any = payload.get("source_names", [])
    source_filter_set: set[str] | None = None
    if isinstance(source_filter_any, list):
        normalized_filter = {
            normalize_source_row_name_for_gemini(str(item or "").strip())
            for item in source_filter_any
            if str(item or "").strip()
        }
        if normalized_filter:
            source_filter_set = normalized_filter

    remaining_rows: list[dict[str, Any]] = []
    requeued_sources: list[str] = []
    requeued_rows: list[dict[str, Any]] = []
    moved_files = 0
    moved_sidecars = 0
    removed_dirs = 0
    errors: list[str] = []

    for row in current_rows:
        source_name = str(row.get("source_name", "") or "").strip()
        if not source_name:
            remaining_rows.append(dict(row))
            continue

        source_key = normalize_source_row_name_for_gemini(source_name)
        source_base_key = normalize_source_row_name_for_gemini(Path(source_name).name)
        if (
            source_filter_set is not None
            and source_key not in source_filter_set
            and source_base_key not in source_filter_set
        ):
            remaining_rows.append(dict(row))
            continue

        ok_move, result, move_error = move_source_to_reenqueue(start_folder, source_name)
        if not ok_move:
            remaining_rows.append(dict(row))
            errors.append(move_error)
            continue

        requeued_sources.append(source_name)
        requeued_rows.append(dict(row))
        moved_files += int(result.get("moved_files", 0) or 0)
        moved_sidecars += int(result.get("moved_sidecars", 0) or 0)
        removed_dirs += int(result.get("removed_dirs", 0) or 0)

    try:
        cleared = clear_editor_override_cache_rows(requeued_sources)
    except Exception as exc:
        return jsonify({"ok": False, "error": f"RE-QUEUE-Cache-Loeschung fehlgeschlagen: {exc}"}), 500

    pending["editor_rows"] = clone_json_like(normalize_editor_rows_payload(remaining_rows, start_folder))
    requeued_store_any = pending.get("editor_rows_requeued", [])
    requeued_store = normalize_editor_rows_payload(requeued_store_any, start_folder) if isinstance(requeued_store_any, list) else []
    remaining_keys = {
        normalize_source_row_name_for_gemini(str(item.get("source_name", "") or "").strip())
        for item in pending["editor_rows"]
    }
    requeued_map: dict[str, dict[str, Any]] = {}
    for item in requeued_store:
        source_name = str(item.get("source_name", "") or "").strip()
        source_key = normalize_source_row_name_for_gemini(source_name)
        if not source_key or source_key in remaining_keys:
            continue
        requeued_map[source_key] = dict(item)
    for item in requeued_rows:
        source_name = str(item.get("source_name", "") or "").strip()
        source_key = normalize_source_row_name_for_gemini(source_name)
        if not source_key or source_key in remaining_keys:
            continue
        requeued_map[source_key] = dict(item)
    pending["editor_rows_requeued"] = clone_json_like(list(requeued_map.values()))
    pending["file_count"] = len(pending["editor_rows"])
    pending["updated_at"] = int(time.time())
    if not write_confirmation_payload(pending):
        return jsonify({"ok": False, "error": "Freigabe konnte nach RE-QUEUE nicht aktualisiert werden"}), 500

    rows_out = collect_editor_rows_from_payload(pending)
    return jsonify(
        {
            "ok": True,
            "rows": rows_out,
            "requeued_sources": len(requeued_sources),
            "moved_files": moved_files,
            "moved_sidecars": moved_sidecars,
            "removed_dirs": removed_dirs,
            "editor_cache_cleared": int(cleared),
            "errors": errors,
        }
    )


@app.route("/api/confirm/editor/reset", methods=["POST"])
def api_confirm_editor_reset():
    payload = request.get_json(silent=True) or {}
    token = str(payload.get("token", "") or "").strip()
    pending, error = get_pending_confirmation_for_token(token)
    if not pending:
        return jsonify({"ok": False, "error": error}), 409

    start_folder = str(pending.get("_start_folder", "") or pending.get("start_folder", "")).strip()
    current_rows_any = pending.get("editor_rows", [])
    if not isinstance(current_rows_any, list):
        current_rows_any = []
    current_rows = normalize_editor_rows_payload(current_rows_any, start_folder)
    if not current_rows:
        current_rows = collect_editor_rows_from_payload(pending)

    original_rows_any = pending.get("editor_rows_original", [])
    if not isinstance(original_rows_any, list):
        original_rows_any = []
    original_rows = normalize_editor_rows_payload(original_rows_any, start_folder)
    original_rows_by_source: dict[str, dict[str, Any]] = {}
    for original in original_rows:
        source_original = str(original.get("source_name", "") or "").strip()
        if source_original:
            original_rows_by_source[normalize_source_row_name_for_gemini(source_original)] = dict(original)
            source_original_base = Path(source_original).name
            if source_original_base:
                original_rows_by_source.setdefault(
                    normalize_source_row_name_for_gemini(source_original_base),
                    dict(original),
                )

    session_rows_any = pending.get("editor_rows_session_start", [])
    if not isinstance(session_rows_any, list):
        session_rows_any = []
    session_rows = normalize_editor_rows_payload(session_rows_any, start_folder)
    session_rows_by_source: dict[str, dict[str, Any]] = {}
    for session_row in session_rows:
        source_session = str(session_row.get("source_name", "") or "").strip()
        if source_session:
            session_rows_by_source[normalize_source_row_name_for_gemini(source_session)] = dict(session_row)
            source_session_base = Path(source_session).name
            if source_session_base:
                session_rows_by_source.setdefault(
                    normalize_source_row_name_for_gemini(source_session_base),
                    dict(session_row),
                )

    source_filter_any = payload.get("source_names", [])
    source_filter_set: set[str] | None = None
    if isinstance(source_filter_any, list):
        normalized_filter = {
            normalize_source_row_name_for_gemini(str(item or "").strip())
            for item in source_filter_any
            if str(item or "").strip()
        }
        if normalized_filter:
            source_filter_set = normalized_filter

    reset_scope = str(payload.get("reset_scope", "") or "").strip().lower()
    if reset_scope not in {"gemini", "session_start"}:
        # Zeilen-Reset => Gemini-Baseline, Tabellen-Reset => Editor-Start-Baseline.
        reset_scope = "gemini" if source_filter_set is not None else "session_start"

    if not init_state_store():
        return jsonify({"ok": False, "error": "MariaDB-State nicht verfuegbar"}), 500

    if reset_scope == "session_start" and source_filter_set is None and session_rows:
        augmented_session_rows = list(session_rows)
        if len(augmented_session_rows) <= len(current_rows):
            runtime_rows_any: list[dict[str, Any]] = []
            raw_runtime = str(STATE_DB_STORE.read_state("runtime.gemini_rows_json") or "").strip()
            if raw_runtime:
                try:
                    parsed_runtime = json.loads(raw_runtime)
                    if isinstance(parsed_runtime, list):
                        runtime_rows_any = [item for item in parsed_runtime if isinstance(item, dict)]
                except Exception:
                    runtime_rows_any = []
            runtime_rows = normalize_editor_rows_payload(runtime_rows_any, start_folder) if runtime_rows_any else []
            if runtime_rows:
                existing_session_keys = {
                    normalize_source_row_name_for_gemini(str(item.get("source_name", "") or "").strip())
                    for item in augmented_session_rows
                    if str(item.get("source_name", "") or "").strip()
                }
                reenqueue_root = resolve_reenqueue_root_for_start(start_folder)
                start_path = Path(start_folder)
                try:
                    start_resolved = start_path.resolve()
                except Exception:
                    start_resolved = start_path
                for runtime_row in runtime_rows:
                    source_name = str(runtime_row.get("source_name", "") or "").strip()
                    if not source_name:
                        continue
                    source_key = normalize_source_row_name_for_gemini(source_name)
                    if source_key in existing_session_keys:
                        continue
                    rel = Path(source_name.lstrip("./"))
                    if rel.is_absolute():
                        try:
                            rel = rel.resolve().relative_to(start_resolved)
                        except Exception:
                            rel = Path(rel.name)
                    reenqueue_candidate = _find_reenqueue_source_candidate(reenqueue_root, rel)
                    if reenqueue_candidate is None:
                        continue
                    augmented_session_rows.append(dict(runtime_row))
                    existing_session_keys.add(source_key)

        current_source_keys: set[str] = set()
        for row in current_rows:
            source_name = str(row.get("source_name", "") or "").strip()
            if not source_name:
                continue
            current_source_keys.add(normalize_source_row_name_for_gemini(source_name))
            source_base = Path(source_name).name
            if source_base:
                current_source_keys.add(normalize_source_row_name_for_gemini(source_base))

        reset_rows: list[dict[str, Any]] = []
        reset_sources: list[str] = []
        restored_source_keys: set[str] = set()
        session_restored = 0
        unchanged_rows = 0
        reverted_reenqueue = 0
        moved_back_files = 0
        moved_back_sidecars = 0
        reenqueue_errors: list[str] = []

        for session_row in augmented_session_rows:
            source_name = str(session_row.get("source_name", "") or "").strip()
            if not source_name:
                continue
            source_key = normalize_source_row_name_for_gemini(source_name)
            source_base_key = normalize_source_row_name_for_gemini(Path(source_name).name)
            restored_row = dict(session_row)
            restored_row["source_name"] = source_name
            restored_row["manual"] = parse_form_bool(str(restored_row.get("manual", "0")))
            restored_row["target_name"] = build_target_name_from_row(restored_row, start_folder)
            reset_rows.append(restored_row)
            reset_sources.append(source_name)
            restored_source_keys.add(source_key)
            if source_base_key:
                restored_source_keys.add(source_base_key)
            session_restored += 1

            if source_key in current_source_keys or source_base_key in current_source_keys:
                unchanged_rows += 1
                continue

            ok_restore, restore_result, restore_error = move_source_from_reenqueue(start_folder, source_name)
            if ok_restore:
                reverted_reenqueue += 1
                moved_back_files += int(restore_result.get("moved_back_files", 0) or 0)
                moved_back_sidecars += int(restore_result.get("moved_back_sidecars", 0) or 0)
            elif restore_error:
                reenqueue_errors.append(restore_error)

        requeued_store_any = pending.get("editor_rows_requeued", [])
        requeued_store = normalize_editor_rows_payload(requeued_store_any, start_folder) if isinstance(requeued_store_any, list) else []
        remaining_requeued_rows: list[dict[str, Any]] = []
        for row in requeued_store:
            source_name = str(row.get("source_name", "") or "").strip()
            source_key = normalize_source_row_name_for_gemini(source_name)
            source_base_key = normalize_source_row_name_for_gemini(Path(source_name).name)
            if source_key in restored_source_keys or source_base_key in restored_source_keys:
                continue
            remaining_requeued_rows.append(dict(row))
        pending["editor_rows_requeued"] = clone_json_like(remaining_requeued_rows)

        try:
            cleared = clear_editor_override_cache_rows(reset_sources)
        except Exception as exc:
            return jsonify({"ok": False, "error": f"Reset-Cache-Loeschung fehlgeschlagen: {exc}"}), 500

        pending["editor_rows"] = clone_json_like(normalize_editor_rows_payload(reset_rows, start_folder))
        pending["file_count"] = len(pending["editor_rows"])
        pending["updated_at"] = int(time.time())
        if not write_confirmation_payload(pending):
            return jsonify({"ok": False, "error": "Freigabe konnte nicht zurückgesetzt werden"}), 500

        rows_out = collect_editor_rows_from_payload(pending)
        return jsonify(
            {
                "ok": True,
                "rows": rows_out,
                "reset_sources": len(reset_sources),
                "reset_scope": reset_scope,
                "gemini_restored": 0,
                "baseline_restored": 0,
                "session_restored": session_restored,
                "source_guess_restored": 0,
                "unchanged_rows": unchanged_rows,
                "editor_cache_cleared": int(cleared),
                "reverted_reenqueue": reverted_reenqueue,
                "moved_back_files": moved_back_files,
                "moved_back_sidecars": moved_back_sidecars,
                "reenqueue_errors": reenqueue_errors,
            }
        )

    gemini_payload_map: dict[str, dict[str, str]] = {}
    if reset_scope == "gemini":
        gemini_cache_keys: list[str] = []
        for row in current_rows:
            source_name = str(row.get("source_name", "") or "").strip()
            if not source_name:
                continue
            source_key = normalize_source_row_name_for_gemini(source_name)
            source_base_key = normalize_source_row_name_for_gemini(Path(source_name).name)
            if (
                source_filter_set is not None
                and source_key not in source_filter_set
                and source_base_key not in source_filter_set
            ):
                continue
            gemini_cache_keys.append(gemini_source_row_cache_key(source_name))
            source_base = Path(source_name).name
            if source_base and source_base != source_name:
                gemini_cache_keys.append(gemini_source_row_cache_key(source_base))
        gemini_payload_map = read_cached_source_row_payloads(gemini_cache_keys)

    reset_rows: list[dict[str, Any]] = []
    reset_sources: list[str] = []
    gemini_restored = 0
    baseline_restored = 0
    session_restored = 0
    unchanged_rows = 0
    for row in current_rows:
        source_name = str(row.get("source_name", "") or "").strip()
        if not source_name:
            reset_rows.append(dict(row))
            continue
        source_key = normalize_source_row_name_for_gemini(source_name)
        source_base_key = normalize_source_row_name_for_gemini(Path(source_name).name)
        if (
            source_filter_set is not None
            and source_key not in source_filter_set
            and source_base_key not in source_filter_set
        ):
            reset_rows.append(dict(row))
            continue

        reset_sources.append(source_name)
        if reset_scope == "session_start":
            session_row = session_rows_by_source.get(source_key) or session_rows_by_source.get(source_base_key)
            if session_row:
                restored_row = dict(session_row)
                restored_row["source_name"] = source_name
                restored_row["manual"] = parse_form_bool(str(restored_row.get("manual", "0")))
                restored_row["target_name"] = build_target_name_from_row(restored_row, start_folder)
                reset_rows.append(restored_row)
                session_restored += 1
                continue
            reset_rows.append(dict(row))
            unchanged_rows += 1
            continue

        original_row = original_rows_by_source.get(source_key) or original_rows_by_source.get(source_base_key)
        if original_row:
            restored_row = dict(original_row)
            restored_row["source_name"] = source_name
            restored_row["manual"] = parse_form_bool(str(restored_row.get("manual", "0")))
            restored_row["target_name"] = build_target_name_from_row(restored_row, start_folder)
            reset_rows.append(restored_row)
            baseline_restored += 1
            continue

        gemini_payload = gemini_payload_map.get(gemini_source_row_cache_key(source_name))
        if not gemini_payload:
            source_base = Path(source_name).name
            if source_base and source_base != source_name:
                gemini_payload = gemini_payload_map.get(gemini_source_row_cache_key(source_base))
        if gemini_payload:
            reset_rows.append(rebuild_editor_row_from_gemini_cache(row, gemini_payload, start_folder))
            gemini_restored += 1
            continue

        reset_rows.append(dict(row))
        unchanged_rows += 1

    try:
        cleared = clear_editor_override_cache_rows(reset_sources)
    except Exception as exc:
        return jsonify({"ok": False, "error": f"Reset-Cache-Loeschung fehlgeschlagen: {exc}"}), 500

    pending["editor_rows"] = clone_json_like(normalize_editor_rows_payload(reset_rows, start_folder))
    pending["file_count"] = len(pending["editor_rows"])
    pending["updated_at"] = int(time.time())
    if not write_confirmation_payload(pending):
        return jsonify({"ok": False, "error": "Freigabe konnte nicht zurückgesetzt werden"}), 500

    rows_out = collect_editor_rows_from_payload(pending)
    return jsonify(
        {
            "ok": True,
            "rows": rows_out,
            "reset_sources": len(reset_sources),
            "reset_scope": reset_scope,
            "gemini_restored": gemini_restored,
            "baseline_restored": baseline_restored,
            "session_restored": session_restored,
            "source_guess_restored": 0,
            "unchanged_rows": unchanged_rows,
            "editor_cache_cleared": int(cleared),
        }
    )


@app.route("/api/confirm/editor/manifest/clean", methods=["POST"])
def api_confirm_editor_manifest_clean():
    payload = request.get_json(silent=True) or {}
    token = str(payload.get("token", "") or "").strip()
    pending, error = get_pending_confirmation_for_token(token)
    if not pending:
        return jsonify({"ok": False, "error": error}), 409

    start_folder = str(pending.get("_start_folder", "") or pending.get("start_folder", "")).strip()
    rows = collect_editor_rows_from_payload(pending)

    source_filter_any = payload.get("source_names", [])
    source_filter_set: set[str] | None = None
    if isinstance(source_filter_any, list):
        normalized_filter = {
            normalize_source_row_name_for_gemini(str(item or "").strip())
            for item in source_filter_any
            if str(item or "").strip()
        }
        if normalized_filter:
            source_filter_set = normalized_filter

    cache_sources: list[str] = []
    cache_seen: set[str] = set()
    for row in rows:
        source_name = str(row.get("source_name", "") or "").strip()
        if not source_name:
            continue
        if not source_name_matches_filter(source_name, source_filter_set):
            continue
        source_key = normalize_source_row_name_for_gemini(source_name)
        if not source_key or source_key in cache_seen:
            continue
        cache_seen.add(source_key)
        cache_sources.append(source_name)

    ok, result, clean_error = clean_manifest_for_editor_rows(
        start_folder,
        rows,
        source_filter_set=source_filter_set,
    )
    if not ok:
        return jsonify({"ok": False, "error": clean_error}), 400

    done_reset_rows = 0
    if isinstance(pending.get("editor_rows"), list):
        updated_rows, done_reset_rows, done_sources = reset_editor_done_state_for_sources(
            pending.get("editor_rows", []),
            source_filter_set=source_filter_set,
        )
        if done_sources:
            done_seen = {normalize_source_row_name_for_gemini(name) for name in cache_sources}
            for source_name in done_sources:
                source_key = normalize_source_row_name_for_gemini(source_name)
                if source_key and source_key not in done_seen:
                    done_seen.add(source_key)
                    cache_sources.append(source_name)
        pending["editor_rows"] = updated_rows
        pending["file_count"] = len(updated_rows)
        pending["updated_at"] = int(time.time())
        if not write_confirmation_payload(pending):
            return jsonify({"ok": False, "error": "Freigabe konnte nicht aktualisiert werden"}), 500

    editor_cache_cleared = 0
    history_cache_cleared = 0
    cache_clear_error = ""
    if cache_sources:
        try:
            editor_cache_cleared = int(clear_editor_override_cache_rows(cache_sources))
            history_cache_cleared = int(clear_processed_history_cache_rows(cache_sources))
        except Exception as exc:
            cache_clear_error = str(exc)

    payload_out = dict(result)
    payload_out["done_reset_rows"] = int(done_reset_rows)
    payload_out["editor_cache_cleared"] = int(editor_cache_cleared)
    payload_out["history_cache_cleared"] = int(history_cache_cleared)
    if cache_clear_error:
        payload_out["cache_clear_error"] = cache_clear_error
    return jsonify({"ok": True, **payload_out})


@app.route("/api/state")
def api_state():
    maybe_auto_dispatch_pending_confirmation()
    with job_lock:
        job = current_job
        if job is not None:
            job_data = job_to_dict(job)
        else:
            job_data = fallback_job_data()
    dispatch_data = dispatch_job_data()
    if dispatch_data:
        local_running = bool(job_data.get("running"))
        dispatch_running = bool(dispatch_data.get("running"))
        dispatch_started = float(dispatch_data.get("started_at") or 0.0)
        dispatch_ended = float(dispatch_data.get("ended_at") or 0.0)
        local_started = float(job_data.get("started_at") or 0.0)
        local_ended = float(job_data.get("ended_at") or 0.0)
        if not local_running and (
            dispatch_running
            or dispatch_started >= local_started
            or dispatch_ended >= local_ended
        ):
            job_data = dispatch_data

    if not job_data.get("exists"):
        fallback = fallback_job_data()
        if fallback.get("exists"):
            job_data = fallback

    current_version = current_release_version()
    if job_data.get("release_version") in (None, "", "-"):
        job_data["release_version"] = current_version
    elif not bool(job_data.get("running")):
        # Keep UI version stable after upgrades even if the newest in-memory/last log entry is older.
        job_data["release_version"] = current_version

    runner_log = tail_file(Path(job_data.get("log_path", "")), lines=180) if job_data.get("exists") else ""
    full_log_requested = (request.args.get("full_log", "") or "").strip().lower() in {"1", "true", "yes", "on"}
    max_log_chars = 2400000
    try:
        max_log_chars = int((request.args.get("log_max_chars", "") or "").strip() or max_log_chars)
    except Exception:
        max_log_chars = 2400000
    max_log_chars = max(32000, min(max_log_chars, 5000000))
    if full_log_requested:
        processing_log_text = read_file_full(PROCESSING_LOG_FILE, max_chars=max_log_chars)
    else:
        log_lines = 1200
        try:
            log_lines = int((request.args.get("log_lines", "") or "").strip() or log_lines)
        except Exception:
            log_lines = 1200
        log_lines = max(120, min(log_lines, 12000))
        processing_log_text = tail_file(PROCESSING_LOG_FILE, lines=log_lines, max_chars=max_log_chars)
    pending_payload = read_pending_confirmation_payload()
    pending_summary = summarize_pending_confirmation(pending_payload)
    status_table_text = read_file_full(STATUS_FILE, max_chars=1200000)
    pending_status_override = build_status_table_override_from_pending_payload(pending_payload)
    if pending_status_override:
        status_table_text = pending_status_override

    runtime_settings = read_runtime_settings()
    worker_states = read_worker_states()
    active_workers = [worker for worker in worker_states if int(worker.get("active_count", 0) or 0) > 0]
    worker_job_map = build_worker_job_map()
    if not worker_job_map and active_workers:
        worker_job_map = build_live_worker_job_map(worker_states)
    live_worker_status_text = ""
    for worker in active_workers:
        candidate = str(worker.get("status_table_text", "") or "").strip()
        if candidate:
            live_worker_status_text = candidate
            break
    live_worker_combined_status_text = build_live_worker_status_table_text(worker_states)
    live_worker_fallback = False
    if active_workers and not bool(job_data.get("running")):
        worker_names = sorted({str(worker.get("name", "") or "").strip() for worker in active_workers if str(worker.get("name", "") or "").strip()})
        if worker_names:
            job_data["running"] = True
            job_data["mode"] = "ffmpeg"
            job_data["worker_name"] = ", ".join(worker_names)
            job_data["_live_worker_fallback"] = True
            live_worker_fallback = True
    folder_hint = str(job_data.get("folder", "") or "").strip()
    dispatch_status_table = build_dispatch_status_table_text(folder_hint)
    dispatch_status_matches_live = _status_text_covers_active_workers(dispatch_status_table, worker_states)
    if active_workers and bool(job_data.get("running")):
        if dispatch_status_table and dispatch_status_matches_live:
            status_table_text = dispatch_status_table
        elif live_worker_combined_status_text:
            status_table_text = live_worker_combined_status_text
        elif live_worker_status_text:
            status_table_text = live_worker_status_text
    elif not status_table_text and active_workers:
        if dispatch_status_table and dispatch_status_matches_live:
            status_table_text = dispatch_status_table
        elif live_worker_combined_status_text:
            status_table_text = live_worker_combined_status_text
        elif live_worker_fallback:
            status_table_text = str(active_workers[0].get("status_table_text", "") or "").strip()
        else:
            status_table_text = str(active_workers[0].get("status_table_text", "") or "").strip()
    elif dispatch_data and dispatch_status_table:
        status_table_text = dispatch_status_table
    if not processing_log_text and active_workers:
        processing_log_text = build_live_processing_log_from_workers(worker_states)
    if live_worker_fallback:
        runner_log = ""
    if not processing_log_text and bool(job_data.get("running")):
        processing_log_text = runner_log
    overlay_text = ""
    if bool(job_data.get("running")) and not live_worker_fallback:
        overlay_text = build_processing_log_overlay(job_data, runner_log)
    if overlay_text:
        if processing_log_text:
            processing_log_text = f"{overlay_text}\n\n{processing_log_text}"
        else:
            processing_log_text = overlay_text
    ui_state = read_state_values(
        {
            STATE_KEY_LAST_MODE: "analyze",
            STATE_KEY_LAST_ENCODER: "cpu",
        }
    )
    mode = normalize_mode(ui_state.get(STATE_KEY_LAST_MODE, "")) or "analyze"
    encoder = coerce_encoder_for_ui(ui_state.get(STATE_KEY_LAST_ENCODER, ""))

    payload = {
        "job": job_data,
        "settings": {
            "mode": mode,
            "encoder": encoder,
            "target_nfs_path": runtime_settings.get("target_nfs_path", DEFAULT_TARGET_NFS_PATH),
            "target_out_path": runtime_settings.get("target_out_path", DEFAULT_TARGET_OUT_PATH),
            "target_reenqueue_path": normalize_target_reenqueue_path(
                runtime_settings.get("target_reenqueue_path", ""),
                runtime_settings.get("target_out_path", DEFAULT_TARGET_OUT_PATH),
            ),
            "nas_ip": runtime_settings.get("nas_ip", DEFAULT_NAS_IP),
            "plex_ip": runtime_settings.get("plex_ip", DEFAULT_PLEX_IP),
            "encode_once_4k": runtime_settings.get("encode_once_4k", "1"),
            "parallel_job_limit": runtime_settings.get("parallel_job_limit", "2"),
            "initial_setup_done": parse_form_bool(runtime_settings.get("initial_setup_done", "1")),
            "initial_setup_required": parse_form_bool(runtime_settings.get("initial_setup_required", "0")),
        },
        "workers": worker_states,
        "worker_job_map": worker_job_map,
        "status_table": status_table_text,
        "processing_log": processing_log_text,
        "out_plan": tail_file(OUT_PLAN_FILE, lines=240),
        "out_tree": read_file_full(OUT_TREE_FILE, max_chars=1200000),
        "runner_log": runner_log,
        "pending_confirmation": pending_summary,
        "paths": {
            "work": str(WORK_DIR),
            "temp": str(TEMP_DIR),
            "status_file": str(STATUS_FILE),
            "processing_file": str(PROCESSING_LOG_FILE),
            "out_plan_file": str(OUT_PLAN_FILE),
        },
        "versioning": {
            "min": format_release_version(VERSION_MIN_PATCH),
            "max": format_release_version(VERSION_MAX_PATCH),
            "current": current_version,
            "range_text": release_range_text(),
        },
        "now": time.time(),
    }
    return jsonify(payload)


@app.route("/log-window")
def log_window():
    source_titles = {
        "job": "Job Status",
        "summary": "Summary",
        "status": "STATUS Queue",
        "proc": "LOG",
        "plan": "OUT Tree",
    }
    source = (request.args.get("source", "") or "").strip().lower()
    if source not in source_titles:
        source = "proc"

    title = (request.args.get("title", "") or "").strip()
    if not title:
        title = source_titles[source]
    title = re.sub(r"\s+", " ", title)[:120] or source_titles[source]
    token = str(request.args.get("token", "") or "").strip()

    return nocache_html_response(
        render_template_string(
            LOG_WINDOW_TEMPLATE,
            log_source=source,
            title=title,
            token=token,
            version_current=current_release_version(),
            site_title=detect_site_title(),
        )
    )


@app.route("/confirm-window")
def confirm_window():
    token = str(request.args.get("token", "") or "").strip()
    return nocache_html_response(
        render_template_string(
            CONFIRM_WINDOW_TEMPLATE,
            version_current=current_release_version(),
            token=token,
            site_title=detect_site_title(),
        )
    )


@app.route("/confirm-editor-window")
def confirm_editor_window():
    token = str(request.args.get("token", "") or "").strip()
    pending_payload: dict[str, Any] | None = None
    if token:
        pending_payload, _ = get_pending_confirmation_for_token(token)
    if not pending_payload:
        pending_payload = read_pending_confirmation_payload()
    initial_rows: list[dict[str, Any]] = []
    initial_start_folder = ""
    initial_target_out_prefix = "__OUT"
    initial_target_reenqueue_prefix = "__RE-ENQUEUE"
    initial_token = token
    if pending_payload:
        try:
            initial_rows = collect_editor_rows_from_payload(pending_payload)
        except Exception:
            initial_rows = []
        initial_start_folder = str(
            pending_payload.get("_start_folder", "") or pending_payload.get("start_folder", "")
        ).strip()
        try:
            initial_target_out_prefix = str(target_out_prefix_for_start(initial_start_folder))
        except Exception:
            initial_target_out_prefix = "__OUT"
        try:
            initial_target_reenqueue_prefix = str(target_reenqueue_prefix_for_start(initial_start_folder))
        except Exception:
            initial_target_reenqueue_prefix = "__RE-ENQUEUE"
        initial_token = str(pending_payload.get("_token", "") or pending_payload.get("token", "")).strip() or token
    return nocache_html_response(
        render_template_string(
            CONFIRM_EDITOR_TEMPLATE,
            version_current=current_release_version(),
            token=initial_token,
            initial_rows=initial_rows,
            initial_start_folder=initial_start_folder,
            initial_target_out_prefix=initial_target_out_prefix,
            initial_target_reenqueue_prefix=initial_target_reenqueue_prefix,
            site_title=detect_site_title(),
        )
    )


@app.route("/stop-window")
def stop_window():
    return nocache_html_response(
        render_template_string(
            STOP_WINDOW_TEMPLATE,
            version_current=current_release_version(),
            site_title=detect_site_title(),
        )
    )


@app.route("/restart-window")
def restart_window():
    return nocache_html_response(
        render_template_string(
            RESTART_WINDOW_TEMPLATE,
            version_current=current_release_version(),
            site_title=detect_site_title(),
        )
    )


@app.route("/update-window")
def update_window():
    return nocache_html_response(
        render_template_string(
            UPDATE_WINDOW_TEMPLATE,
            version_current=current_release_version(),
            site_title=detect_site_title(),
        )
    )


@app.route("/browse")
def browse():
    current = normalize_browse_path(request.args.get("folder"))
    target = normalize_browse_target(request.args.get("target"))
    root = normalize_browse_root(BROWSE_ROOT)
    parent = current.parent if current != root else root
    entries = list_child_dirs(current)
    crumbs = build_browse_crumbs(current, root)

    return nocache_html_response(
        render_template_string(
            BROWSE_TEMPLATE,
            target=target,
            current=str(current),
            parent=str(parent),
            root=str(root),
            entries=entries,
            crumbs=crumbs,
            site_title=detect_site_title(),
        )
    )


LOG_WINDOW_TEMPLATE = """
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ site_title }} | {{ title }}</title>
  <script>
    (function () {
      try {
        const theme = (localStorage.getItem('managemovie.theme') || 'light').toLowerCase() === 'dark' ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', theme);
      } catch (err) {
        document.documentElement.setAttribute('data-theme', 'light');
      }
    })();
  </script>
  <style>
    html, body {
      height: 100%;
    }
    :root {
      --bg: #e8eef8;
      --bg-soft: #d9e5fb;
      --ink: #101828;
      --panel: rgba(255, 255, 255, 0.86);
      --line: rgba(70, 84, 104, 0.2);
    }
    body {
      margin: 0;
      padding: clamp(12px, 1.8vw, 20px);
      font-family: "SF Pro Rounded", "SF Pro Display", "SF Pro Text", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", sans-serif;
      background:
        radial-gradient(900px 420px at 8% -6%, #ffffff 0%, var(--bg-soft) 42%, transparent 70%),
        linear-gradient(180deg, #edf3ff 0%, var(--bg) 100%);
      color: var(--ink);
      box-sizing: border-box;
      overflow: hidden;
    }
    .card {
      max-width: 98vw;
      margin: 0 auto;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 12px;
      box-shadow: 0 24px 48px rgba(24, 39, 75, 0.18);
      display: flex;
      flex-direction: column;
      gap: 8px;
      height: 100%;
      min-height: 0;
      box-sizing: border-box;
      backdrop-filter: blur(18px) saturate(130%);
      -webkit-backdrop-filter: blur(18px) saturate(130%);
    }
    .window-main {
      flex: 1 1 auto;
      min-height: 0;
      display: flex;
      flex-direction: column;
      gap: 8px;
      padding-right: 8px;
      overflow: hidden;
    }
    .head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      flex-wrap: wrap;
    }
    .head.status-waiting-head {
      position: relative;
      min-height: 44px;
    }
    .head.status-waiting-head h1 {
      position: absolute;
      left: 50%;
      transform: translateX(-50%);
      max-width: calc(100% - 220px);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      text-align: center;
    }
    .head.status-waiting-head .actions {
      margin-left: auto;
      position: relative;
      z-index: 1;
    }
    h1 {
      margin: 0;
      font-size: 1.1rem;
      display: inline-flex;
      align-items: center;
      gap: 10px;
    }
    .run-dot {
      width: 18px;
      height: 18px;
      border-radius: 999px;
      border: 2px solid rgba(0, 0, 0, 0.2);
      background: #9aa3ad;
      box-shadow: 0 0 0 3px rgba(255, 255, 255, 0.9) inset;
      flex: 0 0 auto;
    }
    .run-dot.running { background: #29a745; }
    .run-dot.stopped { background: #9aa3ad; }
    .actions {
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .status-error-indicator {
      display: inline-flex;
      align-items: center;
      min-height: 0;
      padding: 0;
      border: 0;
      background: transparent;
      color: #1f3f77;
      font-size: 0.88rem;
      font-weight: 600;
      white-space: nowrap;
    }
    .status-exit-btn {
      min-width: 92px !important;
      font-size: 0.95rem !important;
      font-weight: 800 !important;
      letter-spacing: 0.01em;
    }
    button {
      border: 1px solid rgba(70, 84, 104, 0.3);
      background: rgba(255, 255, 255, 0.88);
      color: #1f3450;
      border-radius: 10px;
      padding: 8px 11px;
      font-size: 0.92rem;
      font-weight: 700;
      line-height: 1;
      min-height: 42px;
      cursor: pointer;
    }
    button[title="Klein"],
    button[title="Einklappen"],
    button[title="Neues Fenster"],
    button[title="Verzeichnis-Auswahl"] {
      font-size: 1.26rem;
      font-weight: 900;
      line-height: 1;
      min-width: 46px;
    }
    button[data-tip] {
      position: relative;
    }
    button[data-tip]:hover::after {
      content: attr(data-tip);
      position: absolute;
      left: 50%;
      bottom: calc(100% + 10px);
      transform: translateX(-50%);
      padding: 8px 11px;
      border-radius: 9px;
      background: rgba(23, 32, 28, 0.96);
      color: #f6fbf8;
      border: 1px solid rgba(227, 239, 233, 0.45);
      font-size: 1.02rem;
      font-weight: 800;
      line-height: 1.15;
      white-space: nowrap;
      box-shadow: 0 8px 20px rgba(0, 0, 0, 0.24);
      z-index: 10030;
      pointer-events: none;
    }
    button[data-tip]:hover::before {
      content: "";
      position: absolute;
      left: 50%;
      bottom: calc(100% + 3px);
      transform: translateX(-50%);
      border-left: 7px solid transparent;
      border-right: 7px solid transparent;
      border-top: 8px solid rgba(23, 32, 28, 0.96);
      z-index: 10031;
      pointer-events: none;
    }
    button.active {
      background: rgba(10, 132, 255, 0.16);
      border-color: rgba(10, 132, 255, 0.52);
      color: #0b4e96;
    }
    .hidden {
      display: none !important;
    }
    .log-box {
      margin: 0;
      border: 1px solid rgba(70, 84, 104, 0.22);
      border-radius: 12px;
      padding: 10px;
      background: rgba(255, 255, 255, 0.88);
      color: #0f1f33;
      flex: 1 1 auto;
      min-height: 0;
      overflow: auto;
      white-space: pre;
      word-break: normal;
      font-family: "SF Pro Rounded", "SF Pro Display", "SF Pro Text", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", sans-serif;
      font-size: 0.86rem;
      line-height: 1.28;
      user-select: text;
      -webkit-user-select: text;
      cursor: text;
    }
    .summary-kv-wrap {
      flex: 1 1 auto;
      min-height: 0;
      max-height: none;
      overflow: auto;
      border: 1px solid rgba(70, 84, 104, 0.22);
      border-radius: 12px;
      background: linear-gradient(145deg, #fbfdff 0%, #eef4ff 100%);
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.85);
      padding: 8px 10px;
    }
    .summary-kv-table {
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      table-layout: auto;
      color: #102c22;
      font-size: 0.94rem;
      line-height: 1.4;
    }
    .summary-kv-table tr + tr th,
    .summary-kv-table tr + tr td {
      border-top: 1px solid #d9e9e1;
    }
    .summary-kv-table th {
      width: 170px;
      max-width: 42%;
      text-align: left;
      white-space: nowrap;
      padding: 8px 10px 8px 4px;
      color: #1f4b3f;
      font-weight: 600;
      vertical-align: top;
    }
    .summary-kv-table td {
      text-align: left;
      white-space: normal;
      word-break: break-word;
      padding: 8px 2px 8px 8px;
      color: #0f2a20;
      font-weight: 500;
      vertical-align: top;
    }
    .summary-kv-table td.summary-file-cell {
      font-size: 0.84rem;
      line-height: 1.32;
    }
    .status-table-wrap {
      flex: 1 1 auto;
      min-height: 0;
      max-height: none;
      overflow: auto;
      border: 1px solid rgba(70, 84, 104, 0.22);
      border-radius: 8px;
      background: #f9faff;
      padding-right: 8px;
      box-sizing: border-box;
    }
    .status-table {
      width: max-content;
      min-width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      color: #0b1a15;
      font-size: 0.84rem;
      line-height: 1.28;
    }
    .status-table thead th {
      position: sticky;
      top: 0;
      z-index: 2;
      text-align: left;
      font-weight: 700;
      background: #edf2ff;
      border-bottom: 1px solid #d8e0f0;
      padding: 8px 10px;
      white-space: nowrap;
      cursor: pointer;
      user-select: none;
    }
    .status-table thead th.sort-asc,
    .status-table thead th.sort-desc {
      background: #dbe6ff;
      color: #133a79;
    }
    .status-table tbody td {
      border-bottom: 1px solid #e4eaf7;
      padding: 7px 10px;
      white-space: nowrap;
      vertical-align: top;
    }
    .status-table th.status-col-source,
    .status-table td.status-col-source {
      width: 1%;
      min-width: 190px;
      max-width: 360px;
    }
    .status-table th.status-col-target,
    .status-table td.status-col-target {
      min-width: 240px;
      max-width: 520px;
    }
    .status-table td.status-col-source {
      white-space: nowrap;
      word-break: normal;
      overflow: hidden;
      text-overflow: ellipsis;
      line-height: 1.28;
    }
    .status-table td.status-col-target {
      white-space: normal;
      word-break: break-word;
      overflow: visible;
      text-overflow: clip;
      line-height: 1.32;
    }
    .status-table tbody tr:nth-child(even) {
      background: #f9faff;
    }
    .status-table tr.status-row-missing td {
      background: #fde8e8 !important;
      color: #7a1b17;
    }
    .status-table tr.status-row-active td {
      background: #f3d76a !important;
      color: #3e2d00;
      box-shadow: none;
    }
    .status-table tr.status-row-done td {
      background: #e8f7ec !important;
      color: #134226;
    }
    .status-table.status-filter-errors tbody tr[data-filter-row]:not(.status-row-missing) {
      display: none;
    }
    .status-table.status-filter-done tbody tr[data-filter-row]:not(.status-row-done) {
      display: none;
    }
    .status-table.status-filter-encode tbody tr[data-filter-row]:not(.status-row-encode) {
      display: none;
    }
    .status-table.status-filter-copy tbody tr[data-filter-row]:not(.status-row-copy) {
      display: none;
    }
    #statusTableEmpty {
      color: #304567;
      font-style: italic;
      white-space: normal;
    }
    .summary-ampel {
      border: 1px solid #d9dfee;
      border-radius: 8px;
      background: #f9fafe;
      padding: 8px 10px;
      display: grid;
      gap: 5px;
    }
    .summary-ampel-row {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 0.9rem;
      color: #12263f;
      line-height: 1.2;
      border-radius: 999px;
      padding: 2px 8px;
      border: 1px solid transparent;
    }
    .summary-ampel-row.bar {
      background: #fff1a8;
      border-color: #efd669;
    }
    .summary-ampel-dot {
      width: 11px;
      height: 11px;
      border-radius: 999px;
      display: inline-block;
      border: 1px solid rgba(0, 0, 0, 0.22);
      box-sizing: border-box;
      flex: 0 0 auto;
    }
    .summary-ampel-dot.gray { background: #aab3be; }
    .summary-ampel-dot.yellow { background: #facc15; }
    .summary-ampel-dot.green { background: #30d158; }
    .confirm-panel {
      display: flex;
      flex-direction: column;
      gap: 8px;
      align-items: stretch;
      border: 1px solid #d9dfee;
      border-radius: 8px;
      background: #f8fafe;
      padding: 8px 10px;
      flex: 0 0 auto;
      margin-top: auto;
    }
    .confirm-panel button {
      min-width: 118px;
      border-color: rgba(70, 84, 104, 0.28);
      background: rgba(255, 255, 255, 0.86);
      color: #20344f;
      font-weight: 800;
    }
    .confirm-panel button.primary {
      border-color: rgba(70, 84, 104, 0.28);
      background: rgba(255, 255, 255, 0.86);
      color: #20344f;
    }
    .confirm-panel button.clean {
      border-color: rgba(70, 84, 104, 0.28);
      background: rgba(255, 255, 255, 0.86);
      color: #20344f;
    }
    .confirm-panel button.stop {
      border-color: rgba(70, 84, 104, 0.28);
      background: rgba(255, 255, 255, 0.86);
      color: #20344f;
    }
    .confirm-msg {
      color: #1a365c;
      min-height: 20px;
      white-space: pre-wrap;
      font-size: 0.9rem;
      flex: 0 0 auto;
    }
    .window-status-footer {
      display: flex;
      justify-content: flex-end;
      margin-top: 8px;
    }
    .footer-exit-btn {
      min-width: 88px;
      min-height: 36px;
      padding: 7px 12px;
      border-color: #0a84ff !important;
      background: #0a84ff !important;
      color: #f7fbff !important;
      font-weight: 800;
    }
    .footer-exit-btn:hover {
      background: #267fff !important;
      border-color: #267fff !important;
      color: #ffffff !important;
    }
    html[data-theme="dark"] body {
      background:
        radial-gradient(900px 420px at 8% -6%, #2b3750 0%, #101521 46%, transparent 70%),
        linear-gradient(180deg, #0c111b 0%, #090d15 100%);
      color: #e6edf8;
    }
    html[data-theme="dark"] .card {
      background: rgba(18, 24, 36, 0.84);
      border-color: rgba(136, 156, 186, 0.34);
      box-shadow: 0 26px 48px rgba(0, 0, 0, 0.42);
    }
    html[data-theme="dark"] pre,
    html[data-theme="dark"] .summary-ampel,
    html[data-theme="dark"] .status-table-wrap,
    html[data-theme="dark"] .confirm-panel {
      background: #111827;
      border-color: rgba(136, 156, 186, 0.34);
      color: #dbe6f7;
    }
    html[data-theme="dark"] .confirm-panel button,
    html[data-theme="dark"] .confirm-panel button.primary,
    html[data-theme="dark"] .confirm-panel button.clean,
    html[data-theme="dark"] .confirm-panel button.stop {
      background: #1b2436;
      border-color: rgba(136, 156, 186, 0.4);
      color: #e6edf8;
    }
    html[data-theme="dark"] .footer-exit-btn {
      background: #2f70ff !important;
      border-color: #2f70ff !important;
      color: #f7fbff !important;
    }
    html[data-theme="dark"] .footer-exit-btn:hover {
      background: #4a84ff !important;
      border-color: #4a84ff !important;
      color: #ffffff !important;
    }
    html[data-theme="dark"] .summary-kv-wrap {
      background: #111827;
      border-color: rgba(136, 156, 186, 0.34);
      color: #cbd8ea;
    }
    html[data-theme="dark"] .summary-kv-table,
    html[data-theme="dark"] .summary-kv-table th,
    html[data-theme="dark"] .summary-kv-table td {
      color: #cbd8ea;
    }
    html[data-theme="dark"] .summary-kv-table tr + tr th,
    html[data-theme="dark"] .summary-kv-table tr + tr td {
      border-top-color: rgba(136, 156, 186, 0.28);
    }
    html[data-theme="dark"] .summary-ampel-row {
      color: #c8d5e8;
    }
    html[data-theme="dark"] .status-table th {
      background: #1b2436;
      color: #dbe6f7;
    }
    html[data-theme="dark"] .status-table tbody tr {
      background: #0f1726;
    }
    html[data-theme="dark"] .status-table tbody tr:nth-child(even) {
      background: #111b2c;
    }
    html[data-theme="dark"] .status-table td {
      border-color: rgba(136, 156, 186, 0.24);
      color: #dbe6f7;
    }
    html[data-theme="dark"] .status-table tr.status-row-missing td {
      background: #4e2630 !important;
      color: #ffd8df;
    }
    html[data-theme="dark"] .status-table tr.status-row-active td {
      background: #6f5913 !important;
      color: #fff2bf;
      box-shadow: none;
    }
    html[data-theme="dark"] .status-table tr.status-row-done td {
      background: #163826 !important;
      color: #cfeedd;
    }
    html[data-theme="dark"] button {
      background: #1b2436;
      border-color: rgba(136, 156, 186, 0.44);
      color: #e5eefc;
    }
    pre,
    .status-table-wrap,
    .summary-kv-wrap {
      scrollbar-color: #b8c8e8 #eef3ff;
      scrollbar-width: thin;
    }
    pre::-webkit-scrollbar,
    .status-table-wrap::-webkit-scrollbar,
    .summary-kv-wrap::-webkit-scrollbar {
      width: 12px;
      height: 12px;
    }
    pre::-webkit-scrollbar-track,
    .status-table-wrap::-webkit-scrollbar-track,
    .summary-kv-wrap::-webkit-scrollbar-track {
      background: #eef3ff;
      border-radius: 999px;
    }
    pre::-webkit-scrollbar-thumb,
    .status-table-wrap::-webkit-scrollbar-thumb,
    .summary-kv-wrap::-webkit-scrollbar-thumb {
      background: #bccbe7;
      border-radius: 999px;
      border: 2px solid #eef3ff;
    }
    html[data-theme="dark"] pre,
    html[data-theme="dark"] .status-table-wrap,
    html[data-theme="dark"] .summary-kv-wrap {
      scrollbar-color: #223349 #050a12;
    }
    html[data-theme="dark"] pre::-webkit-scrollbar-track,
    html[data-theme="dark"] .status-table-wrap::-webkit-scrollbar-track,
    html[data-theme="dark"] .summary-kv-wrap::-webkit-scrollbar-track {
      background: #050a12;
    }
    html[data-theme="dark"] pre::-webkit-scrollbar-thumb,
    html[data-theme="dark"] .status-table-wrap::-webkit-scrollbar-thumb,
    html[data-theme="dark"] .summary-kv-wrap::-webkit-scrollbar-thumb {
      background: #223349;
      border-color: #050a12;
    }
  </style>
</head>
<body>
  <div class="card">
    <div id="logWindowHead" class="head">
      <h1 id="pageTitle"><span id="pageTitleText">{{ site_title }} {{ version_current }} | {{ title }}</span><span id="pageRunDot" class="run-dot stopped" aria-hidden="true"></span></h1>
      <div class="actions">
        <span id="onlyMissingInfo" class="status-error-indicator hidden">Fehler 0/0 | Erledigt 0/0</span>
        <button id="onlyMissingBtn" class="hidden" type="button" onclick="toggleOnlyMissing()">Alle</button>
        <button id="windowCloseBtn" type="button" title="Einklappen" aria-label="Einklappen" onclick="window.close()">↙</button>
      </div>
    </div>
    <div class="window-main">
      <div id="summaryAmpelBox" class="summary-ampel hidden"></div>
      <pre id="logBox" class="log-box">lade...</pre>
      <div id="summaryWrap" class="summary-kv-wrap hidden">
        <table id="summaryKvTable" class="summary-kv-table">
          <tbody id="summaryKvBody">
            <tr><th>Status</th><td>lade...</td></tr>
          </tbody>
        </table>
      </div>
      <div id="statusWrap" class="status-table-wrap hidden">
        <table id="statusTable" class="status-table">
          <thead id="statusHead"></thead>
          <tbody id="statusBody">
            <tr><td id="statusTableEmpty" colspan="1">lade...</td></tr>
          </tbody>
        </table>
      </div>
    </div>
    <div id="confirmPanel" class="confirm-panel hidden">
      <button id="confirmCopyBtn" type="button" class="primary hidden" onclick="submitPendingDecision('copy')">Copy</button>
      <button id="confirmEncodeBtn" type="button" class="primary hidden" onclick="submitPendingDecision('encode')">Encode</button>
      <button id="confirmAnalyzeBtn" type="button" class="primary hidden" onclick="submitPendingDecision('ok')">Analyze OK</button>
      <button id="confirmCleanBtn" type="button" class="clean hidden" onclick="submitPendingDecision('clean')">Reset "Erledigt"</button>
      <button id="confirmEditBtn" type="button" onclick="submitPendingDecision('edit')">Editor</button>
      <button id="confirmCancelBtn" type="button" class="stop hidden" onclick="submitPendingDecision('cancel')">Exit</button>
    </div>
    <div id="windowStatusFooter" class="window-status-footer">
      <button type="button" class="footer-exit-btn" onclick="window.close()">Exit</button>
    </div>
    <div id="confirmMsg" class="confirm-msg"></div>
  </div>
  <script>
    function applyThemeFromStorage() {
      try {
        const t = (localStorage.getItem('managemovie.theme') || 'light').toLowerCase() === 'dark' ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', t);
      } catch (err) {
        document.documentElement.setAttribute('data-theme', 'light');
      }
    }
    applyThemeFromStorage();
    window.addEventListener('storage', (event) => {
      if (event && event.key === 'managemovie.theme') applyThemeFromStorage();
    });
    const LOG_SOURCE = "{{ log_source }}";
    const LOG_TITLE = "{{ title }}";
    const SITE_TITLE = {{ site_title|tojson }};
    let modalVersion = "{{ version_current }}";
    const state = {
      headers: [],
      rows: [],
      sortIndex: -1,
      sortDir: "asc",
      filterMode: "all",
      emptyStreak: 0,
      activeKey: "",
      lastAutoScrollKey: "",
    };
    let pendingConfirm = null;
    let pendingConfirmInFlight = false;
    let pendingConfirmFilterToken = "";
    let pendingTokenFromUrl = "{{ token }}";
    let statusFilterModeContext = "analyze";
    let logWindowJobRunning = false;
    let currentWorkerStates = [];
    let currentWorkerJobMap = {};
    let currentEncodeWorkerName = "";
    const preLocks = {};
    const summaryAmpelRows = [
      { key: "analyze", label: "Analyze" },
      { key: "copy", label: "Copy" },
      { key: "encode", label: "Encode" },
      { key: "sync_nas", label: "Sync NAS" },
      { key: "sync_plex", label: "Sync Plex" },
      { key: "del_out", label: "Lösche OUT" },
      { key: "del_source", label: "Lösche Quelle" },
    ];

    function renderWorkers(workers) {
      currentWorkerStates = Array.isArray(workers) ? workers : [];
    }

    function updatePageTitle() {
      const isWaiting = String(LOG_SOURCE || "").trim() === "status" && !!pendingConfirm && !!String(pendingConfirm.token || "").trim();
      const titleText = isWaiting ? "Warte auf Freigabe" : (LOG_TITLE || "Log");
      const text = `${SITE_TITLE} ${modalVersion || "-"} | ${titleText}`;
      document.title = text;
      const titleEl = document.getElementById("pageTitleText");
      if (titleEl) titleEl.innerText = text;
      const headEl = document.getElementById("logWindowHead");
      if (headEl) headEl.classList.toggle("status-waiting-head", isWaiting);
      updateWindowCloseButton();
    }

    function updateWindowCloseButton() {
      const btn = document.getElementById("windowCloseBtn");
      if (!btn) return;
      btn.classList.remove("status-exit-btn");
      btn.title = "Einklappen";
      btn.setAttribute("aria-label", "Einklappen");
      btn.innerText = "↙";
    }

    function setRunDot(running) {
      const dot = document.getElementById("pageRunDot");
      if (!dot) return;
      const isRunning = !!running;
      dot.classList.toggle("running", isRunning);
      dot.classList.toggle("stopped", !isRunning);
      dot.title = isRunning ? "Job läuft" : "Kein laufender Job";
      dot.setAttribute("aria-label", isRunning ? "Job läuft" : "Kein laufender Job");
    }

    function lockPre(id, durationMs = 5000) {
      const key = String(id || "").trim();
      if (!key) return;
      preLocks[key] = Date.now() + Math.max(0, Number(durationMs) || 0);
    }

    function isPreLocked(id) {
      const key = String(id || "").trim();
      if (!key) return false;
      const until = Number(preLocks[key] || 0);
      if (!until) return false;
      if (Date.now() <= until) return true;
      delete preLocks[key];
      return false;
    }

    function isSelectionInside(el) {
      if (!el || typeof window.getSelection !== "function") return false;
      const sel = window.getSelection();
      if (!sel || sel.rangeCount <= 0 || sel.isCollapsed) return false;
      const anchorNode = sel.anchorNode;
      const focusNode = sel.focusNode;
      return (!!anchorNode && el.contains(anchorNode)) || (!!focusNode && el.contains(focusNode));
    }

    function stripAnsi(text) {
      return String(text || "").replace(/\\x1B\\[[0-9;]*[A-Za-z]/g, "");
    }

    function isTableBorderLine(line) {
      return /^\\+(?:[=+\\-]+\\+)+$/.test(String(line || "").trim());
    }

    function splitStatusPanel(raw) {
      const lf = String.fromCharCode(10);
      const cr = String.fromCharCode(13);
      const text = stripAnsi(String(raw || "")).split(cr + lf).join(lf);
      const lines = text.split(lf);
      const tableStart = lines.findIndex((line) => {
        const t = String(line || "").trim();
        return t.startsWith("|") || isTableBorderLine(t);
      });
      const tableLines = tableStart >= 0 ? lines.slice(tableStart).filter((line) => !isTableBorderLine(line)) : [];
      const metaLines = tableStart >= 0 ? lines.slice(0, tableStart) : lines.slice();
      return {
        meta: metaLines.join(lf).trim(),
        table: tableLines.join(lf).trim(),
      };
    }

    function normalizeStatusFraction(value) {
      const m = String(value || "").trim().match(/^(\\d+)\\s*\\/\\s*(\\d+)$/);
      if (!m) return "";
      const left = Number(m[1]);
      const right = Number(m[2]);
      if (!Number.isFinite(left) || !Number.isFinite(right) || right <= 0) return "";
      return `${left}/${right}`;
    }

    function extractActiveStatusKey(metaText) {
      const lines = String(metaText || "").split(/\\r?\\n/);
      for (const line of lines) {
        const match = String(line || "").match(/^\\s*Aktiv:\\s*([0-9]+\\s*\\/\\s*[0-9]+)/i);
        if (match) return normalizeStatusFraction(match[1]);
      }
      return "";
    }

    function parseStatusRow(line) {
      const raw = String(line || "").trim();
      if (!raw.startsWith("|")) return [];
      let body = raw;
      if (body.startsWith("|")) body = body.slice(1);
      if (body.endsWith("|")) body = body.slice(0, -1);
      return body.split("|").map((cell) => cell.trim());
    }

    function normHeaderKey(text) {
      return String(text || "").toLowerCase().replace(/[^a-z0-9]/g, "");
    }

    function statusColumnRole(label) {
      const key = normHeaderKey(label);
      if (key === "quelle" || key.startsWith("quelle")) return "source";
      if (key === "ziel" || key.startsWith("ziel")) return "target";
      return "";
    }

    function splitSourceTargetCell(value) {
      const text = String(value || "").trim();
      if (!text) return { source: "", target: "" };
      const match = text.match(/^(.*?)\\s*->\\s*(.*?)$/);
      if (match) {
        return {
          source: String(match[1] || "").trim(),
          target: String(match[2] || "").trim(),
        };
      }
      return { source: text, target: "" };
    }

    function splitCombinedSourceTargetStatusColumns(headers, rows) {
      const outHeaders = Array.isArray(headers) ? headers.slice() : [];
      const outRows = Array.isArray(rows)
        ? rows.map((row) => ({
            ...(row || {}),
            cells: Array.isArray((row || {}).cells) ? row.cells.slice() : [],
          }))
        : [];
      const combinedIdx = outHeaders.findIndex((label) => {
        const key = normHeaderKey(label);
        return key === "quelleziel" || key === "quelletarget" || key.includes("quelleziel");
      });
      if (combinedIdx < 0) return { headers: outHeaders, rows: outRows };

      outHeaders.splice(combinedIdx, 1, "Quelle", "Ziel");
      outRows.forEach((row) => {
        const cells = Array.isArray(row.cells) ? row.cells : [];
        const pair = splitSourceTargetCell(cells[combinedIdx] || "");
        cells.splice(combinedIdx, 1, pair.source || "-", pair.target || "-");
        row.cells = cells;
      });
      return { headers: outHeaders, rows: outRows };
    }

    function findStatusColumnIndex(headers, aliases) {
      const aliasList = aliases.map((a) => String(a || "").toLowerCase());
      for (let i = 0; i < headers.length; i += 1) {
        const key = normHeaderKey(headers[i]);
        for (const alias of aliasList) {
          if (key === alias) return i;
          if (alias.length >= 2 && key.includes(alias)) return i;
        }
      }
      return -1;
    }

    function isMissingText(value) {
      const t = String(value || "").trim().toLowerCase();
      return !t || t === "n/a" || t === "na" || t === "-" || t === "none" || t === "null";
    }

    function isMissingYear(value) {
      const t = String(value || "").trim();
      if (isMissingText(t)) return true;
      return !/\\b(18|19|20)\\d{2}\\b/.test(t);
    }

    function normalizeImdbValue(value) {
      let txt = String(value || "").trim().toLowerCase();
      if (!txt) return "";
      txt = txt.replace(/[\\[\\]\\(\\)\\{\\}]/g, "");
      txt = txt.replace(/[^a-z0-9]/g, "");
      if (/^\\d{7,10}$/.test(txt)) txt = `tt${txt}`;
      return txt;
    }

    function isMissingImdb(value) {
      const imdb = normalizeImdbValue(value);
      if (isMissingText(imdb)) return true;
      if (!/^tt\\d{7,10}$/i.test(imdb)) return true;
      return /^tt0+$/.test(imdb) || imdb === "tt1234567";
    }

    function parseStatusTable(rawTable) {
      const lines = String(rawTable || "")
        .split("\\n")
        .map((line) => stripAnsi(line).trim())
        .filter((line) => line.startsWith("|"));
      const parsed = lines.map(parseStatusRow).filter((cells) => cells.length > 0);
      if (!parsed.length) return { headers: [], rows: [] };

      const headers = parsed[0];
      const width = headers.length;
      const yearIndex = findStatusColumnIndex(headers, ["jahr"]);
      const imdbIndex = findStatusColumnIndex(headers, ["imdbid", "imdb"]);
      const speedIndex = findStatusColumnIndex(headers, ["speed"]);
      const etaIndex = findStatusColumnIndex(headers, ["eta"]);
      const rows = parsed.slice(1).map((cells) => {
        const out = [];
        for (let i = 0; i < width; i += 1) out.push((cells[i] || "").trim());
        const yearMissing = yearIndex >= 0 ? isMissingYear(out[yearIndex]) : false;
        const imdbMissing = imdbIndex >= 0 ? isMissingImdb(out[imdbIndex]) : false;
        const speedText = speedIndex >= 0 ? String(out[speedIndex] || "").trim().toLowerCase() : "";
        const etaText = etaIndex >= 0 ? String(out[etaIndex] || "").trim().toLowerCase() : "";
        const completed = (
          speedText.includes("copied")
          || speedText.includes("encoded")
          || speedText.includes("manual")
          || etaText === "copied"
          || etaText === "encoded"
          || etaText === "manual"
          || etaText === "00:00"
        );
        const rowKey = out.length > 0 ? normalizeStatusFraction(out[0]) : "";
        return { cells: out, missing: yearMissing || imdbMissing, completed, rowKey };
      });
      return splitCombinedSourceTargetStatusColumns(headers, rows);
    }

    function parseFractionValue(value) {
      const m = String(value || "").trim().match(/^(\\d+)\\s*\\/\\s*(\\d+)$/);
      if (!m) return null;
      return [Number(m[1]), Number(m[2])];
    }

    function parseNumericValue(value) {
      const txt = String(value || "").trim().replace(",", ".");
      const m = txt.match(/-?\\d+(?:\\.\\d+)?/);
      if (!m) return null;
      const n = Number(m[0]);
      return Number.isFinite(n) ? n : null;
    }

    function compareStatusCells(a, b) {
      const aEmpty = isMissingText(a);
      const bEmpty = isMissingText(b);
      if (aEmpty && !bEmpty) return 1;
      if (!aEmpty && bEmpty) return -1;

      const aFrac = parseFractionValue(a);
      const bFrac = parseFractionValue(b);
      if (aFrac && bFrac) {
        if (aFrac[0] !== bFrac[0]) return aFrac[0] - bFrac[0];
        if (aFrac[1] !== bFrac[1]) return aFrac[1] - bFrac[1];
      }

      const aNum = parseNumericValue(a);
      const bNum = parseNumericValue(b);
      if (aNum !== null && bNum !== null) {
        if (aNum < bNum) return -1;
        if (aNum > bNum) return 1;
        return 0;
      }
      return String(a || "").localeCompare(String(b || ""), "de", { numeric: true, sensitivity: "base" });
    }

    function normalizeDisplayUmlauts(text) {
      let out = String(text || "");
      const replacements = [
        ["Bestaetig", "Bestätig"],
        ["bestaetig", "bestätig"],
        ["Pruef", "Prüf"],
        ["pruef", "prüf"],
        ["Uebers", "Übers"],
        ["uebers", "übers"],
        ["Ueber", "Über"],
        ["ueber", "über"],
        ["Zurueck", "Zurück"],
        ["zurueck", "zurück"],
        ["Geloesch", "Gelösch"],
        ["geloesch", "gelösch"],
        ["Koenn", "Könn"],
        ["koenn", "könn"],
        ["Aender", "Änder"],
        ["aender", "änder"],
        ["Waehr", "Währ"],
        ["waehr", "währ"],
        ["Laeuft", "Läuft"],
        ["laeuft", "läuft"],
        ["Oeffn", "Öffn"],
        ["oeffn", "öffn"],
        ["Fuer", "Für"],
        ["fuer", "für"],
        ["Eintraege", "Einträge"],
        ["eintraege", "einträge"],
        ["Loes", "Lös"],
        ["loes", "lös"],
        ["ausfuehr", "ausführ"],
        ["Ausfuehr", "Ausführ"],
        ["unveraendert", "unverändert"],
        ["Unveraendert", "Unverändert"],
      ];
      replacements.forEach(([src, dst]) => {
        out = out.split(src).join(dst);
      });
      return out;
    }

    function displayStatusCellValue(cell, headerLabel) {
      const key = normHeaderKey(headerLabel || "");
      let text = String(cell || "").trim();
      if (/^n\\/a$/i.test(text)) text = "";
      if (key === "quelle" || key.startsWith("quelle")) {
        text = text.replace(/\\s+/g, " ").trim();
      }
      if (key.includes("speed")) {
        text = formatStatusSpeedText(text);
      } else if (key === "fps") {
        text = formatStatusFpsText(text);
      }
      return normalizeDisplayUmlauts(text);
    }

    function formatStatusSpeedText(raw = "") {
      const text = String(raw || "").trim();
      if (!text || /^n\\/a$/i.test(text)) return "";
      const match = text.match(/^([0-9]+(?:[.,][0-9]+)?)(?:\\s*(x|mb\\/s|mib\\/s))?$/i);
      if (!match) return text;
      const value = Number(String(match[1] || "").replace(",", "."));
      if (!Number.isFinite(value)) return text;
      const suffix = String(match[2] || "").trim().toLowerCase();
      if (suffix === "x") return `${value.toFixed(1)}x`;
      return `${value.toFixed(1)} MB/s`;
    }

    function formatStatusFpsText(raw = "") {
      const text = String(raw || "").trim();
      if (!text || /^n\\/a$/i.test(text)) return "";
      const value = Number(text.replace(",", "."));
      if (!Number.isFinite(value)) return text;
      return String(Math.round(value));
    }

    function hasPendingStatusApproval() {
      return String(LOG_SOURCE || "").trim() === "status" && !!pendingConfirm && !!String(pendingConfirm.token || "").trim();
    }

    function formatJob(job) {
      if (!job || !job.exists) return "Kein Job gestartet.";
      const lines = [];
      lines.push(`ID: ${job.job_id}`);
      lines.push(`Mode: ${job.mode}`);
      lines.push(`Folder: ${job.folder}`);
      lines.push(`Encoder: ${job.encoder}`);
      lines.push(`Running: ${job.running}`);
      if (job.exit_code !== null && job.exit_code !== undefined) lines.push(`Exit-Code: ${job.exit_code}`);
      lines.push(`Log: ${job.log_path || "-"}`);
      lines.push(`Release: ${modalVersion || job.release_version || "-"}`);
      return lines.join("\\n");
    }

    function isJobRunningState(job) {
      if (!job || typeof job !== "object") return false;
      const rawRunning = job.running;
      let running = (rawRunning === true || rawRunning === 1 || rawRunning === "1" || rawRunning === "true");
      if (job.job_id === "last-run") running = false;
      if (job.mode === "unknown" && !job.started_at && !job.ended_at) running = false;
      return running;
    }

    function isPostOptionCompletionLine(tag, body) {
      const t = String(tag || '').toUpperCase();
      const b = String(body || '').trim();
      if (!b || /^START\\b/i.test(b)) return false;
      if (t === 'SYNC-NAS') {
        return /^Sync\\s+NAS\\s+(ok|unvollstaendig)\\b/i.test(b) || /^Abbruch\\b/i.test(b);
      }
      if (t === 'SYNC-PLEX') {
        return /^Plex-Rescan\\s+(ok|fehlgeschlagen)\\b/i.test(b) || /^Abbruch\\b/i.test(b);
      }
      if (t === 'DEL-OUT' || t === 'DEL-QUELLE') return true;
      return true;
    }

    function detectRunningPostOptionKey(processingLog) {
      const lines = String(processingLog || '').split('\\n');
      const map = {
        'SYNC-NAS': 'sync_nas',
        'SYNC-PLEX': 'sync_plex',
        'DEL-OUT': 'del_out',
        'DEL-QUELLE': 'del_source',
      };
      let runningKey = '';
      for (const rawLine of lines) {
        const line = String(rawLine || '').trim();
        if (!line) continue;
        const match = line.match(/\\[(SYNC-NAS|SYNC-PLEX|DEL-OUT|DEL-QUELLE)\\]\\s*(.*)$/i);
        if (!match) continue;
        const tag = String(match[1] || '').toUpperCase();
        const body = String(match[2] || '').trim();
        const key = map[tag] || '';
        if (!key) continue;
        if (/^START\\b/i.test(body)) {
          runningKey = key;
        } else if (runningKey === key && isPostOptionCompletionLine(tag, body)) {
          runningKey = '';
        }
      }
      return runningKey;
    }

    function hasInFlightStatusProgress(statusTableRaw) {
      const parts = splitStatusPanel(statusTableRaw || '');
      const activeKey = extractActiveStatusKey(parts.meta || '');
      const m = String(activeKey || '').match(/^(\\d+)\\/(\\d+)$/);
      if (!m) return false;
      const left = Number(m[1] || 0);
      const right = Number(m[2] || 0);
      if (!Number.isFinite(left) || !Number.isFinite(right) || right <= 0) return false;
      return left > 0 && left < right;
    }

    function normalizeModeForAmpel(rawMode) {
      const m = String(rawMode || '').trim().toLowerCase();
      if (m === 'analyze' || m === 'copy' || m === 'ffmpeg') return m;
      return '';
    }

    function detectCompletedPostOptionKeys(processingLog) {
      const completed = new Set();
      const lines = String(processingLog || '').split(/\\r?\\n/);
      const map = {
        'SYNC-NAS': 'sync_nas',
        'SYNC-PLEX': 'sync_plex',
        'DEL-OUT': 'del_out',
        'DEL-QUELLE': 'del_source',
      };
      for (const rawLine of lines) {
        const line = String(rawLine || '').trim();
        if (!line) continue;
        const match = line.match(/\\[(SYNC-NAS|SYNC-PLEX|DEL-OUT|DEL-QUELLE)\\]\\s*(.*)$/i);
        if (!match) continue;
        const tag = String(match[1] || '').toUpperCase();
        const body = String(match[2] || '').trim();
        const key = map[tag] || '';
        if (!key) continue;
        if (!/^START\\b/i.test(body)) completed.add(key);
      }
      return completed;
    }

    function detectActiveMainStep(modeRaw, processingLog) {
      const mode = String(modeRaw || '');
      const logText = String(processingLog || '');
      const copyStarted = /\\[COPY\\]/i.test(logText);
      const encodeStarted = /\\[FFMPEG\\]/i.test(logText);
      if (mode === 'copy') return copyStarted ? 'copy' : 'analyze';
      if (mode === 'ffmpeg') return encodeStarted ? 'encode' : 'analyze';
      return 'analyze';
    }

    function detectCompletedMainStepKeys(modeRaw, processingLog, running, runningPostKey) {
      const done = new Set();
      const mode = String(modeRaw || '').trim().toLowerCase();
      const logText = String(processingLog || '');
      const hasAnalyze = /\\[ANALYZE\\].*(ENDE|Fortschritt:\\s*\\d+\\s*\\/\\s*\\d+)/i.test(logText);
      const hasCopy = /\\[COPY\\].*(COPY OK|Fallback -> Copy|Manual ->)/i.test(logText);
      const hasEncode = /\\[FFMPEG\\].*(FFMPEG abgeschlossen|Fallback -> Copy)/i.test(logText);
      const mainFinished = !!runningPostKey || (!running && (hasAnalyze || hasCopy || hasEncode));

      if (!mainFinished) return done;
      if (mode === 'analyze') {
        done.add('analyze');
        return done;
      }
      if (mode === 'copy') {
        done.add('analyze');
        done.add('copy');
        return done;
      }
      if (mode === 'ffmpeg') {
        done.add('analyze');
        done.add('encode');
      }
      return done;
    }

    function buildSummaryAmpelState(data) {
      const colors = {
        analyze: 'gray',
        copy: 'gray',
        encode: 'gray',
        sync_nas: 'gray',
        sync_plex: 'gray',
        del_out: 'gray',
        del_source: 'gray',
      };
      const payload = (data && typeof data === 'object') ? data : {};
      const job = (payload.job && typeof payload.job === 'object') ? payload.job : {};
      const settings = (payload.settings && typeof payload.settings === 'object') ? payload.settings : {};
      const running = isJobRunningState(job);
      const processingLog = payload.processing_log || '';
      const runningPostKey = detectRunningPostOptionKey(processingLog);
      const completedPostKeys = detectCompletedPostOptionKeys(processingLog);
      const effectivelyRunning = running;

      const modeRaw = normalizeModeForAmpel(
        effectivelyRunning ? (job.mode || '') : (settings.mode || job.mode || '')
      );

      const opts = (effectivelyRunning && job)
        ? {
            sync_nas: !!job.sync_nas,
            sync_plex: !!job.sync_plex,
            del_out: !!job.del_out,
            del_source: !!job.del_source,
          }
        : {
            sync_nas: !!settings.sync_nas,
            sync_plex: !!settings.sync_plex,
            del_out: !!settings.del_out,
            del_source: !!settings.del_source,
          };

      const selectedKeys = [];
      if (modeRaw === 'analyze') selectedKeys.push('analyze');
      if (modeRaw === 'copy') selectedKeys.push('copy');
      if (modeRaw === 'ffmpeg') selectedKeys.push('encode');
      if (opts.sync_nas) selectedKeys.push('sync_nas');
      if (opts.sync_plex) selectedKeys.push('sync_plex');
      if (opts.del_out) selectedKeys.push('del_out');
      if (opts.del_source) selectedKeys.push('del_source');
      selectedKeys.forEach((key) => {
        if (colors[key] === 'gray') colors[key] = 'yellow';
      });
      if (effectivelyRunning && (modeRaw === 'copy' || modeRaw === 'ffmpeg')) {
        colors.analyze = 'green';
      }
      detectCompletedMainStepKeys(modeRaw, processingLog, effectivelyRunning, runningPostKey).forEach((key) => {
        if (key in colors) colors[key] = 'green';
      });
      completedPostKeys.forEach((key) => {
        if (key in colors) colors[key] = 'green';
      });

      let activeKey = '';
      if (effectivelyRunning) {
        if (runningPostKey) {
          activeKey = runningPostKey;
        } else {
          activeKey = detectActiveMainStep(modeRaw, processingLog);
        }
      }

      if (activeKey && (activeKey in colors)) {
        colors[activeKey] = 'yellow';
      }

      return { colors, activeKey, running: effectivelyRunning };
    }

    function renderSummaryAmpel(data) {
      const box = document.getElementById('summaryAmpelBox');
      if (!box) return;
      const visible = LOG_SOURCE === 'summary';
      box.classList.toggle('hidden', !visible);
      if (!visible) return;
      const ampel = buildSummaryAmpelState(data);
      box.innerHTML = summaryAmpelRows
        .map((row) => {
          const color = String((ampel.colors || {})[row.key] || 'gray');
          const isActive = !!ampel.running && String(ampel.activeKey || '') === row.key;
          const barClass = isActive ? ' bar' : '';
          return `<div class="summary-ampel-row${barClass}"><span class="summary-ampel-dot ${color}"></span><span>${row.label}</span></div>`;
        })
        .join('');
    }

    function setConfirmMsg(text) {
      const box = document.getElementById("confirmMsg");
      if (!box) return;
      box.innerText = normalizeDisplayUmlauts(text);
    }

    function setPendingConfirmation(pending) {
      if (!pending || typeof pending !== "object") {
        pendingConfirm = null;
        pendingConfirmFilterToken = "";
        updatePageTitle();
        renderConfirmPanel();
        return;
      }
      const token = String(pending.token || "").trim();
      const mode = String(pending.mode || "").trim().toLowerCase();
      if (!token || !mode) {
        pendingConfirm = null;
        pendingConfirmFilterToken = "";
        updatePageTitle();
        renderConfirmPanel();
        return;
      }
      pendingConfirm = {
        token,
        mode,
        start_folder: String(pending.start_folder || "").trim(),
      };
      pendingTokenFromUrl = token;
      pendingConfirmFilterToken = token;
      updatePageTitle();
      renderConfirmPanel();
    }

    function renderConfirmPanel() {
      const panel = document.getElementById("confirmPanel");
      const copyBtn = document.getElementById("confirmCopyBtn");
      const encodeBtn = document.getElementById("confirmEncodeBtn");
      const analyzeBtn = document.getElementById("confirmAnalyzeBtn");
      const cleanBtn = document.getElementById("confirmCleanBtn");
      const editBtn = document.getElementById("confirmEditBtn");
      const cancelBtn = document.getElementById("confirmCancelBtn");
      if (!panel) return;
      const hasPending = !!pendingConfirm && !!String(pendingConfirm.token || "").trim();
      const visible = LOG_SOURCE === "status";
      panel.classList.toggle("hidden", !visible);
      if (!visible) {
        setConfirmMsg("");
        return;
      }
      const mode = hasPending ? String(pendingConfirm.mode || "").toLowerCase() : "";
      if (copyBtn) copyBtn.classList.toggle("hidden", !hasPending || mode !== "copy");
      if (encodeBtn) encodeBtn.classList.toggle("hidden", !hasPending || mode !== "ffmpeg");
      if (analyzeBtn) analyzeBtn.classList.toggle("hidden", !hasPending || mode !== "analyze");
      const disabled = !!pendingConfirmInFlight || !hasPending;
      if (copyBtn) copyBtn.disabled = disabled;
      if (encodeBtn) encodeBtn.disabled = disabled;
      if (analyzeBtn) analyzeBtn.disabled = disabled;
      if (cleanBtn) cleanBtn.disabled = disabled;
      if (editBtn) editBtn.disabled = disabled;
      if (cancelBtn) cancelBtn.disabled = disabled;
      if (!hasPending) {
        panel.classList.add("hidden");
        setConfirmMsg("");
      }
    }

    function pendingToken() {
      if (pendingConfirm && pendingConfirm.token) return String(pendingConfirm.token);
      return String(pendingTokenFromUrl || "").trim();
    }

    function editorPopupFeatures() {
      const availW = Math.max(900, Number(window.screen && window.screen.availWidth) || 1366);
      const availH = Math.max(720, Number(window.screen && window.screen.availHeight) || 900);
      const width = Math.max(920, Math.min(1220, availW - 90));
      const height = Math.max(720, Math.min(860, availH - 90));
      return `noopener,noreferrer,width=${Math.round(width)},height=${Math.round(height)}`;
    }

    function openConfirmEditorWindow() {
      const token = pendingToken();
      const baseUrl = token
        ? `/confirm-editor-window?token=${encodeURIComponent(token)}`
        : "/confirm-editor-window";
      const theme = (document.documentElement.getAttribute('data-theme') || '').toLowerCase() === 'dark' ? 'dark' : 'light';
      const url = `${baseUrl}${baseUrl.includes('?') ? '&' : '?'}theme=${encodeURIComponent(theme)}`;
      const w = window.open(url, "_blank", editorPopupFeatures());
      if (w) {
        w.focus();
        return;
      }
      window.location.href = url;
    }

    async function submitPendingDecision(action) {
      if (action === "edit") {
        openConfirmEditorWindow();
        return;
      }
      const token = pendingToken();
      if (!token || pendingConfirmInFlight) return;
      pendingConfirmInFlight = true;
      renderConfirmPanel();
      setConfirmMsg("Bitte warten...");
      try {
        if (action === "clean") {
          const resClean = await fetch("/api/confirm/clean", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              token,
              start_folder: (pendingConfirm && pendingConfirm.start_folder) ? pendingConfirm.start_folder : "",
            }),
          });
          const dataClean = await resClean.json().catch(() => ({}));
          if (resClean.ok && dataClean && dataClean.ok) {
            setConfirmMsg(`Reset "Erledigt" erledigt: gelöscht ${Number(dataClean.deleted || 0)}, Fehler ${Number(dataClean.failed || 0)}`);
            pendingConfirmInFlight = false;
            renderConfirmPanel();
            await refreshNow();
            return;
          }
          const errClean = (dataClean && dataClean.error) ? String(dataClean.error) : 'Reset "Erledigt" fehlgeschlagen';
          setConfirmMsg(errClean);
          pendingConfirmInFlight = false;
          renderConfirmPanel();
          return;
        }

        const decision = action === "cancel" ? "cancel" : "start";
        const res = await fetch("/api/confirm", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ token, state: decision, encoder: "" }),
        });
        const data = await res.json().catch(() => ({}));
        if (!(res.ok && data && data.ok)) {
          const err = (data && data.error) ? String(data.error) : "Freigabe fehlgeschlagen";
          setConfirmMsg(err);
          pendingConfirmInFlight = false;
          renderConfirmPanel();
          return;
        }
        setConfirmMsg("Freigabe gesendet.");
        pendingConfirmInFlight = false;
        renderConfirmPanel();
        await refreshNow();
        if (action === "ok" || action === "copy" || action === "encode") {
          try {
            if (window.opener && typeof window.opener.setCardCollapsed === "function") {
              window.opener.setCardCollapsed("statusCard", true);
            }
            if (window.opener && typeof window.opener.closeLogModal === "function") {
              window.opener.closeLogModal();
            }
          } catch (err) {
          }
          try {
            window.setTimeout(() => window.close(), 120);
          } catch (err) {
          }
        }
      } catch (err) {
        setConfirmMsg("Freigabe fehlgeschlagen");
        pendingConfirmInFlight = false;
        renderConfirmPanel();
      }
    }

    function detectSummaryMode(data) {
      const payload = (data && typeof data === "object") ? data : {};
      const job = (payload.job && typeof payload.job === "object") ? payload.job : {};
      const settings = (payload.settings && typeof payload.settings === "object") ? payload.settings : {};
      const running = isJobRunningState(job);
      const raw = String(running ? (job.mode || "") : (settings.mode || job.mode || "")).trim().toLowerCase();
      if (raw === "copy") return "c";
      if (raw === "ffmpeg" || raw === "encode") return "f";
      return "a";
    }

    function parseSummaryMetaMap(metaText = "") {
      const out = {};
      const lines = String(metaText || "").split(/\\r?\\n/);
      lines.forEach((rawLine) => {
        const line = String(rawLine || "").trim();
        if (!line) return;
      const m = line.match(/^([A-Za-zÄÖÜäöüß.-]+:)\\s*(.*)$/);
        if (!m) return;
        const key = String(m[1] || "").trim();
        const value = String(m[2] || "").trim();
        if (!(key in out)) out[key] = value;
      });
      return out;
    }

    function formatHhMmSs(totalSec) {
      const sec = Math.max(0, Math.floor(Number(totalSec) || 0));
      const h = Math.floor(sec / 3600);
      const m = Math.floor((sec % 3600) / 60);
      const s = sec % 60;
      return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
    }

    function pausedSecondsFromConfirmLog(payload = {}) {
      const logText = String(payload.processing_log || "");
      if (!logText) {
        const pending = (payload.pending_confirmation && typeof payload.pending_confirmation === "object") ? payload.pending_confirmation : {};
        const createdAt = Number(pending.created_at || 0);
        const nowTs = Number(payload.now || 0);
        if (createdAt > 0 && nowTs > createdAt) return Math.max(0, nowTs - createdAt);
        return 0;
      }
      const lines = logText.split(/\\r?\\n/);
      let waitStart = null;
      let totalPaused = 0;
      let dayOffset = 0;
      let prevSecOfDay = -1;
      for (const rawLine of lines) {
        const line = String(rawLine || "").trim();
        const m = line.match(/^\\[(\\d{2}):(\\d{2}):(\\d{2})\\]\\s+\\[CONFIRM\\]\\s+(.*)$/i);
        if (!m) continue;
        const secOfDay = (Number(m[1]) * 3600) + (Number(m[2]) * 60) + Number(m[3]);
        if (prevSecOfDay >= 0 && secOfDay + 60 < prevSecOfDay) dayOffset += 86400;
        prevSecOfDay = secOfDay;
        const absoluteSec = secOfDay + dayOffset;
        const msg = String(m[4] || "").toLowerCase();
        if (/warte auf freigabe/.test(msg)) {
          waitStart = absoluteSec;
          continue;
        }
        if (waitStart !== null && /freigabe erhalten|start nach analyse abgebrochen/.test(msg)) {
          totalPaused += Math.max(0, absoluteSec - waitStart);
          waitStart = null;
        }
      }
      if (waitStart !== null) {
        const pending = (payload.pending_confirmation && typeof payload.pending_confirmation === "object") ? payload.pending_confirmation : {};
        const createdAt = Number(pending.created_at || 0);
        const nowTs = Number(payload.now || 0);
        if (createdAt > 0 && nowTs > createdAt) {
          totalPaused += Math.max(0, nowTs - createdAt);
        }
      }
      return Math.max(0, totalPaused);
    }

    function runtimeFromJob(data, fallback = "") {
      const payload = (data && typeof data === "object") ? data : {};
      const job = (payload.job && typeof payload.job === "object") ? payload.job : {};
      const nowTs = Number(payload.now || 0);
      const startTs = Number(job.started_at || 0);
      const endTs = Number(job.ended_at || 0);
      const running = isJobRunningState(job);
      if (startTs > 0) {
        const ref = running ? (nowTs > 0 ? nowTs : (Date.now() / 1000.0)) : (endTs > 0 ? endTs : (nowTs > 0 ? nowTs : (Date.now() / 1000.0)));
        const paused = pausedSecondsFromConfirmLog(payload);
        return formatHhMmSs(Math.max(0, (ref - startTs) - paused));
      }
      const fb = String(fallback || "").trim();
      if (/^\\d{1,2}:\\d{2}:\\d{2}$/.test(fb)) return fb;
      if (/^\\d{1,2}:\\d{2}$/.test(fb)) return `${fb}:00`;
      return "-";
    }

    function formatActiveFileText(activeText = "") {
      const raw = String(activeText || "").trim();
      if (!raw || raw.toLowerCase() === "n/a") return "Aktiv: -";
      const m = raw.match(/^([0-9]+\\s*\\/\\s*[0-9]+)\\s*(.*)$/);
      if (!m) return `Aktiv: ${raw}`;
      const ratio = String(m[1] || "").replace(/\\s+/g, "");
      const name = String(m[2] || "").trim() || "-";
      return `Aktiv: #${ratio} ${name}`;
    }

    function detectRunningPostStepLabel(data) {
      const payload = (data && typeof data === "object") ? data : {};
      const job = (payload.job && typeof payload.job === "object") ? payload.job : {};
      if (!isJobRunningState(job)) return "";
      const key = detectRunningPostOptionKey(payload.processing_log || "");
      if (!key) return "";
      const labels = {
        sync_nas: "Sync NAS",
        sync_plex: "Sync Plex",
        del_out: "Lösche OUT",
        del_source: "Lösche Quelle",
      };
      return String(labels[key] || "").trim();
    }

    function parseActiveRowMetrics(statusTable = "", activeKey = "") {
      const parsed = parseStatusTable(statusTable || "");
      const headers = parsed && Array.isArray(parsed.headers) ? parsed.headers : [];
      const rows = parsed && Array.isArray(parsed.rows) ? parsed.rows : [];
      const speedIdx = findStatusColumnIndex(headers, ["speed"]);
      const fpsIdx = findStatusColumnIndex(headers, ["fps"]);
      const activeNorm = normalizeStatusFraction(activeKey || "");
      let row = null;
      if (activeNorm) row = rows.find((r) => normalizeStatusFraction((r && r.rowKey) || "") === activeNorm) || null;
      const speed = (row && speedIdx >= 0 && row.cells && row.cells[speedIdx]) ? String(row.cells[speedIdx]).trim() : "";
      const fps = (row && fpsIdx >= 0 && row.cells && row.cells[fpsIdx]) ? String(row.cells[fpsIdx]).trim() : "";
      return { speed: speed || "-", fps: fps || "-" };
    }

    function parseSizeGbValue(raw = "") {
      const text = String(raw || "").trim().replace(",", ".");
      const m = text.match(/-?\\d+(?:\\.\\d+)?/);
      if (!m) return null;
      const n = Number(m[0]);
      if (!Number.isFinite(n)) return null;
      return Math.max(0, n);
    }

    function formatSizeGb(value) {
      const n = Number(value);
      if (!Number.isFinite(n)) return "-";
      return `${n.toFixed(1).replace(".", ",")} GB`;
    }

    function parseSpeedMbPerSec(raw = "") {
      const original = String(raw || "").trim();
      if (!/(?:MiB|MB)\\/s\\b/i.test(original)) return null;
      const text = original.replace(",", ".");
      const m = text.match(/([0-9]+(?:\\.[0-9]+)?)\\s*(?:MiB|MB)\\/s/i);
      if (!m) return null;
      const n = Number(m[1]);
      if (!Number.isFinite(n) || n <= 0) return null;
      return n;
    }

    function parseActiveMeta(activeText = "") {
      const raw = String(activeText || "").trim();
      if (!raw) return { ratio: "", name: "" };
      const m = raw.match(/^#?\\s*([0-9]+\\s*\\/\\s*[0-9]+)\\s*(.*)$/);
      if (!m) return { ratio: "", name: raw };
      return {
        ratio: String(m[1] || "").replace(/\\s+/g, ""),
        name: String(m[2] || "").trim(),
      };
    }

    function collectStatusProgress(statusTable = "", activeKey = "", mode = "a", running = true, forceAllCompleted = false) {
      const parsed = parseStatusTable(statusTable || "");
      const headers = parsed && Array.isArray(parsed.headers) ? parsed.headers : [];
      const rows = parsed && Array.isArray(parsed.rows) ? parsed.rows : [];
      const totalRows = rows.length;
      const qIdx = findStatusColumnIndex(headers, ["qgb", "q"]);
      const zIdx = findStatusColumnIndex(headers, ["zgb", "z"]);
      const targetIdx = findStatusColumnIndex(headers, ["ziel", "target"]);
      const activeNorm = normalizeStatusFraction(activeKey || "");
      let activePos = 0;
      if (activeNorm) {
        const m = activeNorm.match(/^(\\d+)\\/(\\d+)$/);
        if (m) activePos = Number(m[1] || 0);
      }
      let completedCount = 0;
      if (forceAllCompleted) {
        completedCount = totalRows;
      } else if (activePos > 0) {
        completedCount = Math.max(0, Math.min(totalRows, activePos - 1));
      } else if (!running) {
        completedCount = totalRows;
      } else if (zIdx >= 0) {
        completedCount = rows.filter((row) => parseSizeGbValue((row.cells || [])[zIdx] || "") !== null).length;
      }

      let qDone = 0;
      let qTotal = 0;
      let zDone = 0;
      rows.forEach((row, idx) => {
        const cells = row && Array.isArray(row.cells) ? row.cells : [];
        const qVal = qIdx >= 0 ? parseSizeGbValue(cells[qIdx] || "") : null;
        const zVal = zIdx >= 0 ? parseSizeGbValue(cells[zIdx] || "") : null;
        if (qVal !== null) {
          qTotal += qVal;
          if (idx < completedCount) qDone += qVal;
        }
        if (zVal !== null && idx < completedCount) {
          zDone += zVal;
        }
      });

      let activeRow = null;
      if (activeNorm) {
        activeRow = rows.find((row) => normalizeStatusFraction((row && row.rowKey) || "") === activeNorm) || null;
      }
      if (!activeRow && running && completedCount < totalRows) {
        activeRow = rows[completedCount] || null;
      }
      const activeTarget = activeRow && targetIdx >= 0
        ? String((activeRow.cells || [])[targetIdx] || "").trim()
        : "";
      return {
        totalRows,
        completedCount,
        qDoneGb: qDone,
        qTotalGb: qTotal,
        zDoneGb: zDone,
        activeRatio: activeNorm,
        activeTarget,
      };
    }

    function extractMbSpeedFromLine(line = "") {
      const text = String(line || "");
      const m = text.match(/Speed\\s*[:=]\\s*(?:[0-9]+%\\s*)?([0-9]+(?:[.,][0-9]+)?)\\s*(?:MiB|MB)\\/s/i);
      if (!m) return "";
      const num = String(m[1] || "").replace(",", ".").trim();
      if (!num) return "";
      return `${num} MB/s`;
    }

    function extractSummarySpeedFromProcessingLog(data, mode) {
      const payload = (data && typeof data === "object") ? data : {};
      const lines = String(payload.processing_log || "").split(/\\r?\\n/);
      let syncNasSpeed = "";
      let copySpeed = "";
      for (const raw of lines) {
        const line = String(raw || "").trim();
        if (!line) continue;
        if (/\\[SYNC-NAS\\]/i.test(line)) {
          const sp = extractMbSpeedFromLine(line);
          if (sp) syncNasSpeed = sp;
          continue;
        }
        if (mode === "c" && /\\[COPY\\]/i.test(line)) {
          const sp = extractMbSpeedFromLine(line);
          if (sp) copySpeed = sp;
        }
      }
      if (syncNasSpeed) return syncNasSpeed;
      if (mode === "c" && copySpeed) return copySpeed;
      return "";
    }

    function extractSummaryEtaFromProcessingLog(data, mode) {
      const payload = (data && typeof data === "object") ? data : {};
      const lines = String(payload.processing_log || "").split(/\\r?\\n/);
      let syncNasEta = "";
      let copyEta = "";
      let ffmpegEta = "";
      for (const raw of lines) {
        const line = String(raw || "").trim();
        if (!line) continue;
        const etaMatch = line.match(/ETA\\s*[:=]\\s*([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?|n\\/a|-)/i);
        if (!etaMatch || !etaMatch[1]) continue;
        const eta = String(etaMatch[1] || "").trim();
        if (/\\[SYNC-NAS\\]/i.test(line)) {
          syncNasEta = eta;
          continue;
        }
        if (mode === "c" && /\\[COPY\\]/i.test(line)) {
          copyEta = eta;
          continue;
        }
        if (mode === "f" && /\\[FFMPEG\\]/i.test(line)) {
          ffmpegEta = eta;
        }
      }
      if (syncNasEta) return syncNasEta;
      if (mode === "c" && copyEta) return copyEta;
      if (mode === "f" && ffmpegEta) return ffmpegEta;
      return "";
    }

    function extractSummaryFpsFromProcessingLog(data, mode) {
      if (mode !== "f") return "";
      const payload = (data && typeof data === "object") ? data : {};
      const lines = String(payload.processing_log || "").split(/\\r?\\n/);
      let fps = "";
      for (const raw of lines) {
        const line = String(raw || "").trim();
        if (!line || !/\\[FFMPEG\\]/i.test(line)) continue;
        const m = line.match(/FPS\\s*[:=]\\s*([0-9]+(?:[.,][0-9]+)?)/i);
        if (m && m[1]) fps = String(m[1]).replace(",", ".").trim();
      }
      return fps;
    }

    function extractTmdbStatusFromProcessingLog(data) {
      const payload = (data && typeof data === "object") ? data : {};
      const lines = String(payload.processing_log || "").split(/\\r?\\n/);
      let checked = 0;
      let total = 0;
      let requests = 0;
      let title = 0;
      let year = 0;
      let cacheHit = 0;
      let cacheWrite = 0;
      let cacheRetention = "";
      let skipped = "";
      for (const raw of lines) {
        const line = String(raw || "").trim();
        if (!line || !/\\[TMDB\\]/i.test(line)) continue;
        const ret = line.match(/Retention\\s*=\\s*([0-9]+)\\s*Tage/i);
        if (ret && ret[1]) cacheRetention = `${ret[1]}d`;
        if (/uebersprungen|übersprungen/i.test(line)) {
          skipped = line.replace(/^.*\\[TMDB\\]\\s*/i, "").trim();
          continue;
        }
        let m = line.match(/geprueft\\s*=\\s*([0-9]+)\\s*\\/\\s*([0-9]+)/i);
        if (!m) m = line.match(/Fortschritt\\s*:\\s*([0-9]+)\\s*\\/\\s*([0-9]+)/i);
        if (m) {
          checked = Number(m[1] || 0);
          total = Number(m[2] || 0);
        } else {
          const s = line.match(/Kandidaten\\s*=\\s*([0-9]+)/i);
          if (s) total = Number(s[1] || 0);
        }
        const req = line.match(/Requests\\s*=\\s*([0-9]+)/i);
        if (req) requests = Number(req[1] || 0);
        const t = line.match(/Titel\\s*=\\s*([0-9]+)/i);
        if (t) title = Number(t[1] || 0);
        const y = line.match(/Jahr\\s*=\\s*([0-9]+)/i);
        if (y) year = Number(y[1] || 0);
        const ch = line.match(/Cache-Hit\\s*=\\s*([0-9]+)/i) || line.match(/\\bHit\\s*=\\s*([0-9]+)/i);
        if (ch) cacheHit = Number(ch[1] || 0);
        const cw = line.match(/Cache-Write\\s*=\\s*([0-9]+)/i) || line.match(/\\bWrite\\s*=\\s*([0-9]+)/i);
        if (cw) cacheWrite = Number(cw[1] || 0);
      }
      if (skipped) return skipped;
      if (checked > 0 || total > 0 || requests > 0) {
        const ratio = total > 0 ? `${checked}/${total}` : `${checked}`;
        const cachePart = (cacheHit > 0 || cacheWrite > 0 || cacheRetention)
          ? ` | Cache ${cacheHit}/${cacheWrite}${cacheRetention ? ` (${cacheRetention})` : ""}`
          : "";
        return `${ratio} | Req ${requests} | Titel ${title} | Jahr ${year}${cachePart}`;
      }
      return "";
    }

    function parseIsoGbFromLine(line = "", label = "Q-GB") {
      const m = String(line || "").match(new RegExp(`${label}\\s*[:=]\\s*([0-9]+(?:[.,][0-9]+)?)`, "i"));
      if (!m || !m[1]) return null;
      const n = Number(String(m[1] || "").replace(",", "."));
      if (!Number.isFinite(n) || n < 0) return null;
      return n;
    }

    function parseIsoProgressFromProcessingLog(data) {
      const payload = (data && typeof data === "object") ? data : {};
      const lines = String(payload.processing_log || "").split(/\\r?\\n/);
      let ratio = "";
      let fileName = "";
      let qGb = null;
      let zGb = null;
      let speed = "";
      let runtime = "";
      let eta = "";
      let lastIsoState = "";
      for (const raw of lines) {
        const line = String(raw || "").trim();
        if (!line || !/\\[ISO\\]/i.test(line)) continue;
        const mExtract = line.match(/\\[ISO\\]\\s*Extrahiere\\s+([0-9]+\\s*\\/\\s*[0-9]+)\\s*:\\s*(.+)$/i);
        if (mExtract) {
          ratio = String(mExtract[1] || "").replace(/\\s+/g, "");
          fileName = String(mExtract[2] || "").trim() || fileName;
          lastIsoState = "extract";
        }
        const mFinish = line.match(/\\[ISO\\]\\s*Fertig:\\s*(.+?)(?:\\s*\\(|$)/i);
        if (mFinish && mFinish[1]) {
          fileName = String(mFinish[1]).trim() || fileName;
          lastIsoState = "finish";
        }
        if (/\\[ISO\\].*(Fehler|Keine geeigneten Titel|Unbekannte Struktur)/i.test(line)) {
          lastIsoState = "done";
        }
        const q = parseIsoGbFromLine(line, "Q-GB");
        if (q !== null) qGb = q;
        const z = parseIsoGbFromLine(line, "Z-GB");
        if (z !== null) zGb = z;
        const sp = extractMbSpeedFromLine(line);
        if (sp) speed = sp;
        const rt = line.match(/Laufzeit\\s*[:=]\\s*([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)/i);
        if (rt && rt[1]) runtime = String(rt[1]).trim();
        const et = line.match(/ETA\\s*[:=]\\s*([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?|n\\/a|-)/i);
        if (et && et[1]) eta = String(et[1]).trim();
        if (q !== null || z !== null || sp || rt || et) {
          lastIsoState = "progress";
        }
      }
      const active = lastIsoState === "extract" || lastIsoState === "progress";
      return {
        active,
        ratio,
        fileName,
        qGb,
        zGb,
        speed,
        runtime,
        eta,
        hasMetrics: active || qGb !== null || zGb !== null || !!speed || !!runtime || !!eta || !!ratio || !!fileName,
      };
    }

    function parseSavingsParts(raw = "") {
      const text = String(raw || "").trim();
      const gbMatch = text.match(/([0-9]+(?:[.,][0-9]+)?)\\s*GB/i);
      const pctMatch = text.match(/([0-9]+(?:[.,][0-9]+)?)\\s*%/i);
      return {
        gb: gbMatch ? `${String(gbMatch[1] || "").replace(".", ",")} GB` : "-",
        percent: pctMatch ? `${String(pctMatch[1] || "").replace(".", ",")}%` : "",
      };
    }

    function formatSummarySpeedText(rawSpeed, mode) {
      const text = String(rawSpeed || "").trim();
      if (!text) return "-";
      if (mode !== "c") return text;
      if (/(?:MiB|MB)\\/s\\b/i.test(text)) return text.replace(/MiB\\/s/ig, "MB/s");
      if (/%/.test(text)) return text;
      const numericOnly = text.match(/^([0-9]+(?:[.,][0-9]+)?)$/);
      if (!numericOnly) return text;
      return `${String(numericOnly[1] || "").replace(",", ".")} MB/s`;
    }

    function buildSummaryText(data, statusMeta, statusTable, activeStatusKey) {
      const payload = (data && typeof data === "object") ? data : {};
      const job = (payload.job && typeof payload.job === "object") ? payload.job : {};
      const running = isJobRunningState(job);
      if (!running) return "";
      const mode = detectSummaryMode(data);
      const meta = parseSummaryMetaMap(statusMeta);
      const activeMeta = parseActiveMeta(meta["Aktiv:"] || "");
      const metrics = parseActiveRowMetrics(statusTable || "", activeStatusKey || activeMeta.ratio);
      const postStepLabel = detectRunningPostStepLabel(data);
      const progress = collectStatusProgress(statusTable || "", activeStatusKey || activeMeta.ratio, mode, running, !!postStepLabel);
      const logSpeed = extractSummarySpeedFromProcessingLog(data, mode);
      const logFps = extractSummaryFpsFromProcessingLog(data, mode);
      const logEta = extractSummaryEtaFromProcessingLog(data, mode);
      const isoProgress = parseIsoProgressFromProcessingLog(data);
      const isoAnalyzeActive = !!isoProgress.active;
      const speedText = (mode === "c" || mode === "f") ? formatSummarySpeedText(logSpeed || metrics.speed || "-", mode) : "-";
      const fpsText = mode === "f" ? (logFps || metrics.fps || "-") : "-";
      const etaRaw = String(meta["ETA:"] || "").trim() || "-";
      const laufz = runtimeFromJob(data, meta["Laufz.:"] || "");
      const ersparnis = String(meta["Ersparnis:"] || "").trim() || "-";
      const lines = [];

      const ratio = isoAnalyzeActive
        ? (isoProgress.ratio || progress.activeRatio || activeMeta.ratio || "-")
        : (progress.activeRatio || activeMeta.ratio || "-");
      const targetName = postStepLabel
        ? "-"
        : (isoAnalyzeActive
          ? (isoProgress.fileName || progress.activeTarget || activeMeta.name || "-")
          : (progress.activeTarget || activeMeta.name || "-"));
      if (mode === "c" || mode === "f") {
        lines.push(`Aktiv: ${postStepLabel || ratio}`);
        lines.push(`Datei: ${targetName}`);
      }

      if (isoAnalyzeActive) {
        lines.push(`Speed: ${isoProgress.speed || "-"}`);
      } else if (mode === "f") {
        lines.push(`Speed: ${speedText}`);
        lines.push(`FPS: ${fpsText}`);
      } else if (mode === "c") {
        lines.push(`Speed: ${speedText}`);
      }
      const totalRows = Number(progress.totalRows || 0);
      const filesQ = `${totalRows}/${totalRows}`;
      const filesZ = `${Number(progress.completedCount || 0)}/${totalRows}`;
      if (isoAnalyzeActive && isoProgress.qGb !== null) {
        lines.push(`GB Quelle: ${formatSizeGb(isoProgress.qGb)}`);
        if (isoProgress.zGb !== null) lines.push(`GB Ziel: ${formatSizeGb(isoProgress.zGb)}`);
      } else {
        lines.push(`GB Quelle: ${formatSizeGb(progress.qTotalGb)} (${filesQ})`);
        if (mode === "c" || mode === "f") lines.push(`GB Ziel: ${formatSizeGb(progress.zDoneGb)} (${filesZ})`);
      }
      if (mode === "f") {
        const savingsMeta = parseSavingsParts(ersparnis);
        const savedGbNum = Math.max(0, Number(progress.qDoneGb || 0) - Number(progress.zDoneGb || 0));
        const savedGbText = savedGbNum > 0 ? formatSizeGb(savedGbNum) : savingsMeta.gb;
        lines.push(`Ersparnis: ${savedGbText} (${filesZ})`);
        const pctFromDone = Number(progress.qDoneGb || 0) > 0
          ? `${Math.round((savedGbNum / Number(progress.qDoneGb || 1)) * 100)}%`
          : "";
        const pctText = savingsMeta.percent || pctFromDone;
        if (pctText) lines.push(`Ersparnis: ${pctText}`);
      }
      lines.push(`Laufzeit: ${isoAnalyzeActive && isoProgress.runtime ? isoProgress.runtime : laufz}`);
      let etaText = etaRaw;
      if (!etaText || etaText === "-" || /^n\\/a$/i.test(etaText)) {
        etaText = logEta || etaText;
      }
      if (mode === "c" && (!etaText || etaText === "-" || /^n\\/a$/i.test(etaText))) {
        const speedMb = parseSpeedMbPerSec(speedText);
        const remainingQGb = Math.max(0, Number(progress.qTotalGb || 0) - Number(progress.qDoneGb || 0));
        if (speedMb && remainingQGb > 0) {
          etaText = formatHhMmSs((remainingQGb * 1024.0) / speedMb);
        }
      }
      if (isoAnalyzeActive) {
        etaText = isoProgress.eta || etaText;
      }
      if (!etaText) etaText = "-";
      if (mode === "c" || mode === "f" || isoAnalyzeActive) lines.push(`ETA: ${etaText}`);
      return lines.join("\\n");
    }

    function summaryPairsFromText(text = "") {
      const seen = new Set();
      const pairs = [];
      const keepPlaceholder = new Set([
        "aktiv",
        "datei",
        "speed",
        "fps",
        "gbquelle",
        "gbziel",
        "ersparnis",
        "ersparnis",
        "laufzeit",
        "eta",
      ]);
      String(text || "")
        .split(/\\r?\\n/)
        .map((line) => String(line || "").trim())
        .filter((line) => !!line)
        .forEach((line) => {
          const m = line.match(/^([^:]+):\\s*(.*)$/);
          let key = "";
          let value = "";
          if (m) {
            key = String(m[1] || "").trim();
            value = String(m[2] || "").trim();
          } else {
            key = line;
          }
          if (!key) return;
          const normalizedKey = normHeaderKey(key);
          const lower = String(value || "").toLowerCase();
          if (value && (lower === "-" || lower === "n/a" || lower === "na" || lower === "...") && !keepPlaceholder.has(normalizedKey)) return;
          const dedupeKey = `${key.toLowerCase()}|${value}`;
          if (seen.has(dedupeKey)) return;
          seen.add(dedupeKey);
          pairs.push({ key, value });
        });
      return pairs;
    }

    function renderSummaryTable(text = "") {
      const body = document.getElementById("summaryKvBody");
      if (!body) return;
      const wrap = document.getElementById("summaryWrap");
      if (wrap && (isPreLocked("summaryWrap") || isSelectionInside(wrap))) return;
      const pairs = summaryPairsFromText(text);
      const prevTop = wrap ? wrap.scrollTop : 0;
      const atBottom = wrap ? ((wrap.scrollHeight - wrap.scrollTop - wrap.clientHeight) < 10) : false;
      body.innerHTML = "";
      if (!pairs.length) {
        const tr = document.createElement("tr");
        const th = document.createElement("th");
        th.innerText = "Status";
        const td = document.createElement("td");
        td.innerText = "Keine laufenden Summary-Daten";
        tr.appendChild(th);
        tr.appendChild(td);
        body.appendChild(tr);
        if (wrap) {
          wrap.scrollTop = atBottom ? wrap.scrollHeight : prevTop;
        }
        return;
      }
      pairs.forEach((pair) => {
        const tr = document.createElement("tr");
        const th = document.createElement("th");
        const keyText = String(pair.key || "").replace(/:$/, "");
        th.innerText = keyText;
        const td = document.createElement("td");
        td.innerText = String(pair.value || "");
        if (["datei", "aktivedatei", "aktiv"].includes(normHeaderKey(keyText))) {
          td.classList.add("summary-file-cell");
        }
        tr.appendChild(th);
        tr.appendChild(td);
        body.appendChild(tr);
      });
      if (wrap) {
        wrap.scrollTop = atBottom ? wrap.scrollHeight : prevTop;
      }
    }

    function wireButtonTips() {
      document.querySelectorAll('button[title]').forEach((btn) => {
        const hint = String(btn.getAttribute('title') || '').trim();
        if (!hint) return;
        btn.setAttribute('data-tip', hint);
        btn.removeAttribute('title');
      });
    }

    function setText(text) {
      const box = document.getElementById("logBox");
      if (!box) return;
      if (isPreLocked("logBox") || isSelectionInside(box)) return;
      const atBottom = (box.scrollHeight - box.scrollTop - box.clientHeight) < 10;
      const prevTop = box.scrollTop;
      const next = normalizeDisplayUmlauts(text);
      if (box.innerText !== next) {
        box.innerText = next;
        box.scrollTop = atBottom ? box.scrollHeight : prevTop;
      }
    }

    function currentStatusFilterOrder() {
      const order = ["all", "errors", "done"];
      if (statusFilterModeContext === "ffmpeg") {
        order.push("encode", "copy");
      }
      return order;
    }

    function updateStatusFilterButton() {
      const btn = document.getElementById("onlyMissingBtn");
      if (btn) {
        if (state.filterMode === "errors") btn.innerText = "Fehler";
        else if (state.filterMode === "done") btn.innerText = "Erledigt";
        else if (state.filterMode === "encode") btn.innerText = "Encode";
        else if (state.filterMode === "copy") btn.innerText = "Copy";
        else btn.innerText = "Alle";
        btn.classList.toggle("active", state.filterMode !== "all");
      }
      const info = document.getElementById("onlyMissingInfo");
      if (info) {
        const rows = Array.isArray(state.rows) ? state.rows : [];
        let errors = 0;
        let done = 0;
        rows.forEach((row) => {
          if (row && row.missing) errors += 1;
          if (row && row.completed) done += 1;
        });
        const hasRows = rows.length > 0;
        const showInfo = hasRows && hasPendingStatusApproval();
        info.classList.toggle("hidden", !showInfo);
        if (showInfo) {
          info.innerText = `Fehler ${errors}/${rows.length} | Erledigt ${done}/${rows.length}`;
        }
      }
    }

    function toggleOnlyMissing() {
      const order = currentStatusFilterOrder();
      const current = String(state.filterMode || "all");
      const idx = order.indexOf(current);
      if (idx < 0) {
        state.filterMode = order[0] || "all";
      } else {
        state.filterMode = order[(idx + 1) % order.length];
      }
      updateStatusFilterButton();
      applyStatusFilterVisibility();
    }

    function statusRowMode(row, headers) {
      const cells = row && Array.isArray(row.cells) ? row.cells : [];
      const egbIdx = findStatusColumnIndex(headers || [], ["egb"]);
      if (egbIdx < 0) return "";
      const raw = String(cells[egbIdx] || "").trim().toLowerCase();
      if (!raw || raw === "-" || raw === "n/a" || raw === "na") return "";
      if (raw.includes("copy")) return "copy";
      return "encode";
    }

    function statusFilterText(mode) {
      if (mode === "done") return "Keine erledigten Zeilen.";
      if (mode === "encode") return "Keine Encode-Zeilen.";
      if (mode === "copy") return "Keine Copy-Zeilen.";
      if (mode === "errors") return "Keine fehlerhaften Zeilen.";
      return "Keine Daten.";
    }

    function statusFilterMatchCount(rowsSource, headers, mode) {
      const rows = Array.isArray(rowsSource) ? rowsSource : [];
      if (mode === "all") return rows.length;
      let count = 0;
      rows.forEach((row) => {
        if (!row) return;
        if (mode === "errors") {
          if (row.missing) count += 1;
          return;
        }
        if (mode === "done") {
          if (row.completed) count += 1;
          return;
        }
        if (mode === "encode" || mode === "copy") {
          if (statusRowMode(row, headers) === mode) count += 1;
        }
      });
      return count;
    }

    function applyStatusFilterVisibility() {
      const table = document.getElementById("statusTable");
      const body = document.getElementById("statusBody");
      if (!table || !body) return;
      const mode = String(state.filterMode || "all");
      table.classList.toggle("status-filter-errors", mode === "errors");
      table.classList.toggle("status-filter-done", mode === "done");
      table.classList.toggle("status-filter-encode", mode === "encode");
      table.classList.toggle("status-filter-copy", mode === "copy");

      const existing = body.querySelector("tr.status-filter-empty");
      if (existing) existing.remove();
      const headers = Array.isArray(state.headers) ? state.headers : [];
      const rowsSource = Array.isArray(state.rows) ? state.rows : [];
      if (mode !== "all" && headers.length && rowsSource.length) {
        const matches = statusFilterMatchCount(rowsSource, headers, mode);
        if (matches <= 0) {
          const tr = document.createElement("tr");
          tr.className = "status-filter-empty";
          const td = document.createElement("td");
          td.id = "statusTableEmpty";
          td.colSpan = headers.length;
          td.innerText = statusFilterText(mode);
          tr.appendChild(td);
          body.appendChild(tr);
        }
      }
    }

    function sortStatusByColumn(index) {
      const idx = Number(index);
      if (!Number.isInteger(idx) || idx < 0) return;
      if (state.sortIndex === idx) {
        state.sortDir = state.sortDir === "asc" ? "desc" : "asc";
      } else {
        state.sortIndex = idx;
        state.sortDir = "asc";
      }
      renderStatusFromState();
    }

    function renderStatusFromState() {
      const head = document.getElementById("statusHead");
      const body = document.getElementById("statusBody");
      const wrap = document.getElementById("statusWrap");
      if (!head || !body || !wrap) return;

      const headers = Array.isArray(state.headers) ? state.headers : [];
      const rowsSource = Array.isArray(state.rows) ? state.rows : [];
      const activeKey = normalizeStatusFraction(state.activeKey || "");
      head.innerHTML = "";
      body.innerHTML = "";

      if (!headers.length) {
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.id = "statusTableEmpty";
        td.colSpan = 1;
        td.innerText = "";
        tr.appendChild(td);
        body.appendChild(tr);
        return;
      }

      const hr = document.createElement("tr");
      headers.forEach((label, idx) => {
        const th = document.createElement("th");
        let title = String(label || "");
        if (state.sortIndex === idx) {
          title += state.sortDir === "asc" ? " ▲" : " ▼";
          th.classList.add(state.sortDir === "asc" ? "sort-asc" : "sort-desc");
        }
        th.innerText = title;
        const role = statusColumnRole(label);
        if (role === "source") th.classList.add("status-col-source");
        if (role === "target") th.classList.add("status-col-target");
        th.setAttribute("onclick", `sortStatusByColumn(${idx})`);
        hr.appendChild(th);
      });
      head.appendChild(hr);

      let rows = rowsSource.slice();
      if (state.sortIndex >= 0 && state.sortIndex < headers.length) {
        const col = state.sortIndex;
        rows.sort((a, b) => {
          const cmp = compareStatusCells(a.cells[col], b.cells[col]);
          return state.sortDir === "asc" ? cmp : -cmp;
        });
      }

      if (!rows.length) {
        const tr = document.createElement("tr");
        const td = document.createElement("td");
        td.id = "statusTableEmpty";
        td.colSpan = headers.length;
        td.innerText = "Keine Daten.";
        tr.appendChild(td);
        body.appendChild(tr);
        return;
      }

      rows.forEach((row) => {
        const tr = document.createElement("tr");
        tr.setAttribute("data-filter-row", "1");
        const rowKey = row ? normalizeStatusFraction(row.rowKey || "") : "";
        if (activeKey && rowKey && activeKey === rowKey) {
          tr.classList.add("status-row-active");
        }
        if (row && row.missing) tr.classList.add("status-row-missing");
        if (row && row.completed && !(activeKey && rowKey && activeKey === rowKey)) tr.classList.add("status-row-done");
        const rowMode = statusRowMode(row, headers);
        if (rowMode === "encode") tr.classList.add("status-row-encode");
        if (rowMode === "copy") tr.classList.add("status-row-copy");
        (row.cells || []).forEach((cell, cellIdx) => {
          const td = document.createElement("td");
          td.innerText = displayStatusCellValue(cell, headers[cellIdx] || "");
          const role = statusColumnRole(headers[cellIdx] || "");
          if (role === "source") td.classList.add("status-col-source");
          if (role === "target") td.classList.add("status-col-target");
          tr.appendChild(td);
        });
        body.appendChild(tr);
      });
      applyStatusFilterVisibility();

      const activeRow = body.querySelector("tr.status-row-active");
      if (!logWindowJobRunning) {
        state.lastAutoScrollKey = "";
      } else if (isPreLocked("statusWrap") || isSelectionInside(wrap)) {
        state.lastAutoScrollKey = "";
      } else if (activeKey && activeRow && state.lastAutoScrollKey !== activeKey) {
        activeRow.scrollIntoView({ block: "nearest", inline: "nearest" });
        state.lastAutoScrollKey = activeKey;
      } else if (!activeKey) {
        state.lastAutoScrollKey = "";
      }
    }

    function renderStatus(rawTable, activeKey) {
      const parsed = parseStatusTable(rawTable);
      if (currentEncodeWorkerName && Array.isArray(parsed.headers) && !parsed.headers.includes('Worker')) {
        parsed.headers = parsed.headers.concat(['Worker']);
        parsed.rows = (Array.isArray(parsed.rows) ? parsed.rows : []).map((row) => {
          const nextRow = Object.assign({}, row || {});
          nextRow.cells = Array.isArray(nextRow.cells) ? nextRow.cells.slice() : [];
          nextRow.cells.push(currentEncodeWorkerName);
          return nextRow;
        });
      }
      if (!parsed.headers.length && state.headers.length > 0) {
        state.emptyStreak = (state.emptyStreak || 0) + 1;
        if (state.emptyStreak < 3) return;
      } else {
        state.emptyStreak = 0;
      }
      state.headers = parsed.headers || [];
      state.rows = parsed.rows || [];
      state.activeKey = normalizeStatusFraction(activeKey || "");
      if (state.sortIndex >= state.headers.length) state.sortIndex = -1;
      updateStatusFilterButton();
      renderStatusFromState();
    }

    function stateApiUrl() {
      const params = new URLSearchParams();
      if (LOG_SOURCE === "proc") {
        params.set("full_log", "1");
        params.set("log_max_chars", "2400000");
      } else {
        params.set("log_lines", "2400");
        params.set("log_max_chars", "1200000");
      }
      return `/api/state?${params.toString()}`;
    }

    async function refreshNow() {
      try {
        const res = await fetch(stateApiUrl(), { cache: "no-store" });
        const data = await res.json();
        const job = (data && data.job) ? data.job : {};
        const running = isJobRunningState(job);
        logWindowJobRunning = running;
        const settings = (data && data.settings && typeof data.settings === "object") ? data.settings : {};
        statusFilterModeContext = normalizeModeForAmpel(
          running ? (job.mode || "") : (settings.mode || job.mode || "")
        ) || "analyze";
        renderWorkers((data && data.workers) || []);
        currentEncodeWorkerName = String((job && job.worker_name) || currentEncodeWorkerName || '').trim();
        updateStatusFilterButton();
        setRunDot(isJobRunningState(job));
        modalVersion = (data && data.versioning && data.versioning.current)
          ? data.versioning.current
          : (job.release_version || modalVersion || "-");
        updatePageTitle();
        renderSummaryAmpel(data);
        setPendingConfirmation((data && data.pending_confirmation) ? data.pending_confirmation : null);

        if (LOG_SOURCE === "status") {
          const parts = splitStatusPanel((data && data.status_table) || "");
          const running = isJobRunningState(job);
          const runningPostKey = detectRunningPostOptionKey((data && data.processing_log) || "");
          const activeKey = (running && !runningPostKey) ? extractActiveStatusKey(parts.meta || "") : "";
          renderStatus(parts.table || "", activeKey);
          return;
        }
        if (LOG_SOURCE === "job") {
          setText(formatJob(job));
          return;
        }
        if (LOG_SOURCE === "summary") {
          const parts = splitStatusPanel((data && data.status_table) || "");
          const activeKey = extractActiveStatusKey(parts.meta || "");
          renderSummaryTable(buildSummaryText(data, parts.meta || "", parts.table || "", activeKey || ""));
          return;
        }
        if (LOG_SOURCE === "plan") {
          setText((data && data.out_tree) || "");
          return;
        }
        setText((data && data.processing_log) || "");
      } catch (err) {
        setPendingConfirmation(null);
        if (LOG_SOURCE === "summary") {
          renderSummaryTable(`Status: Fehler beim Laden\\nMeldung: ${err}`);
          return;
        }
        setText(`Fehler beim Laden: ${err}`);
      }
    }

    function init() {
      const isStatus = LOG_SOURCE === "status";
      const isSummary = LOG_SOURCE === "summary";
      const onlyMissingBtn = document.getElementById("onlyMissingBtn");
      const onlyMissingInfo = document.getElementById("onlyMissingInfo");
      const logBox = document.getElementById("logBox");
      const summaryWrap = document.getElementById("summaryWrap");
      const statusWrap = document.getElementById("statusWrap");
      const confirmPanel = document.getElementById("confirmPanel");
      if (onlyMissingBtn) onlyMissingBtn.classList.toggle("hidden", !isStatus);
      if (onlyMissingInfo) onlyMissingInfo.classList.toggle("hidden", !isStatus);
      if (logBox) logBox.classList.toggle("hidden", isStatus || isSummary);
      if (summaryWrap) summaryWrap.classList.toggle("hidden", !isSummary);
      if (statusWrap) statusWrap.classList.toggle("hidden", !isStatus);
      if (confirmPanel) confirmPanel.classList.toggle("hidden", !isStatus);
      [
        ["logBox", logBox],
        ["summaryWrap", summaryWrap],
        ["statusWrap", statusWrap],
      ].forEach(([id, el]) => {
        if (!el) return;
        ["mousedown", "mouseup", "wheel", "scroll", "touchstart", "keydown"].forEach((ev) => {
          el.addEventListener(ev, () => lockPre(id, 5000), { passive: true });
        });
      });
      wireButtonTips();
      updateStatusFilterButton();
      updatePageTitle();
      refreshNow();
      setInterval(refreshNow, 1000);
      window.addEventListener('storage', (event) => {
        if (event && event.key === 'managemovie.ui.refresh') {
          refreshNow();
        }
      });
    }

    init();
  </script>
</body>
</html>
"""


STOP_WINDOW_TEMPLATE = """
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ site_title }} | Abbruch bestaetigen</title>
  <script>
    (function () {
      try {
        const theme = (localStorage.getItem('managemovie.theme') || 'light').toLowerCase() === 'dark' ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', theme);
      } catch (err) {
        document.documentElement.setAttribute('data-theme', 'light');
      }
    })();
  </script>
  <style>
    body { font-family: "SF Pro Text", "SF Pro Display", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", sans-serif; margin: 0; background: linear-gradient(180deg, #edf3ff 0%, #e8eef8 100%); color: #101828; }
    .card { max-width: 540px; margin: 28px auto; background: rgba(255,255,255,0.9); border: 1px solid rgba(70,84,104,0.22); border-radius: 16px; padding: 16px; box-shadow: 0 18px 36px rgba(24,39,75,0.18); backdrop-filter: blur(14px) saturate(130%); }
    .head { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 10px; }
    h1 { margin: 0; font-size: 1.05rem; display: inline-flex; align-items: center; gap: 10px; }
    .run-dot { width: 18px; height: 18px; border-radius: 999px; border: 2px solid rgba(0,0,0,0.2); background: #9aa3ad; box-shadow: 0 0 0 3px rgba(255,255,255,0.9) inset; flex: 0 0 auto; }
    .run-dot.running { background: #29a745; }
    .run-dot.stopped { background: #9aa3ad; }
    p { margin: 0 0 14px 0; line-height: 1.35; }
    .row { display: grid; gap: 8px; grid-template-columns: 1fr 1fr; }
    button { border: 1px solid #6b8f84; background: #eef5f2; color: #173630; border-radius: 8px; padding: 8px 12px; font-weight: 700; cursor: pointer; }
    button[title="Klein"],
    button[title="Einklappen"] { font-size: 1.28rem; font-weight: 900; line-height: 1; min-width: 46px; }
    .row button { width: 100%; }
    .danger { border-color: #b42318; background: #d92d20; color: #fff; }
    #msg { margin-top: 10px; color: #0d352c; font-size: 0.92rem; white-space: pre-wrap; }
    html[data-theme="dark"] body { background: linear-gradient(180deg, #0e1320 0%, #080c14 100%); color: #e6edf8; }
    html[data-theme="dark"] .card { background: rgba(18,24,36,0.88); border-color: rgba(136,156,186,0.34); box-shadow: 0 18px 36px rgba(0,0,0,0.48); }
    html[data-theme="dark"] button { background: #1b2436; border-color: rgba(136,156,186,0.4); color: #e6edf8; }
  </style>
</head>
<body>
  <div class="card">
    <div class="head">
      <h1><span>{{ site_title }} {{ version_current }} | Exit</span><span id="stopRunDot" class="run-dot stopped" aria-hidden="true"></span></h1>
      <button type="button" title="Einklappen" aria-label="Einklappen" onclick="window.close()">↙</button>
    </div>
    <p>Lauf wirklich abbrechen?</p>
    <div class="row">
      <button type="button" onclick="window.close()">Zurueck</button>
      <button type="button" class="danger" onclick="confirmStop()">Exit</button>
    </div>
    <div id="msg"></div>
  </div>
  <script>
    function applyThemeFromStorage() {
      try {
        const t = (localStorage.getItem('managemovie.theme') || 'light').toLowerCase() === 'dark' ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', t);
      } catch (err) {
        document.documentElement.setAttribute('data-theme', 'light');
      }
    }
    applyThemeFromStorage();
    window.addEventListener('storage', (event) => {
      if (event && event.key === 'managemovie.theme') applyThemeFromStorage();
    });
    function isJobRunningState(job) {
      if (!job || typeof job !== 'object') return false;
      const rawRunning = job.running;
      let running = (rawRunning === true || rawRunning === 1 || rawRunning === '1' || rawRunning === 'true');
      if (job.job_id === 'last-run') running = false;
      if (job.mode === 'unknown' && !job.started_at && !job.ended_at) running = false;
      return running;
    }

    function setRunDot(running) {
      const dot = document.getElementById('stopRunDot');
      if (!dot) return;
      const isRunning = !!running;
      dot.classList.toggle('running', isRunning);
      dot.classList.toggle('stopped', !isRunning);
      dot.title = isRunning ? 'Job läuft' : 'Kein laufender Job';
      dot.setAttribute('aria-label', isRunning ? 'Job läuft' : 'Kein laufender Job');
    }

    async function refreshRunDot() {
      try {
        const res = await fetch('/api/state', { cache: 'no-store' });
        const data = await res.json().catch(() => ({}));
        setRunDot(isJobRunningState((data && data.job) ? data.job : {}));
      } catch (err) {
        setRunDot(false);
      }
    }

    async function confirmStop() {
      const msg = document.getElementById('msg');
      if (msg) msg.innerText = 'Stoppe Job...';
      try {
        const res = await fetch('/api/stop', { method: 'POST' });
        const data = await res.json().catch(() => ({}));
        if (res.ok && data && data.ok) {
          if (msg) msg.innerText = 'Job gestoppt.';
          try {
            if (window.opener && typeof window.opener.refreshState === 'function') {
              await window.opener.refreshState();
            }
            if (window.opener && typeof window.opener.collapseToHomeLayout === 'function') {
              window.opener.collapseToHomeLayout();
            }
          } catch (err) {
          }
          setTimeout(() => { window.close(); }, 600);
          return;
        }
        const err = (data && data.error) ? String(data.error) : 'Stop fehlgeschlagen.';
        if (msg) msg.innerText = err;
      } catch (err) {
        if (msg) msg.innerText = 'Stop fehlgeschlagen.';
      }
    }
    refreshRunDot();
    setInterval(refreshRunDot, 1200);
  </script>
</body>
</html>
"""


RESTART_WINDOW_TEMPLATE = """
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ site_title }} | Restart bestätigen</title>
  <script>
    (function () {
      try {
        const theme = (localStorage.getItem('managemovie.theme') || 'light').toLowerCase() === 'dark' ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', theme);
      } catch (err) {
        document.documentElement.setAttribute('data-theme', 'light');
      }
    })();
  </script>
  <style>
    body { font-family: "SF Pro Text", "SF Pro Display", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", sans-serif; margin: 0; background: linear-gradient(180deg, #edf3ff 0%, #e8eef8 100%); color: #101828; }
    .card { max-width: 620px; margin: 28px auto; background: rgba(255,255,255,0.9); border: 1px solid rgba(70,84,104,0.22); border-radius: 18px; padding: 18px; box-shadow: 0 18px 36px rgba(24,39,75,0.18); backdrop-filter: blur(14px) saturate(130%); }
    .head { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 14px; }
    h1 { margin: 0; font-size: 1.05rem; display: inline-flex; align-items: center; gap: 10px; }
    .title-stack { display: flex; flex-direction: column; gap: 4px; }
    .title-sub { font-size: 0.84rem; color: #475467; }
    .panel { margin: 0 0 16px 0; padding: 14px 16px; border-radius: 14px; background: rgba(244, 247, 252, 0.92); border: 1px solid rgba(70,84,104,0.18); }
    .panel strong { display: block; margin-bottom: 6px; font-size: 0.95rem; }
    .run-dot { width: 18px; height: 18px; border-radius: 999px; border: 2px solid rgba(0,0,0,0.2); background: #9aa3ad; box-shadow: 0 0 0 3px rgba(255,255,255,0.9) inset; flex: 0 0 auto; }
    .run-dot.running { background: #29a745; }
    .run-dot.stopped { background: #9aa3ad; }
    p { margin: 0; line-height: 1.45; }
    .row { display: flex; gap: 10px; justify-content: flex-end; }
    button { border: 1px solid #6b8f84; background: #eef5f2; color: #173630; border-radius: 10px; padding: 10px 14px; min-width: 138px; font-weight: 700; cursor: pointer; transition: transform 120ms ease, opacity 120ms ease; }
    button:hover { transform: translateY(-1px); }
    button[title="Klein"],
    button[title="Einklappen"] { font-size: 1.28rem; font-weight: 900; line-height: 1; min-width: 46px; }
    .secondary { border-color: rgba(70,84,104,0.24); background: #f7f9fc; color: #344054; }
    .danger { border-color: #b42318; background: #d92d20; color: #fff; }
    #msg { margin-top: 10px; color: #0d352c; font-size: 0.92rem; white-space: pre-wrap; }
    html[data-theme="dark"] body { background: linear-gradient(180deg, #0e1320 0%, #080c14 100%); color: #e6edf8; }
    html[data-theme="dark"] .card { background: rgba(18,24,36,0.88); border-color: rgba(136,156,186,0.34); box-shadow: 0 18px 36px rgba(0,0,0,0.48); }
    html[data-theme="dark"] .title-sub { color: #98a9c2; }
    html[data-theme="dark"] .panel { background: rgba(11,17,28,0.88); border-color: rgba(136,156,186,0.28); }
    html[data-theme="dark"] button { background: #1b2436; border-color: rgba(136,156,186,0.4); color: #e6edf8; }
    html[data-theme="dark"] .secondary { background: #121b2a; color: #d8e4f5; }
  </style>
</head>
<body>
  <div class="card">
    <div class="head">
      <div class="title-stack">
        <h1><span>{{ site_title }} {{ version_current }} | Restart</span><span id="restartRunDot" class="run-dot stopped" aria-hidden="true"></span></h1>
        <div class="title-sub">Dienst sauber neu starten und Fenster danach automatisch neu laden.</div>
      </div>
      <button type="button" title="Einklappen" aria-label="Einklappen" onclick="window.close()">↙</button>
    </div>
    <div class="panel">
      <strong>Aktion</strong>
      <p>App und DB neu starten. Laufende Prozesse werden beendet und der Webdienst danach automatisch wieder hochgefahren.</p>
    </div>
    <div class="row">
      <button type="button" class="secondary" onclick="window.close()">Zurück</button>
      <button type="button" class="danger" onclick="confirmRestart()">Restart</button>
    </div>
    <div id="msg"></div>
  </div>
  <script>
    function applyThemeFromStorage() {
      try {
        const t = (localStorage.getItem('managemovie.theme') || 'light').toLowerCase() === 'dark' ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', t);
      } catch (err) {
        document.documentElement.setAttribute('data-theme', 'light');
      }
    }
    applyThemeFromStorage();
    window.addEventListener('storage', (event) => {
      if (event && event.key === 'managemovie.theme') applyThemeFromStorage();
    });
    function isJobRunningState(job) {
      if (!job || typeof job !== 'object') return false;
      const rawRunning = job.running;
      let running = (rawRunning === true || rawRunning === 1 || rawRunning === '1' || rawRunning === 'true');
      if (job.job_id === 'last-run') running = false;
      if (job.mode === 'unknown' && !job.started_at && !job.ended_at) running = false;
      return running;
    }

    function setRunDot(running) {
      const dot = document.getElementById('restartRunDot');
      if (!dot) return;
      const isRunning = !!running;
      dot.classList.toggle('running', isRunning);
      dot.classList.toggle('stopped', !isRunning);
      dot.title = isRunning ? 'Job läuft' : 'Kein laufender Job';
      dot.setAttribute('aria-label', isRunning ? 'Job läuft' : 'Kein laufender Job');
    }

    async function refreshRunDot() {
      try {
        const res = await fetch('/api/state', { cache: 'no-store' });
        const data = await res.json().catch(() => ({}));
        setRunDot(isJobRunningState((data && data.job) ? data.job : {}));
      } catch (err) {
        setRunDot(false);
      }
    }

    async function confirmRestart() {
      const msg = document.getElementById('msg');
      if (msg) msg.innerText = 'Restart wird gestartet...';
      try {
        const res = await fetch('/api/system/restart', { method: 'POST' });
        const data = await res.json().catch(() => ({}));
        if (res.ok && data && data.ok) {
          if (msg) msg.innerText = 'Restart ausgelöst. Warte auf Neustart...';
          await waitForAppAndReload();
          return;
        }
        const err = (data && data.error) ? String(data.error) : 'Restart fehlgeschlagen.';
        if (msg) msg.innerText = err;
      } catch (err) {
        if (msg) msg.innerText = 'Verbindung getrennt. Pruefe Neustart...';
        await waitForAppAndReload();
      }
    }

    async function waitForAppAndReload() {
      const msg = document.getElementById('msg');
      const deadline = Date.now() + 120000;
      let attempt = 0;
      while (Date.now() < deadline) {
        attempt += 1;
        try {
          const res = await fetch('/api/state', { cache: 'no-store' });
          if (res.ok) {
            if (msg) msg.innerText = 'Restart abgeschlossen. Lade neu...';
            try {
              if (window.opener && !window.opener.closed) {
                window.opener.location.reload();
                window.close();
                return;
              }
            } catch (err) {
            }
            window.location.href = '/';
            return;
          }
        } catch (err) {
        }
        if (msg) msg.innerText = `Warte auf Neustart... (${attempt})`;
        await new Promise((resolve) => setTimeout(resolve, 1200));
      }
      if (msg) msg.innerText = 'Neustart läuft noch. Bitte Seite manuell neu laden.';
    }

    refreshRunDot();
    setInterval(refreshRunDot, 1200);
  </script>
</body>
</html>
"""


UPDATE_WINDOW_TEMPLATE = """
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ site_title }} | Update bestätigen</title>
  <script>
    (function () {
      try {
        const theme = (localStorage.getItem('managemovie.theme') || 'light').toLowerCase() === 'dark' ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', theme);
      } catch (err) {
        document.documentElement.setAttribute('data-theme', 'light');
      }
    })();
  </script>
  <style>
    body { font-family: "SF Pro Text", "SF Pro Display", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", sans-serif; margin: 0; background: linear-gradient(180deg, #edf3ff 0%, #e8eef8 100%); color: #101828; }
    .card { max-width: 620px; margin: 28px auto; background: rgba(255,255,255,0.9); border: 1px solid rgba(70,84,104,0.22); border-radius: 18px; padding: 18px; box-shadow: 0 18px 36px rgba(24,39,75,0.18); backdrop-filter: blur(14px) saturate(130%); }
    .head { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 14px; }
    h1 { margin: 0; font-size: 1.05rem; display: inline-flex; align-items: center; gap: 10px; }
    .title-stack { display: flex; flex-direction: column; gap: 4px; }
    .title-sub { font-size: 0.84rem; color: #475467; }
    .panel { margin: 0 0 16px 0; padding: 14px 16px; border-radius: 14px; background: rgba(244, 247, 252, 0.92); border: 1px solid rgba(70,84,104,0.18); }
    .panel strong { display: block; margin-bottom: 6px; font-size: 0.95rem; }
    .run-dot { width: 18px; height: 18px; border-radius: 999px; border: 2px solid rgba(0,0,0,0.2); background: #9aa3ad; box-shadow: 0 0 0 3px rgba(255,255,255,0.9) inset; flex: 0 0 auto; }
    .run-dot.running { background: #29a745; }
    .run-dot.stopped { background: #9aa3ad; }
    p { margin: 0; line-height: 1.45; }
    .row { display: flex; gap: 10px; justify-content: flex-end; }
    button { border: 1px solid #6b8f84; background: #eef5f2; color: #173630; border-radius: 10px; padding: 10px 14px; min-width: 138px; font-weight: 700; cursor: pointer; transition: transform 120ms ease, opacity 120ms ease; }
    button:hover { transform: translateY(-1px); }
    button[title="Klein"],
    button[title="Einklappen"] { font-size: 1.28rem; font-weight: 900; line-height: 1; min-width: 46px; }
    .secondary { border-color: rgba(70,84,104,0.24); background: #f7f9fc; color: #344054; }
    .danger { border-color: #0a84ff; background: #0a84ff; color: #fff; }
    #msg { margin-top: 10px; color: #0d352c; font-size: 0.92rem; white-space: pre-wrap; }
    html[data-theme="dark"] body { background: linear-gradient(180deg, #0e1320 0%, #080c14 100%); color: #e6edf8; }
    html[data-theme="dark"] .card { background: rgba(18,24,36,0.88); border-color: rgba(136,156,186,0.34); box-shadow: 0 18px 36px rgba(0,0,0,0.48); }
    html[data-theme="dark"] .title-sub { color: #98a9c2; }
    html[data-theme="dark"] .panel { background: rgba(11,17,28,0.88); border-color: rgba(136,156,186,0.28); }
    html[data-theme="dark"] button { background: #1b2436; border-color: rgba(136,156,186,0.4); color: #e6edf8; }
    html[data-theme="dark"] .secondary { background: #121b2a; color: #d8e4f5; }
    html[data-theme="dark"] .danger { background: #2f70ff; border-color: #2f70ff; color: #f7fbff; }
  </style>
</head>
<body>
  <div class="card">
    <div class="head">
      <div class="title-stack">
        <h1><span>{{ site_title }} {{ version_current }} | Update</span><span id="updateRunDot" class="run-dot stopped" aria-hidden="true"></span></h1>
        <div class="title-sub">Neuestes GitHub-Release übernehmen und den Dienst sauber neu laden.</div>
      </div>
      <button type="button" title="Einklappen" aria-label="Einklappen" onclick="window.close()">↙</button>
    </div>
    <div class="panel">
      <strong>Aktion</strong>
      <p>Es wird automatisch der neueste veröffentlichte Release-Tag geholt, lokal installiert und danach der Webdienst neu gestartet.</p>
    </div>
    <div class="row">
      <button type="button" class="secondary" onclick="window.close()">Zurück</button>
      <button type="button" class="danger" onclick="confirmUpdate()">Update</button>
    </div>
    <div id="msg"></div>
  </div>
  <script>
    function applyThemeFromStorage() {
      try {
        const t = (localStorage.getItem('managemovie.theme') || 'light').toLowerCase() === 'dark' ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', t);
      } catch (err) {
        document.documentElement.setAttribute('data-theme', 'light');
      }
    }
    applyThemeFromStorage();
    window.addEventListener('storage', (event) => {
      if (event && event.key === 'managemovie.theme') applyThemeFromStorage();
    });
    function isJobRunningState(job) {
      if (!job || typeof job !== 'object') return false;
      const rawRunning = job.running;
      let running = (rawRunning === true || rawRunning === 1 || rawRunning === '1' || rawRunning === 'true');
      if (job.job_id === 'last-run') running = false;
      if (job.mode === 'unknown' && !job.started_at && !job.ended_at) running = false;
      return running;
    }

    function setRunDot(running) {
      const dot = document.getElementById('updateRunDot');
      if (!dot) return;
      const isRunning = !!running;
      dot.classList.toggle('running', isRunning);
      dot.classList.toggle('stopped', !isRunning);
      dot.title = isRunning ? 'Job läuft' : 'Kein laufender Job';
      dot.setAttribute('aria-label', isRunning ? 'Job läuft' : 'Kein laufender Job');
    }

    async function refreshRunDot() {
      try {
        const res = await fetch('/api/state', { cache: 'no-store' });
        const data = await res.json().catch(() => ({}));
        setRunDot(isJobRunningState((data && data.job) ? data.job : {}));
      } catch (err) {
        setRunDot(false);
      }
    }

    async function confirmUpdate() {
      const msg = document.getElementById('msg');
      if (msg) msg.innerText = 'Update wird gestartet...';
      try {
        const res = await fetch('/api/system/update', { method: 'POST' });
        const data = await res.json().catch(() => ({}));
        if (res.ok && data && data.ok) {
          if (msg) msg.innerText = 'Update ausgelöst. Warte auf Neustart...';
          await waitForAppAndReload();
          return;
        }
        const err = (data && data.error) ? String(data.error) : 'Update fehlgeschlagen.';
        if (msg) msg.innerText = err;
      } catch (err) {
        if (msg) msg.innerText = 'Verbindung getrennt. Pruefe Update...';
        await waitForAppAndReload();
      }
    }

    async function waitForAppAndReload() {
      const msg = document.getElementById('msg');
      const deadline = Date.now() + 180000;
      let attempt = 0;
      while (Date.now() < deadline) {
        attempt += 1;
        try {
          const res = await fetch('/api/state', { cache: 'no-store' });
          if (res.ok) {
            if (msg) msg.innerText = 'Update abgeschlossen. Lade neu...';
            try {
              if (window.opener && !window.opener.closed) {
                window.opener.location.reload();
                window.close();
                return;
              }
            } catch (err) {
            }
            window.location.href = '/';
            return;
          }
        } catch (err) {
        }
        if (msg) msg.innerText = `Warte auf Update... (${attempt})`;
        await new Promise((resolve) => setTimeout(resolve, 1500));
      }
      if (msg) msg.innerText = 'Update läuft noch. Bitte Seite manuell neu laden.';
    }

    refreshRunDot();
    setInterval(refreshRunDot, 1200);
  </script>
</body>
</html>
"""


CONFIRM_WINDOW_TEMPLATE = """
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ site_title }} | Freigabe</title>
  <script>
    (function () {
      try {
        const theme = (localStorage.getItem('managemovie.theme') || 'light').toLowerCase() === 'dark' ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', theme);
      } catch (err) {
        document.documentElement.setAttribute('data-theme', 'light');
      }
    })();
  </script>
  <style>
    html, body { height: 100%; }
    body { font-family: "SF Pro Text", "SF Pro Display", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", sans-serif; margin: 0; background: linear-gradient(180deg, #edf3ff 0%, #e8eef8 100%); color: #101828; }
    body { padding: 12px; box-sizing: border-box; overflow: hidden; }
    .card { max-width: 1320px; height: 100%; min-height: 0; margin: 0 auto; background: rgba(255,255,255,0.9); border: 1px solid rgba(70,84,104,0.22); border-radius: 16px; padding: 12px; box-shadow: 0 18px 36px rgba(24,39,75,0.2); display: flex; flex-direction: column; gap: 10px; box-sizing: border-box; }
    .head { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 10px; }
    h1 { margin: 0; font-size: 1.05rem; display: inline-flex; align-items: center; gap: 10px; }
    .run-dot { width: 18px; height: 18px; border-radius: 999px; border: 2px solid rgba(0,0,0,0.2); background: #9aa3ad; box-shadow: 0 0 0 3px rgba(255,255,255,0.9) inset; flex: 0 0 auto; }
    .run-dot.running { background: #29a745; }
    .run-dot.stopped { background: #9aa3ad; }
    .preview { margin-top: 0; border: 1px solid rgba(70,84,104,0.2); border-radius: 10px; background: rgba(255,255,255,0.86); overflow: auto; flex: 1 1 auto; min-height: 0; max-height: none; }
	    .preview table { width: 100%; border-collapse: collapse; min-width: 960px; }
	    .preview th, .preview td { border-bottom: 1px solid #e4ebe2; padding: 6px 8px; font-size: 0.8rem; white-space: nowrap; text-align: left; color: #11231c; }
	    .preview th { position: sticky; top: 0; background: #edf4f1; z-index: 1; }
	    .preview th.col-source, .preview td.col-source { width: 1%; min-width: 180px; max-width: 320px; }
	    .preview th.col-target, .preview td.col-target { min-width: 240px; max-width: 480px; }
	    .preview td.col-source, .preview td.col-target { white-space: normal; word-break: break-word; overflow: visible; text-overflow: clip; line-height: 1.32; }
	    .preview tr.row-missing td { background: #fde8e8 !important; color: #7a1b17; }
    .row { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 0; flex: 0 0 auto; justify-content: flex-end; align-items: center; }
    .row-secondary-actions { display: inline-flex; gap: 8px; align-items: center; flex-wrap: nowrap; white-space: nowrap; }
    button { border: 1px solid rgba(70, 84, 104, 0.28); background: rgba(255,255,255,0.86); color: #20344f; border-radius: 10px; padding: 8px 12px; font-weight: 800; cursor: pointer; }
    .action-btn { min-width: 148px; }
    .icon-btn { min-width: 44px; padding: 6px 10px; font-size: 1.28rem; font-weight: 900; line-height: 1; }
    #bExit { min-width: 92px; border-color: #0a84ff; background: #0a84ff; color: #f7fbff; }
    .hidden { display: none !important; }
    #msg { margin-top: 0; color: #0d352c; min-height: 20px; white-space: pre-wrap; flex: 0 0 auto; }
    html[data-theme="dark"] body { background: linear-gradient(180deg, #0e1320 0%, #080c14 100%); color: #e6edf8; }
    html[data-theme="dark"] .card { background: rgba(18,24,36,0.88); border-color: rgba(136,156,186,0.34); box-shadow: 0 18px 36px rgba(0,0,0,0.48); }
    html[data-theme="dark"] .preview { background: #111827; border-color: rgba(136,156,186,0.34); }
    html[data-theme="dark"] .preview th { background: #1b2436; color: #e6edf8; }
    html[data-theme="dark"] .preview td { border-color: rgba(136,156,186,0.22); color: #d7e3f6; }
    html[data-theme="dark"] button { background: #1b2436; border-color: rgba(136,156,186,0.4); color: #e6edf8; }
    html[data-theme="dark"] #bExit { background: #2f70ff; border-color: #2f70ff; color: #f7fbff; }
    .preview { scrollbar-width: thin; scrollbar-color: #bccbe7 #eef3ff; }
    .preview::-webkit-scrollbar { width: 12px; height: 12px; }
    .preview::-webkit-scrollbar-track { background: #eef3ff; border-radius: 999px; }
    .preview::-webkit-scrollbar-thumb { background: #bccbe7; border-radius: 999px; border: 2px solid #eef3ff; }
    html[data-theme="dark"] .preview { scrollbar-color: #223349 #050a12; }
    html[data-theme="dark"] .preview::-webkit-scrollbar-track { background: #050a12; }
    html[data-theme="dark"] .preview::-webkit-scrollbar-thumb { background: #223349; border-color: #050a12; }
  </style>
</head>
<body>
  <div class="card">
    <div class="head">
      <h1><span>{{ site_title }} {{ version_current }} | Freigabe</span><span id="confirmRunDot" class="run-dot stopped" aria-hidden="true"></span></h1>
      <button type="button" class="icon-btn" title="Einklappen" aria-label="Einklappen" onclick="window.close()">↙</button>
    </div>
    <div class="preview" id="previewBox">
      <table>
        <thead>
	          <tr>
	            <th>Nr</th>
	            <th class="col-source">Quelle</th>
	            <th class="col-target">Ziel</th>
	            <th>Jahr</th>
	            <th>St/E-M</th>
            <th>IMDB-ID</th>
            <th>Q-GB</th>
            <th>Z-GB</th>
            <th>E-GB</th>
            <th>Speed</th>
            <th>ETA</th>
          </tr>
        </thead>
	        <tbody id="previewBody">
	          <tr><td colspan="11">lade...</td></tr>
	        </tbody>
	      </table>
    </div>
    <div class="row">
      <button id="bCopy" class="action-btn hidden" type="button" onclick="decide('copy')">Copy</button>
      <button id="bEncode" class="action-btn hidden" type="button" onclick="decide('encode')">Encode</button>
      <button id="bAnalyze" class="action-btn hidden" type="button" onclick="decide('ok')">Analyze OK</button>
      <button id="bClean" class="action-btn" type="button" onclick="decide('clean')">Reset "Erledigt"</button>
      <div class="row-secondary-actions">
        <button id="bEdit" class="action-btn" type="button" onclick="openEditorInline()">Editor</button>
        <button id="bExit" class="action-btn" type="button" onclick="window.close()">Exit</button>
      </div>
      <button id="bEditPopout" type="button" class="icon-btn hidden" title="Editor neues Fenster" aria-label="Editor neues Fenster" onclick="openEditorPopout()">⧉</button>
      <button id="bCancel" class="action-btn hidden" type="button" onclick="decide('cancel')">Exit</button>
    </div>
    <div id="msg"></div>
  </div>
  <script>
    function applyThemeFromStorage() {
      try {
        const t = (localStorage.getItem('managemovie.theme') || 'light').toLowerCase() === 'dark' ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', t);
      } catch (err) {
        document.documentElement.setAttribute('data-theme', 'light');
      }
    }
    applyThemeFromStorage();
    window.addEventListener('storage', (event) => {
      if (event && event.key === 'managemovie.theme') applyThemeFromStorage();
    });
    let pending = null;
    let tokenFromUrl = "{{ token }}";
    let previewRows = [];
    let previewToken = '';

    function setMsg(text) {
      const el = document.getElementById('msg');
      if (el) el.innerText = String(text || '');
    }

    function isJobRunningState(job) {
      if (!job || typeof job !== 'object') return false;
      const rawRunning = job.running;
      let running = (rawRunning === true || rawRunning === 1 || rawRunning === '1' || rawRunning === 'true');
      if (job.job_id === 'last-run') running = false;
      if (job.mode === 'unknown' && !job.started_at && !job.ended_at) running = false;
      return running;
    }

    function setRunDot(running) {
      const dot = document.getElementById('confirmRunDot');
      if (!dot) return;
      const isRunning = !!running;
      dot.classList.toggle('running', isRunning);
      dot.classList.toggle('stopped', !isRunning);
      dot.title = isRunning ? 'Job läuft' : 'Kein laufender Job';
      dot.setAttribute('aria-label', isRunning ? 'Job läuft' : 'Kein laufender Job');
    }

    function esc(v) {
      return String(v || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
    }

    function formatRatio(pos, total) {
      const p = Math.max(1, Number(pos || 1));
      const t = Math.max(1, Number(total || 1));
      const width = Math.max(2, String(t).length);
      return `${String(p).padStart(width, '0')}/${String(t).padStart(width, '0')}`;
    }

    function fileNameOnly(v) {
      const text = String(v || '').replace(/\\\\/g, '/').trim();
      if (!text) return '';
      const parts = text.split('/');
      return (parts[parts.length - 1] || '').trim();
    }

    function renderPreview() {
      const body = document.getElementById('previewBody');
      if (!body) return;
	      const hasToken = !!String((pending && pending.token) ? pending.token : (tokenFromUrl || '')).trim();
	      if (!pending && !hasToken) {
	        body.innerHTML = '<tr><td colspan="11">Keine aktive Freigabe.</td></tr>';
	        return;
	      }
	      if (!Array.isArray(previewRows) || previewRows.length === 0) {
	        body.innerHTML = '<tr><td colspan="11">Keine Dateiliste verfuegbar.</td></tr>';
	        return;
	      }
      const total = previewRows.length;
      body.innerHTML = previewRows.map((row, idx) => {
        const season = String(row.season || '').trim();
        const episode = String(row.episode || '').trim();
        const stEm = (season && episode) ? `S${season}E${episode}` : 'Movie';
        const qGb = String(row.q_gb || '').trim() || 'n/a';
        const zGb = String(row.z_gb || '').trim() || 'n/a';
        const eGb = String(row.e_gb || '').trim() || 'n/a';
        const speed = String(row.speed || '').trim() || 'n/a';
        const eta = String(row.eta || '').trim() || 'n/a';
        const year = String(row.year || '').trim() || '0000';
        const imdbId = String(row.imdb_id || '').trim() || 'tt0000000';
        const missing = (year === '0000') || (imdbId.toLowerCase() === 'tt0000000');
        const source = fileNameOnly(row.source_name || '') || '-';
        const target = fileNameOnly(row.target_name || '') || '-';
	        return `
	          <tr class="${missing ? 'row-missing' : ''}">
	            <td>${formatRatio(Number(row.nr || (idx + 1)), total)}</td>
	            <td class="col-source" title="${esc(row.source_name || '')}">${esc(source)}</td>
	            <td class="col-target" title="${esc(row.target_name || '')}">${esc(target)}</td>
	            <td>${esc(year)}</td>
	            <td>${esc(stEm)}</td>
            <td>${esc(imdbId)}</td>
            <td>${esc(qGb)}</td>
            <td>${esc(zGb)}</td>
            <td>${esc(eGb)}</td>
            <td>${esc(speed)}</td>
            <td>${esc(eta)}</td>
          </tr>
        `;
      }).join('');
    }

    async function refreshPreviewRows(force = false) {
      const token = String((pending && pending.token) ? pending.token : (tokenFromUrl || '')).trim();
      if (!token) {
        previewRows = [];
        previewToken = '';
        renderPreview();
        return;
      }
      try {
        const res = await fetch(`/api/confirm/editor?token=${encodeURIComponent(token)}`, { cache: 'no-store' });
        const data = await res.json().catch(() => ({}));
        if (res.ok && data && data.ok) {
          previewRows = Array.isArray(data.rows) ? data.rows : [];
          previewToken = token;
        } else {
          previewRows = [];
          previewToken = token;
        }
      } catch (err) {
        previewRows = [];
      }
      renderPreview();
    }

    function render() {
      const bCopy = document.getElementById('bCopy');
      const bEncode = document.getElementById('bEncode');
      const bAnalyze = document.getElementById('bAnalyze');
      const hasToken = !!String(tokenFromUrl || '').trim();
      if (!pending && !hasToken) {
        if (bCopy) bCopy.classList.add('hidden');
        if (bEncode) bEncode.classList.add('hidden');
        if (bAnalyze) bAnalyze.classList.add('hidden');
        renderPreview();
        return;
      }
      const mode = String((pending && pending.mode) ? pending.mode : '').toLowerCase();
      if (bCopy) bCopy.classList.toggle('hidden', mode !== 'copy');
      if (bEncode) bEncode.classList.toggle('hidden', mode !== 'ffmpeg');
      if (bAnalyze) bAnalyze.classList.toggle('hidden', mode !== 'analyze' && !!pending);
      renderPreview();
    }

    function mainStateApiUrl() {
      const params = new URLSearchParams();
      params.set('log_lines', '2400');
      params.set('log_max_chars', '1200000');
      return `/api/state?${params.toString()}`;
    }

    async function refreshState() {
      try {
        const res = await fetch(mainStateApiUrl(), { cache: 'no-store' });
        const data = await res.json();
        const p = data && data.pending_confirmation ? data.pending_confirmation : null;
        setRunDot(isJobRunningState((data && data.job) ? data.job : {}));
        pending = p;
        if (p && p.token) tokenFromUrl = String(p.token);
      } catch (err) {
        setRunDot(false);
        pending = null;
      }
      render();
      await refreshPreviewRows();
    }

    async function decide(state) {
      const token = String((pending && pending.token) ? pending.token : (tokenFromUrl || ''));
      if (!token) return;
      setMsg('Bitte warten...');
      try {
        if (state === 'clean') {
          const res = await fetch('/api/confirm/clean', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ token, start_folder: (pending && pending.start_folder) ? pending.start_folder : '' }),
          });
          const data = await res.json().catch(() => ({}));
          if (res.ok && data && data.ok) {
            setMsg(`Reset "Erledigt" erledigt: gelöscht ${Number(data.deleted || 0)}, Fehler ${Number(data.failed || 0)}`);
            await refreshState();
            return;
          }
          setMsg((data && data.error) ? String(data.error) : 'Reset "Erledigt" fehlgeschlagen');
          return;
        }

        const encoderEl = window.opener ? window.opener.document.getElementById('encoderSetting') : null;
        const encoder = (encoderEl && encoderEl.value) ? encoderEl.value : '';
        const decision = state === 'cancel' ? 'cancel' : 'start';
        const res = await fetch('/api/confirm', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ token, state: decision, encoder }),
        });
        const data = await res.json().catch(() => ({}));
        if (res.ok && data && data.ok) {
          setMsg('Freigabe gesendet.');
          if (state === 'cancel') {
            try {
              if (window.opener && typeof window.opener.refreshState === 'function') {
                window.opener.refreshState();
              }
              if (window.opener && typeof window.opener.collapseToHomeLayout === 'function') {
                window.opener.collapseToHomeLayout();
              }
            } catch (err) {
            }
          } else {
            try {
              if (window.opener && typeof window.opener.closeLogModal === 'function') {
                window.opener.closeLogModal();
              }
              if (window.opener && typeof window.opener.setCardCollapsed === 'function') {
                window.opener.setCardCollapsed('statusCard', true);
              }
            } catch (err) {
            }
            setTimeout(() => {
              try {
                window.close();
              } catch (err) {
              }
              try {
                if (!window.closed) {
                  window.location.href = '/';
                }
              } catch (err) {
              }
            }, 120);
          }
          if (state === 'cancel') {
            setTimeout(() => {
              try {
                window.close();
              } catch (err) {
              }
              try {
                if (!window.closed) {
                  window.location.href = '/';
                }
              } catch (err) {
              }
            }, 120);
          }
          return;
        }
        setMsg((data && data.error) ? String(data.error) : 'Freigabe fehlgeschlagen');
      } catch (err) {
        setMsg('Freigabe fehlgeschlagen');
      }
    }

    function openEditorInline() {
      const token = (pending && pending.token) ? String(pending.token) : String(tokenFromUrl || '');
      const baseUrl = token
        ? `/confirm-editor-window?token=${encodeURIComponent(token)}`
        : '/confirm-editor-window';
      const theme = (document.documentElement.getAttribute('data-theme') || '').toLowerCase() === 'dark' ? 'dark' : 'light';
      const url = `${baseUrl}${baseUrl.includes('?') ? '&' : '?'}theme=${encodeURIComponent(theme)}`;
      window.location.href = url;
    }

    function editorPopupFeatures() {
      const availW = Math.max(900, Number(window.screen && window.screen.availWidth) || 1366);
      const availH = Math.max(720, Number(window.screen && window.screen.availHeight) || 900);
      const width = Math.max(920, Math.min(1220, availW - 90));
      const height = Math.max(720, Math.min(860, availH - 90));
      return `noopener,noreferrer,width=${Math.round(width)},height=${Math.round(height)}`;
    }

    function openEditorPopout() {
      const token = (pending && pending.token) ? String(pending.token) : String(tokenFromUrl || '');
      const baseUrl = token
        ? `/confirm-editor-window?token=${encodeURIComponent(token)}`
        : '/confirm-editor-window';
      const theme = (document.documentElement.getAttribute('data-theme') || '').toLowerCase() === 'dark' ? 'dark' : 'light';
      const url = `${baseUrl}${baseUrl.includes('?') ? '&' : '?'}theme=${encodeURIComponent(theme)}`;
      const w = window.open(url, '_blank', editorPopupFeatures());
      if (w) {
        w.focus();
        return;
      }
      window.location.href = url;
    }

    refreshState();
    setInterval(refreshState, 1200);
  </script>
</body>
</html>
"""


CONFIRM_EDITOR_TEMPLATE = """
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ site_title }} | Editor</title>
  <script>
    (function () {
      try {
        const params = new URLSearchParams(window.location.search || '');
        const forcedTheme = (params.get('theme') || '').toLowerCase();
        const theme = forcedTheme === 'dark' || forcedTheme === 'light'
          ? forcedTheme
          : ((localStorage.getItem('managemovie.theme') || 'light').toLowerCase() === 'dark' ? 'dark' : 'light');
        document.documentElement.setAttribute('data-theme', theme);
      } catch (err) {
        document.documentElement.setAttribute('data-theme', 'light');
      }
    })();
  </script>
  <style>
    html, body {
      height: 100%;
    }
    :root {
      --bg0: #edf3ff;
      --bg1: #f7faff;
      --line: #d5deef;
      --line-soft: #e6ecf7;
      --ink: #101828;
      --muted: #36527a;
      --ok: #0a84ff;
      --ok-soft: #dcebff;
      --sel: #cfdbef;
      --active: #dbe5f7;
      --done: #e8f7ec;
      --editor-imdb-col-width: 132px;
    }
    body {
      margin: 0;
      font-family: "SF Pro Rounded", "SF Pro Display", "SF Pro Text", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at 80% -20%, #ddebff 0%, transparent 42%),
        linear-gradient(165deg, var(--bg0) 0%, var(--bg1) 70%);
      color: var(--ink);
      overflow: hidden;
    }
    .page {
      display: flex;
      padding: 10px;
      box-sizing: border-box;
      height: 100%;
      min-height: 0;
    }
    .card {
      background: rgba(255,255,255,0.9);
      border: 1px solid rgba(70,84,104,0.22);
      border-radius: 16px;
      padding: 12px;
      box-shadow: 0 18px 36px rgba(24,39,75,0.2);
      display: flex;
      flex-direction: column;
      gap: 8px;
      width: 100%;
      min-height: 0;
    }
    h1 { margin: 0 0 8px 0; font-size: 1.05rem; letter-spacing: 0.01em; display: inline-flex; align-items: center; gap: 10px; }
    .run-dot { width: 18px; height: 18px; border-radius: 999px; border: 2px solid rgba(0,0,0,0.2); background: #9aa3ad; box-shadow: 0 0 0 3px rgba(255,255,255,0.9) inset; flex: 0 0 auto; }
    .run-dot.running { background: #29a745; }
    .run-dot.stopped { background: #9aa3ad; }
    .title-row { display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 8px; }
    .title-row h1 { margin: 0; }
    .title-actions { display: flex; align-items: center; gap: 8px; }
    .table-wrap {
      flex: 1 1 auto;
      min-height: 0;
      max-height: none;
      overflow: auto;
      overflow-x: auto;
      border: 1px solid rgba(70,84,104,0.2);
      border-radius: 12px;
      background: rgba(255,255,255,0.86);
    }
    table { width: max-content; min-width: 100%; border-collapse: collapse; }
    th, td { border-bottom: 1px solid var(--line-soft); padding: 6px 8px; white-space: nowrap; font-size: 0.82rem; vertical-align: top; }
    th { position: sticky; top: 0; z-index: 2; background: #edf2ff; font-size: 0.74rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--muted); }
    tbody tr:nth-child(even) td { background: rgba(255,255,255,0.92); }
    tbody tr:hover td { background: #e7eefc; }
    tr.active td { background: var(--active) !important; }
    tr.selected td { background: #cfdbef !important; }
    tr.row-error td { background: #fde8e8 !important; color: #7a1b17; }
    tr.row-done td { background: var(--done) !important; color: #134226; }
    #tb.editor-filter-errors tr[data-row-idx]:not(.row-error) { display: none; }
    #tb.editor-filter-done tr[data-row-idx]:not(.row-done) { display: none; }
    tr.row-filtered { display: none; }
    .hidden { display: none !important; }
    input[type="text"] {
      width: 100%;
      min-width: 72px;
      box-sizing: border-box;
      border: 0;
      border-radius: 10px;
      padding: 7px 9px;
      background: rgba(245,248,255,0.98);
      color: var(--ink);
      font-size: 0.82rem;
      outline: none;
      box-shadow: inset 0 0 0 1px rgba(101,125,168,0.18);
      transition: box-shadow 0.12s ease, background-color 0.12s ease;
    }
    input[type="text"]:focus {
      box-shadow: 0 0 0 2px var(--ok-soft);
      background: #ffffff;
    }
    .nr-cell { width: 68px; min-width: 68px; max-width: 68px; }
    .source-cell { width: clamp(180px, 16vw, 300px); min-width: 180px; }
    .target-cell { width: clamp(340px, 31vw, 720px); min-width: 340px; }
    .title-cell { width: clamp(260px, 24vw, 520px); min-width: 260px; }
    .year-cell { width: 58px; min-width: 58px; max-width: 58px; }
    .se-cell { width: 54px; min-width: 54px; max-width: 54px; }
    .imdb-cell { width: var(--editor-imdb-col-width); min-width: var(--editor-imdb-col-width); max-width: var(--editor-imdb-col-width); }
    .row-action-cell { width: 92px; min-width: 92px; max-width: 92px; background: transparent; overflow: visible; }
    .row-inline-actions {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      justify-content: flex-end;
      align-items: center;
      width: min(100%, 520px);
      margin-left: auto;
      white-space: normal;
      padding: 0;
      border: 0;
      border-radius: 0;
      background: transparent;
      box-shadow: none;
    }
    .row-inline-actions.hidden { visibility: hidden; pointer-events: none; }
    .row-actions-tr td {
      padding-top: 0;
      padding-bottom: 10px;
      background: transparent !important;
      border-bottom: 1px solid var(--line-soft);
    }
    .row-actions-tr td:first-child {
      border-right: 0;
    }
    .row-actions-cell {
      padding-right: 8px;
    }
    .source-name-input {
      width: 100%;
      min-width: 72px;
      box-sizing: border-box;
      border: 0;
      border-radius: 10px;
      padding: 7px 9px;
      background: #f2f6ff;
      color: #102a4d;
      font-size: 0.82rem;
      font-family: inherit;
      box-shadow: inset 0 0 0 1px rgba(101,125,168,0.16);
    }
    .target-name-input { font-family: inherit; }
    button { border: 1px solid rgba(70,84,104,0.28); background: rgba(255,255,255,0.86); color: #20344f; border-radius: 10px; padding: 8px 10px; font-weight: 800; cursor: pointer; }
    .editor-toolbar-btn,
    .row-btn {
      width: auto;
      min-width: 116px;
      padding: 8px 12px;
      border-color: rgba(95, 122, 169, 0.36);
      background: linear-gradient(180deg, rgba(255,255,255,0.98) 0%, rgba(233,240,255,0.96) 100%);
      color: #1c3f77;
      font-weight: 800;
      font-size: 0.95rem;
      line-height: 1;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.9), 0 4px 12px rgba(52, 86, 138, 0.12);
    }
    .editor-toolbar-icon {
      min-width: 52px;
      padding: 6px 10px;
      font-size: 1.64rem;
      font-weight: 900;
    }
    .editor-filter-btn {
      min-width: 78px;
    }
    .editor-action-btn {
      border-color: #0a84ff;
      background: #0a84ff;
      color: #f7fbff;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.2);
    }
    .editor-action-btn:hover {
      background: #267fff;
      border-color: #267fff;
      color: #ffffff;
      filter: none;
    }
    button:hover { filter: brightness(0.98); }
    .row-btn {
      border-radius: 10px;
      min-width: 0;
      width: auto;
      flex: 1 1 120px;
      padding: 8px 6px;
      font-size: 0.74rem;
      line-height: 1;
      text-align: center;
    }
    .ok { border-color: var(--ok); background: var(--ok); color: #fff; }
    .danger { border-color: #b42318; background: #d92d20; color: #fff; }
    .warn { border-color: #ca8a04; background: #facc15; color: #3f2c00; }
    .editor-top-info {
      display: inline-flex;
      align-items: center;
      min-height: 0;
      padding: 0;
      border: 0;
      background: transparent;
      color: #1f3f77;
      font-weight: 600;
      font-size: 0.88rem;
      white-space: nowrap;
    }
    .muted { color: var(--muted); font-size: 0.82rem; line-height: 1.35; }
    .footer { display: flex; gap: 8px; justify-content: flex-end; margin-top: 0; flex: 0 0 auto; padding-right: 10px; }
    .footer .footer-btn { min-width: 148px; }
    .footer .footer-btn:last-child { margin-left: 14px; }
    #msg { margin-top: 0; min-height: 20px; white-space: pre-wrap; font-size: 0.9rem; color: #1b3f77; flex: 0 0 auto; }
    .confirm-overlay {
      position: fixed;
      inset: 0;
      background: rgba(17, 29, 48, 0.58);
      z-index: 1400;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 18px;
    }
    .confirm-overlay.hidden {
      display: none !important;
    }
    .confirm-dialog {
      width: min(560px, 92vw);
      background: #fff;
      border: 1px solid #d5deef;
      border-radius: 12px;
      box-shadow: 0 16px 36px rgba(0, 0, 0, 0.25);
      padding: 14px;
      display: grid;
      gap: 10px;
    }
    .confirm-dialog h3 {
      margin: 0;
      font-size: 1rem;
      color: #12305f;
    }
    .confirm-dialog p {
      margin: 0;
      white-space: pre-wrap;
      color: #1b335a;
      line-height: 1.4;
      font-size: 0.92rem;
    }
    .confirm-dialog-actions {
      display: flex;
      justify-content: flex-end;
      gap: 8px;
    }
    .confirm-dialog-actions button {
      width: auto;
      min-width: 132px;
      padding: 8px 12px;
    }
    @media (max-width: 1500px) {
      .table-wrap { max-height: none; }
    }
    @media (max-width: 980px) {
      th, td { font-size: 0.73rem; padding: 3px 4px; }
      .nr-cell { width: 60px; min-width: 60px; max-width: 60px; }
      .source-cell { width: clamp(160px, 22vw, 240px); min-width: 160px; }
      .target-cell { width: clamp(260px, 34vw, 420px); min-width: 260px; }
      .title-cell { width: clamp(220px, 28vw, 340px); min-width: 220px; }
      .row-action-cell { width: 86px; min-width: 86px; max-width: 86px; }
      .row-btn { padding: 7px 4px; font-size: 0.68rem; }
    }
    html[data-theme="dark"] body {
      background:
        radial-gradient(circle at 80% -20%, #2b3750 0%, transparent 42%),
        linear-gradient(165deg, #0d121d 0%, #090d15 70%);
      color: #e6edf8;
    }
    html[data-theme="dark"] .card,
    html[data-theme="dark"] .table-wrap,
    html[data-theme="dark"] .confirm-dialog {
      background: rgba(17,24,39,0.98);
      border-color: rgba(136, 156, 186, 0.34);
      color: #d7e3f6;
    }
    html[data-theme="dark"] th { background: #1b2436; color: #dce8fb; }
    html[data-theme="dark"] td { border-color: rgba(136, 156, 186, 0.2); }
    html[data-theme="dark"] tbody tr:nth-child(even) td {
      background: #111827;
    }
    html[data-theme="dark"] tbody tr:hover td {
      background: #1a2538;
    }
    html[data-theme="dark"] tr.active td {
      background: #233149 !important;
      color: #f1f6ff;
    }
    html[data-theme="dark"] tr.active .row-action-cell,
    html[data-theme="dark"] tr.active .imdb-cell {
      background: #233149 !important;
      color: #f1f6ff;
    }
    html[data-theme="dark"] tr.selected td {
      background: #1f2b3f !important;
      color: #f1f6ff;
    }
    html[data-theme="dark"] tr.selected .row-action-cell,
    html[data-theme="dark"] tr.selected .imdb-cell {
      background: #1f2b3f !important;
      color: #f1f6ff;
    }
    html[data-theme="dark"] .row-inline-actions {
      background: transparent;
      border-color: transparent;
      box-shadow: none;
    }
    html[data-theme="dark"] input[type="text"],
    html[data-theme="dark"] button,
    html[data-theme="dark"] .editor-toolbar-btn,
    html[data-theme="dark"] .row-btn {
      background: linear-gradient(180deg, #2d3c57 0%, #213047 100%);
      border-color: rgba(155, 180, 220, 0.34);
      color: #eef5ff;
    }
    html[data-theme="dark"] input[type="text"] {
      border: 0;
      background: #172132;
      box-shadow: inset 0 0 0 1px rgba(136,156,186,0.22);
    }
    html[data-theme="dark"] .source-name-input {
      background: #1b2638;
      color: #dce8fb;
      box-shadow: inset 0 0 0 1px rgba(136,156,186,0.2);
    }
    html[data-theme="dark"] tr.row-done td {
      background: #163826 !important;
      color: #cfeedd;
    }
    html[data-theme="dark"] tr.row-done .row-action-cell,
    html[data-theme="dark"] tr.row-done .imdb-cell {
      background: #163826 !important;
      color: #cfeedd;
    }
    html[data-theme="dark"] .editor-top-info {
      color: #dce8fb;
    }
    html[data-theme="dark"] .editor-toolbar-btn.active {
      background: rgba(80, 132, 255, 0.28);
      border-color: rgba(137, 173, 255, 0.65);
      color: #f0f6ff;
    }
    html[data-theme="dark"] .editor-action-btn {
      background: #2f70ff;
      border-color: #2f70ff;
      color: #f7fbff;
    }
    html[data-theme="dark"] .editor-action-btn:hover {
      background: #4a84ff;
      border-color: #4a84ff;
      color: #ffffff;
    }
    html[data-theme="dark"] .muted,
    html[data-theme="dark"] #msg {
      color: #dce8fb;
    }
    html[data-theme="dark"] input[type="text"]::placeholder {
      color: #97a9c6;
    }
    html[data-theme="dark"] .confirm-overlay {
      background: rgba(5, 10, 18, 0.72);
    }
    html[data-theme="dark"] .confirm-dialog h3 {
      color: #dce8fb;
    }
    html[data-theme="dark"] .confirm-dialog p {
      color: #c8d6ea;
    }
    .table-wrap::-webkit-scrollbar {
      width: 12px;
      height: 12px;
    }
    .table-wrap::-webkit-scrollbar-track {
      background: rgba(214, 224, 242, 0.72);
      border-radius: 999px;
    }
    .table-wrap::-webkit-scrollbar-thumb {
      background: rgba(94, 121, 162, 0.72);
      border-radius: 999px;
      border: 2px solid rgba(214, 224, 242, 0.72);
    }
    html[data-theme="dark"] .table-wrap::-webkit-scrollbar-track {
      background: rgba(5, 10, 18, 0.98);
    }
    html[data-theme="dark"] .table-wrap::-webkit-scrollbar-thumb {
      background: rgba(34, 51, 73, 0.96);
      border-color: rgba(5, 10, 18, 0.98);
    }
  </style>
</head>
<body>
  <div class="page">
    <div class="card">
      <div class="title-row">
        <h1><span>{{ site_title }} {{ version_current }} | Editor</span><span id="editorRunDot" class="run-dot stopped" aria-hidden="true"></span></h1>
        <div class="title-actions">
          <span id="editorInfo" class="editor-top-info">Fehler 0/0 | Erledigt 0/0</span>
          <button id="bFilter" class="editor-toolbar-btn editor-filter-btn" type="button" onclick="toggleEditorModeFilter()">Alle</button>
          <button type="button" class="editor-toolbar-btn editor-toolbar-icon" title="Editor neues Fenster" aria-label="Editor neues Fenster" onclick="openEditorPopout()">⧉</button>
          <button type="button" class="editor-toolbar-btn editor-toolbar-icon" title="Einklappen" aria-label="Einklappen" onclick="closeEditorWindow()">↙</button>
        </div>
      </div>
      <div class="table-wrap">
        <table id="tbl">
          <thead>
            <tr>
              <th>Nr</th>
              <th>Quelle</th>
              <th>Ziel</th>
              <th>Name</th>
              <th>Jahr</th>
              <th>Staffel</th>
              <th>Episode</th>
              <th class="imdb-head">IMDB-ID</th>
              <th>Aktion</th>
            </tr>
          </thead>
          <tbody id="tb"></tbody>
        </table>
      </div>
      <div class="footer">
        <button class="footer-btn editor-action-btn" type="button" onclick="saveAll()">Save</button>
        <button class="footer-btn editor-action-btn" type="button" onclick="resetAll()">Reset Editor</button>
        <button class="footer-btn editor-action-btn" type="button" onclick="cleanAllManifests()">Reset "Erledigt"</button>
        <button class="footer-btn editor-action-btn" type="button" onclick="closeEditorWindow()">Exit</button>
      </div>
      <div id="msg"></div>
    </div>
  </div>
  <div id="editorConfirmOverlay" class="confirm-overlay hidden" onclick="if (event.target && event.target.id === 'editorConfirmOverlay') resolveEditorConfirm(false);">
    <div class="confirm-dialog">
      <h3 id="editorConfirmTitle">Bestätigen</h3>
      <p id="editorConfirmText"></p>
      <div class="confirm-dialog-actions">
        <button type="button" onclick="resolveEditorConfirm(false)">Zurück</button>
        <button id="editorConfirmOkBtn" type="button" class="editor-action-btn" onclick="resolveEditorConfirm(true)">Abbrechen</button>
      </div>
    </div>
  </div>
  <script>
    function activeThemeMode() {
      try {
        const params = new URLSearchParams(window.location.search || '');
        const forced = (params.get('theme') || '').toLowerCase();
        if (forced === 'dark' || forced === 'light') return forced;
      } catch (err) {
      }
      try {
        return (localStorage.getItem('managemovie.theme') || 'light').toLowerCase() === 'dark' ? 'dark' : 'light';
      } catch (err) {
        return 'light';
      }
    }

    function applyThemeFromStorage() {
      try {
        const t = activeThemeMode();
        document.documentElement.setAttribute('data-theme', t);
      } catch (err) {
        document.documentElement.setAttribute('data-theme', 'light');
      }
    }
    applyThemeFromStorage();
    window.addEventListener('storage', (event) => {
      if (event && event.key === 'managemovie.theme') applyThemeFromStorage();
    });
    let token = "{{ token }}";
    let rows = [];
    let baselineRows = [];
    let activeIndex = -1;
    let selected = new Set();
    let editorFilterMode = 'all';
    let startFolder = {{ initial_start_folder|tojson }};
    let outPrefix = {{ initial_target_out_prefix|tojson }};
    let reenqueuePrefix = {{ initial_target_reenqueue_prefix|tojson }};
    let initialRows = {{ initial_rows|tojson }};
    let editorConfirmResolver = null;
    let editorConfirmApproveLabel = 'Abbrechen';

    function setMsg(text) {
      const el = document.getElementById('msg');
      if (el) el.innerText = String(text || '');
    }

    function resolveEditorConfirm(ok) {
      const overlay = document.getElementById('editorConfirmOverlay');
      if (overlay) overlay.classList.add('hidden');
      const resolver = editorConfirmResolver;
      editorConfirmResolver = null;
      if (resolver) resolver(!!ok);
    }

    function askEditorConfirm(message, title = 'Bestätigen', confirmLabel = 'Abbrechen') {
      const overlay = document.getElementById('editorConfirmOverlay');
      const titleEl = document.getElementById('editorConfirmTitle');
      const textEl = document.getElementById('editorConfirmText');
      const okBtn = document.getElementById('editorConfirmOkBtn');
      if (!overlay || !textEl) return Promise.resolve(false);
      if (titleEl) titleEl.innerText = String(title || 'Bestätigen');
      editorConfirmApproveLabel = String(confirmLabel || 'Abbrechen').trim() || 'Abbrechen';
      if (okBtn) okBtn.innerText = editorConfirmApproveLabel;
      textEl.innerText = String(message || '').trim();
      if (editorConfirmResolver) {
        editorConfirmResolver(false);
      }
      overlay.classList.remove('hidden');
      return new Promise((resolve) => {
        editorConfirmResolver = resolve;
      });
    }

    function notifyEditorRefresh(reason = 'editor') {
      try {
        window.localStorage.setItem(
          'managemovie.ui.refresh',
          JSON.stringify({ ts: Date.now(), reason: String(reason || 'editor'), token: String(token || '') })
        );
      } catch (err) {
      }
    }

    async function closeEditorWindow() {
      if (hasPendingChanges()) {
        const ok = await saveAll({ silent: true, reason: 'exit' });
        if (!ok) {
          setMsg('Speichern vor Exit fehlgeschlagen. Bitte erneut Save klicken.');
          return;
        }
      }
      try {
        if (window.opener && typeof window.opener.refreshNow === 'function') {
          await window.opener.refreshNow();
        } else if (window.opener && typeof window.opener.refreshState === 'function') {
          await window.opener.refreshState();
        }
      } catch (err) {
      }
      notifyEditorRefresh('editor-exit');
      try {
        if (window.opener && !window.opener.closed) {
          window.close();
          return;
        }
      } catch (err) {
      }
      window.location.href = `/?editor_exit=${Date.now()}`;
    }

    function editorPopupFeatures() {
      const availW = Math.max(900, Number(window.screen && window.screen.availWidth) || 1366);
      const availH = Math.max(720, Number(window.screen && window.screen.availHeight) || 900);
      const width = Math.max(920, Math.min(1220, availW - 90));
      const height = Math.max(720, Math.min(860, availH - 90));
      return `noopener,noreferrer,width=${Math.round(width)},height=${Math.round(height)}`;
    }

    function openEditorPopout() {
      const t = String(token || '').trim();
      const theme = activeThemeMode();
      const baseUrl = t ? `/confirm-editor-window?token=${encodeURIComponent(t)}` : '/confirm-editor-window';
      const url = `${baseUrl}${baseUrl.includes('?') ? '&' : '?'}theme=${encodeURIComponent(theme)}`;
      const w = window.open(url, '_blank', editorPopupFeatures());
      if (w) {
        w.focus();
        return;
      }
      window.location.href = url;
    }

    function isJobRunningState(job) {
      if (!job || typeof job !== 'object') return false;
      const rawRunning = job.running;
      let running = (rawRunning === true || rawRunning === 1 || rawRunning === '1' || rawRunning === 'true');
      if (job.job_id === 'last-run') running = false;
      if (job.mode === 'unknown' && !job.started_at && !job.ended_at) running = false;
      return running;
    }

    function setRunDot(running) {
      const dot = document.getElementById('editorRunDot');
      if (!dot) return;
      const isRunning = !!running;
      dot.classList.toggle('running', isRunning);
      dot.classList.toggle('stopped', !isRunning);
      dot.title = isRunning ? 'Job läuft' : 'Kein laufender Job';
      dot.setAttribute('aria-label', isRunning ? 'Job läuft' : 'Kein laufender Job');
    }

    async function refreshRunDot() {
      try {
        const res = await fetch('/api/state', { cache: 'no-store' });
        const data = await res.json().catch(() => ({}));
        setRunDot(isJobRunningState((data && data.job) ? data.job : {}));
      } catch (err) {
        setRunDot(false);
      }
    }

    function clone(v) {
      return JSON.parse(JSON.stringify(v));
    }

    function hasPendingChanges() {
      try {
        return JSON.stringify(rows || []) !== JSON.stringify(baselineRows || []);
      } catch (err) {
        return true;
      }
    }

    function formatRatio(pos, total) {
      const p = Math.max(1, Number(pos || 1));
      const t = Math.max(1, Number(total || 1));
      const width = Math.max(2, String(t).length);
      return `${String(p).padStart(width, '0')}/${String(t).padStart(width, '0')}`;
    }

    function fileNameOnly(v) {
      const text = String(v || '').replace(/\\\\/g, '/').trim();
      if (!text) return '';
      const parts = text.split('/');
      return (parts[parts.length - 1] || '').trim();
    }

    function esc(v) {
      return String(v || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
    }

    function norm2(v) {
      const n = String(v || '').replace(/[^0-9]/g, '');
      if (!n) return '';
      return String(Number(n)).padStart(2, '0');
    }

    function cleanTitle(v) {
      return String(v || '').replace(/[._-]+/g, ' ').replace(/\\s+/g, ' ').trim();
    }

    function safeFolder(v) {
      const t = cleanTitle(v).replace(/[\\\\/:*?"<>|]+/g, '').replace(/\\s+/g, ' ').trim();
      return t || 'Unknown';
    }

    function transliterateGerman(v) {
      return String(v || '')
        .replace(/Ä/g, 'Ae')
        .replace(/Ö/g, 'Oe')
        .replace(/Ü/g, 'Ue')
        .replace(/ä/g, 'ae')
        .replace(/ö/g, 'oe')
        .replace(/ü/g, 'ue')
        .replace(/ß/g, 'ss');
    }

    function dotted(v) {
      const t = transliterateGerman(cleanTitle(v))
        .replace(/[^A-Za-z0-9 ]+/g, ' ')
        .replace(/\\s+/g, ' ')
        .trim();
      return t ? t.replace(/ /g, '.') : 'Unknown';
    }

    function parseYear(v) {
      const m = String(v || '').match(/(19|20)\\d{2}/);
      return m ? m[0] : '';
    }

    function parseImdb(v) {
      const text = String(v || '').toLowerCase().trim();
      if (!text) return '';
      let m = text.match(/tt\\d{7,10}/);
      if (m) return m[0];
      m = text.match(/(^|[^0-9])(\\d{7,10})(?!\\d)/);
      if (m && m[2]) return `tt${m[2]}`;
      return '';
    }

    function isSeriesRow(row) {
      const s = norm2(row.season);
      const e = norm2(row.episode);
      if (s && e) return true;
      return /(?:^|[ ./_\\-])s\\d{1,2}[ ._\\-]*e\\d{1,2}(?:[ ._\\-]*e\\d{1,2})*(?:$|[ ./_\\-])/i.test(String(row.source_name || ''));
    }

    function isMissingRow(row) {
      const year = String(row && row.year ? row.year : '').trim();
      const imdb = String(row && row.imdb_id ? row.imdb_id : '').trim().toLowerCase();
      return year === '' || year === '0000' || imdb === '' || imdb === 'tt0000000';
    }

    function isCompletedRow(row) {
      if (!row || typeof row !== 'object') return false;
      if (row.completed === true || row.completed === 1 || row.completed === '1') return true;
      const speed = String(row.speed || '').trim().toLowerCase();
      const eta = String(row.eta || '').trim().toLowerCase();
      const zGb = String(row.z_gb || '').trim().toLowerCase();
      return (
        speed.includes('copied')
        || speed.includes('encoded')
        || speed.includes('manual')
        || eta === 'copied'
        || eta === 'encoded'
        || eta === 'manual'
        || eta === '00:00'
        || (!!zGb && zGb !== 'n/a' && zGb !== '-' && zGb !== '0.0')
      );
    }

    function seriesGroupKey(row) {
      if (!row) return '';
      const season = norm2(row.season || '');
      if (!season) return '';
      const imdb = parseImdb(row.imdb_id || '');
      if (imdb) return `imdb:${imdb}|s:${season}`;
      const title = cleanTitle(row.title || '').toLowerCase();
      if (title) return `title:${title}|s:${season}`;
      const src = String(row.source_name || '').toLowerCase();
      const m = src.match(/^([^._\\-/]+(?:[ ._\\-][^._\\-/]+){0,5})/);
      const rough = m ? cleanTitle(m[1]).toLowerCase() : '';
      return rough ? `src:${rough}|s:${season}` : '';
    }

    function applyEditorModeFilter() {
      const btn = document.getElementById('bFilter');
      const info = document.getElementById('editorInfo');
      if (btn) {
        if (editorFilterMode === 'errors') btn.innerText = 'Fehler';
        else if (editorFilterMode === 'done') btn.innerText = 'Erledigt';
        else btn.innerText = 'Alle';
        btn.classList.toggle('active', editorFilterMode !== 'all');
      }
      const tb = document.getElementById('tb');
      if (!tb) return;
      tb.classList.toggle('editor-filter-errors', editorFilterMode === 'errors');
      tb.classList.toggle('editor-filter-done', editorFilterMode === 'done');
      if (info) {
        const total = Array.isArray(rows) ? rows.length : 0;
        const errors = (Array.isArray(rows) ? rows : []).filter((row) => isMissingRow(row)).length;
        const done = (Array.isArray(rows) ? rows : []).filter((row) => isCompletedRow(row)).length;
        info.innerText = `Fehler ${errors}/${total} | Erledigt ${done}/${total}`;
      }
    }

    function toggleEditorModeFilter() {
      const order = ['all', 'errors', 'done'];
      const idx = Math.max(0, order.indexOf(String(editorFilterMode || 'all')));
      editorFilterMode = order[(idx + 1) % order.length];
      applyEditorModeFilter();
    }

    function parseHintsFromTarget(row) {
      const target = String(row.target_name || '');
      const file = target.split('/').pop() || '';
      const yy = parseYear(file);
      const ii = parseImdb(file);
      const se = file.match(/s(\\d{1,2})[ ._-]*e(\\d{1,2})/i);
      if (yy) row.year = yy;
      if (ii) row.imdb_id = ii;
      if (se) {
        row.season = norm2(se[1]);
        row.episode = norm2(se[2]);
      }
      const clean = file.replace(/\\.[^.]+$/, '').replace(/\\.s\\d{1,2}\\.e\\d{1,2}.*/i, '').replace(/\\.(19|20)\\d{2}.*/i, '').replace(/\\./g, ' ').trim();
      if (clean) row.title = cleanTitle(clean);
    }

    function targetForRow(row) {
      const src = String(row.source_name || '').replace(/^\\.\\//, '');
      const extMatch = src.match(/(\\.[A-Za-z0-9]+)$/);
      const ext = extMatch ? extMatch[1].toLowerCase() : '.mkv';
      const title = cleanTitle(row.title || '') || src.replace(/\\.[^.]+$/, '') || 'Unknown';
      const year = parseYear(row.year || '') || '0000';
      const imdb = parseImdb(row.imdb_id || '') || 'tt0000000';
      const season = norm2(row.season || '');
      const episode = norm2(row.episode || '');
      const folder = safeFolder(title);
      const dot = dotted(title);
      if (season && episode) {
        const fn = `${dot}.${year}.S${season}.E${episode}.h264.{${imdb}}${ext}`;
        return `${outPrefix}/Serien/${folder} (${year})/S${season}/${fn}`;
      }
      const fn = `${dot}.${year}.h264.{${imdb}}${ext}`;
      return `${outPrefix}/Movie/${folder} (${year})/${fn}`;
    }

    function handleEditorRowAction(idx, action) {
      activeIndex = idx;
      if (!selected.has(idx)) {
        selected.clear();
        selected.add(idx);
      }
      paintSelection();
      const normalized = String(action || '');
      if (normalized === 'requeue') {
        requeueSelectedRows();
        return;
      }
      if (normalized === 'serie') {
        selectSeries();
        return;
      }
      if (normalized === 'clean') {
        cleanSelectedManifests();
        return;
      }
      if (normalized === 'cancel') {
        resetSelectedRows();
      }
    }

    function buildEditorActionRow(idx) {
      const row = rows[idx];
      if (!row) return null;
      const seriesRow = isSeriesRow(row);
      const actionTr = document.createElement('tr');
      actionTr.className = 'row-actions-tr';
      actionTr.setAttribute('data-row-actions-idx', String(idx));
      actionTr.innerHTML = `
        <td colspan="9" class="row-actions-cell">
          <div class="row-inline-actions">
            <button type="button" class="row-btn" data-row-action="requeue" data-row-idx="${idx}">Re-Queue</button>
            <button type="button" class="row-btn" data-row-action="serie" data-row-idx="${idx}"${seriesRow ? '' : ' disabled title="Nur bei Serien aktiv"'}>Serie</button>
            <button type="button" class="row-btn" data-row-action="clean" data-row-idx="${idx}">Reset "Erledigt"</button>
            <button type="button" class="row-btn" data-row-action="cancel" data-row-idx="${idx}">Reset Edit</button>
          </div>
        </td>
      `;
      actionTr.querySelectorAll('button[data-row-action]').forEach((btn) => {
        btn.addEventListener('mousedown', (ev) => ev.stopPropagation());
        btn.addEventListener('click', (ev) => {
          ev.stopPropagation();
          handleEditorRowAction(idx, String(btn.getAttribute('data-row-action') || ''));
        });
      });
      return actionTr;
    }

    function render() {
      const tb = document.getElementById('tb');
      if (!tb) return;
      tb.innerHTML = '';
      const total = rows.length;
      rows.forEach((row, idx) => {
        const tr = document.createElement('tr');
        tr.setAttribute('data-row-idx', String(idx));
        if (isMissingRow(row)) tr.classList.add('row-error');
        if (isCompletedRow(row)) tr.classList.add('row-done');
        tr.onclick = (ev) => {
          if (ev.target && ev.target.closest('button,select,a,label')) return;
          if (ev.ctrlKey || ev.metaKey) {
            if (selected.has(idx)) selected.delete(idx); else selected.add(idx);
          } else {
            selected.clear();
            selected.add(idx);
          }
          activeIndex = idx;
          paintSelection();
        };

        const sourceDisplay = fileNameOnly(row.source_name || '');
        const targetDisplay = fileNameOnly(row.target_name || '');
        const seriesRow = isSeriesRow(row);
        const seasonDisplay = seriesRow ? (norm2(row.season || '') || '00') : 'Movie';
        const episodeDisplay = seriesRow ? (norm2(row.episode || '') || '00') : '';
        tr.innerHTML = `
          <td class="nr-cell">${formatRatio(Number(row.nr || (idx + 1)), total)}</td>
          <td class="source-cell">
            <input class="source-name-input" type="text" readonly value="${esc(sourceDisplay || '-')}" title="${esc(sourceDisplay || '')}" />
          </td>
          <td class="target-cell">
            <input class="target-name-input" type="text" data-k="target_name" value="${esc(targetDisplay)}" />
          </td>
          <td class="title-cell"><input type="text" data-k="title" value="${esc(row.title || '')}" /></td>
          <td class="year-cell"><input type="text" data-k="year" value="${esc(row.year || '')}" /></td>
          <td class="se-cell"><input type="text" data-k="season" value="${esc(seasonDisplay)}" /></td>
          <td class="se-cell"><input type="text" data-k="episode" value="${esc(episodeDisplay)}" /></td>
          <td class="imdb-cell"><input type="text" data-k="imdb_id" value="${esc(row.imdb_id || '')}" /></td>
          <td class="row-action-cell"></td>
        `;
        tr.querySelectorAll('input').forEach((inp) => {
          inp.addEventListener('mousedown', () => {
            activeIndex = idx;
            if (!selected.has(idx)) {
              selected.clear();
              selected.add(idx);
            }
            paintSelection();
          });
          inp.addEventListener('click', () => {
            activeIndex = idx;
            if (!selected.has(idx)) {
              selected.clear();
              selected.add(idx);
            }
            paintSelection();
          });
          inp.addEventListener('focus', () => {
            activeIndex = idx;
            if (!selected.has(idx)) {
              selected.clear();
              selected.add(idx);
            }
            paintSelection();
          });
        });
        tr.querySelectorAll('input[data-k]').forEach((inp) => {
          inp.addEventListener('change', () => {
            applyInputChange(idx, String(inp.getAttribute('data-k') || ''), String(inp.value || ''));
          });
        });
        tb.appendChild(tr);
      });
      applyEditorModeFilter();
      paintSelection();
    }

    function paintSelection() {
      const tb = document.getElementById('tb');
      if (!tb) return;
      tb.querySelectorAll('tr[data-row-actions-idx]').forEach((tr) => tr.remove());
      tb.querySelectorAll('tr[data-row-idx]').forEach((tr) => {
        const idx = Number(tr.getAttribute('data-row-idx') || '-1');
        tr.classList.toggle('active', idx === activeIndex);
        tr.classList.toggle('selected', selected.has(idx));
      });
      if (activeIndex >= 0) {
        const activeRow = tb.querySelector(`tr[data-row-idx="${activeIndex}"]`);
        if (activeRow) {
          const actionRow = buildEditorActionRow(activeIndex);
          if (actionRow) activeRow.insertAdjacentElement('afterend', actionRow);
        }
      }
    }

    function clearEditorSelection() {
      activeIndex = -1;
      selected.clear();
      paintSelection();
    }

    function syncSelectedRowsFromActive(activeIdx) {
      const active = rows[activeIdx];
      if (!active) return;
      const targets = selected.size ? Array.from(selected) : [];
      targets.forEach((idx) => {
        if (idx === activeIdx) return;
        const row = rows[idx];
        if (!row) return;
        row.title = active.title;
        row.year = active.year;
        row.season = active.season;
        row.episode = active.episode;
        row.imdb_id = active.imdb_id;
        row.target_name = targetForRow(row);
      });
    }

    function applyInputChange(idx, key, value) {
      const row = rows[idx];
      if (!row) return;
      const k = String(key || '');
      if (k === 'target_name') {
        row.target_name = String(value || '').trim();
        parseHintsFromTarget(row);
      } else if (k === 'title') {
        row.title = cleanTitle(value || '');
      } else if (k === 'year') {
        row.year = parseYear(value || '');
      } else if (k === 'season') {
        row.season = norm2(value || '');
      } else if (k === 'episode') {
        row.episode = norm2(value || '');
      } else if (k === 'imdb_id') {
        row.imdb_id = parseImdb(value || '');
      }
      row.target_name = targetForRow(row);
      if (selected.has(idx) && selected.size > 1) {
        syncSelectedRowsFromActive(idx);
      }
      render();
    }

    function readInputsBack() {
      const tb = document.getElementById('tb');
      if (!tb) return;
      const trs = Array.from(tb.querySelectorAll('tr'));
      trs.forEach((tr, idx) => {
        const row = rows[idx];
        if (!row) return;
        const map = {};
        tr.querySelectorAll('input[data-k]').forEach((inp) => {
          map[String(inp.getAttribute('data-k') || '')] = String(inp.value || '');
        });
        const oldTarget = fileNameOnly(String(row.target_name || '').trim());
        const typedTarget = String(map.target_name || '').trim();
        row.target_name = typedTarget;
        row.title = cleanTitle(map.title || '');
        row.year = parseYear(map.year || '');
        row.season = norm2(map.season || '');
        row.episode = norm2(map.episode || '');
        row.imdb_id = parseImdb(map.imdb_id || '');

        if (typedTarget && typedTarget !== oldTarget) {
          parseHintsFromTarget(row);
        }
        row.target_name = targetForRow(row);
      });
    }

    async function resetSelectedRows() {
      if (activeIndex < 0) return;
      readInputsBack();
      const targets = selected.size ? Array.from(selected) : [activeIndex];
      const sourceNames = targets
        .map((idx) => (rows[idx] && rows[idx].source_name) ? String(rows[idx].source_name) : '')
        .filter((v) => !!String(v || '').trim());
      if (!sourceNames.length) return;

      setMsg('Reset: Originaldaten (Gemini) fuer ausgewaehlte Zeilen...');
      try {
        const res = await fetch('/api/confirm/editor/reset', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ token, source_names: sourceNames, reset_scope: 'gemini' }),
        });
        const data = await res.json().catch(() => ({}));
        if (!(res.ok && data && data.ok)) {
          const err = (data && data.error) ? String(data.error) : 'Reset fehlgeschlagen';
          if (/token/i.test(err)) {
            setMsg('Token veraltet. Editor wird neu geladen...');
            await loadRows();
            return;
          }
          setMsg(err);
          return;
        }
        rows = Array.isArray(data.rows) ? data.rows : [];
        baselineRows = clone(rows);
        selected.clear();
        activeIndex = -1;
        render();
        setMsg(`Reset OK (Gemini-Baseline | Zeilen: ${Number(data.reset_sources || sourceNames.length || 0)}, Gemini: ${Number(data.gemini_restored || 0)}, Baseline: ${Number(data.baseline_restored || 0)}, Unveraendert: ${Number(data.unchanged_rows || 0)}, Cache gelöscht: ${Number(data.editor_cache_cleared || 0)})`);
      } catch (err) {
        setMsg('Reset fehlgeschlagen');
      }
    }

    async function requeueSelectedRows() {
      if (activeIndex < 0) return;
      const confirmed = await askEditorConfirm(
        'Re-Queue: Datei, Sidecars und Ordnerstruktur bis Startordner jetzt nach __RE-ENQUEUE verschieben?',
        'Re-Queue bestaetigen',
        'Re-Queue'
      );
      if (!confirmed) return;
      readInputsBack();
      const targets = selected.size ? Array.from(selected) : [activeIndex];
      const sourceNames = targets
        .map((idx) => (rows[idx] && rows[idx].source_name) ? String(rows[idx].source_name) : '')
        .filter((v) => !!String(v || '').trim());
      if (!sourceNames.length) return;

      setMsg('Re-Queue: verschiebe Dateien...');
      try {
        const res = await fetch('/api/confirm/editor/requeue', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ token, source_names: sourceNames }),
        });
        const data = await res.json().catch(() => ({}));
        if (!(res.ok && data && data.ok)) {
          const err = (data && data.error) ? String(data.error) : 'Re-Queue fehlgeschlagen';
          if (/token/i.test(err)) {
            setMsg('Token veraltet. Editor wird neu geladen...');
            await loadRows();
            return;
          }
          setMsg(err);
          return;
        }
        rows = Array.isArray(data.rows) ? data.rows : [];
        baselineRows = clone(rows);
        selected.clear();
        activeIndex = -1;
        render();
        const errs = Array.isArray(data.errors) ? data.errors.filter((e) => String(e || '').trim()) : [];
        const suffix = errs.length ? ` | Fehler: ${errs[0]}` : '';
        setMsg(`Re-Queue OK (Quellen: ${Number(data.requeued_sources || 0)}, Dateien: ${Number(data.moved_files || 0)}, Sidecars: ${Number(data.moved_sidecars || 0)}, Cache gelöscht: ${Number(data.editor_cache_cleared || 0)})${suffix}`);
        notifyEditorRefresh('editor-requeue');
        try {
          if (window.opener && typeof window.opener.refreshNow === 'function') {
            window.opener.refreshNow();
          }
        } catch (err) {
        }
      } catch (err) {
        setMsg('Re-Queue fehlgeschlagen');
      }
    }

    async function cleanEditorManifests(sourceNames = []) {
      const payload = { token };
      if (Array.isArray(sourceNames) && sourceNames.length > 0) {
        payload.source_names = sourceNames;
      }
      const res = await fetch('/api/confirm/editor/manifest/clean', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await res.json().catch(() => ({}));
      return { ok: !!(res.ok && data && data.ok), data };
    }

    async function cleanSelectedManifests() {
      if (activeIndex < 0) return;
      readInputsBack();
      const targets = selected.size ? Array.from(selected) : [activeIndex];
      const sourceNames = targets
        .map((idx) => (rows[idx] && rows[idx].source_name) ? String(rows[idx].source_name) : '')
        .filter((v) => !!String(v || '').trim());
      if (!sourceNames.length) return;
      const confirmed = await askEditorConfirm(
        `Erledigt für ${sourceNames.length} Zeile(n) zurücksetzen und Manifest löschen?`,
        'Reset Erledigt (Zeile)',
        'Reset "Erledigt"'
      );
      if (!confirmed) return;
      setMsg('Reset Erledigt (Zeile): bearbeite Einträge...');
      try {
        const result = await cleanEditorManifests(sourceNames);
        const data = result.data || {};
        if (!result.ok) {
          const err = (data && data.error) ? String(data.error) : 'Manifest Reset "Erledigt" fehlgeschlagen';
          if (/token/i.test(err)) {
            setMsg('Token veraltet. Editor wird neu geladen...');
            await loadRows();
            return;
          }
          setMsg(err);
          return;
        }
        setMsg(
          `Reset Erledigt OK (Zeilen: ${Number(data.rows || sourceNames.length || 0)}, `
          + `Erledigt zurückgesetzt: ${Number(data.done_reset_rows || 0)}, `
          + `History-Cache: ${Number(data.history_cache_cleared || 0)}, `
          + `Quelle: ${Number(data.source_sidecars_deleted || 0)}, `
          + `Ziel: ${Number(data.target_sidecars_deleted || 0)}, `
          + `Track-Einträge: ${Number(data.track_entries_deleted || 0)}, `
          + `Fehler: ${Number(data.failed || 0)})`
          + (data.cache_clear_error ? ` | Cache-Warnung: ${String(data.cache_clear_error)}` : '')
        );
        await loadRows({ silent: true });
        notifyEditorRefresh('editor-manifest-clean-row');
      } catch (err) {
        setMsg('Reset Erledigt fehlgeschlagen');
      }
    }

    async function cleanAllManifests() {
      const confirmed = await askEditorConfirm(
        'Erledigt für alle aktuellen Editor-Zeilen zurücksetzen und Manifeste löschen?',
        'Reset Erledigt (Alle)',
        'Reset "Erledigt"'
      );
      if (!confirmed) return;
      setMsg('Reset Erledigt (Alle): bearbeite Einträge...');
      try {
        const result = await cleanEditorManifests([]);
        const data = result.data || {};
        if (!result.ok) {
          const err = (data && data.error) ? String(data.error) : 'Manifest Reset "Erledigt" fehlgeschlagen';
          if (/token/i.test(err)) {
            setMsg('Token veraltet. Editor wird neu geladen...');
            await loadRows();
            return;
          }
          setMsg(err);
          return;
        }
        setMsg(
          `Reset Erledigt OK (Alle | Zeilen: ${Number(data.rows || rows.length || 0)}, `
          + `Erledigt zurückgesetzt: ${Number(data.done_reset_rows || 0)}, `
          + `History-Cache: ${Number(data.history_cache_cleared || 0)}, `
          + `Quelle: ${Number(data.source_sidecars_deleted || 0)}, `
          + `Ziel: ${Number(data.target_sidecars_deleted || 0)}, `
          + `Track-Einträge: ${Number(data.track_entries_deleted || 0)}, `
          + `Fehler: ${Number(data.failed || 0)})`
          + (data.cache_clear_error ? ` | Cache-Warnung: ${String(data.cache_clear_error)}` : '')
        );
        await loadRows({ silent: true });
        notifyEditorRefresh('editor-manifest-clean-all');
      } catch (err) {
        setMsg('Reset Erledigt fehlgeschlagen');
      }
    }

    function selectSeries() {
      const row = rows[activeIndex];
      if (!row || !isSeriesRow(row)) return;
      const key = seriesGroupKey(row);
      if (!key) return;
      selected.clear();
      rows.forEach((item, idx) => {
        if (!isSeriesRow(item)) return;
        if (seriesGroupKey(item) !== key) return;
        selected.add(idx);
      });
      render();
    }

    async function saveAll(options = {}) {
      const silent = !!(options && options.silent);
      readInputsBack();
      if (selected.has(activeIndex) && selected.size > 1) {
        syncSelectedRowsFromActive(activeIndex);
      }
      if (!hasPendingChanges()) {
        if (!silent) setMsg('Keine Aenderungen.');
        return true;
      }
      if (!silent) setMsg('Speichere in DB und aktualisiere...');
      try {
        const res = await fetch('/api/confirm/editor/save', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ token, rows }),
        });
        const data = await res.json().catch(() => ({}));
        if (!(res.ok && data && data.ok)) {
          const err = (data && data.error) ? String(data.error) : 'Save fehlgeschlagen';
          if (/token/i.test(err)) {
            if (!silent) setMsg('Token veraltet. Editor wird neu geladen...');
            await loadRows();
            return false;
          }
          if (!silent) setMsg(err);
          return false;
        }
        rows = Array.isArray(data.rows) ? data.rows : [];
        baselineRows = clone(rows);
        render();
        if (!silent) {
          setMsg(`Save OK. Reanalyse abgeschlossen. Zeilen: ${Number(data.saved || rows.length || 0)}`);
        }
        try {
          if (window.opener && typeof window.opener.refreshNow === 'function') {
            window.opener.refreshNow();
          }
        } catch (err) {
        }
        notifyEditorRefresh('editor-save');
        return true;
      } catch (err) {
        if (!silent) setMsg('Save fehlgeschlagen');
        return false;
      }
    }

    async function resetAll() {
      setMsg('Reset: Stand beim Editor-Start fuer gesamte Tabelle...');
      try {
        const res = await fetch('/api/confirm/editor/reset', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ token, reset_scope: 'session_start' }),
        });
        const data = await res.json().catch(() => ({}));
        if (!(res.ok && data && data.ok)) {
          const err = (data && data.error) ? String(data.error) : 'Reset fehlgeschlagen';
          if (/token/i.test(err)) {
            setMsg('Token veraltet. Editor wird neu geladen...');
            await loadRows();
            return;
          }
          setMsg(err);
          return;
        }
        rows = Array.isArray(data.rows) ? data.rows : [];
        baselineRows = clone(rows);
        selected.clear();
        activeIndex = -1;
        render();
        const rqErrors = Array.isArray(data.reenqueue_errors) ? data.reenqueue_errors.filter((e) => String(e || '').trim()) : [];
        const rqSuffix = Number(data.reverted_reenqueue || 0) > 0
          ? `, Re-Queue rueckgaengig: ${Number(data.reverted_reenqueue || 0)}, Rueck-Dateien: ${Number(data.moved_back_files || 0)}, Rueck-Sidecars: ${Number(data.moved_back_sidecars || 0)}`
          : '';
        const errSuffix = rqErrors.length ? ` | Fehler: ${rqErrors[0]}` : '';
        setMsg(`Reset OK (Editor-Startstand | Zeilen: ${Number(data.reset_sources || rows.length || 0)}, Session: ${Number(data.session_restored || 0)}, Gemini: ${Number(data.gemini_restored || 0)}, Baseline: ${Number(data.baseline_restored || 0)}, Unveraendert: ${Number(data.unchanged_rows || 0)}, Cache gelöscht: ${Number(data.editor_cache_cleared || 0)}${rqSuffix})${errSuffix}`);
        notifyEditorRefresh('editor-reset');
      } catch (err) {
        setMsg('Reset fehlgeschlagen');
      }
    }

    async function loadRows(options = {}) {
      const silent = !!(options && options.silent);
      if (!silent) setMsg('Lade...');
      try {
        let res = await fetch(`/api/confirm/editor?token=${encodeURIComponent(token)}`, { cache: 'no-store' });
        let data = await res.json().catch(() => ({}));
        if (!(res.ok && data && data.ok) && token) {
          res = await fetch('/api/confirm/editor', { cache: 'no-store' });
          data = await res.json().catch(() => ({}));
        }
        if (!(res.ok && data && data.ok)) {
          setMsg((data && data.error) ? String(data.error) : 'Laden fehlgeschlagen');
          return;
        }
        token = String(data.token || token || '');
        startFolder = String(data.start_folder || '');
        outPrefix = String(data.target_out_prefix || '__OUT').replace(/\\\\/g, '/').replace(/^\\.\\//, '');
        reenqueuePrefix = String(data.target_reenqueue_prefix || data.target_manual_prefix || '__RE-ENQUEUE').replace(/\\\\/g, '/').replace(/^\\.\\//, '');
        rows = Array.isArray(data.rows) ? data.rows : [];
        baselineRows = clone(rows);
        render();
        if (!silent) setMsg(`Editor bereit. Dateien: ${rows.length}`);
      } catch (err) {
        if (!silent) setMsg('Laden fehlgeschlagen');
      }
    }

    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') {
        resolveEditorConfirm(false);
      }
    });

    document.addEventListener('mousedown', (event) => {
      const wrap = document.querySelector('.table-wrap');
      if (!wrap || !event || !(event.target instanceof Element)) return;
      if (!wrap.contains(event.target)) return;
      if (event.target.closest('tr[data-row-idx]')) return;
      if (event.target.closest('button,input,textarea,select,a,label')) return;
      clearEditorSelection();
    });

    if (Array.isArray(initialRows) && initialRows.length > 0) {
      rows = initialRows.slice();
      baselineRows = clone(rows);
      render();
      setMsg(`Editor bereit. Dateien: ${rows.length}`);
    } else {
      loadRows();
    }
    refreshRunDot();
    setInterval(refreshRunDot, 1200);
  </script>
</body>
</html>
"""


BROWSE_TEMPLATE = """
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ site_title }} | Ordner auswaehlen</title>
  <script>
    (function () {
      try {
        const theme = (localStorage.getItem('managemovie.theme') || 'light').toLowerCase() === 'dark' ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', theme);
      } catch (err) {
        document.documentElement.setAttribute('data-theme', 'light');
      }
    })();
  </script>
  <style>
    :root {
      --bg: #e9eef8;
      --bg-soft: #d9e5fb;
      --ink: #101828;
      --muted: #5f6f89;
      --panel: rgba(255, 255, 255, 0.84);
      --panel-strong: #ffffff;
      --line: rgba(70, 84, 104, 0.2);
      --accent: #0a84ff;
      --accent-press: #0066d6;
      --sidebar: #f4f7fc;
      --row-hover: rgba(10, 132, 255, 0.1);
      --row-active: rgba(10, 132, 255, 0.14);
    }
    html[data-theme="dark"] {
      --bg: #0c111b;
      --bg-soft: #1e2a40;
      --ink: #e6edf8;
      --muted: #9db0cf;
      --panel: rgba(18, 24, 36, 0.84);
      --panel-strong: #111827;
      --line: rgba(136, 156, 186, 0.3);
      --accent: #5fa8ff;
      --accent-press: #3f8deb;
      --sidebar: #151f31;
      --row-hover: rgba(95, 168, 255, 0.16);
      --row-active: rgba(95, 168, 255, 0.24);
    }
    * {
      box-sizing: border-box;
    }
    body {
      font-family: "SF Pro Rounded", "SF Pro Display", "SF Pro Text", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", sans-serif;
      margin: 0;
      padding: clamp(14px, 2.4vw, 26px);
      min-height: 100vh;
      background:
        radial-gradient(1100px 540px at 8% -6%, #ffffff 0%, var(--bg-soft) 44%, transparent 72%),
        linear-gradient(180deg, #edf3ff 0%, var(--bg) 100%);
      color: var(--ink);
    }
    .card {
      max-width: 1180px;
      margin: 0 auto;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 22px;
      padding: clamp(14px, 2vw, 22px);
      box-shadow: 0 24px 46px rgba(24, 39, 75, 0.16);
      backdrop-filter: blur(16px) saturate(130%);
      -webkit-backdrop-filter: blur(16px) saturate(130%);
    }
    h2 {
      margin: 0 0 14px 0;
      font-size: clamp(1.2rem, 2.1vw, 1.62rem);
      font-weight: 760;
      letter-spacing: 0.01em;
    }
    .toolbar {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: wrap;
      margin-bottom: 12px;
    }
    .btn {
      text-decoration: none;
      border: 1px solid var(--accent);
      background: var(--accent);
      color: #fff;
      padding: 10px 14px;
      border-radius: 12px;
      font-weight: 680;
      min-height: 44px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      transition: transform 0.12s ease, background 0.14s ease, border-color 0.14s ease;
    }
    .btn:hover {
      background: var(--accent-press);
      border-color: var(--accent-press);
      transform: translateY(-1px);
    }
    .btn-alt {
      background: #f6f9ff;
      color: #12345d;
      border-color: rgba(10, 132, 255, 0.32);
    }
    .btn-alt:hover {
      background: #ecf3ff;
      border-color: rgba(10, 132, 255, 0.52);
    }
    .crumbs {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      overflow-x: auto;
      white-space: nowrap;
      width: 100%;
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-radius: 13px;
      background: rgba(255, 255, 255, 0.76);
      margin-bottom: 12px;
    }
    .crumb {
      text-decoration: none;
      color: #1f4f93;
      font-weight: 620;
      font-size: 0.92rem;
      flex: 0 0 auto;
    }
    .crumb:hover {
      text-decoration: underline;
    }
    .crumb-current {
      color: #0f1726;
      font-weight: 740;
    }
    .crumb-sep {
      color: var(--muted);
      flex: 0 0 auto;
    }
    .finder-shell {
      display: block;
      min-height: 420px;
    }
    .finder-main {
      border: 1px solid var(--line);
      border-radius: 14px;
      background: var(--panel-strong);
      overflow: hidden;
      display: flex;
      flex-direction: column;
      min-height: 0;
    }
    .finder-head {
      padding: 10px 14px;
      border-bottom: 1px solid var(--line);
      font-size: 0.84rem;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--muted);
      font-weight: 700;
      background: #f8fafd;
    }
    .finder-list {
      overflow: auto;
      padding: 6px;
      display: flex;
      flex-direction: column;
      gap: 2px;
    }
    .finder-row {
      display: flex;
      align-items: center;
      gap: 10px;
      border-radius: 10px;
      padding: 5px 8px;
      transition: background 0.12s ease;
    }
    .finder-row:hover {
      background: var(--row-hover);
    }
    .finder-name {
      text-decoration: none;
      color: #10213c;
      font-weight: 620;
      min-height: 36px;
      display: inline-flex;
      align-items: center;
      gap: 10px;
      min-width: 0;
      flex: 1 1 auto;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .finder-name::before {
      content: "";
      width: 19px;
      height: 14px;
      border-radius: 3px 3px 4px 4px;
      flex: 0 0 auto;
      background: linear-gradient(180deg, #7dc2ff 0%, #3c95ff 100%);
      border: 1px solid rgba(17, 68, 132, 0.2);
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.7);
    }
    .finder-empty {
      padding: 18px 12px;
      color: var(--muted);
      font-size: 0.92rem;
    }
    html[data-theme="dark"] .finder-head {
      background: #151f31;
    }
    html[data-theme="dark"] .crumb,
    html[data-theme="dark"] .crumb-current,
    html[data-theme="dark"] .finder-name {
      color: #dce8fb;
    }
    @media (max-width: 720px) {
      .toolbar .btn {
        flex: 1 1 46%;
      }
      .finder-row {
        align-items: center;
      }
    }
  </style>
</head>
<body>
  <div class="card">
    <h2>
      {% if target == "settings_target_nfs" %}
      Zielpfad-NFS auswaehlen
      {% elif target == "settings_target_out" %}
      Ziel __OUT auswaehlen
      {% elif target == "settings_target_reenqueue" %}
      Ziel __RE-ENQUEUE auswaehlen
      {% else %}
      Startordner auswaehlen
      {% endif %}
    </h2>
    <div class="toolbar">
      <a class="btn btn-alt" href="/browse?folder={{ parent }}&target={{ target }}">Nach oben</a>
      {% if target == "settings_target_nfs" %}
      <a class="btn" href="/?settings_target_nfs={{ current }}">Wähle diesen Ordner</a>
      {% elif target == "settings_target_out" %}
      <a class="btn" href="/?settings_target_out={{ current }}">Wähle diesen Ordner</a>
      {% elif target == "settings_target_reenqueue" %}
      <a class="btn" href="/?settings_target_reenqueue={{ current }}">Wähle diesen Ordner</a>
      {% else %}
      <a class="btn" href="/?folder={{ current }}">Wähle diesen Ordner</a>
      {% endif %}
      <a class="btn btn-alt" href="/">Zurück</a>
    </div>

    <div class="crumbs">
      {% for crumb in crumbs %}
        <a class="crumb{% if loop.last %} crumb-current{% endif %}" href="/browse?folder={{ crumb.path }}&target={{ target }}">{{ crumb.name }}</a>
        {% if not loop.last %}<span class="crumb-sep">/</span>{% endif %}
      {% endfor %}
    </div>

    <div class="finder-shell">
      <section class="finder-main">
        <div class="finder-head">Ordner</div>
        <div class="finder-list">
          {% for entry in entries %}
          <div class="finder-row">
            <a class="finder-name" href="/browse?folder={{ entry.path }}&target={{ target }}">{{ entry.name }}</a>
          </div>
          {% else %}
          <div class="finder-empty">Keine Unterordner gefunden.</div>
          {% endfor %}
        </div>
      </section>
    </div>
  </div>
</body>
</html>
"""


TEMPLATE = """
<!doctype html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{{ site_title }}</title>
  <script>
    (function () {
      try {
        const theme = (localStorage.getItem('managemovie.theme') || 'light').toLowerCase() === 'dark' ? 'dark' : 'light';
        document.documentElement.setAttribute('data-theme', theme);
      } catch (err) {
        document.documentElement.setAttribute('data-theme', 'light');
      }
    })();
  </script>
  <style>
    :root {
      --bg: #e8eef8;
      --bg-soft: #d9e5fb;
      --ink: #101828;
      --panel: rgba(255, 255, 255, 0.84);
      --accent: #0a84ff;
      --accent-press: #0066d6;
      --line: rgba(70, 84, 104, 0.2);
    }
    html[data-theme="dark"] {
      --bg: #0b101a;
      --bg-soft: #1f2b41;
      --ink: #e6edf8;
      --panel: rgba(18, 24, 36, 0.9);
      --accent: #5fa8ff;
      --accent-press: #3f8deb;
      --line: rgba(136, 156, 186, 0.3);
    }
    body {
      margin: 0;
      padding: clamp(12px, 2vw, 24px);
      font-family: "SF Pro Rounded", "SF Pro Display", "SF Pro Text", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", sans-serif;
      background:
        radial-gradient(1100px 520px at 8% -5%, #ffffff 0%, var(--bg-soft) 42%, transparent 70%),
        linear-gradient(180deg, #edf3ff 0%, var(--bg) 100%);
      color: var(--ink);
    }
    .grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 16px;
      max-width: 1380px;
      margin: 0 auto;
    }
    @media (min-width: 1280px) {
      .grid {
        grid-template-columns: 1fr 1fr;
      }
      .wide {
        grid-column: 1 / -1;
      }
    }
    .card {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 20px;
      padding: 16px;
      box-shadow: 0 22px 42px rgba(24, 39, 75, 0.14);
      backdrop-filter: blur(16px) saturate(130%);
      -webkit-backdrop-filter: blur(16px) saturate(130%);
    }
    h1 {
      margin: 0 0 10px;
      font-size: 1.42rem;
    }
    .title-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 10px;
    }
    .title-row h1 {
      margin: 0;
      display: inline-flex;
      align-items: center;
      gap: 10px;
    }
    .gear-btn {
      min-width: 56px;
      width: 56px;
      margin-top: 0;
      font-size: 2rem;
      line-height: 1;
      padding: 8px 10px;
    }
    .restart-btn {
      min-width: 64px;
      width: 64px;
      margin-top: 0;
      font-size: 2.5rem;
      line-height: 1;
      padding: 6px 10px;
      font-weight: 900;
    }
    .update-badge-btn {
      min-width: 110px;
      width: auto;
      padding: 6px 12px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      font-weight: 900;
      letter-spacing: 0.04em;
    }
    .update-badge-btn .update-glyph {
      font-size: 1.6rem;
      line-height: 1;
    }
    .update-badge-btn .update-word {
      font-size: 0.82rem;
      line-height: 1;
    }
    h2 {
      margin: 0 0 8px;
      font-size: 1.05rem;
    }
    .job-title {
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }
    .job-status-dot {
      width: 11px;
      height: 11px;
      border-radius: 999px;
      border: 1px solid rgba(0, 0, 0, 0.16);
      background: #9aa3ad;
      box-shadow: 0 0 0 2px rgba(255, 255, 255, 0.9) inset;
      flex: 0 0 auto;
    }
    .job-status-dot.running {
      background: #29a745;
    }
    .job-status-dot.stopped {
      background: #9aa3ad;
    }
    .job-status-dot.title-dot {
      width: 18px;
      height: 18px;
      border-width: 2px;
      box-shadow: 0 0 0 3px rgba(255, 255, 255, 0.9) inset;
    }
    label {
      display: block;
      margin: 8px 0 4px;
      font-weight: 600;
    }
    input, select, button {
      width: 100%;
      box-sizing: border-box;
      border: 1px solid rgba(70, 84, 104, 0.28);
      border-radius: 13px;
      padding: 10px 11px;
      font-size: 0.96rem;
      min-height: 44px;
      background: rgba(255, 255, 255, 0.9);
    }
    .btn {
      cursor: pointer;
      border: 1px solid var(--accent);
      background: var(--accent);
      color: #fff;
      font-weight: 680;
      margin-top: 10px;
    }
    .btn:hover {
      background: var(--accent-press);
      border-color: var(--accent-press);
    }
    .btn-stop {
      background: #ff3b30;
      border-color: #ff3b30;
    }
    .btn-stop.btn-muted-stop {
      background: #34c759;
      border-color: #2aa44a;
      filter: grayscale(0.85) saturate(0.2) brightness(0.85);
      opacity: 0.48;
      box-shadow: none;
    }
    .btn-stop.btn-muted-stop:hover {
      background: #34c759;
      border-color: #2aa44a;
    }
    .btn-clean {
      background: #30b0c7;
      border-color: #30b0c7;
    }
    pre {
      margin: 0;
      white-space: pre;
      word-break: normal;
      max-height: 360px;
      overflow: auto;
      overflow-x: auto;
      background: linear-gradient(145deg, #f8faff 0%, #edf2ff 100%);
      color: #1b3257;
      border: 1px solid #d5deef;
      border-radius: 12px;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.85);
      padding: 12px 14px;
      font-size: 0.92rem;
      line-height: 1.4;
      font-family: "SF Pro Rounded", "SF Pro Display", "SF Pro Text", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", sans-serif;
      user-select: text;
      -webkit-user-select: text;
      cursor: text;
    }
    #procBox {
      min-height: 420px;
      max-height: 560px;
    }

    .meta {
      font-size: 0.9rem;
      color: #122921;
      line-height: 1.45;
    }
    .msg {
      padding: 8px 10px;
      border-radius: 10px;
      background: rgba(10, 132, 255, 0.1);
      border: 1px solid rgba(10, 132, 255, 0.24);
      margin-bottom: 8px;
    }

    .path-row {
      display: flex;
      gap: 8px;
      align-items: center;
    }
    .path-row input {
      flex: 1;
    }
    .path-row button {
      width: auto;
      white-space: nowrap;
      margin-top: 0;
    }
    .mode-row {
      display: grid;
      gap: 8px;
      margin-top: 2px;
      margin-bottom: 4px;
    }
    .mode-main-buttons,
    .mode-option-buttons {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
    }
    .mode-main-buttons .mode-btn {
      width: auto;
      min-width: 104px;
      font-weight: 800;
      color: #fff;
      border-width: 1px;
      margin-top: 0;
      padding: 9px 14px;
    }
    .mode-btn.mode-analyze {
      background: #0a84ff;
      border-color: #0a84ff;
    }
    .mode-btn.mode-copy {
      background: #34c759;
      border-color: #2aa44a;
    }
    .mode-btn.mode-encode {
      background: #ff9f0a;
      border-color: #d98a08;
    }
    .mode-btn.mode-active {
      border-width: 2px;
      box-shadow: 0 0 0 2px rgba(255, 255, 255, 0.75) inset, 0 0 0 1px rgba(0, 0, 0, 0.24);
      transform: translateY(-1px);
    }
    .mode-btn.mode-inactive {
      filter: grayscale(0.85) saturate(0.2) brightness(0.85);
      opacity: 0.48;
    }
    .mode-option-buttons .opt-btn {
      width: auto;
      min-width: 92px;
      margin-top: 0;
      padding: 6px 10px;
      font-size: 0.82rem;
      font-weight: 800;
      color: #1f2d42;
      background: #e8edf6;
      border-color: rgba(70, 84, 104, 0.32);
    }
    .mode-option-buttons .opt-btn.active {
      background: #fff0a3;
      border-color: #e0c34a;
      color: #5d4500;
    }
    .mode-option-buttons .opt-btn.disabled {
      background: #e7ebf2;
      border-color: rgba(70, 84, 104, 0.24);
      color: #6c7688;
      cursor: not-allowed;
      opacity: 0.9;
    }
    .chooser {
      margin-top: 8px;
      border: 1px solid #d7dfd5;
      border-radius: 8px;
      padding: 8px;
      background: #f8fbf8;
    }
    .hidden {
      display: none !important;
    }
    .log-modal.hidden {
      display: none !important;
    }
    #dirList {
      margin-top: 8px;
      min-height: 180px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    }
    .card-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 8px;
    }
    .card-head.status-waiting-head {
      position: relative;
      min-height: 46px;
    }
    .card-head h2 {
      margin: 0;
    }
    .card-head.status-waiting-head h2 {
      position: absolute;
      left: 50%;
      transform: translateX(-50%);
      max-width: calc(100% - 260px);
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      text-align: center;
    }
    .card-actions {
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .card-head.status-waiting-head .card-actions {
      margin-left: auto;
      position: relative;
      z-index: 1;
    }
    .title-row .card-actions #themeModeBtn {
      order: 1;
    }
    .title-row .card-actions #clearAllBtn {
      order: 2;
    }
    .title-row .card-actions .restart-btn {
      order: 3;
    }
    .title-row .card-actions .gear-btn {
      order: 4;
    }
    #themeModeBtn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 0;
      min-width: 58px;
      padding: 6px 11px;
    }
    #themeModeBtn .theme-icon {
      font-size: 1.36rem;
      line-height: 1;
      opacity: 1;
    }
    .status-error-indicator {
      display: inline-flex;
      align-items: center;
      min-height: 0;
      padding: 0;
      border: 0;
      background: transparent;
      color: #1f3f77;
      font-size: 0.88rem;
      font-weight: 600;
      white-space: nowrap;
    }
    .collapsible-card .collapse-body {
      min-height: 0;
    }
    .collapsible-card.collapsed .collapse-body {
      display: none;
    }
    #statusCard.collapsed #statusFilterBtn {
      display: none !important;
    }
    .collapse-toggle-btn {
      min-width: 56px;
      width: auto;
      padding: 6px 12px;
      font-size: 2.05rem;
      font-weight: 900;
      line-height: 1;
    }
    .gross-btn {
      min-width: 44px;
      width: auto;
      padding: 6px 10px;
      font-size: 2.15rem;
      font-weight: 900;
      line-height: 1;
    }
    .popout-btn {
      min-width: 44px;
      width: auto;
      padding: 6px 10px;
      font-size: 1.75rem;
      font-weight: 900;
      line-height: 1;
    }
    .log-close-btn,
    .restart-btn,
    .gear-btn,
    #clearAllBtn,
    button[title="Klein"],
    button[title="Einklappen"],
    button[title="Neues Fenster"],
    button[title="Verzeichnis-Auswahl"] {
      font-size: 1.28rem;
      font-weight: 900;
      line-height: 1;
    }
    .status-card {
      display: flex;
      flex-direction: column;
      min-height: 0;
    }
    .status-card .card-head {
      flex: 0 0 auto;
    }
    .status-card .status-table-wrap {
      flex: 1 1 auto;
      min-height: 280px;
      padding-right: 8px;
      box-sizing: border-box;
    }
    .log-expand-btn {
      width: auto;
      margin-top: 0;
      min-width: 40px;
      padding: 6px 10px;
      border-color: rgba(70, 84, 104, 0.28);
      background: rgba(255, 255, 255, 0.86);
      color: #20344f;
      font-weight: 800;
      font-size: 0.95rem;
      line-height: 1;
      cursor: pointer;
    }
    button[data-tip] {
      position: relative;
    }
    button[data-tip]:hover::after {
      content: attr(data-tip);
      position: absolute;
      left: 50%;
      bottom: calc(100% + 10px);
      transform: translateX(-50%);
      padding: 8px 11px;
      border-radius: 9px;
      background: rgba(23, 32, 28, 0.96);
      color: #f6fbf8;
      border: 1px solid rgba(227, 239, 233, 0.45);
      font-size: 1.02rem;
      font-weight: 800;
      line-height: 1.15;
      white-space: nowrap;
      box-shadow: 0 8px 20px rgba(0, 0, 0, 0.24);
      z-index: 10030;
      pointer-events: none;
    }
    button[data-tip]:hover::before {
      content: "";
      position: absolute;
      left: 50%;
      bottom: calc(100% + 3px);
      transform: translateX(-50%);
      border-left: 7px solid transparent;
      border-right: 7px solid transparent;
      border-top: 8px solid rgba(23, 32, 28, 0.96);
      z-index: 10031;
      pointer-events: none;
    }
    .log-modal {
      position: fixed;
      inset: 0;
      z-index: 9999;
      background: rgba(8, 18, 32, 0.46);
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 20px;
    }
    .log-modal-panel {
      width: min(1200px, 98vw);
      max-height: 92vh;
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 16px;
      box-shadow: 0 26px 54px rgba(14, 28, 56, 0.3);
      padding: 14px;
      display: flex;
      flex-direction: column;
      gap: 8px;
      overflow: hidden;
      backdrop-filter: blur(20px) saturate(130%);
      -webkit-backdrop-filter: blur(20px) saturate(130%);
    }
    .log-modal-panel.status-wide {
      width: 98vw;
      max-width: 98vw;
    }
    #logModalStatusWrap .status-table {
      width: max-content;
      min-width: max-content;
      font-size: 0.78rem;
      line-height: 1.2;
    }
    #logModalStatusWrap .status-table thead th,
    #logModalStatusWrap .status-table tbody td {
      padding: 5px 7px;
      white-space: nowrap;
    }
    #logModalStatusWrap.summary-wrap {
      border: 1px solid #d5deef;
      border-radius: 12px;
      background: linear-gradient(145deg, #f8faff 0%, #edf2ff 100%);
      padding: 10px 12px;
      overflow: auto;
    }
    #logModalStatusWrap.summary-wrap .status-meta {
      margin: 0;
      border: 0;
      background: transparent;
      padding: 0;
      box-shadow: none;
      width: 100%;
    }
    .log-modal-body {
      display: flex;
      flex-direction: column;
      gap: 8px;
      min-height: 0;
      padding-right: 6px;
      box-sizing: border-box;
    }
    .settings-modal-panel {
      width: min(520px, 96vw);
      max-height: 92vh;
      border-color: #d5deef;
      background: linear-gradient(165deg, #f8faff 0%, #edf2ff 100%);
    }
    .confirm-modal-panel {
      width: min(560px, 94vw);
      max-height: none;
    }
    .update-progress-panel {
      width: min(960px, 96vw);
      max-height: 88vh;
    }
    #inlineConfirmModal {
      z-index: 12050;
    }
    #updateProgressModal {
      z-index: 12040;
    }
    .confirm-modal-text {
      font-size: 0.95rem;
      line-height: 1.4;
      color: #10221c;
      white-space: pre-wrap;
      margin: 4px 0 2px 0;
    }
    .update-progress-body {
      display: grid;
      gap: 10px;
      min-height: 0;
    }
    .update-progress-status {
      font-size: 0.95rem;
      line-height: 1.4;
      color: #17325c;
      white-space: pre-wrap;
    }
    .update-progress-pre {
      margin: 0;
      min-height: min(48vh, 420px);
      max-height: min(52vh, 520px);
      overflow: auto;
      padding: 12px 14px;
      border-radius: 12px;
      border: 1px solid #d5deef;
      background: rgba(244, 247, 252, 0.96);
      color: #17325c;
      font-size: 0.88rem;
      line-height: 1.45;
      white-space: pre-wrap;
      word-break: break-word;
      font-family: "SF Mono", "SFMono-Regular", ui-monospace, Menlo, Monaco, Consolas, "Liberation Mono", monospace;
    }
    .confirm-modal-actions {
      display: flex;
      justify-content: flex-end;
      gap: 8px;
      margin-top: 6px;
    }
    .confirm-modal-actions .btn,
    .confirm-modal-actions button {
      width: auto;
      margin-top: 0;
      min-width: 140px;
      min-height: 40px;
      font-weight: 800;
      border-radius: 10px;
    }
    .settings-body {
      display: grid;
      gap: 8px;
      flex: 1 1 auto;
      min-height: 0;
      overflow-y: auto;
      padding-right: 2px;
      font-family: "SF Pro Rounded", "SF Pro Display", "SF Pro Text", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", sans-serif;
      font-size: 0.95rem;
      color: #1b345f;
    }
    .settings-grid {
      display: grid;
      gap: 8px;
    }
    .settings-main-toggle {
      display: inline-flex;
      flex-direction: column;
      align-items: flex-start;
      gap: 8px;
      padding: 8px 10px;
      border: 1px solid #d5deef;
      border-radius: 10px;
      background: #ffffff;
      color: #21467f;
      font-weight: 700;
      white-space: nowrap;
    }
    .settings-actions {
      display: flex;
      gap: 8px;
      align-items: center;
      justify-content: flex-end;
      margin-top: 4px;
    }
    .settings-actions .btn {
      width: auto;
      min-width: 160px;
      margin-top: 0;
      background: #0a84ff;
      border-color: #0a84ff;
      color: #f7fbff;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.2);
    }
    .settings-actions .btn:hover {
      background: #267fff;
      border-color: #267fff;
      color: #ffffff;
      filter: none;
    }
    .settings-status {
      min-height: 1.2em;
      font-size: 0.85rem;
      color: #1b3f77;
    }
    .settings-note {
      font-size: 0.85rem;
      color: #2d4f84;
    }
    .settings-secrets {
      border: 1px solid #d5deef;
      border-radius: 10px;
      background: #f7faff;
      padding: 8px;
    }
    .settings-secrets summary {
      cursor: pointer;
      font-weight: 700;
      color: #21467f;
      font-size: 0.92rem;
      margin-bottom: 8px;
    }
    .settings-advanced-toggle {
      margin: 0 0 10px 0;
      padding: 6px 8px;
      border: 1px solid #d5deef;
      border-radius: 8px;
      background: #ffffff;
    }
    .settings-cache-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin: 10px 0 0 0;
      padding: 8px;
      border: 1px solid #d5deef;
      border-radius: 8px;
      background: #ffffff;
    }
    .settings-cache-meta {
      min-width: 0;
    }
    .settings-cache-count {
      font-size: 0.96rem;
      color: #12305f;
      font-weight: 800;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      max-width: 420px;
    }
    .settings-cache-row .btn-clean {
      margin-top: 0;
      min-width: 124px;
      width: auto;
    }
    .settings-cache-reset-btn {
      margin-top: 0;
      min-width: 124px;
      width: auto;
      background: #d92d20;
      border-color: #d92d20;
      color: #ffffff;
    }
    .settings-cache-reset-btn:hover {
      background: #ff453a;
      border-color: #ff453a;
      color: #ffffff;
      filter: none;
    }
    .settings-grid label,
    .settings-toggle-label,
    .settings-note,
    .settings-status,
    .settings-cache-count {
      color: #1b345f;
      font-size: 0.95rem;
      font-weight: 600;
    }
    .settings-grid input,
    .settings-grid select,
    .settings-grid .path-row input,
    .settings-grid .path-row button,
    .settings-secrets input {
      font-family: "SF Pro Rounded", "SF Pro Display", "SF Pro Text", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", sans-serif;
      font-size: 0.95rem;
      color: #102a4e;
    }
    html[data-theme="dark"] .card,
    html[data-theme="dark"] .log-modal-panel,
    html[data-theme="dark"] .settings-modal-panel {
      background: rgba(18, 24, 36, 0.9);
      border-color: rgba(136, 156, 186, 0.34);
      box-shadow: 0 24px 44px rgba(0, 0, 0, 0.45);
    }
    html[data-theme="dark"] pre,
    html[data-theme="dark"] input,
    html[data-theme="dark"] select,
    html[data-theme="dark"] textarea,
    html[data-theme="dark"] .status-table-wrap,
    html[data-theme="dark"] .confirm-panel,
    html[data-theme="dark"] .summary-ampel,
    html[data-theme="dark"] .settings-secrets,
    html[data-theme="dark"] #logModalStatusWrap.summary-wrap {
      background: #111827;
      border-color: rgba(136, 156, 186, 0.34);
      color: #dce8fb;
    }
    html[data-theme="dark"] .settings-grid label,
    html[data-theme="dark"] .settings-toggle-label,
    html[data-theme="dark"] .settings-secrets summary,
    html[data-theme="dark"] .settings-note,
    html[data-theme="dark"] .settings-status,
    html[data-theme="dark"] .settings-cache-count {
      color: #f2f7ff;
    }
    html[data-theme="dark"] .settings-grid input,
    html[data-theme="dark"] .settings-grid select,
    html[data-theme="dark"] .settings-grid .path-row input,
    html[data-theme="dark"] .settings-secrets input,
    html[data-theme="dark"] .settings-grid input::placeholder,
    html[data-theme="dark"] .settings-grid select::placeholder,
    html[data-theme="dark"] .settings-grid .path-row input::placeholder,
    html[data-theme="dark"] .settings-secrets input::placeholder {
      color: #f2f7ff;
      opacity: 1;
      -webkit-text-fill-color: #f2f7ff;
    }
    html[data-theme="dark"] .update-progress-status,
    html[data-theme="dark"] .confirm-modal-text {
      color: #eef4ff;
    }
    html[data-theme="dark"] .update-progress-pre {
      background: rgba(11, 17, 28, 0.94);
      border-color: rgba(136, 156, 186, 0.28);
      color: #eef4ff;
    }
    html[data-theme="dark"] .status-table th {
      background: #1b2436;
      color: #dce8fb;
    }
    html[data-theme="dark"] .status-table tbody tr {
      background: #111827;
    }
    html[data-theme="dark"] .status-table tbody tr:nth-child(even) {
      background: #111827;
    }
    html[data-theme="dark"] .status-table td {
      border-color: rgba(136, 156, 186, 0.24);
      color: #d7e3f6;
    }
    html[data-theme="dark"] .status-table tr.status-row-missing td {
      background: #4e2630 !important;
      color: #ffd8df;
    }
    html[data-theme="dark"] .status-table tr.status-row-active td {
      background: #6f5913 !important;
      color: #fff2bf;
      box-shadow: none;
    }
    html[data-theme="dark"] .status-table tr.status-row-done td {
      background: #163826 !important;
      color: #cfeedd;
    }
    html[data-theme="dark"] .summary-kv-table,
    html[data-theme="dark"] .summary-kv-table td,
    html[data-theme="dark"] .status-meta .summary-kv-table,
    html[data-theme="dark"] .status-meta .summary-kv-table td {
      color: #cbd8ea;
    }
    html[data-theme="dark"] .summary-kv-table th,
    html[data-theme="dark"] .status-meta .summary-kv-table th {
      color: #d6e2f4;
    }
    html[data-theme="dark"] .summary-kv-table tr + tr th,
    html[data-theme="dark"] .summary-kv-table tr + tr td,
    html[data-theme="dark"] .status-meta .summary-kv-table tr + tr th,
    html[data-theme="dark"] .status-meta .summary-kv-table tr + tr td {
      border-top-color: rgba(136, 156, 186, 0.28);
    }
    html[data-theme="dark"] .summary-kv-wrap,
    html[data-theme="dark"] .status-meta,
    html[data-theme="dark"] #logModalStatusWrap .status-meta,
    html[data-theme="dark"] #logModalStatusWrap.summary-wrap .status-meta {
      background: #111827;
      border-color: rgba(136, 156, 186, 0.34);
      color: #c4d2e8;
    }
    html[data-theme="dark"] .summary-ampel-row {
      color: #c8d5e8;
    }
    html[data-theme="dark"] .settings-note,
    html[data-theme="dark"] .meta {
      color: #b9c9e4;
    }
    html[data-theme="dark"] button,
    html[data-theme="dark"] .log-expand-btn,
    html[data-theme="dark"] .mode-option-buttons .opt-btn,
    html[data-theme="dark"] .settings-cache-row .btn-clean,
    html[data-theme="dark"] .settings-actions .btn,
    html[data-theme="dark"] .path-row button {
      background: #1b2436;
      border-color: rgba(136, 156, 186, 0.4);
      color: #e6edf8;
    }
    html[data-theme="dark"] .log-expand-btn:hover,
    html[data-theme="dark"] .mode-option-buttons .opt-btn:hover,
    html[data-theme="dark"] .settings-actions .btn:hover,
    html[data-theme="dark"] .path-row button:hover {
      background: #26344d;
      border-color: rgba(160, 181, 213, 0.52);
      color: #f2f7ff;
    }
    html[data-theme="dark"] .mode-main-buttons .mode-btn {
      color: #f4f8ff;
    }
    html[data-theme="dark"] .mode-btn.mode-active {
      box-shadow: 0 0 0 2px rgba(8, 12, 20, 0.9) inset, 0 0 0 1px rgba(185, 205, 236, 0.6);
    }
    html[data-theme="dark"] .mode-btn.mode-inactive {
      filter: grayscale(0.6) saturate(0.7) brightness(0.85);
      opacity: 0.72;
    }
    html[data-theme="dark"] .mode-option-buttons .opt-btn.active {
      background: #ffe680;
      border-color: #e0c34a;
      color: #4a3700;
    }
    html[data-theme="dark"] .mode-option-buttons .opt-btn.disabled {
      background: #1f2738;
      border-color: rgba(136, 156, 186, 0.28);
      color: #8fa3c3;
      opacity: 0.85;
    }
    html[data-theme="dark"] .btn {
      background: #2f70ff;
      border-color: #2f70ff;
      color: #f7fbff;
    }
    html[data-theme="dark"] #confirmExitBtn {
      background: #2f70ff;
      border-color: #2f70ff;
      color: #f7fbff;
    }
    html[data-theme="dark"] .settings-actions .btn {
      background: #2f70ff;
      border-color: #2f70ff;
      color: #f7fbff;
    }
    html[data-theme="dark"] .btn:hover {
      background: #4a84ff;
      border-color: #4a84ff;
    }
    html[data-theme="dark"] #confirmExitBtn:hover {
      background: #4a84ff;
      border-color: #4a84ff;
      color: #ffffff;
    }
    html[data-theme="dark"] .settings-actions .btn:hover {
      background: #4a84ff;
      border-color: #4a84ff;
      color: #ffffff;
    }
    html[data-theme="dark"] .settings-cache-reset-btn,
    html[data-theme="dark"] .settings-cache-reset-btn:hover {
      background: #ff453a;
      border-color: #ff453a;
      color: #ffffff;
    }
    html[data-theme="dark"] .btn-stop {
      background: #ff453a;
      border-color: #ff453a;
      color: #fff;
    }
    html[data-theme="dark"] .btn-clean {
      background: #30b0c7;
      border-color: #30b0c7;
      color: #f6fcff;
    }
    html[data-theme="dark"] .settings-advanced-toggle,
    html[data-theme="dark"] .settings-cache-row,
    html[data-theme="dark"] .settings-main-toggle {
      background: #111827;
      border-color: rgba(136, 156, 186, 0.34);
    }
    html[data-theme="dark"] .settings-toggle-label {
      color: #dce8fb;
    }
    html[data-theme="dark"] .settings-cache-count {
      color: #f2f7ff;
    }
    html[data-theme="dark"] .settings-secrets summary,
    html[data-theme="dark"] .status-error-indicator,
    html[data-theme="dark"] #statusTableEmpty,
    html[data-theme="dark"] .confirm-modal-text {
      color: #dce8fb;
    }
    html[data-theme="dark"] .log-expand-btn.active {
      background: rgba(80, 132, 255, 0.28);
      border-color: rgba(137, 173, 255, 0.65);
      color: #f0f6ff;
    }
    html[data-theme="dark"] .settings-status {
      color: #c8f1df;
    }
    .settings-toggle-label {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      font-weight: 600;
      color: #21467f;
      cursor: pointer;
      white-space: nowrap;
      flex-wrap: nowrap;
    }
    .settings-toggle-label input[type="checkbox"] {
      flex: 0 0 auto;
      margin: 0;
      width: 18px;
      min-width: 18px;
      max-width: 18px;
      height: 18px;
      min-height: 18px;
      padding: 0;
      border-radius: 4px;
      vertical-align: middle;
      accent-color: #0a84ff;
    }
    #logModalPre {
      max-height: none;
      min-height: 320px;
      font-size: 0.94rem;
      line-height: 1.45;
      flex: 1 1 auto;
      font-family: "SF Pro Rounded", "SF Pro Display", "SF Pro Text", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", sans-serif;
    }
    .log-modal-panel.summary-view {
      width: min(980px, 96vw);
    }
    .log-modal-panel.summary-view #logModalPre {
      white-space: pre-line !important;
      word-break: keep-all;
      font-family: "SF Pro Rounded", "SF Pro Display", "SF Pro Text", -apple-system, BlinkMacSystemFont, "Helvetica Neue", "Segoe UI", sans-serif;
      font-size: 0.94rem;
      line-height: 1.45;
      border-radius: 12px;
      border-color: #d5deef;
      background: linear-gradient(145deg, #f8faff 0%, #edf2ff 100%);
      color: #1b3257;
      padding: 12px 14px;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.85);
    }
    .log-modal-panel.summary-view #logModalStatusWrap {
      max-height: none;
      min-height: 320px;
      flex: 1 1 auto;
    }
    #logModalStatusWrap {
      max-height: none;
      min-height: 320px;
      flex: 1 1 auto;
      padding-right: 8px;
      box-sizing: border-box;
    }
    .confirm-panel {
      border: 1px solid #d7dff0;
      border-radius: 8px;
      background: #f8fafe;
      padding: 8px 10px;
      margin-top: auto;
    }
    .confirm-panel button {
      width: auto;
      min-width: 118px;
      margin-top: 0;
    }
    .confirm-actions-row {
      display: flex;
      justify-content: flex-end;
      align-items: center;
      gap: 8px;
      flex-wrap: nowrap;
    }
    .confirm-actions-left,
    .confirm-actions-right {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: nowrap;
      justify-content: flex-end;
    }
    #confirmExitBtn {
      min-width: 92px;
      padding: 7px 12px;
      background: #0a84ff;
      border-color: #0a84ff;
      color: #f7fbff;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.18);
    }
    #confirmExitBtn:hover {
      background: #267fff;
      border-color: #267fff;
      color: #ffffff;
      filter: none;
    }
    .confirm-actions-left .btn-primary {
      background: #0a84ff;
      border-color: #0a84ff;
      color: #fff;
      font-weight: 800;
    }
    .confirm-actions-left.hidden,
    .confirm-actions-right.hidden {
      display: none !important;
    }
    .confirm-text {
      font-size: 0.9rem;
      color: #1a3258;
      white-space: pre-wrap;
      margin-bottom: 6px;
    }
    .confirm-text.hidden {
      display: none;
    }
    .log-close-btn {
      width: auto;
      margin-top: 0;
      min-width: 44px;
      padding: 7px 12px;
      cursor: pointer;
      font-size: 1rem;
      line-height: 1;
    }
    .status-modal-footer {
      display: flex;
      justify-content: flex-end;
      margin-top: 8px;
    }
    .status-modal-footer .btn {
      min-width: 96px;
      padding: 7px 12px;
      margin-top: 0;
    }
    .status-modal-footer.hidden {
      display: none !important;
    }
    .confirm-panel:not(.hidden) + .status-modal-footer {
      display: none !important;
    }
    .status-meta {
      display: block;
      overflow-x: auto;
      overflow-y: hidden;
      background: linear-gradient(145deg, #f8faff 0%, #eef3ff 100%);
      color: #12233d;
      border: 1px solid #d5deef;
      border-radius: 11px;
      padding: 8px 10px;
      margin-bottom: 0;
      min-height: 52px;
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.85);
    }
    .status-meta.is-empty {
      opacity: 0.8;
    }
    .status-meta .summary-kv-table {
      width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      table-layout: auto;
      font-size: 0.87rem;
      line-height: 1.3;
      color: #1a3155;
    }
    .status-meta .summary-kv-table tr + tr th,
    .status-meta .summary-kv-table tr + tr td {
      border-top: 1px solid #dde5f4;
    }
    .status-meta .summary-kv-table th {
      width: 168px;
      max-width: 42%;
      text-align: left;
      padding: 6px 10px 6px 4px;
      color: #21467f;
      font-weight: 600;
      white-space: nowrap;
      vertical-align: top;
    }
    .status-meta .summary-kv-table td {
      text-align: left;
      padding: 6px 2px 6px 8px;
      color: #1a3155;
      font-weight: 500;
      white-space: normal;
      word-break: break-word;
      vertical-align: top;
    }
    html[data-theme="dark"] .status-meta .summary-kv-table,
    html[data-theme="dark"] .status-meta .summary-kv-table th,
    html[data-theme="dark"] .status-meta .summary-kv-table td {
      color: #cbd8ea;
    }
    html[data-theme="dark"] .status-meta .summary-kv-table tr + tr th,
    html[data-theme="dark"] .status-meta .summary-kv-table tr + tr td {
      border-top-color: rgba(136, 156, 186, 0.28);
    }
    .summary-top-row {
      display: grid;
      grid-template-columns: minmax(190px, 230px) 1fr;
      gap: 10px;
      align-items: start;
      margin-bottom: 8px;
    }
    #summaryCard.summary-right-hidden .summary-top-row {
      grid-template-columns: minmax(190px, 230px);
    }
    #summaryCard.summary-right-hidden #statusSummaryBox {
      display: none;
    }
    .summary-ampel {
      border: 1px solid #d7dff0;
      border-radius: 8px;
      background: #f9fafe;
      padding: 8px 10px;
      margin-bottom: 0;
      display: grid;
      gap: 5px;
    }
    @media (max-width: 980px) {
      .summary-top-row {
        grid-template-columns: 1fr;
      }
    }
    .summary-ampel-row {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 0.9rem;
      color: #12253f;
      line-height: 1.2;
      border-radius: 999px;
      padding: 2px 8px;
      border: 1px solid transparent;
    }
    .summary-ampel-row.bar {
      background: #fff1a8;
      border-color: #efd669;
    }
    .summary-ampel-dot {
      width: 11px;
      height: 11px;
      border-radius: 999px;
      display: inline-block;
      border: 1px solid rgba(0, 0, 0, 0.22);
      box-sizing: border-box;
      flex: 0 0 auto;
    }
    .summary-ampel-dot.gray {
      background: #aab3be;
    }
    .summary-ampel-dot.yellow {
      background: #facc15;
    }
    .summary-ampel-dot.green {
      background: #30d158;
    }
    .status-table-wrap {
      max-height: 460px;
      overflow: auto;
      border: 1px solid #d8e0f0;
      border-radius: 8px;
      background: #f9faff;
      padding-right: 8px;
      box-sizing: border-box;
    }
    .status-table {
      width: max-content;
      min-width: 100%;
      border-collapse: separate;
      border-spacing: 0;
      color: #152a46;
      font-size: 0.84rem;
      line-height: 1.3;
    }
    .status-table thead th {
      position: sticky;
      top: 0;
      z-index: 2;
      text-align: left;
      font-weight: 700;
      background: #edf2ff;
      border-bottom: 1px solid #d8e0f0;
      padding: 8px 10px;
      white-space: nowrap;
    }
    .status-table tbody td {
      border-bottom: 1px solid #e4eaf7;
      padding: 7px 10px;
      white-space: nowrap;
      vertical-align: top;
    }
    .status-table tbody tr:nth-child(even) {
      background: #f9faff;
    }
    .status-table th.sortable {
      cursor: pointer;
      user-select: none;
    }
    .status-table th.sort-asc,
    .status-table th.sort-desc {
      background: #dbe6ff;
      color: #133a79;
    }
    .status-table tr.status-row-missing td {
      background: #fde8e8 !important;
      color: #7a1b17;
    }
    .status-table tr.status-row-active td {
      background: #f3d76a !important;
      color: #3e2d00;
      box-shadow: none;
    }
    .status-table tr.status-row-done td {
      background: #e8f7ec !important;
      color: #134226;
    }
    .status-table.status-filter-errors tbody tr[data-filter-row]:not(.status-row-missing) {
      display: none;
    }
    .status-table.status-filter-done tbody tr[data-filter-row]:not(.status-row-done) {
      display: none;
    }
    .status-table.status-filter-encode tbody tr[data-filter-row]:not(.status-row-encode) {
      display: none;
    }
    .status-table.status-filter-copy tbody tr[data-filter-row]:not(.status-row-copy) {
      display: none;
    }
    .log-expand-btn.active {
      background: rgba(10, 132, 255, 0.16);
      border-color: rgba(10, 132, 255, 0.52);
      color: #0b4e96;
    }
    #statusTableEmpty {
      color: #223c31;
      font-style: italic;
      white-space: normal;
    }
    #statusTable th.status-col-main-hidden,
    #statusTable td.status-col-main-hidden {
      display: none;
    }
    #statusBox {
      display: none;
    }
    #summaryBox {
      display: none;
    }
    #logModalPre.nowrap {
      white-space: pre-wrap;
      word-break: break-word;
      overflow-x: auto;
    }
    .status-table-wrap,
    #logModalPre,
    #logModalStatusWrap,
    pre,
    textarea,
    .table-wrap {
      scrollbar-color: #b8c8e8 #eef3ff;
      scrollbar-width: thin;
    }
    .status-table-wrap::-webkit-scrollbar,
    #logModalPre::-webkit-scrollbar,
    #logModalStatusWrap::-webkit-scrollbar,
    pre::-webkit-scrollbar,
    textarea::-webkit-scrollbar,
    .table-wrap::-webkit-scrollbar {
      width: 12px;
      height: 12px;
    }
    .status-table-wrap::-webkit-scrollbar-track,
    #logModalPre::-webkit-scrollbar-track,
    #logModalStatusWrap::-webkit-scrollbar-track,
    pre::-webkit-scrollbar-track,
    textarea::-webkit-scrollbar-track,
    .table-wrap::-webkit-scrollbar-track {
      background: #eef3ff;
      border-radius: 999px;
    }
    .status-table-wrap::-webkit-scrollbar-thumb,
    #logModalPre::-webkit-scrollbar-thumb,
    #logModalStatusWrap::-webkit-scrollbar-thumb,
    pre::-webkit-scrollbar-thumb,
    textarea::-webkit-scrollbar-thumb,
    .table-wrap::-webkit-scrollbar-thumb {
      background: #bccbe7;
      border-radius: 999px;
      border: 2px solid #eef3ff;
    }
    html[data-theme="dark"] .status-table-wrap,
    html[data-theme="dark"] #logModalPre,
    html[data-theme="dark"] #logModalStatusWrap,
    html[data-theme="dark"] pre,
    html[data-theme="dark"] textarea,
    html[data-theme="dark"] .table-wrap {
      scrollbar-color: #223349 #050a12;
    }
    html[data-theme="dark"] .status-table-wrap::-webkit-scrollbar-track,
    html[data-theme="dark"] #logModalPre::-webkit-scrollbar-track,
    html[data-theme="dark"] #logModalStatusWrap::-webkit-scrollbar-track,
    html[data-theme="dark"] pre::-webkit-scrollbar-track,
    html[data-theme="dark"] textarea::-webkit-scrollbar-track,
    html[data-theme="dark"] .table-wrap::-webkit-scrollbar-track {
      background: #050a12;
    }
    html[data-theme="dark"] .status-table-wrap::-webkit-scrollbar-thumb,
    html[data-theme="dark"] #logModalPre::-webkit-scrollbar-thumb,
    html[data-theme="dark"] #logModalStatusWrap::-webkit-scrollbar-thumb,
    html[data-theme="dark"] pre::-webkit-scrollbar-thumb,
    html[data-theme="dark"] textarea::-webkit-scrollbar-thumb,
    html[data-theme="dark"] .table-wrap::-webkit-scrollbar-thumb {
      background: #223349;
      border-color: #050a12;
    }
    .worker-grid {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      align-items: start;
    }
    @media (max-width: 1100px) {
      .worker-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }
    }
    @media (max-width: 900px) {
      .worker-grid {
        grid-template-columns: 1fr;
      }
    }
    .worker-tile {
      position: relative;
      border: 1px solid rgba(91, 128, 196, 0.22);
      border-radius: 14px;
      padding: 12px 14px;
      background:
        linear-gradient(180deg, rgba(28, 37, 55, 0.98), rgba(18, 25, 38, 0.98));
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
    }
    .worker-tile:has(.worker-menu[open]) {
      padding-bottom: 126px;
    }
    .worker-head {
      display: flex;
      align-items: flex-start;
      justify-content: space-between;
      gap: 10px;
      margin-bottom: 0;
    }
    .worker-head-main {
      display: inline-flex;
      align-items: center;
      gap: 10px;
      flex: 1 1 auto;
      min-width: 0;
    }
    .worker-name {
      font-weight: 700;
      letter-spacing: 0.02em;
      color: #f4f7ff;
      white-space: nowrap;
    }
    .worker-dot {
      width: 12px;
      height: 12px;
      border-radius: 999px;
      display: inline-block;
      flex: 0 0 12px;
      background: #98a3b8;
    }
    .worker-dot.idle {
      background: #8ef0a0;
    }
    .worker-dot.encoding {
      background: #14923f;
    }
    .worker-head-status {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      min-height: 18px;
      min-width: 0;
    }
    .worker-label {
      font-size: 12px;
      color: #d6e0f5;
      min-width: 0;
      white-space: nowrap;
    }
    .worker-menu {
      margin: 0;
      margin-left: auto;
      flex: 0 0 auto;
    }
    .worker-menu[open] .worker-menu-btn {
      background: rgba(43, 67, 112, 0.95);
      border-color: rgba(113, 156, 255, 0.5);
    }
    .worker-menu-btn {
      list-style: none;
      width: 34px;
      height: 34px;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      border-radius: 10px;
      border: 1px solid rgba(113, 156, 255, 0.28);
      background: rgba(26, 36, 54, 0.96);
      color: #dce7ff;
      cursor: pointer;
      user-select: none;
      font-size: 16px;
      font-weight: 600;
    }
    .worker-menu-btn::-webkit-details-marker {
      display: none;
    }
    .worker-menu-content {
      display: none;
    }
    .worker-menu[open] .worker-menu-content {
      display: block;
      position: absolute;
      left: 14px;
      right: 14px;
      top: 56px;
      margin-top: 0;
    }
    .worker-actions {
      display: flex;
      flex-wrap: nowrap;
      gap: 8px;
      overflow-x: auto;
      padding-bottom: 2px;
    }
    .worker-actions button {
      padding: 7px 10px;
      border-radius: 10px;
      border: 1px solid transparent;
      cursor: pointer;
      font-size: 12px;
      font-weight: 700;
      color: #f8fbff;
      min-width: 0;
      flex: 1 1 0;
      white-space: nowrap;
      transition: transform 0.15s ease, filter 0.15s ease, box-shadow 0.15s ease;
    }
    .worker-actions button:disabled {
      opacity: 0.45;
      filter: grayscale(0.25);
      cursor: not-allowed;
      transform: none;
      box-shadow: none;
    }
    .worker-actions button:hover {
      filter: brightness(1.05);
      transform: translateY(-1px);
    }
    .worker-actions button:active {
      transform: translateY(0);
    }
    .worker-actions .worker-action-kill {
      background: linear-gradient(180deg, #f26e6e, #ca3d3d);
      box-shadow: 0 8px 18px rgba(202, 61, 61, 0.18);
    }
    .worker-actions .worker-action-init {
      background: linear-gradient(180deg, #34d07f, #159d57);
      box-shadow: 0 8px 18px rgba(21, 157, 87, 0.2);
    }
    .worker-actions .worker-action-pause {
      background: linear-gradient(180deg, #f6c356, #d08a14);
      box-shadow: 0 8px 18px rgba(208, 138, 20, 0.2);
    }
    .worker-name-note {
      margin-left: 8px;
      font-size: 0.82rem;
      font-weight: 700;
      color: #8fa0c2;
    }
    .worker-meta {
      font-size: 11px;
      color: #8fa0c2;
      min-height: 0;
      margin-bottom: 10px;
    }
  </style>
</head>
<body>
  <div class="grid">
    <div class="card wide">
      <div class="title-row">
      <h1 id="mainTitle"><span id="mainTitleText">{{ site_title }} {{ version_current }}</span><span id="mainRunDot" class="job-status-dot title-dot stopped" aria-hidden="true"></span></h1>
      <div class="card-actions">
          <button type="button" id="themeModeBtn" class="log-expand-btn" title="Anzeige umschalten" aria-label="Anzeige umschalten" onclick="toggleThemeMode()"><span id="themeModeIcon" class="theme-icon" aria-hidden="true">&#9728;</span></button>
          <button type="button" id="clearAllBtn" class="log-expand-btn" title="Alle Fenster leeren" aria-label="Alle Fenster leeren" onclick="clearAllPanels()">&#128465;</button>
          <button type="button" class="log-expand-btn restart-btn update-badge-btn" title="Update" aria-label="Update" onclick="openUpdateWindow()"><span class="update-glyph" aria-hidden="true">⟳</span><span class="update-word">UPDATE</span></button>
          <button type="button" class="log-expand-btn restart-btn" title="Restart" aria-label="Restart" onclick="openRestartWindow()">⟳</button>
          <button type="button" class="log-expand-btn gear-btn" title="Einstellungen" aria-label="Einstellungen" onclick="openSettingsModal()">&#9881;&#65039;</button>
      </div>
      </div>
      {% if message %}<div id="flashMsg" class="msg">{{ message }}</div>{% endif %}
    </div>

    <div id="workersCard" class="card wide collapsible-card">
      <div class="card-head">
        <h2>Worker</h2>
        <div class="card-actions">
          <button type="button" class="log-expand-btn collapse-toggle-btn" title="Einklappen" aria-label="Einklappen" onclick="toggleCardCollapse('workersCard', this)">↙</button>
        </div>
      </div>
      <div class="collapse-body">
        <div id="workerGrid" class="worker-grid">
          {% for worker_name in worker_names %}
          <div class="worker-tile" data-worker="{{ worker_name }}">
            <div class="worker-head">
              <div class="worker-head-main">
                <div class="worker-name" data-worker-title="{{ worker_name }}">{{ worker_name }}</div>
                <div class="worker-head-status">
                  <span class="worker-dot" data-worker-dot="{{ worker_name }}" aria-hidden="true"></span>
                  <span class="worker-label" data-worker-label="{{ worker_name }}">N/A</span>
                </div>
              </div>
              <details class="worker-menu">
                <summary class="worker-menu-btn" title="Worker-Aktionen" aria-label="Worker-Aktionen">↙</summary>
                <div class="worker-menu-content">
                  <div class="worker-meta" data-worker-meta="{{ worker_name }}"></div>
                  <div class="worker-actions">
                    <button type="button" class="worker-action-kill" data-worker-name="{{ worker_name }}" data-worker-action="kill" onclick="controlWorker('{{ worker_name }}','kill')">Kill</button>
                    <button type="button" class="worker-action-init" data-worker-name="{{ worker_name }}" data-worker-action="init" onclick="controlWorker('{{ worker_name }}','init')">Init</button>
                    <button type="button" class="worker-action-pause" data-worker-name="{{ worker_name }}" data-worker-action="pause" onclick="controlWorker('{{ worker_name }}','pause')">Pause</button>
                  </div>
                </div>
              </details>
            </div>
          </div>
          {% endfor %}
        </div>
      </div>
    </div>

    <div id="jobCard" class="card collapsible-card">
      <div class="card-head">
        <h2 class="job-title"><span id="jobRunDot" class="job-status-dot stopped" aria-hidden="true"></span>Job Steuerung</h2>
        <div class="card-actions">
          <button type="button" class="log-expand-btn gross-btn" title="Groß" aria-label="Groß" onclick="openLogModal('Job Status', 'jobBox')">⇗⇗</button>
          <button type="button" class="log-expand-btn popout-btn" title="Neues Fenster" aria-label="Neues Fenster" onclick="openLogWindow('Job Status', 'jobBox')">⧉</button>
          <button type="button" class="log-expand-btn collapse-toggle-btn" title="Einklappen" aria-label="Einklappen" onclick="toggleCardCollapse('jobCard', this)">↙</button>
        </div>
      </div>
      <div class="collapse-body">
        <div id="leftRunState" class="meta"><b>Status:</b> wird geladen...</div>
        <form id="startForm" method="post" action="/start">
          <input type="hidden" id="mode" name="mode" value="{{ default_mode }}" />
          <input type="hidden" id="startEncoder" name="encoder" value="{{ default_encoder }}" />
          <input type="hidden" id="syncNasInput" name="sync_nas" value="0" />
          <input type="hidden" id="syncPlexInput" name="sync_plex" value="0" />
          <input type="hidden" id="delOutInput" name="del_out" value="0" />
          <input type="hidden" id="delSourceInput" name="del_source" value="0" />
          <input type="hidden" id="delSourceConfirmedInput" name="del_source_confirmed" value="0" />
          <label for="folder">Startordner</label>
          <div class="path-row">
            <input id="folder" name="folder" value="{{ default_folder }}" required />
            <button id="folderBrowseBtn" type="submit" formaction="/browse" formmethod="get" title="Verzeichnis-Auswahl" aria-label="Verzeichnis-Auswahl">&#128193;</button>
          </div>

          <label>Modus</label>
          <div class="mode-row">
            <div class="mode-main-buttons">
              <button type="button" id="modeAnalyzeBtn" class="mode-btn mode-analyze" onclick="setModeControls('analyze')">Analyze</button>
              <button type="button" id="modeCopyBtn" class="mode-btn mode-copy" onclick="setModeControls('copy')">Copy</button>
              <button type="button" id="modeEncodeBtn" class="mode-btn mode-encode" onclick="setModeControls('ffmpeg')">Encode</button>
            </div>
            <div class="mode-option-buttons">
              <button type="button" id="syncNasBtn" class="opt-btn" onclick="togglePostOption('sync_nas')">Sync NAS</button>
              <button type="button" id="syncPlexBtn" class="opt-btn" onclick="togglePostOption('sync_plex')">Sync Plex</button>
              <button type="button" id="delOutBtn" class="opt-btn" onclick="togglePostOption('del_out')">&#128465; __OUT</button>
              <button type="button" id="delSourceBtn" class="opt-btn" onclick="togglePostOption('del_source')">&#128465; Quelle</button>
            </div>
          </div>

          <button id="startSubmitBtn" class="btn" type="submit">Start</button>
        </form>

        <form id="stopForm" class="hidden" method="post" action="/stop" onsubmit="return false;">
          <button class="btn btn-stop btn-muted-stop" type="button" onclick="requestStopFromMain()">ABBRUCH</button>
        </form>
        <pre id="jobBox" class="hidden">(leer)</pre>
      </div>
    </div>

    <div id="summaryCard" class="card collapsible-card summary-right-hidden">
      <div class="card-head">
        <h2>Summary</h2>
        <div class="card-actions">
          <button type="button" class="log-expand-btn gross-btn" title="Groß" aria-label="Groß" onclick="openLogModal('Summary', 'summaryBox')">⇗⇗</button>
          <button type="button" class="log-expand-btn popout-btn" title="Neues Fenster" aria-label="Neues Fenster" onclick="openLogWindow('Summary', 'summaryBox')">⧉</button>
          <button type="button" class="log-expand-btn collapse-toggle-btn" title="Einklappen" aria-label="Einklappen" onclick="toggleCardCollapse('summaryCard', this)">↙</button>
        </div>
      </div>
      <div class="collapse-body">
        <div class="summary-top-row">
          <div id="summaryAmpelBox" class="summary-ampel"></div>
          <div id="statusSummaryBox" class="status-meta">lade...</div>
        </div>
        <pre id="summaryBox" class="hidden">lade...</pre>
      </div>
    </div>

    <div id="statusCard" class="card wide status-card collapsible-card">
      <div id="statusCardHead" class="card-head">
        <h2 id="statusCardTitle">STATUS Queue</h2>
        <div class="card-actions">
          <span id="statusErrorInfo" class="status-error-indicator hidden">Fehler 0/0 | Erledigt 0/0</span>
          <button id="statusFilterBtn" type="button" class="log-expand-btn" title="Status-Filter" aria-label="Status-Filter" onclick="toggleStatusMissingFilter()">Alle</button>
          <button type="button" class="log-expand-btn gross-btn" title="Groß" aria-label="Groß" onclick="openLogModal('STATUS Queue', 'statusBox')">⇗⇗</button>
          <button type="button" class="log-expand-btn popout-btn" title="Neues Fenster" aria-label="Neues Fenster" onclick="openLogWindow('STATUS Queue', 'statusBox')">⧉</button>
          <button type="button" class="log-expand-btn collapse-toggle-btn" title="Einklappen" aria-label="Einklappen" onclick="toggleCardCollapse('statusCard', this)">↙</button>
        </div>
      </div>
      <div class="collapse-body">
        <div id="statusTableWrap" class="status-table-wrap">
          <table id="statusTable" class="status-table">
            <thead id="statusTableHead"></thead>
            <tbody id="statusTableBody">
              <tr><td id="statusTableEmpty" colspan="1">lade...</td></tr>
            </tbody>
          </table>
        </div>
        <pre id="statusBox" class="hidden"></pre>
      </div>
    </div>

    <div id="procCard" class="card wide collapsible-card">
      <div class="card-head">
        <h2>LOG</h2>
        <div class="card-actions">
          <button type="button" class="log-expand-btn gross-btn" title="Groß" aria-label="Groß" onclick="openLogModal('LOG', 'procBox')">⇗⇗</button>
          <button type="button" class="log-expand-btn popout-btn" title="Neues Fenster" aria-label="Neues Fenster" onclick="openLogWindow('LOG', 'procBox')">⧉</button>
          <button type="button" class="log-expand-btn collapse-toggle-btn" title="Einklappen" aria-label="Einklappen" onclick="toggleCardCollapse('procCard', this)">↙</button>
        </div>
      </div>
      <div class="collapse-body">
        <pre id="procBox">lade...</pre>
      </div>
    </div>
    <div id="planCard" class="card wide collapsible-card">
      <div class="card-head">
        <h2>OUT Tree</h2>
        <div class="card-actions">
          <button type="button" class="log-expand-btn gross-btn" title="Groß" aria-label="Groß" onclick="openLogModal('OUT Tree', 'planBox')">⇗⇗</button>
          <button type="button" class="log-expand-btn popout-btn" title="Neues Fenster" aria-label="Neues Fenster" onclick="openLogWindow('OUT Tree', 'planBox')">⧉</button>
          <button type="button" class="log-expand-btn collapse-toggle-btn" title="Einklappen" aria-label="Einklappen" onclick="toggleCardCollapse('planCard', this)">↙</button>
        </div>
      </div>
      <div class="collapse-body">
        <pre id="planBox">lade...</pre>
      </div>
    </div>
  </div>

  <div id="logModal" class="log-modal hidden" onclick="if (event.target && event.target.id === 'logModal') closeLogModal();">
    <div class="log-modal-panel">
      <div id="logModalHead" class="card-head">
        <h2 id="logModalTitle">{{ site_title }} - | Log</h2>
        <div class="card-actions">
          <span id="statusErrorModalInfo" class="status-error-indicator hidden">Fehler 0/0 | Erledigt 0/0</span>
          <button id="statusFilterModalBtn" type="button" class="log-expand-btn hidden" title="Status-Filter" aria-label="Status-Filter" onclick="toggleStatusMissingFilter()">Alle</button>
          <button id="logModalPopoutBtn" type="button" class="log-expand-btn popout-btn" title="Neues Fenster" aria-label="Neues Fenster" onclick="openCurrentModalInWindow()">⧉</button>
          <button id="logModalCloseBtn" type="button" class="log-expand-btn log-close-btn" title="Einklappen" aria-label="Einklappen" onclick="closeLogModal()">↙</button>
        </div>
      </div>
      <div class="log-modal-body">
        <div id="logModalSummaryAmpel" class="summary-ampel hidden"></div>
        <pre id="logModalPre">(leer)</pre>
        <div id="logModalStatusWrap" class="status-table-wrap hidden"></div>
        <div id="confirmPanel" class="confirm-panel hidden">
          <div id="confirmText" class="confirm-text hidden"></div>
          <div class="confirm-actions-row">
            <div id="confirmPrimaryActions" class="confirm-actions-left">
              <button id="confirmCopyBtn" type="button" class="btn btn-primary" onclick="submitPendingConfirmation('copy')">Copy</button>
              <button id="confirmEncodeBtn" type="button" class="btn btn-primary" onclick="submitPendingConfirmation('encode')">Encode</button>
              <button id="confirmAnalyzeBtn" type="button" class="btn btn-primary" onclick="submitPendingConfirmation('ok')">Analyze OK</button>
            </div>
            <div id="confirmSecondaryActions" class="confirm-actions-right">
              <button id="confirmCleanBtn" type="button" class="btn btn-clean hidden" onclick="submitPendingConfirmation('clean')">Reset "Erledigt"</button>
              <button id="confirmEditBtn" type="button" class="btn" onclick="openConfirmEditorInline()">Editor</button>
              <button id="confirmExitBtn" type="button" class="btn" onclick="closeLogModal()">Exit</button>
              <button id="confirmEditPopoutBtn" type="button" class="btn hidden" title="Editor neues Fenster" aria-label="Editor neues Fenster" onclick="openConfirmEditorWindow()">⧉</button>
              <button id="confirmCancelBtn" type="button" class="btn btn-stop hidden" onclick="submitPendingConfirmation('cancel')">Exit</button>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <div id="inlineConfirmModal" class="log-modal hidden" onclick="if (event.target && event.target.id === 'inlineConfirmModal') resolveInlineConfirm(false);">
    <div class="log-modal-panel confirm-modal-panel">
      <div class="card-head">
        <h2 id="inlineConfirmTitle">{{ site_title }} {{ version_current }} | Bestätigen</h2>
      </div>
      <div id="inlineConfirmText" class="confirm-modal-text"></div>
      <div class="confirm-modal-actions">
        <button id="inlineConfirmCancelBtn" type="button" class="btn btn-primary" onclick="resolveInlineConfirm(false)">Zurück</button>
        <button id="inlineConfirmOkBtn" type="button" class="btn btn-stop" onclick="resolveInlineConfirm(true)">Abbruch</button>
      </div>
    </div>
  </div>

  <div id="updateProgressModal" class="log-modal hidden" onclick="if (event.target && event.target.id === 'updateProgressModal') closeUpdateProgressModal();">
    <div class="log-modal-panel update-progress-panel">
      <div class="card-head">
        <h2 id="updateProgressTitle">{{ site_title }} {{ version_current }} | Update</h2>
        <div class="card-actions">
          <button type="button" class="log-expand-btn log-close-btn" title="Einklappen" aria-label="Einklappen" onclick="closeUpdateProgressModal()">↙</button>
        </div>
      </div>
      <div class="update-progress-body">
        <div id="updateProgressStatus" class="update-progress-status">Noch kein Update gestartet.</div>
        <pre id="updateProgressPre" class="update-progress-pre">(leer)</pre>
      </div>
    </div>
  </div>

  <div id="settingsModal" class="log-modal hidden" onclick="if (event.target && event.target.id === 'settingsModal') closeSettingsModal();">
    <div class="log-modal-panel settings-modal-panel">
      <div class="card-head">
        <h2>{{ site_title }} {{ version_current }} | Einstellungen</h2>
        <div class="card-actions">
          <button type="button" class="log-expand-btn log-close-btn" title="Einklappen" aria-label="Einklappen" onclick="closeSettingsModal()">↙</button>
        </div>
      </div>
      <div class="settings-body">
        <div class="settings-grid">
          <label for="encoderSetting">Encoder</label>
          <select id="encoderSetting">
            {% for encoder_value, encoder_label in encoder_options %}
            <option value="{{ encoder_value }}"{% if default_encoder == encoder_value %} selected{% endif %}>{{ encoder_label }}</option>
            {% endfor %}
          </select>

          <div class="settings-main-toggle">
            <label class="settings-toggle-label" for="startOnBootSetting">
              <input id="startOnBootSetting" type="checkbox"{% if settings_start_on_boot %} checked{% endif %} />
              Beim Booten starten
            </label>
            <label class="settings-toggle-label" for="encodeOnce4kSetting">
              <input id="encodeOnce4kSetting" type="checkbox"{% if settings_encode_once_4k %} checked{% endif %} />
              1x Encode bei 4k
            </label>
            <label class="settings-toggle-label" for="oneEncodePerWorkerSetting">
              <input id="oneEncodePerWorkerSetting" type="checkbox"{% if settings_one_encode_per_worker %} checked{% endif %} />
              Nur 1 Encode pro Worker
            </label>
            <label class="settings-toggle-label" for="allowWorker4Setting">
              <input id="allowWorker4Setting" type="checkbox"{% if settings_allow_worker_4 %} checked{% endif %} />
              4.Worker erlauben
            </label>
            <label class="settings-toggle-label" for="precheckEgbSetting">
              <input id="precheckEgbSetting" type="checkbox"{% if settings_precheck_egb %} checked{% endif %} />
              Pre-Check E-GB
            </label>
            <label class="settings-toggle-label" for="speedFallbackCopySetting">
              <input id="speedFallbackCopySetting" type="checkbox"{% if settings_speed_fallback_copy %} checked{% endif %} />
              Speed-Fallback auf Copy
            </label>
          </div>

          <label for="plexIpSetting">Plex IP</label>
          <input id="plexIpSetting" value="{{ settings_plex_ip }}" placeholder="192.168.52.5" />

          <label for="nasIpSetting">NAS IP</label>
          <input id="nasIpSetting" value="{{ settings_nas_ip }}" placeholder="192.168.52.4" />

          <label for="targetNfsSetting">Zielpfad-NFS</label>
          <div class="path-row">
            <input id="targetNfsSetting" value="{{ settings_target_nfs_path }}" placeholder="/Volumes/Data/Movie/" />
            <button type="button" onclick="openTargetNfsBrowse()" title="Verzeichnis-Auswahl" aria-label="Verzeichnis-Auswahl">&#128193;</button>
          </div>

          <label for="targetOutSetting">Ziel __OUT</label>
          <div class="path-row">
            <input id="targetOutSetting" value="{{ settings_target_out_path }}" placeholder="Startordner/__OUT" />
            <button type="button" onclick="openTargetOutBrowse()" title="Verzeichnis-Auswahl" aria-label="Verzeichnis-Auswahl">&#128193;</button>
            <button type="button" onclick="setTargetOutDefault()" title="Standard setzen" aria-label="Standard setzen">Default</button>
          </div>

          <label for="targetReenqueueSetting">Ziel __RE-ENQUEUE</label>
          <div class="path-row">
            <input id="targetReenqueueSetting" value="{{ settings_target_reenqueue_path }}" placeholder="Startordner/__RE-ENQUEUE" />
            <button type="button" onclick="openTargetReenqueueBrowse()" title="Verzeichnis-Auswahl" aria-label="Verzeichnis-Auswahl">&#128193;</button>
            <button type="button" onclick="setTargetReenqueueDefault()" title="Standard setzen" aria-label="Standard setzen">Default</button>
          </div>

          <label for="parallelJobLimitSetting">Parallele Jobs</label>
          <input id="parallelJobLimitSetting" type="number" min="1" max="4" step="1" value="{{ settings_parallel_job_limit }}" />
        </div>
        <details class="settings-secrets">
          <summary>Advanced</summary>
          <div class="settings-cache-row">
            <div class="settings-cache-meta">
              <div id="cacheDbCount" class="settings-cache-count">Quelldateien im Cache werden geladen...</div>
            </div>
            <button id="cacheDbResetBtn" type="button" class="btn btn-stop settings-cache-reset-btn" onclick="resetCacheDbFromSettings()">Cache Reset</button>
          </div>
          <div class="settings-advanced-toggle">
            <label class="settings-toggle-label" for="aiQueryDisabledSetting">
              <input id="aiQueryDisabledSetting" type="checkbox"{% if settings_ai_query_disabled %} checked{% endif %} />
              KI-Abfrage deaktiviert
            </label>
          </div>
          <div class="settings-grid">
            <label for="plexApiSetting">Plex-API</label>
            <input id="plexApiSetting" type="password" value="{{ settings_plex_api }}" placeholder="Plex API Token" />

            <label for="tmdbApiSetting">TMDB-API</label>
            <input id="tmdbApiSetting" type="password" value="{{ settings_tmdb_api }}" placeholder="TMDB API Key" />

            <label for="geminiApiSetting">Gemini API</label>
            <input id="geminiApiSetting" type="password" value="{{ settings_gemini_api }}" placeholder="Gemini API Key" />
          </div>
          <div class="settings-note">Einstellungen und API-Keys werden in MariaDB gespeichert. API-Keys werden in der UI nicht angezeigt. Leer lassen = unverändert.</div>
        </details>
        <div id="settingsStatus" class="settings-status"></div>
        <div class="settings-actions">
          <button type="button" class="btn" onclick="saveSettings()">Einstellungen speichern</button>
          <button type="button" class="btn" onclick="closeSettingsModal()">Exit</button>
        </div>
      </div>
    </div>
  </div>

  <script>
    const SITE_TITLE = {{ site_title|tojson }};
    const ALLOWED_ENCODERS = {{ encoder_values|tojson }};
    const WORKER_NAMES = {{ worker_names|tojson }};
    const MM_THEME_KEY = 'managemovie.theme';
    const MM_UPDATE_MODAL_KEY = 'managemovie.update.modal';
    const MM_UPDATE_RELOADED_KEY = 'managemovie.update.reloaded';
    function normalizeThemeMode(value) {
      return String(value || '').trim().toLowerCase() === 'dark' ? 'dark' : 'light';
    }
    function getThemeMode() {
      try {
        return normalizeThemeMode(localStorage.getItem(MM_THEME_KEY) || 'light');
      } catch (err) {
        return 'light';
      }
    }
    function applyThemeMode(mode) {
      const normalized = normalizeThemeMode(mode);
      document.documentElement.setAttribute('data-theme', normalized);
      const homeBtn = document.getElementById('themeModeBtn');
      if (homeBtn) {
        homeBtn.classList.toggle('active', normalized === 'dark');
        homeBtn.title = 'Anzeige umschalten';
        homeBtn.setAttribute('aria-label', 'Anzeige umschalten');
        const iconEl = document.getElementById('themeModeIcon');
        if (iconEl) {
          iconEl.textContent = normalized === 'dark' ? '☾' : '☀';
        }
      }
      return normalized;
    }
    function toggleThemeMode() {
      const next = getThemeMode() === 'dark' ? 'light' : 'dark';
      try {
        localStorage.setItem(MM_THEME_KEY, next);
      } catch (err) {
      }
      applyThemeMode(next);
    }
    applyThemeMode(getThemeMode());
    window.addEventListener('storage', (event) => {
      if (event && event.key === MM_THEME_KEY) {
        applyThemeMode(getThemeMode());
      }
    });
    let modalSourceId = '';
    let modalVersion = '-';
    let modalLogTitle = 'Log';
    const preLockUntil = {};
    let pendingConfirmToken = '';
    let pendingConfirmInFlight = false;
    let pendingConfirmData = null;
    let pendingConfirmFilterToken = '';
    let pendingConfirmModalToken = '';
    let pendingConfirmNotice = '';
    let confirmWindowRef = null;
    let lastJobRunning = false;
    let lastJobMode = '';
    let idlePanelsInitialized = false;
    let inlineConfirmResolver = null;
    let inlineConfirmApply = null;
    let stopRequestInFlight = false;
    let updateRequestInFlight = false;
    let updateStatusInFlight = false;
    let updateStatusPollHandle = null;
    let lastKnownJobFolder = '';
    let bypassStartConfirmOnce = false;
    let initialSetupRequired = false;
    let initialSetupDone = true;
    let initialSetupNoticeShown = false;
    let currentWorkerStates = [];
    let currentWorkerJobMap = {};
    let currentEncodeWorkerName = '';
    const workerUiBusyActions = {};
    let pendingTargetNfsSelection = {{ settings_target_nfs_selected|tojson }};
    let pendingTargetOutSelection = {{ settings_target_out_selected|tojson }};
    let pendingTargetReenqueueSelection = {{ settings_target_reenqueue_selected|tojson }};
    let selectedMode = {{ default_mode|tojson }};
    const postOptions = {
      sync_nas: false,
      sync_plex: false,
      del_out: false,
      del_source: false,
    };
    const summaryAmpelRows = [
      { key: 'analyze', label: 'Analyze' },
      { key: 'copy', label: 'Copy' },
      { key: 'encode', label: 'Encode' },
      { key: 'sync_nas', label: 'Sync NAS' },
      { key: 'sync_plex', label: 'Sync Plex' },
      { key: 'del_out', label: 'Lösche OUT' },
      { key: 'del_source', label: 'Lösche Quelle' },
    ];
    const statusTableState = {
      headers: [],
      rows: [],
      sortIndex: -1,
      sortDir: 'asc',
      filterMode: 'all',
      emptyStreak: 0,
      activeKey: '',
      lastAutoScrollMainKey: '',
      lastAutoScrollModalKey: '',
    };
    let statusFilterModeContext = 'analyze';

    function nowMs() {
      return Date.now();
    }

    function workerDotClassForState(state) {
      const raw = String(state || '').trim().toLowerCase();
      if (raw === 'encoding') return 'worker-dot encoding';
      if (raw === 'idle') return 'worker-dot idle';
      return 'worker-dot';
    }

    function displayWorkerName(name) {
      const raw = String(name || '').trim();
      const match = raw.match(/^mamow?([0-9]+)$/i);
      if (match) return `Worker-${Number(match[1])}`;
      return raw;
    }

    function statusWorkerLabel(name) {
      const raw = String(name || '').trim();
      if (!raw) return '-';
      let match = raw.match(/^mamow?([0-9]+)$/i);
      if (match) return String(Number(match[1]));
      match = raw.match(/^worker[- ]?([0-9]+)$/i);
      if (match) return String(Number(match[1]));
      return raw;
    }

    function hasAvailableEncodeWorker() {
      const workers = Array.isArray(currentWorkerStates) ? currentWorkerStates : [];
      return workers.some((worker) => {
        if (!worker || typeof worker !== 'object') return false;
        if (!worker.connected) return false;
        if (!worker.enabled) return false;
        if (String(worker.state || '').trim().toLowerCase() === 'paused') return false;
        const active = Number(worker.active_count || 0);
        const maxActive = Number(worker.max_active || 1);
        if (!Number.isFinite(active) || !Number.isFinite(maxActive)) return true;
        return active < maxActive;
      });
    }

    function isMissingUiMetric(value) {
      const text = String(value || '').trim().toLowerCase();
      return !text || text === 'n/a' || text === 'na' || text === '-' || text === '0' || text === '0.0' || text === '0,0' || text === '0 mb/s' || text === '0 fps';
    }

    function normalizeStatusFileKey(value) {
      return fileNameOnlyForUi(String(value || '')).trim().toLowerCase();
    }

    function findWorkerInfoForStatusRow(sourceValue, targetValue) {
      const lookup = buildWorkerLookupMap();
      const keys = [
        normalizeStatusFileKey(sourceValue),
        normalizeStatusFileKey(targetValue),
      ].filter((value, index, arr) => !!value && arr.indexOf(value) === index);
      for (const key of keys) {
        const match = lookup[key];
        if (match) return match;
      }
      return null;
    }

    function buildWorkerLookupMap() {
      const out = {};
      const currentMap = (currentWorkerJobMap && typeof currentWorkerJobMap === 'object') ? currentWorkerJobMap : {};
      Object.entries(currentMap).forEach(([workerName, jobs]) => {
        (Array.isArray(jobs) ? jobs : []).forEach((job) => {
          const next = {
            worker_name: String(workerName || '').trim(),
            running: !!job.running,
            speed: String(job.speed || '').trim(),
            fps: String(job.fps || '').trim(),
            eta: String(job.eta || '').trim(),
            z_gb: String(job.z_gb || '').trim(),
            e_gb: String(job.e_gb || '').trim(),
            lzeit: String(job.lzeit || '').trim(),
          };
          [
            normalizeStatusFileKey(job.file_name || job.source_name || ''),
            normalizeStatusFileKey(job.source_name || ''),
            normalizeStatusFileKey(job.target_name || ''),
          ].filter((value, index, arr) => !!value && arr.indexOf(value) === index).forEach((key) => {
            const prev = out[key];
            if (!prev || (!prev.running && next.running)) out[key] = next;
          });
        });
      });
      return out;
    }

    function collectRunningWorkerNames() {
      const out = [];
      const map = (currentWorkerJobMap && typeof currentWorkerJobMap === 'object') ? currentWorkerJobMap : {};
      Object.entries(map).forEach(([workerName, jobs]) => {
        const hasRunning = (Array.isArray(jobs) ? jobs : []).some((job) => !!(job && job.running));
        if (!hasRunning) return;
        const label = displayWorkerName(workerName);
        if (label && !out.includes(label)) out.push(label);
      });
      return out;
    }

    function workerLabelForState(worker) {
      const item = worker && typeof worker === 'object' ? worker : {};
      const busyAction = String(item.busy_action || '').trim().toLowerCase();
      if (busyAction === 'kill') return 'kill läuft';
      if (busyAction === 'init' || item.reinit_running) return 'init läuft';
      if (!item.connected) return 'N/A';
      if (item.state === 'encoding') {
        const fpsText = formatStatusFpsText(item.active_fps || '');
        return fpsText ? `${fpsText} FPS` : 'encoding';
      }
      if (item.state === 'paused') return 'pausiert';
      if (item.state === 'stopped') return 'gestoppt';
      return 'idle';
    }

    function setWorkerActionPending(workerName, action, active) {
      const name = String(workerName || '').trim();
      const verb = String(action || '').trim().toLowerCase();
      if (!name) return;
      if (active && verb) workerUiBusyActions[name] = verb;
      else delete workerUiBusyActions[name];
      document.querySelectorAll(`[data-worker-name="${name}"][data-worker-action]`).forEach((btn) => {
        const btnAction = String(btn.getAttribute('data-worker-action') || '').trim().toLowerCase();
        if (active && (btnAction === 'kill' || btnAction === 'init' || btnAction === 'pause')) {
          btn.disabled = true;
        }
      });
    }

    function renderWorkers(workers) {
      currentWorkerStates = Array.isArray(workers) ? workers : [];
      currentEncodeWorkerName = '';
      currentWorkerStates.forEach((worker) => {
        const name = String((worker && worker.name) || '').trim();
        if (!name) return;
        if (worker.state === 'encoding' && !currentEncodeWorkerName) {
          currentEncodeWorkerName = name;
        }
        const title = document.querySelector(`[data-worker-title="${name}"]`);
        const dot = document.querySelector(`[data-worker-dot="${name}"]`);
        const label = document.querySelector(`[data-worker-label="${name}"]`);
        const meta = document.querySelector(`[data-worker-meta="${name}"]`);
        const workerJobs = Array.isArray(currentWorkerJobMap[name]) ? currentWorkerJobMap[name] : [];
        if (title) title.innerHTML = `${escHtmlUi(displayWorkerName(name))}`;
        if (dot) dot.className = workerDotClassForState(worker.state);
        if (label) label.innerText = workerLabelForState(worker);
        if (meta) {
          const lines = [];
          const metaParts = [];
          if (worker.node) metaParts.push(worker.node);
          if (worker.ctid) metaParts.push(`CT ${worker.ctid}`);
          if (worker.installed_version) metaParts.push(worker.installed_version);
          if (metaParts.length) {
            lines.push(`<span>${escHtmlUi(metaParts.join(' | '))}</span>`);
          }
          if (worker.connected && (worker.state === 'encoding' || worker.state === 'paused')) {
            const runningWorkerJobs = workerJobs.filter((job) => !!(job && job.running));
            runningWorkerJobs.forEach((job) => {
              const fileText = fileNameOnlyForUi(job.file_name || job.source_name || '');
              const speedText = formatStatusSpeedText(job.speed || '');
              const fpsText = formatStatusFpsText(job.fps || '');
              const infoParts = [];
              if (speedText) infoParts.push(speedText);
              if (fpsText) infoParts.push(`${fpsText} FPS`);
              const infoText = infoParts.length ? ` | ${infoParts.join(' | ')}` : '';
              lines.push(`<span>${escHtmlUi(fileText || '-')}<span style="color:#8fa0c2">${escHtmlUi(infoText)}</span></span>`);
            });
            if (!runningWorkerJobs.length && worker.active_job) {
              const speedText = formatStatusSpeedText(worker.active_speed || '');
              const fpsText = formatStatusFpsText(worker.active_fps || '');
              const infoParts = [];
              if (speedText) infoParts.push(speedText);
              if (fpsText) infoParts.push(`${fpsText} FPS`);
              const infoText = infoParts.length ? ` | ${infoParts.join(' | ')}` : '';
              lines.push(`<span>${escHtmlUi(fileNameOnlyForUi(worker.active_job))}<span style="color:#8fa0c2">${escHtmlUi(infoText)}</span></span>`);
            }
          }
          meta.innerHTML = lines.join('<br>');
        }
        const busyAction = String(worker.busy_action || workerUiBusyActions[name] || '').trim().toLowerCase();
        document.querySelectorAll(`[data-worker-name="${name}"][data-worker-action]`).forEach((btn) => {
          const action = String(btn.getAttribute('data-worker-action') || '').trim().toLowerCase();
          if (busyAction === 'kill' || busyAction === 'init' || worker.reinit_running) {
            btn.disabled = true;
            return;
          }
          if (action === 'pause') {
            const processing = worker.connected && worker.enabled && (worker.state === 'encoding' || worker.state === 'paused');
            btn.disabled = !processing;
            return;
          }
          if (action === 'init') {
            btn.disabled = !!worker.connected;
            return;
          }
          if (action === 'kill') {
            const hasContainerRef = !!String(worker.ctid || '').trim() && !!String(worker.node || '').trim();
            btn.disabled = !hasContainerRef;
          }
        });
      });
    }

    async function controlWorker(workerName, action) {
      const name = String(workerName || '').trim();
      const verb = String(action || '').trim().toLowerCase();
      if (!name || !verb) return;
      setWorkerActionPending(name, verb, true);
      try {
        const res = await fetch(`/api/workers/${encodeURIComponent(name)}/action`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: verb }),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || !data.ok) {
          throw new Error((data && data.message) ? data.message : `HTTP ${res.status}`);
        }
        renderWorkers(data.workers || []);
        setSettingsStatus(String(data.message || `${name}: ${verb}`));
      } catch (err) {
        setSettingsStatus(`Worker-Aktion fehlgeschlagen: ${err}`, true);
      } finally {
        setWorkerActionPending(name, '', false);
      }
    }

    function clearFlashMessage() {
      const msgEl = document.getElementById('flashMsg');
      if (!msgEl) return;

      const txt = (msgEl.innerText || '').trim().toLowerCase();
      if (txt.startsWith('job gestartet:') || txt.startsWith('job gestoppt.')) {
        setTimeout(() => {
          if (msgEl && msgEl.parentNode) {
            msgEl.parentNode.removeChild(msgEl);
          }
        }, 2500);
      }

      if (window.history && window.history.replaceState) {
        const url = new URL(window.location.href);
        let changed = false;
        if (url.searchParams.has('msg')) {
          url.searchParams.delete('msg');
          changed = true;
        }
        if (url.searchParams.has('settings_target_nfs')) {
          url.searchParams.delete('settings_target_nfs');
          changed = true;
        }
        if (url.searchParams.has('settings_target_out')) {
          url.searchParams.delete('settings_target_out');
          changed = true;
        }
        if (url.searchParams.has('settings_target_reenqueue')) {
          url.searchParams.delete('settings_target_reenqueue');
          changed = true;
        }
        if (changed) {
          const query = url.searchParams.toString();
          const next = query ? (url.pathname + '?' + query) : url.pathname;
          window.history.replaceState({}, '', next);
        }
      }
    }

    function lockPre(id, ms = 3000) {
      preLockUntil[id] = nowMs() + ms;
    }

    function isPreLocked(id) {
      return (preLockUntil[id] || 0) > nowMs();
    }

    function isSelectionInside(el) {
      if (!el) return false;
      const sel = window.getSelection ? window.getSelection() : null;
      if (!sel || sel.rangeCount === 0 || sel.isCollapsed) return false;
      const anchor = sel.anchorNode;
      const focus = sel.focusNode;
      return (!!anchor && el.contains(anchor)) || (!!focus && el.contains(focus));
    }

    function hasActiveTextSelection() {
      const sel = window.getSelection ? window.getSelection() : null;
      if (!sel || sel.rangeCount === 0 || sel.isCollapsed) return false;
      const text = String(sel.toString() || '').trim();
      return text.length > 0;
    }

    function shouldPauseUiRefreshForSelection() {
      if (!hasActiveTextSelection()) return false;
      const sel = window.getSelection ? window.getSelection() : null;
      if (!sel || sel.rangeCount === 0) return false;
      const anchor = sel.anchorNode;
      const focus = sel.focusNode;
      const trackedIds = [
        'statusTableWrap',
        'statusBox',
        'summaryBox',
        'procBox',
        'planBox',
        'jobBox',
        'logModalPre',
        'logModalStatusWrap',
        'statusSummaryBox',
      ];
      return trackedIds.some((id) => {
        const el = document.getElementById(id);
        if (!el) return false;
        return (!!anchor && el.contains(anchor)) || (!!focus && el.contains(focus));
      });
    }

    function normalizeDisplayUmlauts(text) {
      let out = String(text || '');
      const replacements = [
        ['Bestaetig', 'Bestätig'],
        ['bestaetig', 'bestätig'],
        ['Pruef', 'Prüf'],
        ['pruef', 'prüf'],
        ['Uebers', 'Übers'],
        ['uebers', 'übers'],
        ['Ueber', 'Über'],
        ['ueber', 'über'],
        ['Zurueck', 'Zurück'],
        ['zurueck', 'zurück'],
        ['Geloesch', 'Gelösch'],
        ['geloesch', 'gelösch'],
        ['Koenn', 'Könn'],
        ['koenn', 'könn'],
        ['Aender', 'Änder'],
        ['aender', 'änder'],
        ['Waehr', 'Währ'],
        ['waehr', 'währ'],
        ['Laeuft', 'Läuft'],
        ['laeuft', 'läuft'],
        ['Oeffn', 'Öffn'],
        ['oeffn', 'öffn'],
        ['Fuer', 'Für'],
        ['fuer', 'für'],
        ['Eintraege', 'Einträge'],
        ['eintraege', 'einträge'],
        ['Loes', 'Lös'],
        ['loes', 'lös'],
        ['ausfuehr', 'ausführ'],
        ['Ausfuehr', 'Ausführ'],
        ['unveraendert', 'unverändert'],
        ['Unveraendert', 'Unverändert'],
      ];
      replacements.forEach(([src, dst]) => {
        out = out.split(src).join(dst);
      });
      return out;
    }

    async function copyText(text) {
      const val = text || '';
      try {
        await navigator.clipboard.writeText(val);
      } catch (err) {
        const ta = document.createElement('textarea');
        ta.value = val;
        ta.setAttribute('readonly', 'readonly');
        ta.style.position = 'fixed';
        ta.style.left = '-9999px';
        document.body.appendChild(ta);
        ta.select();
        document.execCommand('copy');
        document.body.removeChild(ta);
      }
    }

    function copyPreText(id) {
      const el = document.getElementById(id);
      if (!el) return;
      lockPre(id, 2000);
      copyText(el.innerText || '');
    }

    function copyModalText() {
      const el = document.getElementById('logModalPre');
      if (!el) return;
      lockPre('logModalPre', 2000);
      copyText(el.innerText || '');
    }

    function resolveInlineConfirm(ok) {
      const modal = document.getElementById('inlineConfirmModal');
      if (modal) modal.classList.add('hidden');
      const apply = inlineConfirmApply;
      inlineConfirmApply = null;
      const resolver = inlineConfirmResolver;
      inlineConfirmResolver = null;
      if (apply) apply(!!ok);
      if (resolver) resolver(!!ok);
    }

    function askBrowserConfirm(message, title = 'Bestätigen', options = {}) {
      const modal = document.getElementById('inlineConfirmModal');
      const textEl = document.getElementById('inlineConfirmText');
      const titleEl = document.getElementById('inlineConfirmTitle');
      const cancelBtn = document.getElementById('inlineConfirmCancelBtn');
      const okBtn = document.getElementById('inlineConfirmOkBtn');
      if (!modal || !textEl) {
        return Promise.resolve(false);
      }
      if (document.body && modal.parentNode === document.body) {
        document.body.appendChild(modal);
      }
      if (titleEl) {
        titleEl.innerText = normalizeDisplayUmlauts(`${SITE_TITLE} ${modalVersion || '-'} | ${title}`);
      }
      textEl.innerText = normalizeDisplayUmlauts(String(message || '').trim());
      if (cancelBtn) {
        cancelBtn.innerText = normalizeDisplayUmlauts(String(options.cancelLabel || 'Zurück'));
      }
      if (okBtn) {
        okBtn.innerText = normalizeDisplayUmlauts(String(options.okLabel || 'Abbruch'));
        okBtn.className = `btn ${options.okClass || 'btn-stop'}`;
      }
      inlineConfirmApply = typeof options.apply === 'function' ? options.apply : null;
      if (inlineConfirmResolver) {
        inlineConfirmResolver(false);
      }
      modal.classList.remove('hidden');
      return new Promise((resolve) => {
        inlineConfirmResolver = resolve;
      });
    }

    function updateModalStoredFlag(key, value = null) {
      try {
        if (value === null) {
          return localStorage.getItem(key) || '';
        }
        if (value === '') {
          localStorage.removeItem(key);
        } else {
          localStorage.setItem(key, value);
        }
      } catch (err) {
      }
      return '';
    }

    function isUpdateProgressModalOpen() {
      const modal = document.getElementById('updateProgressModal');
      return !!modal && !modal.classList.contains('hidden');
    }

    function setUpdateProgressStatus(message = '') {
      const el = document.getElementById('updateProgressStatus');
      if (el) el.innerText = normalizeDisplayUmlauts(String(message || '').trim());
    }

    function setUpdateProgressLog(text = '') {
      const el = document.getElementById('updateProgressPre');
      if (!el) return;
      const next = String(text || '').trim();
      const nearBottom = (el.scrollTop + el.clientHeight + 40) >= el.scrollHeight;
      el.innerText = next || '(leer)';
      if (nearBottom) {
        el.scrollTop = el.scrollHeight;
      }
    }

    function stopUpdateStatusPolling() {
      if (updateStatusPollHandle) {
        clearInterval(updateStatusPollHandle);
        updateStatusPollHandle = null;
      }
    }

    async function refreshUpdateProgressStatus(force = false) {
      if (updateStatusInFlight && !force) return;
      updateStatusInFlight = true;
      try {
        const res = await fetch('/api/system/update-status', { cache: 'no-store' });
        const data = await res.json().catch(() => ({}));
        const running = !!(data && data.running);
        const done = !!(data && data.done);
        const success = !!(data && data.success);
        const returnCode = (data && typeof data.return_code !== 'undefined') ? data.return_code : null;
        const logText = (data && data.log) ? String(data.log) : '';
        setUpdateProgressLog(logText);
        if (running) {
          setUpdateProgressStatus('Update läuft. Der Verlauf wird live aktualisiert.');
        } else if (done && success) {
          setUpdateProgressStatus('Update abgeschlossen. Die App wurde neu geladen.');
          if (updateModalStoredFlag(MM_UPDATE_RELOADED_KEY) !== '1') {
            updateModalStoredFlag(MM_UPDATE_RELOADED_KEY, '1');
            window.setTimeout(() => {
              window.location.reload();
            }, 900);
          }
        } else if (done) {
          setUpdateProgressStatus(`Update fehlgeschlagen${returnCode !== null ? ` (rc=${returnCode})` : ''}.`);
        } else if (logText.trim()) {
          setUpdateProgressStatus('Update vorbereitet...');
        } else {
          setUpdateProgressStatus('Warte auf Update-Status...');
        }
      } catch (err) {
        setUpdateProgressStatus('Verbindung getrennt. Warte auf Neustart der App...');
      } finally {
        updateStatusInFlight = false;
      }
    }

    function startUpdateStatusPolling() {
      if (updateStatusPollHandle) return;
      refreshUpdateProgressStatus(true);
      updateStatusPollHandle = window.setInterval(() => {
        refreshUpdateProgressStatus(false);
      }, 1500);
    }

    function openUpdateProgressModal() {
      const modal = document.getElementById('updateProgressModal');
      const titleEl = document.getElementById('updateProgressTitle');
      if (!modal) return;
      if (titleEl) {
        titleEl.innerText = normalizeDisplayUmlauts(`${SITE_TITLE} ${modalVersion || '-'} | Update`);
      }
      modal.classList.remove('hidden');
      updateModalStoredFlag(MM_UPDATE_MODAL_KEY, '1');
      startUpdateStatusPolling();
    }

    function closeUpdateProgressModal() {
      const modal = document.getElementById('updateProgressModal');
      if (modal) modal.classList.add('hidden');
      stopUpdateStatusPolling();
      updateModalStoredFlag(MM_UPDATE_MODAL_KEY, '');
      updateModalStoredFlag(MM_UPDATE_RELOADED_KEY, '');
    }

    async function confirmUpdate() {
      if (updateRequestInFlight) return;
      openUpdateProgressModal();
      setUpdateProgressStatus('Update wird gestartet...');
      setUpdateProgressLog('');
      updateModalStoredFlag(MM_UPDATE_RELOADED_KEY, '');
      updateRequestInFlight = true;
      try {
        const res = await fetch('/api/system/update', { method: 'POST' });
        const data = await res.json().catch(() => ({}));
        if (res.ok && data && data.ok) {
          setUpdateProgressStatus('Update ausgelöst. Warte auf Verlauf...');
          await refreshUpdateProgressStatus(true);
          return;
        }
        const err = (data && data.error) ? String(data.error) : 'Update fehlgeschlagen.';
        setUpdateProgressStatus(err);
      } catch (err) {
        setUpdateProgressStatus('Verbindung getrennt. Warte auf Neustart der App...');
      } finally {
        updateRequestInFlight = false;
      }
    }

    function openStopWindow() {
      const w = window.open('/stop-window', '_blank', 'width=540,height=300');
      if (w) {
        w.focus();
        return;
      }
      window.location.href = '/stop-window';
    }

    async function requestStopFromMain() {
      if (stopRequestInFlight) return;
      const ok = await askBrowserConfirm('Lauf wirklich abbrechen?', 'ABBRUCH');
      if (!ok) return;
      const leftRunState = document.getElementById('leftRunState');
      if (leftRunState) leftRunState.innerHTML = '<b>Status:</b> Stoppe Job...';
      stopRequestInFlight = true;
      try {
        const res = await fetch('/api/stop', { method: 'POST' });
        const data = await res.json().catch(() => ({}));
        if (res.ok && data && data.ok) {
          if (leftRunState) leftRunState.innerHTML = '<b>Status:</b> Job gestoppt.';
          await refreshState();
          collapseToHomeLayout();
          return;
        }
        const err = (data && data.error) ? String(data.error) : 'Abbruch fehlgeschlagen.';
        if (leftRunState) leftRunState.innerHTML = `<b>Status:</b> ${escapeHtml(err)}`;
      } catch (err) {
        if (leftRunState) leftRunState.innerHTML = '<b>Status:</b> Abbruch fehlgeschlagen.';
      } finally {
        stopRequestInFlight = false;
      }
    }

    function openRestartWindow() {
      askBrowserConfirm(
        'App und Datenbank wirklich neu starten? Laufende Prozesse werden beendet und danach automatisch neu geladen.',
        'Restart bestätigen',
        {
          cancelLabel: 'Zurück',
          okLabel: 'Restart',
          okClass: 'btn-stop',
          apply: (ok) => {
            if (ok) {
              confirmRestart();
            }
          },
        },
      );
    }

    function openUpdateWindow() {
      askBrowserConfirm(
        'Neuestes Release von GitHub holen und die App danach sauber neu starten?',
        'Update bestätigen',
        {
          cancelLabel: 'Zurück',
          okLabel: 'Update',
          okClass: 'btn-primary',
          apply: (ok) => {
            if (ok) {
              confirmUpdate();
            }
          },
        },
      );
    }

    function openConfirmDecisionWindow(token = '') {
      // Freigabe startet immer in der grossen STATUS-Ansicht (Pfeil-Logik),
      // nicht als separates Browserfenster.
      confirmWindowRef = null;
      openLogModal('STATUS Queue', 'statusBox');
    }

    function openConfirmEditorInline() {
      const token = pendingConfirmData && pendingConfirmData.token ? String(pendingConfirmData.token) : '';
      const baseUrl = token
        ? `/confirm-editor-window?token=${encodeURIComponent(token)}`
        : '/confirm-editor-window';
      const theme = (document.documentElement.getAttribute('data-theme') || '').toLowerCase() === 'dark' ? 'dark' : 'light';
      const url = `${baseUrl}${baseUrl.includes('?') ? '&' : '?'}theme=${encodeURIComponent(theme)}`;
      window.location.href = url;
    }

    function editorPopupFeatures() {
      const availW = Math.max(900, Number(window.screen && window.screen.availWidth) || 1366);
      const availH = Math.max(720, Number(window.screen && window.screen.availHeight) || 900);
      const width = Math.max(920, Math.min(1220, availW - 90));
      const height = Math.max(720, Math.min(860, availH - 90));
      return `noopener,noreferrer,width=${Math.round(width)},height=${Math.round(height)}`;
    }

    function openConfirmEditorWindow() {
      const token = pendingConfirmData && pendingConfirmData.token ? String(pendingConfirmData.token) : '';
      const baseUrl = token
        ? `/confirm-editor-window?token=${encodeURIComponent(token)}`
        : '/confirm-editor-window';
      const theme = (document.documentElement.getAttribute('data-theme') || '').toLowerCase() === 'dark' ? 'dark' : 'light';
      const url = `${baseUrl}${baseUrl.includes('?') ? '&' : '?'}theme=${encodeURIComponent(theme)}`;
      const w = window.open(url, '_blank', editorPopupFeatures());
      if (w) {
        w.focus();
        return;
      }
      window.location.href = url;
    }

    function fileNameOnlyForUi(value) {
      const text = String(value || '').replace(/\\\\/g, '/').trim();
      if (!text) return '';
      const parts = text.split('/');
      return (parts[parts.length - 1] || '').trim();
    }

    function escHtmlUi(value) {
      return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
    }

    function formatRatioUi(pos, total) {
      const p = Math.max(1, Number(pos || 1));
      const t = Math.max(1, Number(total || 1));
      const width = Math.max(2, String(t).length);
      return `${String(p).padStart(width, '0')}/${String(t).padStart(width, '0')}`;
    }

    async function persistModeSelection(mode) {
      const normalized = String(mode || '').trim().toLowerCase();
      if (normalized !== 'analyze' && normalized !== 'copy' && normalized !== 'ffmpeg') return false;
      try {
        const res = await fetch('/settings/mode', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ mode: normalized }),
        });
        return !!res.ok;
      } catch (err) {
      }
      return false;
    }

    function isInitialSetupLocked() {
      return !!initialSetupRequired && !initialSetupDone && !lastJobRunning;
    }

    function applyInitialSetupUi(running = false) {
      const locked = !!initialSetupRequired && !initialSetupDone && !running;
      ['modeAnalyzeBtn', 'modeCopyBtn', 'modeEncodeBtn'].forEach((id) => {
        const btn = document.getElementById(id);
        if (!btn) return;
        const encodeBlocked = id === 'modeEncodeBtn' && !running && !hasAvailableEncodeWorker();
        btn.disabled = locked || encodeBlocked;
        btn.classList.toggle('disabled', locked || encodeBlocked);
        btn.title = locked
          ? 'Zuerst Einstellungen und API-Keys speichern'
          : (encodeBlocked ? 'Kein Worker verfügbar' : '');
      });
      ['syncNasBtn', 'syncPlexBtn', 'delOutBtn', 'delSourceBtn'].forEach((id) => {
        const btn = document.getElementById(id);
        if (!btn) return;
        btn.disabled = locked;
      });
      const startSubmitBtn = document.getElementById('startSubmitBtn');
      if (startSubmitBtn && !running) {
        startSubmitBtn.disabled = locked;
        startSubmitBtn.innerText = locked ? 'Einstellungen speichern' : 'Start';
      }
      if (locked && !initialSetupNoticeShown) {
        initialSetupNoticeShown = true;
        setSettingsStatus('Erststart: Zuerst Einstellungen und API-Keys speichern. Analyze, Copy und Encode bleiben bis dahin gesperrt.');
        openSettingsModal();
      } else if (!locked) {
        initialSetupNoticeShown = false;
      }
      return locked;
    }

    function canUsePostOptions(mode) {
      if (isInitialSetupLocked()) return false;
      const normalized = String(mode || '').trim().toLowerCase();
      return normalized === 'copy' || normalized === 'ffmpeg';
    }

    function canUseSyncPlexOption() {
      return canUsePostOptions(selectedMode);
    }

    function canUseDelOutOption() {
      return canUsePostOptions(selectedMode) && !!postOptions.sync_nas;
    }

    function isJobRunningState(job) {
      if (!job || typeof job !== 'object') return false;
      const rawRunning = job.running;
      let running = (rawRunning === true || rawRunning === 1 || rawRunning === '1' || rawRunning === 'true');
      if (job.job_id === 'last-run') {
        running = false;
      }
      if (job.mode === 'unknown' && !job.started_at && !job.ended_at) {
        running = false;
      }
      return running;
    }

    function isPostOptionCompletionLine(tag, body) {
      const t = String(tag || '').toUpperCase();
      const b = String(body || '').trim();
      if (!b || /^START\\b/i.test(b)) return false;
      if (t === 'SYNC-NAS') {
        return /^Sync\\s+NAS\\s+(ok|unvollstaendig)\\b/i.test(b) || /^Abbruch\\b/i.test(b);
      }
      if (t === 'SYNC-PLEX') {
        return /^Plex-Rescan\\s+(ok|fehlgeschlagen)\\b/i.test(b) || /^Abbruch\\b/i.test(b);
      }
      if (t === 'DEL-OUT' || t === 'DEL-QUELLE') return true;
      return true;
    }

    function detectRunningPostOptionKey(processingLog) {
      const lines = String(processingLog || '').split('\\n');
      const map = {
        'SYNC-NAS': 'sync_nas',
        'SYNC-PLEX': 'sync_plex',
        'DEL-OUT': 'del_out',
        'DEL-QUELLE': 'del_source',
      };
      let runningKey = '';
      for (const rawLine of lines) {
        const line = String(rawLine || '').trim();
        if (!line) continue;
        const match = line.match(/\\[(SYNC-NAS|SYNC-PLEX|DEL-OUT|DEL-QUELLE)\\]\\s*(.*)$/i);
        if (!match) continue;
        const tag = String(match[1] || '').toUpperCase();
        const body = String(match[2] || '').trim();
        const key = map[tag] || '';
        if (!key) continue;
        if (/^START\\b/i.test(body)) {
          runningKey = key;
        } else if (runningKey === key && isPostOptionCompletionLine(tag, body)) {
          runningKey = '';
        }
      }
      return runningKey;
    }

    function hasInFlightStatusProgress(statusTableRaw) {
      const parts = splitStatusPanel(statusTableRaw || '');
      const activeKey = extractActiveStatusKey(parts.meta || '');
      const m = String(activeKey || '').match(/^(\\d+)\\/(\\d+)$/);
      if (!m) return false;
      const left = Number(m[1] || 0);
      const right = Number(m[2] || 0);
      if (!Number.isFinite(left) || !Number.isFinite(right) || right <= 0) return false;
      return left > 0 && left < right;
    }

    function normalizeModeForAmpel(rawMode) {
      const m = String(rawMode || '').trim().toLowerCase();
      if (m === 'analyze' || m === 'copy' || m === 'ffmpeg') return m;
      return '';
    }

    function detectCompletedPostOptionKeys(processingLog) {
      const completed = new Set();
      const lines = String(processingLog || '').split(/\\r?\\n/);
      const map = {
        'SYNC-NAS': 'sync_nas',
        'SYNC-PLEX': 'sync_plex',
        'DEL-OUT': 'del_out',
        'DEL-QUELLE': 'del_source',
      };
      for (const rawLine of lines) {
        const line = String(rawLine || '').trim();
        if (!line) continue;
        const match = line.match(/\\[(SYNC-NAS|SYNC-PLEX|DEL-OUT|DEL-QUELLE)\\]\\s*(.*)$/i);
        if (!match) continue;
        const tag = String(match[1] || '').toUpperCase();
        const body = String(match[2] || '').trim();
        const key = map[tag] || '';
        if (!key) continue;
        if (!/^START\b/i.test(body)) {
          completed.add(key);
        }
      }
      return completed;
    }

    function detectActiveMainStep(modeRaw = '', processingLog = '') {
      const mode = String(modeRaw || '');
      const logText = String(processingLog || '');
      const copyStarted = /\\[COPY\\]/i.test(logText);
      const encodeStarted = /\\[FFMPEG\\]/i.test(logText);
      if (mode === 'copy') return copyStarted ? 'copy' : 'analyze';
      if (mode === 'ffmpeg') return encodeStarted ? 'encode' : 'analyze';
      return 'analyze';
    }

    function detectCompletedMainStepKeys(modeRaw = '', processingLog = '', running = false, runningPostKey = '') {
      const done = new Set();
      const mode = String(modeRaw || '').trim().toLowerCase();
      const logText = String(processingLog || '');
      const hasAnalyze = /\\[ANALYZE\\].*(ENDE|Fortschritt:\\s*\\d+\\s*\\/\\s*\\d+)/i.test(logText);
      const hasCopy = /\\[COPY\\].*(COPY OK|Fallback -> Copy|Manual ->)/i.test(logText);
      const hasEncode = /\\[FFMPEG\\].*(FFMPEG abgeschlossen|Fallback -> Copy)/i.test(logText);
      const mainFinished = !!runningPostKey || (!running && (hasAnalyze || hasCopy || hasEncode));

      if (!mainFinished) return done;
      if (mode === 'analyze') {
        done.add('analyze');
        return done;
      }
      if (mode === 'copy') {
        done.add('analyze');
        done.add('copy');
        return done;
      }
      if (mode === 'ffmpeg') {
        done.add('analyze');
        done.add('encode');
      }
      return done;
    }

    function buildSummaryAmpelState(data = null) {
      const colors = {
        analyze: 'gray',
        copy: 'gray',
        encode: 'gray',
        sync_nas: 'gray',
        sync_plex: 'gray',
        del_out: 'gray',
        del_source: 'gray',
      };

      const payload = (data && typeof data === 'object') ? data : null;
      const job = payload && payload.job && typeof payload.job === 'object' ? payload.job : null;
      const running = isJobRunningState(job);
      const processingLog = (payload && payload.processing_log) || '';
      const runningPostKey = detectRunningPostOptionKey(processingLog);
      const completedPostKeys = detectCompletedPostOptionKeys(processingLog);
      const effectivelyRunning = running;

      let modeRaw = effectivelyRunning
        ? normalizeModeForAmpel(job && job.mode)
        : '';
      if (!modeRaw) {
        modeRaw = normalizeModeForAmpel(selectedMode);
      }

      const opts = (effectivelyRunning && job)
        ? {
            sync_nas: !!job.sync_nas,
            sync_plex: !!job.sync_plex,
            del_out: !!job.del_out,
            del_source: !!job.del_source,
          }
        : {
            sync_nas: !!postOptions.sync_nas,
            sync_plex: !!postOptions.sync_plex,
            del_out: !!postOptions.del_out,
            del_source: !!postOptions.del_source,
          };

      const selectedKeys = [];
      if (modeRaw === 'analyze') selectedKeys.push('analyze');
      if (modeRaw === 'copy') selectedKeys.push('copy');
      if (modeRaw === 'ffmpeg') selectedKeys.push('encode');
      if (opts.sync_nas) selectedKeys.push('sync_nas');
      if (opts.sync_plex) selectedKeys.push('sync_plex');
      if (opts.del_out) selectedKeys.push('del_out');
      if (opts.del_source) selectedKeys.push('del_source');
      selectedKeys.forEach((key) => {
        if (colors[key] === 'gray') colors[key] = 'yellow';
      });
      if (effectivelyRunning && (modeRaw === 'copy' || modeRaw === 'ffmpeg')) {
        colors.analyze = 'green';
      }
      detectCompletedMainStepKeys(modeRaw, processingLog, effectivelyRunning, runningPostKey).forEach((key) => {
        if (key in colors) colors[key] = 'green';
      });
      completedPostKeys.forEach((key) => {
        if (key in colors) colors[key] = 'green';
      });

      let activeKey = '';
      if (effectivelyRunning) {
        if (runningPostKey) {
          activeKey = runningPostKey;
        } else {
          activeKey = detectActiveMainStep(modeRaw, processingLog);
        }
      }

      if (activeKey && (activeKey in colors)) {
        colors[activeKey] = 'yellow';
      }

      return { colors, activeKey, running: effectivelyRunning };
    }

    function renderSummaryAmpel(data = null) {
      const ampel = buildSummaryAmpelState(data);
      const html = summaryAmpelRows
        .map((row) => {
          const color = String((ampel.colors || {})[row.key] || 'gray');
          const isActive = !!ampel.running && String(ampel.activeKey || '') === row.key;
          const barClass = isActive ? ' bar' : '';
          return `<div class="summary-ampel-row${barClass}"><span class="summary-ampel-dot ${color}"></span><span>${row.label}</span></div>`;
        })
        .join('');

      const box = document.getElementById('summaryAmpelBox');
      if (box) box.innerHTML = html;

      const modalBox = document.getElementById('logModalSummaryAmpel');
      if (modalBox) {
        modalBox.innerHTML = html;
        modalBox.classList.toggle('hidden', modalSourceId !== 'summaryBox');
      }
    }

    function detectSummaryMode(data = null) {
      const payload = (data && typeof data === 'object') ? data : {};
      const job = payload && payload.job && typeof payload.job === 'object' ? payload.job : {};
      const settings = payload && payload.settings && typeof payload.settings === 'object' ? payload.settings : {};
      const running = isJobRunningState(job);
      const raw = String(running ? (job.mode || '') : (selectedMode || settings.mode || job.mode || '')).trim().toLowerCase();
      if (raw === 'copy') return 'c';
      if (raw === 'ffmpeg' || raw === 'encode') return 'f';
      return 'a';
    }

    function parseSummaryMetaMap(metaText = '') {
      const out = {};
      const lines = String(metaText || '').split(/\\r?\\n/);
      lines.forEach((rawLine) => {
        const line = String(rawLine || '').trim();
        if (!line) return;
        const m = line.match(/^([A-Za-zÄÖÜäöüß.-]+:)\\s*(.*)$/);
        if (!m) return;
        const key = String(m[1] || '').trim();
        const value = String(m[2] || '').trim();
        if (!(key in out)) {
          out[key] = value;
        }
      });
      return out;
    }

    function formatHhMmSs(totalSec) {
      const sec = Math.max(0, Math.floor(Number(totalSec) || 0));
      const h = Math.floor(sec / 3600);
      const m = Math.floor((sec % 3600) / 60);
      const s = sec % 60;
      const hh = String(h).padStart(2, '0');
      const mm = String(m).padStart(2, '0');
      const ss = String(s).padStart(2, '0');
      return `${hh}:${mm}:${ss}`;
    }

    function pausedSecondsFromConfirmLog(payload = {}) {
      const logText = String(payload.processing_log || '');
      if (!logText) {
        const pending = (payload.pending_confirmation && typeof payload.pending_confirmation === 'object') ? payload.pending_confirmation : {};
        const createdAt = Number(pending.created_at || 0);
        const nowTs = Number(payload.now || 0);
        if (createdAt > 0 && nowTs > createdAt) return Math.max(0, nowTs - createdAt);
        return 0;
      }
      const lines = logText.split(/\\r?\\n/);
      let waitStart = null;
      let totalPaused = 0;
      let dayOffset = 0;
      let prevSecOfDay = -1;
      for (const rawLine of lines) {
        const line = String(rawLine || '').trim();
        const m = line.match(/^\\[(\\d{2}):(\\d{2}):(\\d{2})\\]\\s+\\[CONFIRM\\]\\s+(.*)$/i);
        if (!m) continue;
        const secOfDay = (Number(m[1]) * 3600) + (Number(m[2]) * 60) + Number(m[3]);
        if (prevSecOfDay >= 0 && secOfDay + 60 < prevSecOfDay) dayOffset += 86400;
        prevSecOfDay = secOfDay;
        const absoluteSec = secOfDay + dayOffset;
        const msg = String(m[4] || '').toLowerCase();
        if (/warte auf freigabe/.test(msg)) {
          waitStart = absoluteSec;
          continue;
        }
        if (waitStart !== null && /freigabe erhalten|start nach analyse abgebrochen/.test(msg)) {
          totalPaused += Math.max(0, absoluteSec - waitStart);
          waitStart = null;
        }
      }
      if (waitStart !== null) {
        const pending = (payload.pending_confirmation && typeof payload.pending_confirmation === 'object') ? payload.pending_confirmation : {};
        const createdAt = Number(pending.created_at || 0);
        const nowTs = Number(payload.now || 0);
        if (createdAt > 0 && nowTs > createdAt) {
          totalPaused += Math.max(0, nowTs - createdAt);
        }
      }
      return Math.max(0, totalPaused);
    }

    function runtimeFromJob(data = null, fallback = '') {
      const payload = (data && typeof data === 'object') ? data : {};
      const job = payload && payload.job && typeof payload.job === 'object' ? payload.job : {};
      const nowTs = Number(payload.now || 0);
      const startTs = Number(job.started_at || 0);
      const endTs = Number(job.ended_at || 0);
      const running = isJobRunningState(job);
      if (startTs > 0) {
        const ref = running ? (nowTs > 0 ? nowTs : (Date.now() / 1000.0)) : (endTs > 0 ? endTs : (nowTs > 0 ? nowTs : (Date.now() / 1000.0)));
        const paused = pausedSecondsFromConfirmLog(payload);
        return formatHhMmSs(Math.max(0, (ref - startTs) - paused));
      }
      const fb = String(fallback || '').trim();
      if (/^\\d{1,2}:\\d{2}:\\d{2}$/.test(fb)) return fb;
      if (/^\\d{1,2}:\\d{2}$/.test(fb)) return `${fb}:00`;
      return '-';
    }

    function formatActiveFileText(activeText = '') {
      const raw = String(activeText || '').trim();
      if (!raw || raw.toLowerCase() === 'n/a') return 'Aktive Datei: -';
      const m = raw.match(/^([0-9]+\\s*\\/\\s*[0-9]+)\\s*(.*)$/);
      if (!m) return `Aktive Datei: ${raw}`;
      const ratio = String(m[1] || '').replace(/\\s+/g, '');
      const name = String(m[2] || '').trim() || '-';
      return `Aktive Datei: #${ratio} ${name}`;
    }

    function parseActiveRowMetrics(statusTable = '', activeKey = '') {
      const parsed = parseStatusTable(statusTable || '');
      const headers = parsed && Array.isArray(parsed.headers) ? parsed.headers : [];
      const rows = parsed && Array.isArray(parsed.rows) ? parsed.rows : [];
      const speedIdx = findStatusColumnIndex(headers, ['speed']);
      const fpsIdx = findStatusColumnIndex(headers, ['fps']);
      const activeNorm = normalizeStatusFraction(activeKey || '');
      let row = null;
      if (activeNorm) {
        row = rows.find((r) => normalizeStatusFraction((r && r.rowKey) || '') === activeNorm) || null;
      }
      const speed = (row && speedIdx >= 0 && row.cells && row.cells[speedIdx]) ? String(row.cells[speedIdx]).trim() : '';
      const fps = (row && fpsIdx >= 0 && row.cells && row.cells[fpsIdx]) ? String(row.cells[fpsIdx]).trim() : '';
      return {
        speed: speed || '-',
        fps: fps || '-',
      };
    }

    function parseSizeGbValue(raw = '') {
      const text = String(raw || '').trim().replace(',', '.');
      const m = text.match(/-?\\d+(?:\\.\\d+)?/);
      if (!m) return null;
      const n = Number(m[0]);
      if (!Number.isFinite(n)) return null;
      return Math.max(0, n);
    }

    function formatSizeGb(value) {
      const n = Number(value);
      if (!Number.isFinite(n)) return '-';
      return `${n.toFixed(1).replace('.', ',')} GB`;
    }

    function parseSpeedMbPerSec(raw = '') {
      const original = String(raw || '').trim();
      if (!/(?:MiB|MB)\\/s\\b/i.test(original)) return null;
      const text = original.replace(',', '.');
      const m = text.match(/([0-9]+(?:\\.[0-9]+)?)\\s*(?:MiB|MB)\\/s/i);
      if (!m) return null;
      const n = Number(m[1]);
      if (!Number.isFinite(n) || n <= 0) return null;
      return n;
    }

    function parseFpsValue(raw = '') {
      const text = String(raw || '').trim().replace(',', '.');
      const m = text.match(/([0-9]+(?:\\.[0-9]+)?)/);
      if (!m) return null;
      const n = Number(m[1]);
      if (!Number.isFinite(n) || n <= 0) return null;
      return n;
    }

    function collectRunningWorkerTotals() {
      const map = (currentWorkerJobMap && typeof currentWorkerJobMap === 'object') ? currentWorkerJobMap : {};
      let activeCount = 0;
      let speedSum = 0;
      let fpsSum = 0;
      let speedSeen = false;
      let fpsSeen = false;
      Object.values(map).forEach((jobs) => {
        (Array.isArray(jobs) ? jobs : []).forEach((job) => {
          if (!job || !job.running) return;
          activeCount += 1;
          const speedVal = parseSpeedMbPerSec(job.speed || '');
          if (speedVal !== null) {
            speedSum += speedVal;
            speedSeen = true;
          }
          const fpsVal = parseFpsValue(job.fps || '');
          if (fpsVal !== null) {
            fpsSum += fpsVal;
            fpsSeen = true;
          }
        });
      });
      return {
        activeCount,
        speedText: speedSeen ? `${speedSum.toFixed(1).replace('.', ',')} MB/s` : '-',
        fpsText: fpsSeen ? String(Math.round(fpsSum)) : '-',
      };
    }

    function parseActiveMeta(activeText = '') {
      const raw = String(activeText || '').trim();
      if (!raw) return { ratio: '', name: '' };
      const m = raw.match(/^#?\\s*([0-9]+\\s*\\/\\s*[0-9]+)\\s*(.*)$/);
      if (!m) return { ratio: '', name: raw };
      return {
        ratio: String(m[1] || '').replace(/\\s+/g, ''),
        name: String(m[2] || '').trim(),
      };
    }

    function collectStatusProgress(statusTable = '', activeKey = '', mode = 'a', running = true, forceAllCompleted = false) {
      const parsed = parseStatusTable(statusTable || '');
      const headers = parsed && Array.isArray(parsed.headers) ? parsed.headers : [];
      const rows = parsed && Array.isArray(parsed.rows) ? parsed.rows : [];
      const totalRows = rows.length;
      const qIdx = findStatusColumnIndex(headers, ['qgb', 'q']);
      const zIdx = findStatusColumnIndex(headers, ['zgb', 'z']);
      const targetIdx = findStatusColumnIndex(headers, ['ziel', 'target']);
      const activeNorm = normalizeStatusFraction(activeKey || '');
      let activePos = 0;
      if (activeNorm) {
        const m = activeNorm.match(/^(\\d+)\\/(\\d+)$/);
        if (m) activePos = Number(m[1] || 0);
      }
      let completedCount = 0;
      if (forceAllCompleted) {
        completedCount = totalRows;
      } else if (activePos > 0) {
        completedCount = Math.max(0, Math.min(totalRows, activePos - 1));
      } else if (!running) {
        completedCount = totalRows;
      } else if (zIdx >= 0) {
        completedCount = rows.filter((row) => parseSizeGbValue((row.cells || [])[zIdx] || '') !== null).length;
      }

      let qDone = 0;
      let qTotal = 0;
      let zDone = 0;
      rows.forEach((row, idx) => {
        const cells = row && Array.isArray(row.cells) ? row.cells : [];
        const qVal = qIdx >= 0 ? parseSizeGbValue(cells[qIdx] || '') : null;
        const zVal = zIdx >= 0 ? parseSizeGbValue(cells[zIdx] || '') : null;
        if (qVal !== null) {
          qTotal += qVal;
          if (idx < completedCount) qDone += qVal;
        }
        if (zVal !== null && idx < completedCount) {
          zDone += zVal;
        }
      });

      let activeRow = null;
      if (activeNorm) {
        activeRow = rows.find((row) => normalizeStatusFraction((row && row.rowKey) || '') === activeNorm) || null;
      }
      if (!activeRow && running && completedCount < totalRows) {
        activeRow = rows[completedCount] || null;
      }
      const activeTarget = activeRow && targetIdx >= 0
        ? String((activeRow.cells || [])[targetIdx] || '').trim()
        : '';
      return {
        totalRows,
        completedCount,
        qDoneGb: qDone,
        qTotalGb: qTotal,
        zDoneGb: zDone,
        activeRatio: activeNorm,
        activeTarget,
      };
    }

    function extractMbSpeedFromLine(line = '') {
      const text = String(line || '');
      const m = text.match(/Speed\\s*[:=]\\s*(?:[0-9]+%\\s*)?([0-9]+(?:[.,][0-9]+)?)\\s*(?:MiB|MB)\\/s/i);
      if (!m) return '';
      const num = String(m[1] || '').replace(',', '.').trim();
      if (!num) return '';
      return `${num} MB/s`;
    }

    function extractSummarySpeedFromProcessingLog(data = null, mode = 'a') {
      const payload = (data && typeof data === 'object') ? data : {};
      const lines = String(payload.processing_log || '').split(/\\r?\\n/);
      let syncNasSpeed = '';
      let copySpeed = '';
      for (const raw of lines) {
        const line = String(raw || '').trim();
        if (!line) continue;
        if (/\\[SYNC-NAS\\]/i.test(line)) {
          const sp = extractMbSpeedFromLine(line);
          if (sp) syncNasSpeed = sp;
          continue;
        }
        if (mode === 'c' && /\\[COPY\\]/i.test(line)) {
          const sp = extractMbSpeedFromLine(line);
          if (sp) copySpeed = sp;
        }
      }
      if (syncNasSpeed) return syncNasSpeed;
      if (mode === 'c' && copySpeed) return copySpeed;
      return '';
    }

    function extractSummaryEtaFromProcessingLog(data = null, mode = 'a') {
      const payload = (data && typeof data === 'object') ? data : {};
      const lines = String(payload.processing_log || '').split(/\\r?\\n/);
      let syncNasEta = '';
      let copyEta = '';
      let ffmpegEta = '';
      for (const raw of lines) {
        const line = String(raw || '').trim();
        if (!line) continue;
        const etaMatch = line.match(/ETA\\s*[:=]\\s*([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?|n\\/a|-)/i);
        if (!etaMatch || !etaMatch[1]) continue;
        const eta = String(etaMatch[1] || '').trim();
        if (/\\[SYNC-NAS\\]/i.test(line)) {
          syncNasEta = eta;
          continue;
        }
        if (mode === 'c' && /\\[COPY\\]/i.test(line)) {
          copyEta = eta;
          continue;
        }
        if (mode === 'f' && /\\[FFMPEG\\]/i.test(line)) {
          ffmpegEta = eta;
        }
      }
      if (syncNasEta) return syncNasEta;
      if (mode === 'c' && copyEta) return copyEta;
      if (mode === 'f' && ffmpegEta) return ffmpegEta;
      return '';
    }

    function extractSummaryFpsFromProcessingLog(data = null, mode = 'a') {
      if (mode !== 'f') return '';
      const payload = (data && typeof data === 'object') ? data : {};
      const lines = String(payload.processing_log || '').split(/\\r?\\n/);
      let fps = '';
      for (const raw of lines) {
        const line = String(raw || '').trim();
        if (!line || !/\\[FFMPEG\\]/i.test(line)) continue;
        const m = line.match(/FPS\\s*[:=]\\s*([0-9]+(?:[.,][0-9]+)?)/i);
        if (m && m[1]) fps = String(m[1]).replace(',', '.').trim();
      }
      return fps;
    }

    function extractTmdbStatusFromProcessingLog(data = null) {
      const payload = (data && typeof data === 'object') ? data : {};
      const lines = String(payload.processing_log || '').split(/\\r?\\n/);
      let checked = 0;
      let total = 0;
      let requests = 0;
      let title = 0;
      let year = 0;
      let cacheHit = 0;
      let cacheWrite = 0;
      let cacheRetention = '';
      let skipped = '';
      for (const raw of lines) {
        const line = String(raw || '').trim();
        if (!line || !/\\[TMDB\\]/i.test(line)) continue;
        const ret = line.match(/Retention\\s*=\\s*([0-9]+)\\s*Tage/i);
        if (ret && ret[1]) cacheRetention = `${ret[1]}d`;
        if (/uebersprungen|übersprungen/i.test(line)) {
          skipped = line.replace(/^.*\\[TMDB\\]\\s*/i, '').trim();
          continue;
        }
        let m = line.match(/geprueft\\s*=\\s*([0-9]+)\\s*\\/\\s*([0-9]+)/i);
        if (!m) m = line.match(/Fortschritt\\s*:\\s*([0-9]+)\\s*\\/\\s*([0-9]+)/i);
        if (m) {
          checked = Number(m[1] || 0);
          total = Number(m[2] || 0);
        } else {
          const s = line.match(/Kandidaten\\s*=\\s*([0-9]+)/i);
          if (s) total = Number(s[1] || 0);
        }
        const req = line.match(/Requests\\s*=\\s*([0-9]+)/i);
        if (req) requests = Number(req[1] || 0);
        const t = line.match(/Titel\\s*=\\s*([0-9]+)/i);
        if (t) title = Number(t[1] || 0);
        const y = line.match(/Jahr\\s*=\\s*([0-9]+)/i);
        if (y) year = Number(y[1] || 0);
        const ch = line.match(/Cache-Hit\\s*=\\s*([0-9]+)/i) || line.match(/\\bHit\\s*=\\s*([0-9]+)/i);
        if (ch) cacheHit = Number(ch[1] || 0);
        const cw = line.match(/Cache-Write\\s*=\\s*([0-9]+)/i) || line.match(/\\bWrite\\s*=\\s*([0-9]+)/i);
        if (cw) cacheWrite = Number(cw[1] || 0);
      }
      if (skipped) return skipped;
      if (checked > 0 || total > 0 || requests > 0) {
        const ratio = total > 0 ? `${checked}/${total}` : `${checked}`;
        const cachePart = (cacheHit > 0 || cacheWrite > 0 || cacheRetention)
          ? ` | Cache ${cacheHit}/${cacheWrite}${cacheRetention ? ` (${cacheRetention})` : ''}`
          : '';
        return `${ratio} | Req ${requests} | Titel ${title} | Jahr ${year}${cachePart}`;
      }
      return '';
    }

    function parseIsoGbFromLine(line = '', label = 'Q-GB') {
      const m = String(line || '').match(new RegExp(`${label}\\s*[:=]\\s*([0-9]+(?:[.,][0-9]+)?)`, 'i'));
      if (!m || !m[1]) return null;
      const n = Number(String(m[1] || '').replace(',', '.'));
      if (!Number.isFinite(n) || n < 0) return null;
      return n;
    }

    function parseIsoProgressFromProcessingLog(data = null) {
      const payload = (data && typeof data === 'object') ? data : {};
      const lines = String(payload.processing_log || '').split(/\\r?\\n/);
      let ratio = '';
      let fileName = '';
      let qGb = null;
      let zGb = null;
      let speed = '';
      let runtime = '';
      let eta = '';
      let lastIsoState = '';
      for (const raw of lines) {
        const line = String(raw || '').trim();
        if (!line || !/\\[ISO\\]/i.test(line)) continue;
        const mExtract = line.match(/\\[ISO\\]\\s*Extrahiere\\s+([0-9]+\\s*\\/\\s*[0-9]+)\\s*:\\s*(.+)$/i);
        if (mExtract) {
          ratio = String(mExtract[1] || '').replace(/\\s+/g, '');
          fileName = String(mExtract[2] || '').trim() || fileName;
          lastIsoState = 'extract';
        }
        const mFinish = line.match(/\\[ISO\\]\\s*Fertig:\\s*(.+?)(?:\\s*\\(|$)/i);
        if (mFinish && mFinish[1]) {
          fileName = String(mFinish[1]).trim() || fileName;
          lastIsoState = 'finish';
        }
        if (/\\[ISO\\].*(Fehler|Keine geeigneten Titel|Unbekannte Struktur)/i.test(line)) {
          lastIsoState = 'done';
        }
        const q = parseIsoGbFromLine(line, 'Q-GB');
        if (q !== null) qGb = q;
        const z = parseIsoGbFromLine(line, 'Z-GB');
        if (z !== null) zGb = z;
        const sp = extractMbSpeedFromLine(line);
        if (sp) speed = sp;
        const rt = line.match(/Laufzeit\\s*[:=]\\s*([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?)/i);
        if (rt && rt[1]) runtime = String(rt[1]).trim();
        const et = line.match(/ETA\\s*[:=]\\s*([0-9]{1,2}:[0-9]{2}(?::[0-9]{2})?|n\\/a|-)/i);
        if (et && et[1]) eta = String(et[1]).trim();
        if (q !== null || z !== null || sp || rt || et) {
          lastIsoState = 'progress';
        }
      }
      const active = lastIsoState === 'extract' || lastIsoState === 'progress';
      return {
        active,
        ratio,
        fileName,
        qGb,
        zGb,
        speed,
        runtime,
        eta,
        hasMetrics: active || qGb !== null || zGb !== null || !!speed || !!runtime || !!eta || !!ratio || !!fileName,
      };
    }

    function parseSavingsParts(raw = '') {
      const text = String(raw || '').trim();
      const gbMatch = text.match(/([0-9]+(?:[.,][0-9]+)?)\\s*GB/i);
      const pctMatch = text.match(/([0-9]+(?:[.,][0-9]+)?)\\s*%/i);
      return {
        gb: gbMatch ? `${String(gbMatch[1] || '').replace('.', ',')} GB` : '-',
        percent: pctMatch ? `${String(pctMatch[1] || '').replace('.', ',')}%` : '',
      };
    }

    function formatSummarySpeedText(rawSpeed = '', mode = 'a') {
      const text = String(rawSpeed || '').trim();
      if (!text) return '-';
      if (mode !== 'c') return text;
      if (/(?:MiB|MB)\\/s\\b/i.test(text)) return text.replace(/MiB\\/s/ig, 'MB/s');
      if (/%/.test(text)) return text;
      const numericOnly = text.match(/^([0-9]+(?:[.,][0-9]+)?)$/);
      if (!numericOnly) return text;
      return `${String(numericOnly[1] || '').replace(',', '.')} MB/s`;
    }

    function detectRunningPostStepLabel(data = null) {
      const payload = (data && typeof data === 'object') ? data : {};
      const job = (payload.job && typeof payload.job === 'object') ? payload.job : {};
      if (!isJobRunningState(job)) return '';
      const key = detectRunningPostOptionKey(payload.processing_log || '');
      if (!key) return '';
      const labels = {
        sync_nas: 'Sync NAS',
        sync_plex: 'Sync Plex',
        del_out: 'Lösche OUT',
        del_source: 'Lösche Quelle',
      };
      return String(labels[key] || '').trim();
    }

    function buildSummaryDetailsText(data = null, statusMeta = '', statusTable = '', activeStatusKey = '') {
      const mode = detectSummaryMode(data);
      const payload = (data && typeof data === "object") ? data : {};
      const job = (payload.job && typeof payload.job === "object") ? payload.job : {};
      const running = isJobRunningState(job);
      if (!running) return '';
      const meta = parseSummaryMetaMap(statusMeta);
      const activeMeta = parseActiveMeta(meta['Aktiv:'] || '');
      const postStepLabel = detectRunningPostStepLabel(data);
      const progress = collectStatusProgress(statusTable, activeStatusKey || activeMeta.ratio, mode, running, !!postStepLabel);
      const workerTotals = collectRunningWorkerTotals();
      const logFps = extractSummaryFpsFromProcessingLog(data, mode);
      const isoProgress = parseIsoProgressFromProcessingLog(data);
      const isoAnalyzeActive = !!isoProgress.active;
      const fpsText = mode === 'f' ? (workerTotals.fpsText !== '-' ? workerTotals.fpsText : (logFps || '-')) : '-';
      const laufz = runtimeFromJob(data, meta['Laufz.:'] || '');
      const parsed = parseStatusTable(statusTable || '');
      const rows = Array.isArray(parsed.rows) ? parsed.rows : [];
      const headers = Array.isArray(parsed.headers) ? parsed.headers : [];
      const qIdx = findStatusColumnIndex(headers, ['qgb', 'q']);
      const zIdx = findStatusColumnIndex(headers, ['zgb', 'z-gb']);
      const eIdx = findStatusColumnIndex(headers, ['egb', 'e-gb']);
      const speedIdx = findStatusColumnIndex(headers, ['speed']);
      const etaIdx = findStatusColumnIndex(headers, ['eta']);
      const fpsIdx = findStatusColumnIndex(headers, ['fps']);
      const lines = [];
      const ratio = isoAnalyzeActive
        ? (isoProgress.ratio || progress.activeRatio || activeMeta.ratio || '-')
        : (progress.activeRatio || activeMeta.ratio || '-');
      if (mode === 'c' || mode === 'f') {
        const totalRows = Math.max(0, Number(progress.totalRows || rows.length || 0));
        const activeRatio = `${String(Math.max(0, workerTotals.activeCount || 0)).padStart(2, '0')}/${String(Math.max(0, totalRows)).padStart(2, '0')}`;
        lines.push(`Aktiv: ${postStepLabel || activeRatio || ratio || '00/00'}`);
      }
      if (isoAnalyzeActive) {
        lines.push(`Speed: ${isoProgress.speed || '-'}`);
      } else if (mode === 'f') {
        lines.push(`FPS: ${fpsText}`);
      }
      const totalRows = Number(progress.totalRows || rows.length || 0);
      const filesQ = `${totalRows}/${totalRows}`;
      const completedRows = rows.filter((row) => !!(row && row.completed));
      const startedRows = rows.filter((row) => {
        if (!row) return false;
        if (row.completed) return true;
        const cells = Array.isArray(row.cells) ? row.cells : [];
        const speedText = speedIdx >= 0 ? String(cells[speedIdx] || '').trim() : '';
        const etaText = etaIdx >= 0 ? String(cells[etaIdx] || '').trim() : '';
        const fpsText = fpsIdx >= 0 ? String(cells[fpsIdx] || '').trim() : '';
        const zVal = zIdx >= 0 ? parseSizeGbValue(cells[zIdx] || '') : null;
        const eVal = eIdx >= 0 ? parseSizeGbValue(cells[eIdx] || '') : null;
        return (
          !isMissingUiMetric(speedText)
          || !isMissingUiMetric(etaText)
          || !isMissingUiMetric(fpsText)
          || (zVal !== null && zVal > 0)
          || (eVal !== null && eVal > 0)
        );
      });
      const filesZ = `${Number(startedRows.length || 0)}/${totalRows}`;
      if (isoAnalyzeActive && isoProgress.qGb !== null) {
        lines.push(`GB Quelle: ${formatSizeGb(isoProgress.qGb)}`);
        if (isoProgress.zGb !== null) lines.push(`GB Ziel: ${formatSizeGb(isoProgress.zGb)}`);
      } else {
        lines.push(`GB Quelle: ${formatSizeGb(progress.qTotalGb)} (${filesQ})`);
        const zTotal = startedRows.reduce((sum, row) => {
          const cells = row && Array.isArray(row.cells) ? row.cells : [];
          const value = zIdx >= 0 ? parseSizeGbValue(cells[zIdx] || '') : null;
          return sum + (value !== null ? value : 0);
        }, 0);
        if (mode === 'c' || mode === 'f') lines.push(`GB Ziel: ${formatSizeGb(zTotal)} (${filesZ})`);
      }
      if (mode === 'f') {
        const qStarted = startedRows.reduce((sum, row) => {
          const cells = row && Array.isArray(row.cells) ? row.cells : [];
          const value = qIdx >= 0 ? parseSizeGbValue(cells[qIdx] || '') : null;
          return sum + (value !== null ? value : 0);
        }, 0);
        const projectedZ = startedRows.reduce((sum, row) => {
          const cells = row && Array.isArray(row.cells) ? row.cells : [];
          const qVal = qIdx >= 0 ? parseSizeGbValue(cells[qIdx] || '') : null;
          const zVal = zIdx >= 0 ? parseSizeGbValue(cells[zIdx] || '') : null;
          const eVal = eIdx >= 0 ? parseSizeGbValue(cells[eIdx] || '') : null;
          if (row.completed) return sum + (zVal !== null ? zVal : 0);
          if (eVal !== null && eVal > 0) return sum + eVal;
          if (zVal !== null && zVal > 0) return sum + zVal;
          if (qVal !== null && qVal > 0) return sum + qVal;
          return sum;
        }, 0);
        const savedGbNum = Math.max(0, qStarted - projectedZ);
        const pctText = qStarted > 0 ? `${Math.round((savedGbNum / qStarted) * 100)}%` : '';
        lines.push(`Ersparnis: ${formatSizeGb(savedGbNum)}${pctText ? ` | ${pctText}` : ''}`);
      }
      lines.push(`Laufzeit: ${isoAnalyzeActive && isoProgress.runtime ? isoProgress.runtime : laufz}`);
      let etaText = String(meta['ETA:'] || '').trim() || '-';
      const totalEtaSeconds = estimateDatasetEtaSeconds(rows);
      if (totalEtaSeconds > 0) etaText = formatHhMmSs(totalEtaSeconds);
      if (isoAnalyzeActive) {
        etaText = isoProgress.eta || etaText;
      }
      if (!etaText) etaText = '-';
      if (mode === 'c' || mode === 'f' || isoAnalyzeActive) lines.push(`ETA: ${etaText}`);
      return lines.join('\\n');
    }

    function summaryPairsFromText(text = '') {
      const seen = new Set();
      const pairs = [];
      const keepPlaceholder = new Set([
        'aktiv',
        'datei',
        'speed',
        'fps',
        'gbquelle',
        'gbziel',
        'ersparnis',
        'laufzeit',
        'eta',
      ]);
      String(text || '')
        .split(/\\r?\\n|\\s*\\|\\s*/)
        .map((line) => String(line || '').trim())
        .filter((line) => !!line)
        .forEach((line) => {
          const m = line.match(/^([^:]+):\\s*(.*)$/);
          let key = '';
          let value = '';
          if (m) {
            key = String(m[1] || '').trim();
            value = String(m[2] || '').trim();
          } else {
            key = String(line || '').trim();
          }
          const keyLower = key.toLowerCase();
          const normalizedKey = normHeaderKey(key);
          const lower = value.toLowerCase();
          if (!key) return;
          if (value && (lower === '-' || lower === 'n/a' || lower === 'na' || lower === '...') && !keepPlaceholder.has(normalizedKey)) return;
          if ((keyLower === 'aktive datei' || keyLower === 'aktiv') && /^0+\\s*\\/\\s*0+\\b/.test(value)) return;
          const dedupeKey = `${keyLower}|${value}`;
          if (seen.has(dedupeKey)) return;
          seen.add(dedupeKey);
          pairs.push({ key, value });
        });
      return pairs;
    }

    function summaryPairsForInline(metaText = '', detailText = '') {
      const detailPairs = summaryPairsFromText(detailText);
      if (detailPairs.length > 0) return detailPairs;
      return summaryPairsFromText(metaText);
    }

    function buildSummaryTableFromPairs(pairs = []) {
      const table = document.createElement('table');
      table.className = 'summary-kv-table';
      const body = document.createElement('tbody');
      (Array.isArray(pairs) ? pairs : []).forEach((pair) => {
        const tr = document.createElement('tr');
        const th = document.createElement('th');
        const keyText = String((pair && pair.key) || '').replace(/:$/, '');
        th.innerText = keyText;
        const td = document.createElement('td');
        td.innerText = String((pair && pair.value) || '').trim();
        if (['datei', 'aktivedatei', 'aktiv'].includes(normHeaderKey(keyText))) {
          td.classList.add('summary-file-cell');
        }
        tr.appendChild(th);
        tr.appendChild(td);
        body.appendChild(tr);
      });
      table.appendChild(body);
      return table;
    }

    function renderSummaryInlineBox(metaText = '', detailText = '') {
      const box = document.getElementById('statusSummaryBox');
      if (!box) return;
      const pairs = summaryPairsForInline(metaText, detailText);
      box.classList.toggle('is-empty', pairs.length === 0);
      box.innerHTML = '';
      if (pairs.length === 0) return;
      const table = buildSummaryTableFromPairs(pairs);
      box.appendChild(table);
    }

    function setButtonTip(btn, hintText) {
      if (!btn) return;
      const hint = String(hintText || '').trim();
      if (hint) {
        btn.setAttribute('data-tip', hint);
        btn.setAttribute('aria-label', hint);
      } else {
        btn.removeAttribute('data-tip');
      }
      btn.removeAttribute('title');
    }

    function wireButtonTips() {
      document.querySelectorAll('button[title]').forEach((btn) => {
        const hint = String(btn.getAttribute('title') || '').trim();
        if (!hint) return;
        btn.setAttribute('data-tip', hint);
        btn.removeAttribute('title');
      });
    }

    function applyPostOptionUI() {
      const enabled = canUsePostOptions(selectedMode);
      const syncBtn = document.getElementById('syncNasBtn');
      const plexBtn = document.getElementById('syncPlexBtn');
      const outBtn = document.getElementById('delOutBtn');
      const sourceBtn = document.getElementById('delSourceBtn');
      const controls = [
        [syncBtn, 'sync_nas'],
        [sourceBtn, 'del_source'],
      ];

      if (!enabled) {
        postOptions.sync_nas = false;
        postOptions.sync_plex = false;
        postOptions.del_out = false;
        postOptions.del_source = false;
      }
      if (!canUseSyncPlexOption()) {
        postOptions.sync_plex = false;
      }
      if (!canUseDelOutOption()) {
        postOptions.del_out = false;
      }

      controls.forEach(([btn, key]) => {
        if (!btn) return;
        const active = enabled && !!postOptions[key];
        btn.classList.toggle('active', active);
        btn.classList.toggle('disabled', !enabled);
      });

      if (plexBtn) {
        const plexEnabled = canUseSyncPlexOption();
        plexBtn.classList.toggle('active', plexEnabled && !!postOptions.sync_plex);
        plexBtn.classList.toggle('disabled', !plexEnabled);
      }
      if (outBtn) {
        const delOutEnabled = canUseDelOutOption();
        outBtn.classList.toggle('active', delOutEnabled && !!postOptions.del_out);
        outBtn.classList.toggle('disabled', !delOutEnabled);
      }

      const syncInput = document.getElementById('syncNasInput');
      const syncPlexInput = document.getElementById('syncPlexInput');
      const delOutInput = document.getElementById('delOutInput');
      const delSourceInput = document.getElementById('delSourceInput');
      if (syncInput) syncInput.value = postOptions.sync_nas ? '1' : '0';
      if (syncPlexInput) syncPlexInput.value = postOptions.sync_plex ? '1' : '0';
      if (delOutInput) delOutInput.value = postOptions.del_out ? '1' : '0';
      if (delSourceInput) delSourceInput.value = postOptions.del_source ? '1' : '0';
    }

    function setModeControls(mode, persist = true) {
      const normalized = String(mode || '').trim().toLowerCase();
      if (normalized !== 'analyze' && normalized !== 'copy' && normalized !== 'ffmpeg') return;
      if (normalized === 'ffmpeg' && !lastJobRunning && !hasAvailableEncodeWorker()) return;
      selectedMode = normalized;

      const hidden = document.getElementById('mode');
      if (hidden) hidden.value = normalized;

      const modeButtons = [
        ['modeAnalyzeBtn', 'analyze'],
        ['modeCopyBtn', 'copy'],
        ['modeEncodeBtn', 'ffmpeg'],
      ];
      modeButtons.forEach(([id, value]) => {
        const btn = document.getElementById(id);
        if (!btn) return;
        const encodeBlocked = value === 'ffmpeg' && !lastJobRunning && !hasAvailableEncodeWorker();
        const active = value === normalized;
        btn.classList.toggle('mode-active', active && !encodeBlocked);
        btn.classList.toggle('mode-inactive', !active || encodeBlocked);
        btn.classList.toggle('disabled', encodeBlocked);
        btn.disabled = encodeBlocked;
        btn.title = encodeBlocked ? 'Kein Worker verfügbar' : '';
        btn.setAttribute('aria-pressed', active && !encodeBlocked ? 'true' : 'false');
      });

      const delSourceConfirmed = document.getElementById('delSourceConfirmedInput');
      if (delSourceConfirmed) delSourceConfirmed.value = '0';
      applyPostOptionUI();
      applyInitialSetupUi(lastJobRunning);
      renderSummaryAmpel();
      if (persist) {
        persistModeSelection(normalized);
      }
    }

    function togglePostOption(name) {
      if (!canUsePostOptions(selectedMode)) return;
      if (!(name in postOptions)) return;
      if (name === 'sync_plex' && !canUseSyncPlexOption()) return;
      if (name === 'del_out' && !canUseDelOutOption()) return;
      postOptions[name] = !postOptions[name];
      if (name === 'sync_nas' && !postOptions.sync_nas) {
        postOptions.del_out = false;
      }
      const delSourceConfirmed = document.getElementById('delSourceConfirmedInput');
      if (delSourceConfirmed) delSourceConfirmed.value = '0';
      applyPostOptionUI();
      renderSummaryAmpel();
    }

    function wireStartConfirm() {
      const form = document.getElementById('startForm');
      if (!form) return;
      form.addEventListener('submit', async (event) => {
        if (isInitialSetupLocked()) {
          event.preventDefault();
          setSettingsStatus('Erststart: Zuerst Einstellungen und API-Keys speichern.');
          openSettingsModal();
          return;
        }
        const folderInput = document.getElementById('folder');
        const folderValue = String((folderInput && folderInput.value) ? folderInput.value : '').trim();
        if (folderValue) lastKnownJobFolder = folderValue;
        if (bypassStartConfirmOnce) {
          bypassStartConfirmOnce = false;
          return;
        }
        const delSourceConfirmed = document.getElementById('delSourceConfirmedInput');
        if (delSourceConfirmed) delSourceConfirmed.value = '0';
        const mode = String(selectedMode || '').trim().toLowerCase();
        if (!canUsePostOptions(mode)) return;
        const delSourceEnabled = !!postOptions.del_source;
        if (!delSourceEnabled) return;
        event.preventDefault();
        const ok = await askBrowserConfirm(
          'Del Quelle ist aktiv. Nach erfolgreichem Lauf werden Quelldaten gelöscht, wenn fuer JEDE Quelldatei ein Ziel in __OUT oder NAS gefunden wird. Fortfahren?'
        );
        if (!ok) {
          return;
        }
        if (delSourceConfirmed) delSourceConfirmed.value = '1';
        bypassStartConfirmOnce = true;
        form.submit();
      });
    }

    function wirePreInteractions() {
      const ids = [
        'jobBox',
        'summaryBox',
        'statusBox',
        'procBox',
        'planBox',
        'logModalPre',
        'statusTableWrap',
        'logModalStatusWrap',
        'statusSummaryBox',
      ];
      ids.forEach((id) => {
        const el = document.getElementById(id);
        if (!el) return;
        ['mousedown', 'mouseup', 'wheel', 'scroll', 'touchstart', 'keydown'].forEach((ev) => {
          el.addEventListener(ev, () => lockPre(id, 5000), { passive: true });
        });
      });
    }


    function updateCollapseButton(btn, collapsed) {
      if (!btn) return;
      btn.innerText = collapsed ? '↗' : '↙';
      setButtonTip(btn, collapsed ? 'Aufklappen' : 'Einklappen');
    }

    function toggleCardCollapse(cardId, triggerBtn) {
      const card = document.getElementById(cardId);
      if (!card) return;
      const collapsed = !card.classList.contains('collapsed');
      card.classList.toggle('collapsed', collapsed);
      const buttons = card.querySelectorAll('.collapse-toggle-btn');
      buttons.forEach((btn) => updateCollapseButton(btn, collapsed));
      if (triggerBtn) updateCollapseButton(triggerBtn, collapsed);
    }

    function wireCardCollapseButtons() {
      document.querySelectorAll('.collapsible-card').forEach((card) => {
        const collapsed = card.classList.contains('collapsed');
        card.querySelectorAll('.collapse-toggle-btn').forEach((btn) => updateCollapseButton(btn, collapsed));
      });
    }

    function setCardCollapsed(cardId, collapsed) {
      const card = document.getElementById(cardId);
      if (!card) return;
      const next = !!collapsed;
      card.classList.toggle('collapsed', next);
      card.querySelectorAll('.collapse-toggle-btn').forEach((btn) => updateCollapseButton(btn, next));
    }

    function collapseToHomeLayout() {
      closeLogModal();
      closeSettingsModal();
      setSummaryDetailsVisible(false);
      applyIdlePanelLayout(true);
    }

    function applyIdlePanelLayout(force = false) {
      if (!force && lastJobRunning) return;
      setCardCollapsed('statusCard', true);
      setCardCollapsed('procCard', true);
      setCardCollapsed('planCard', true);
    }

    function setSummaryDetailsVisible(visible) {
      const card = document.getElementById('summaryCard');
      if (!card) return;
      card.classList.toggle('summary-right-hidden', !visible);
    }

    function mapSourceToWindowKey(sourceId) {
      const key = String(sourceId || '').trim();
      if (key === 'jobBox') return 'job';
      if (key === 'summaryBox') return 'summary';
      if (key === 'statusBox') return 'status';
      if (key === 'procBox') return 'proc';
      if (key === 'planBox') return 'plan';
      return '';
    }

    function popupFeaturesForSource(source) {
      const defaults = {
        job: [900, 600],
        summary: [1280, 920],
        status: [1480, 1020],
        proc: [1420, 980],
        plan: [1320, 940],
      };
      const pair = defaults[source] || [1080, 760];
      const availW = Math.max(720, Number(window.screen && window.screen.availWidth) || 1366);
      const availH = Math.max(560, Number(window.screen && window.screen.availHeight) || 900);
      const width = Math.max(720, Math.min(pair[0], availW - 80));
      const height = Math.max(560, Math.min(pair[1], availH - 90));
      return `noopener,noreferrer,width=${Math.round(width)},height=${Math.round(height)}`;
    }

    function collapseSourceFromPanel(sourceId) {
      const sourceMap = {
        jobBox: 'jobCard',
        summaryBox: 'summaryCard',
        statusBox: 'statusCard',
        procBox: 'procCard',
        planBox: 'planCard',
      };
      const cardId = sourceMap[String(sourceId || '').trim()];
      if (cardId) setCardCollapsed(cardId, true);
      if (modalSourceId && modalSourceId === sourceId) closeLogModal();
    }

    function openLogWindow(title, sourceId) {
      const source = mapSourceToWindowKey(sourceId);
      if (!source) return;
      const safeTitle = String(title || 'Log').trim() || 'Log';
      const params = new URLSearchParams();
      params.set('source', source);
      params.set('title', safeTitle);
      params.set('ts', String(Date.now()));
      if (source === 'status' && pendingConfirmData && pendingConfirmData.token) {
        params.set('token', String(pendingConfirmData.token));
      }
      const url = `/log-window?${params.toString()}`;
      const win = window.open(url, '_blank', popupFeaturesForSource(source));
      if (win && typeof win.focus === 'function') {
        win.focus();
        collapseSourceFromPanel(sourceId);
      }
    }

    function openCurrentModalInWindow() {
      if (!modalSourceId) return;
      const sourceId = modalSourceId;
      const title = modalLogTitle || 'Log';
      openLogWindow(title, sourceId);
      collapseSourceFromPanel(sourceId);
      closeLogModal();
    }

    function syncLogModal() {
      if (!modalSourceId) return;
      const dst = document.getElementById('logModalPre');
      const modalStatusWrap = document.getElementById('logModalStatusWrap');
      if (!dst || !modalStatusWrap) return;

      if (modalSourceId === 'statusBox') {
        const srcTable = document.getElementById('statusTable');
        if (!srcTable) return;
        modalStatusWrap.classList.remove('summary-wrap');
        dst.classList.add('hidden');
        modalStatusWrap.classList.remove('hidden');
        const clone = srcTable.cloneNode(true);
        clone.id = 'statusTableModal';
        clone.querySelectorAll('[id]').forEach((el) => el.removeAttribute('id'));
        modalStatusWrap.innerHTML = '';
        modalStatusWrap.appendChild(clone);
        const activeModalRow = modalStatusWrap.querySelector('tr.status-row-active');
        autoScrollStatusWrap(modalStatusWrap, activeModalRow, true);
        return;
      }

      if (modalSourceId === 'summaryBox') {
        dst.classList.add('hidden');
        modalStatusWrap.classList.remove('hidden');
        modalStatusWrap.classList.add('summary-wrap');
        modalStatusWrap.innerHTML = '';

        const srcSummary = document.getElementById('statusSummaryBox');
        let pairs = [];
        if (srcSummary && srcSummary.querySelector('table.summary-kv-table')) {
          const clone = srcSummary.cloneNode(true);
          clone.removeAttribute('id');
          clone.querySelectorAll('[id]').forEach((el) => el.removeAttribute('id'));
          modalStatusWrap.appendChild(clone);
          return;
        }

        const src = document.getElementById(modalSourceId);
        pairs = summaryPairsFromText(src ? (src.innerText || '') : '');
        const fallbackBox = document.createElement('div');
        fallbackBox.className = 'status-meta';
        if (pairs.length > 0) {
          fallbackBox.appendChild(buildSummaryTableFromPairs(pairs));
        } else {
          fallbackBox.innerText = 'Keine laufenden Summary-Daten';
        }
        modalStatusWrap.appendChild(fallbackBox);
        return;
      }

      modalStatusWrap.classList.add('hidden');
      modalStatusWrap.classList.remove('summary-wrap');
      modalStatusWrap.innerHTML = '';
      dst.classList.remove('hidden');

      const src = document.getElementById(modalSourceId);
      if (!src) return;
      if (isPreLocked('logModalPre') || isSelectionInside(dst)) return;
      const nextText = src.textContent || '';
      const atBottom = (dst.scrollHeight - dst.scrollTop - dst.clientHeight) < 10;
      if ((dst.textContent || '') !== nextText) {
        const prevTop = dst.scrollTop;
        dst.textContent = nextText;
        dst.scrollTop = atBottom ? dst.scrollHeight : prevTop;
      }
    }

    function openLogModal(title, sourceId) {
      const modal = document.getElementById('logModal');
      const titleEl = document.getElementById('logModalTitle');
      const modalPre = document.getElementById('logModalPre');
      const modalStatusWrap = document.getElementById('logModalStatusWrap');
      const modalSummaryAmpel = document.getElementById('logModalSummaryAmpel');
      const modalFilterBtn = document.getElementById('statusFilterModalBtn');
      const modalFilterInfo = document.getElementById('statusErrorModalInfo');
      const modalCloseBtn = document.getElementById('logModalCloseBtn');
      const modalPanel = modal ? modal.querySelector('.log-modal-panel') : null;
      modalSourceId = sourceId || '';
      modalLogTitle = title || 'Log';
      if (titleEl) titleEl.innerText = `${SITE_TITLE} ${modalVersion || '-'} | ${title || 'Log'}`;
      if (modalPre) {
        modalPre.classList.toggle('nowrap', modalSourceId === 'jobBox');
        modalPre.classList.toggle('hidden', modalSourceId === 'statusBox' || modalSourceId === 'summaryBox');
      }
      if (modalStatusWrap) {
        modalStatusWrap.classList.toggle('hidden', modalSourceId !== 'statusBox' && modalSourceId !== 'summaryBox');
      }
      if (modalSummaryAmpel) {
        modalSummaryAmpel.classList.toggle('hidden', modalSourceId !== 'summaryBox');
      }
      if (modalFilterBtn) {
        modalFilterBtn.classList.toggle('hidden', modalSourceId !== 'statusBox');
      }
      if (modalFilterInfo) {
        modalFilterInfo.classList.toggle('hidden', modalSourceId !== 'statusBox');
      }
      if (modalCloseBtn) {
        modalCloseBtn.classList.remove('status-exit-btn');
        modalCloseBtn.title = 'Einklappen';
        modalCloseBtn.setAttribute('aria-label', 'Einklappen');
        modalCloseBtn.innerText = '↙';
      }
      if (modalPanel) {
        modalPanel.classList.toggle('status-wide', modalSourceId === 'statusBox');
        modalPanel.classList.toggle('summary-view', modalSourceId === 'summaryBox');
      }
      if (modal) modal.classList.remove('hidden');
      updateStatusWaitingTitles();
      updateStatusFilterButton();
      syncLogModal();
      renderPendingConfirmation();
    }

    function closeLogModal() {
      modalSourceId = '';
      modalLogTitle = 'Log';
      const modal = document.getElementById('logModal');
      const modalPre = document.getElementById('logModalPre');
      const modalStatusWrap = document.getElementById('logModalStatusWrap');
      const modalSummaryAmpel = document.getElementById('logModalSummaryAmpel');
      const modalFilterBtn = document.getElementById('statusFilterModalBtn');
      const modalFilterInfo = document.getElementById('statusErrorModalInfo');
      const modalCloseBtn = document.getElementById('logModalCloseBtn');
      const modalPanel = modal ? modal.querySelector('.log-modal-panel') : null;
      if (modalPre) {
        modalPre.classList.remove('nowrap');
        modalPre.classList.remove('hidden');
      }
      if (modalStatusWrap) {
        modalStatusWrap.classList.add('hidden');
        modalStatusWrap.classList.remove('summary-wrap');
        modalStatusWrap.innerHTML = '';
      }
      if (modalSummaryAmpel) {
        modalSummaryAmpel.classList.add('hidden');
      }
      if (modalFilterBtn) {
        modalFilterBtn.classList.add('hidden');
      }
      if (modalFilterInfo) {
        modalFilterInfo.classList.add('hidden');
      }
      if (modalCloseBtn) {
        modalCloseBtn.classList.remove('status-exit-btn');
        modalCloseBtn.title = 'Einklappen';
        modalCloseBtn.setAttribute('aria-label', 'Einklappen');
        modalCloseBtn.innerText = '↙';
      }
      if (modalPanel) {
        modalPanel.classList.remove('status-wide');
        modalPanel.classList.remove('summary-view');
      }
      if (modal) modal.classList.add('hidden');
      updateStatusWaitingTitles();
      renderPendingConfirmation();
    }

    function openSettingsModal() {
      const modal = document.getElementById('settingsModal');
      if (modal) modal.classList.remove('hidden');
      loadSettingsFromApi();
    }

    function closeSettingsModal() {
      const modal = document.getElementById('settingsModal');
      if (modal) modal.classList.add('hidden');
    }

    function trimTrailingSlashes(path) {
      const text = String(path || '').trim().replace(/\\\\/g, '/');
      if (text === '/') return '/';
      return text.replace(/\\/+$/, '');
    }

    function currentStartFolderForUi() {
      const input = document.getElementById('folder');
      return trimTrailingSlashes(input ? String(input.value || '').trim() : '');
    }

    function getDefaultTargetOutDisplayPath() {
      const base = currentStartFolderForUi();
      if (!base) return '__OUT';
      return `${base}/__OUT`;
    }

    function defaultTargetReenqueueStoredFromOutStored(outStored) {
      const raw = String(outStored || '').trim() || '__OUT';
      if (raw === '__OUT') return '__RE-ENQUEUE';
      const normalized = trimTrailingSlashes(raw).replace(/^\\.?\\//, '');
      if (!normalized) return '__RE-ENQUEUE';
      if (raw.startsWith('/')) {
        const parent = raw.replace(/\\/[^/]*$/, '') || '/';
        return `${parent.replace(/\\/+$/, '')}/__RE-ENQUEUE`;
      }
      const parts = normalized.split('/').filter((p) => !!p);
      if (!parts.length) return '__RE-ENQUEUE';
      parts.pop();
      return parts.length ? `${parts.join('/')}/__RE-ENQUEUE` : '__RE-ENQUEUE';
    }

    function getDefaultTargetReenqueueDisplayPath() {
      const targetOut = document.getElementById('targetOutSetting');
      const outStored = targetOut ? targetOutDisplayToStored(targetOut.value || '') : '__OUT';
      const reenqueueStored = defaultTargetReenqueueStoredFromOutStored(outStored);
      const base = currentStartFolderForUi();
      if (reenqueueStored.startsWith('/')) return reenqueueStored;
      if (!base) return reenqueueStored;
      return `${base}/${reenqueueStored.replace(/^\\.?\\//, '')}`;
    }

    function normalizePathForCompare(path) {
      return trimTrailingSlashes(path).toLowerCase();
    }

    function targetOutStoredToDisplay(value) {
      const raw = String(value || '').trim();
      if (!raw || raw === '__OUT') {
        return getDefaultTargetOutDisplayPath();
      }
      if (raw.startsWith('/')) return raw;
      const base = currentStartFolderForUi();
      if (!base) return raw;
      const rel = raw.replace(/^\\.?\\//, '');
      return `${base}/${rel}`;
    }

    function targetOutDisplayToStored(value) {
      const raw = String(value || '').trim();
      if (!raw) return '__OUT';
      const defaultDisplay = getDefaultTargetOutDisplayPath();
      if (normalizePathForCompare(raw) === normalizePathForCompare(defaultDisplay)) {
        return '__OUT';
      }
      return raw;
    }

    function targetReenqueueStoredToDisplay(value, outStoredValue = '__OUT') {
      const raw = String(value || '').trim();
      const defaultStored = defaultTargetReenqueueStoredFromOutStored(outStoredValue);
      if (!raw || normalizePathForCompare(raw) === normalizePathForCompare(defaultStored)) {
        return getDefaultTargetReenqueueDisplayPath();
      }
      if (raw.startsWith('/')) return raw;
      const base = currentStartFolderForUi();
      if (!base) return raw;
      const rel = raw.replace(/^\\.?\\//, '');
      return `${base}/${rel}`;
    }

    function targetReenqueueDisplayToStored(value, outStoredValue = '__OUT') {
      const raw = String(value || '').trim();
      const defaultDisplay = getDefaultTargetReenqueueDisplayPath();
      if (!raw || normalizePathForCompare(raw) === normalizePathForCompare(defaultDisplay)) {
        return defaultTargetReenqueueStoredFromOutStored(outStoredValue);
      }
      return raw;
    }

    function setTargetOutDefault() {
      const input = document.getElementById('targetOutSetting');
      if (!input) return;
      input.value = getDefaultTargetOutDisplayPath();
      setSettingsStatus('Ziel __OUT auf Standard gesetzt. Bitte speichern.');
    }

    function setTargetReenqueueDefault() {
      const input = document.getElementById('targetReenqueueSetting');
      if (!input) return;
      input.value = getDefaultTargetReenqueueDisplayPath();
      setSettingsStatus('Ziel __RE-ENQUEUE auf Standard gesetzt. Bitte speichern.');
    }

    function openTargetNfsBrowse() {
      const input = document.getElementById('targetNfsSetting');
      const folder = input ? String(input.value || '').trim() : '';
      const params = new URLSearchParams();
      if (folder) params.set('folder', folder);
      params.set('target', 'settings_target_nfs');
      window.location.href = `/browse?${params.toString()}`;
    }

    function openTargetOutBrowse() {
      const input = document.getElementById('targetOutSetting');
      const folder = input ? String(input.value || '').trim() : '';
      const params = new URLSearchParams();
      if (folder) params.set('folder', folder);
      params.set('target', 'settings_target_out');
      window.location.href = `/browse?${params.toString()}`;
    }

    function openTargetReenqueueBrowse() {
      const input = document.getElementById('targetReenqueueSetting');
      const folder = input ? String(input.value || '').trim() : '';
      const params = new URLSearchParams();
      if (folder) params.set('folder', folder);
      params.set('target', 'settings_target_reenqueue');
      window.location.href = `/browse?${params.toString()}`;
    }

    async function persistEncoderSelection(encoder) {
      const normalized = String(encoder || '').trim();
      if (!normalized) return false;
      try {
        const res = await fetch('/settings/encoder', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ encoder: normalized }),
        });
        if (!res.ok) return false;
        try {
          window.localStorage.setItem('managemovie.encoder', normalized);
        } catch (err) {
        }
        return true;
      } catch (err) {
      }
      return false;
    }

    function setSettingsStatus(message, isError = false) {
      const statusEl = document.getElementById('settingsStatus');
      if (!statusEl) return;
      statusEl.innerText = normalizeDisplayUmlauts(String(message || '').trim());
      const isDark = normalizeThemeMode(document.documentElement.getAttribute('data-theme')) === 'dark';
      statusEl.style.color = isError
        ? (isDark ? '#ffb4ad' : '#7a1b17')
        : (isDark ? '#c8f1df' : '#163228');
    }

    function normalizeCacheDbCount(value) {
      const n = Number(value);
      if (!Number.isFinite(n) || n < 0) return 0;
      return Math.floor(n);
    }

    function renderCacheDbSummary(cacheDb) {
      const data = cacheDb && typeof cacheDb === 'object' ? cacheDb : {};
      const totalEl = document.getElementById('cacheDbCount');
      const resetBtn = document.getElementById('cacheDbResetBtn');
      const sourceFileCount = normalizeCacheDbCount(data.source_file_cache_rows);
      const error = String(data.error || '').trim();

      if (totalEl) {
        totalEl.innerText = `${sourceFileCount} Quelldateien im Cache`;
      }
      if (resetBtn) {
        resetBtn.disabled = !!error;
      }
    }

    async function resetCacheDbFromSettings() {
      const ok = await askBrowserConfirm(
        'Cache Reset wirklich ausführen? Alle Cache-Daten werden gelöscht (settings.* bleiben erhalten).',
        'CACHE RESET'
      );
      if (!ok) return;
      const resetBtn = document.getElementById('cacheDbResetBtn');
      if (resetBtn) resetBtn.disabled = true;
      let keepDisabled = false;
      try {
        setSettingsStatus('Cache DB wird zurückgesetzt...');
        const res = await fetch('/api/settings/cache/reset', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ reset: true }),
        });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || !data.ok) {
          throw new Error((data && data.error) ? data.error : `HTTP ${res.status}`);
        }
        const cacheDb = (data && data.cache_db) || {};
        renderCacheDbSummary(cacheDb);
        keepDisabled = !!(cacheDb && cacheDb.error);
        const clearedTotal = normalizeCacheDbCount(((data && data.cleared) || {}).total_cache_rows);
        setSettingsStatus(`Cache DB reset erfolgreich (${clearedTotal} Cache-Einträge gelöscht, settings unverändert).`);
      } catch (err) {
        setSettingsStatus(`Cache DB reset fehlgeschlagen: ${err}`, true);
      } finally {
        if (resetBtn) resetBtn.disabled = keepDisabled;
      }
    }

    function parseBoolSetting(value) {
      if (value === true) return true;
      const text = String(value == null ? '' : value).trim().toLowerCase();
      return text === '1' || text === 'true' || text === 'yes' || text === 'y' || text === 'on';
    }

    function applySettingsValues(settings) {
      const data = settings && typeof settings === 'object' ? settings : {};
      const targetNfs = document.getElementById('targetNfsSetting');
      const targetOut = document.getElementById('targetOutSetting');
      const targetReenqueue = document.getElementById('targetReenqueueSetting');
      const nasIp = document.getElementById('nasIpSetting');
      const plexIp = document.getElementById('plexIpSetting');
      const plexApi = document.getElementById('plexApiSetting');
      const tmdbApi = document.getElementById('tmdbApiSetting');
      const geminiApi = document.getElementById('geminiApiSetting');
      const aiQueryDisabled = document.getElementById('aiQueryDisabledSetting');
      const startOnBoot = document.getElementById('startOnBootSetting');
      const skip4kH265Encode = document.getElementById('skip4kH265EncodeSetting');
      const encodeOnce4k = document.getElementById('encodeOnce4kSetting');
      const oneEncodePerWorker = document.getElementById('oneEncodePerWorkerSetting');
      const allowWorker4 = document.getElementById('allowWorker4Setting');
      const parallelJobLimit = document.getElementById('parallelJobLimitSetting');
      const precheckEgb = document.getElementById('precheckEgbSetting');
      const speedFallbackCopy = document.getElementById('speedFallbackCopySetting');

      if (targetNfs) targetNfs.value = String(data.target_nfs_path || targetNfs.value || '').trim();
      const outStored = String(data.target_out_path || '').trim() || '__OUT';
      if (targetOut) targetOut.value = targetOutStoredToDisplay(outStored);
      if (targetReenqueue) targetReenqueue.value = targetReenqueueStoredToDisplay(String(data.target_reenqueue_path || '').trim(), outStored);
      if (nasIp) nasIp.value = String(data.nas_ip || nasIp.value || '').trim();
      if (plexIp) plexIp.value = String(data.plex_ip || plexIp.value || '').trim();
      if (plexApi) {
        plexApi.value = '';
        plexApi.placeholder = data.has_plex_api ? 'Gesetzt (neu eingeben zum Ändern)' : 'Plex API Token';
      }
      if (tmdbApi) {
        tmdbApi.value = '';
        tmdbApi.placeholder = data.has_tmdb_api ? 'Gesetzt (neu eingeben zum Ändern)' : 'TMDB API Key';
      }
      if (geminiApi) {
        geminiApi.value = '';
        geminiApi.placeholder = data.has_gemini_api ? 'Gesetzt (neu eingeben zum Ändern)' : 'Gemini API Key';
      }
      if (aiQueryDisabled) {
        aiQueryDisabled.checked = parseBoolSetting(data.ai_query_disabled);
      }
      if (startOnBoot) {
        startOnBoot.checked = parseBoolSetting(data.start_on_boot);
      }
      if (skip4kH265Encode) {
        skip4kH265Encode.checked = parseBoolSetting(data.skip_4k_h265_encode);
      }
      if (encodeOnce4k) {
        encodeOnce4k.checked = parseBoolSetting(data.encode_once_4k);
      }
      if (oneEncodePerWorker) {
        oneEncodePerWorker.checked = parseBoolSetting(data.one_encode_per_worker);
      }
      if (allowWorker4) {
        allowWorker4.checked = parseBoolSetting(data.allow_worker_4);
      }
      if (parallelJobLimit) {
        parallelJobLimit.value = String(data.parallel_job_limit || '2').trim() || '2';
      }
      if (precheckEgb) {
        precheckEgb.checked = parseBoolSetting(data.precheck_egb);
      }
      if (speedFallbackCopy) {
        speedFallbackCopy.checked = parseBoolSetting(data.speed_fallback_copy);
      }
      initialSetupRequired = parseBoolSetting(data.initial_setup_required);
      initialSetupDone = !initialSetupRequired || parseBoolSetting(data.initial_setup_done);

      const encoder = String(data.encoder || '').trim();
      if (encoder) {
        setEncoderControls(encoder);
      }
      const mode = String(data.mode || '').trim().toLowerCase();
      if (mode) {
        setModeControls(mode, false);
      }

      if (targetNfs && pendingTargetNfsSelection) {
        targetNfs.value = String(pendingTargetNfsSelection).trim();
        pendingTargetNfsSelection = '';
        setSettingsStatus('Zielpfad aus Auswahl übernommen. Bitte speichern.');
      }

      if (targetOut && pendingTargetOutSelection) {
        targetOut.value = String(pendingTargetOutSelection).trim();
        pendingTargetOutSelection = '';
        setSettingsStatus('Ziel __OUT aus Auswahl übernommen. Bitte speichern.');
      }

      if (targetReenqueue && pendingTargetReenqueueSelection) {
        targetReenqueue.value = String(pendingTargetReenqueueSelection).trim();
        pendingTargetReenqueueSelection = '';
        setSettingsStatus('Ziel __RE-ENQUEUE aus Auswahl übernommen. Bitte speichern.');
      }
      applyInitialSetupUi(lastJobRunning);
    }

    function collectSettingsPayload() {
      const targetNfs = document.getElementById('targetNfsSetting');
      const targetOut = document.getElementById('targetOutSetting');
      const targetReenqueue = document.getElementById('targetReenqueueSetting');
      const nasIp = document.getElementById('nasIpSetting');
      const plexIp = document.getElementById('plexIpSetting');
      const plexApi = document.getElementById('plexApiSetting');
      const tmdbApi = document.getElementById('tmdbApiSetting');
      const geminiApi = document.getElementById('geminiApiSetting');
      const aiQueryDisabled = document.getElementById('aiQueryDisabledSetting');
      const startOnBoot = document.getElementById('startOnBootSetting');
      const skip4kH265Encode = document.getElementById('skip4kH265EncodeSetting');
      const encodeOnce4k = document.getElementById('encodeOnce4kSetting');
      const oneEncodePerWorker = document.getElementById('oneEncodePerWorkerSetting');
      const allowWorker4 = document.getElementById('allowWorker4Setting');
      const parallelJobLimit = document.getElementById('parallelJobLimitSetting');
      const precheckEgb = document.getElementById('precheckEgbSetting');
      const speedFallbackCopy = document.getElementById('speedFallbackCopySetting');
      const encoder = document.getElementById('encoderSetting');
      const targetOutStored = targetOut ? targetOutDisplayToStored(targetOut.value || '') : '__OUT';
      const payload = {
        target_nfs_path: targetNfs ? String(targetNfs.value || '').trim() : '',
        target_out_path: targetOutStored,
        target_reenqueue_path: targetReenqueue
          ? targetReenqueueDisplayToStored(targetReenqueue.value || '', targetOutStored)
          : defaultTargetReenqueueStoredFromOutStored(targetOutStored),
        nas_ip: nasIp ? String(nasIp.value || '').trim() : '',
        plex_ip: plexIp ? String(plexIp.value || '').trim() : '',
        encoder: encoder ? String(encoder.value || '').trim() : '',
        ai_query_disabled: !!(aiQueryDisabled && aiQueryDisabled.checked),
        start_on_boot: !!(startOnBoot && startOnBoot.checked),
        skip_4k_h265_encode: !!(skip4kH265Encode && skip4kH265Encode.checked),
        encode_once_4k: !!(encodeOnce4k && encodeOnce4k.checked),
        one_encode_per_worker: !!(oneEncodePerWorker && oneEncodePerWorker.checked),
        allow_worker_4: !!(allowWorker4 && allowWorker4.checked),
        parallel_job_limit: parallelJobLimit ? String(parallelJobLimit.value || '2').trim() : '2',
        precheck_egb: !!(precheckEgb && precheckEgb.checked),
        speed_fallback_copy: !!(speedFallbackCopy && speedFallbackCopy.checked),
      };
      if (plexApi) {
        const value = String(plexApi.value || '').trim();
        if (value) payload.plex_api = value;
      }
      if (tmdbApi) {
        const value = String(tmdbApi.value || '').trim();
        if (value) payload.tmdb_api = value;
      }
      if (geminiApi) {
        const value = String(geminiApi.value || '').trim();
        if (value) payload.gemini_api = value;
      }
      return payload;
    }

    async function loadSettingsFromApi() {
      try {
        const res = await fetch('/api/settings', { cache: 'no-store' });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const payload = await res.json();
        const hadPendingTarget = !!pendingTargetNfsSelection || !!pendingTargetOutSelection || !!pendingTargetReenqueueSelection;
        applySettingsValues(payload.settings || {});
        renderCacheDbSummary(payload.cache_db || {});
        if (!hadPendingTarget) {
          setSettingsStatus('');
        }
      } catch (err) {
        renderCacheDbSummary({ error: String(err || 'Cache DB nicht erreichbar') });
        setSettingsStatus(`Einstellungen konnten nicht geladen werden: ${err}`, true);
      }
    }

    async function saveSettings() {
      const payload = collectSettingsPayload();
      setEncoderControls(payload.encoder || '');
      const wasLocked = isInitialSetupLocked();
      try {
        const res = await fetch('/api/settings', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        const data = await res.json();
        if (!res.ok || !data.ok) {
          throw new Error((data && data.error) ? data.error : `HTTP ${res.status}`);
        }
        applySettingsValues(data.settings || {});
        renderCacheDbSummary(data.cache_db || {});
        await persistEncoderSelection(payload.encoder || '');
        setSettingsStatus(wasLocked ? 'Einstellungen gespeichert. Analyze, Copy und Encode sind jetzt freigegeben.' : 'Einstellungen gespeichert.');
      } catch (err) {
        setSettingsStatus(`Speichern fehlgeschlagen: ${err}`, true);
      }
    }

    function setEncoderControls(encoder) {
      let value = String(encoder || '').trim();
      const allowed = new Set(Array.isArray(ALLOWED_ENCODERS) ? ALLOWED_ENCODERS : ['cpu', 'intel_qsv']);
      if (!allowed.has(value)) {
        value = allowed.has('cpu') ? 'cpu' : (Array.from(allowed)[0] || 'cpu');
      }
      const encoderEl = document.getElementById('encoderSetting');
      if (encoderEl && encoderEl.value !== value) {
        encoderEl.value = value;
      }
      const startEncoderEl = document.getElementById('startEncoder');
      if (startEncoderEl) {
        startEncoderEl.value = value;
      }
    }

    function wireEncoderSetting() {
      const encoderEl = document.getElementById('encoderSetting');
      if (!encoderEl) return;
      const startEncoderEl = document.getElementById('startEncoder');
      if (startEncoderEl) {
        startEncoderEl.value = encoderEl.value;
      }

      try {
        const saved = window.localStorage.getItem('managemovie.encoder') || '';
        if (saved) {
          setEncoderControls(saved);
        }
      } catch (err) {
      }

      encoderEl.addEventListener('change', async () => {
        setEncoderControls(encoderEl.value);
        const ok = await persistEncoderSelection(encoderEl.value);
        if (ok) {
          setSettingsStatus('Encoder gespeichert.');
        } else {
          setSettingsStatus('Encoder konnte nicht gespeichert werden.', true);
        }
      });
    }

    async function clearAllPanels() {
      try {
        await fetch('/logs/clear', { method: 'POST' });
      } catch (err) {
      }

      const statusSummaryBox = document.getElementById('statusSummaryBox');
      if (statusSummaryBox) {
        renderSummaryInlineBox('');
      }
      setSummaryDetailsVisible(false);
      renderStatusTable('', true);
      setPreText('statusBox', '', false);
      setPreText('procBox', '', false);
      setPreText('planBox', '', false);
      if (!lastJobRunning) {
        applyIdlePanelLayout(true);
      }
    }

    function updateStatusWaitingTitles() {
      const waiting = !!pendingConfirmData && !!String((pendingConfirmData || {}).token || '').trim();
      const statusTitle = document.getElementById('statusCardTitle');
      const statusHead = document.getElementById('statusCardHead');
      if (statusTitle) {
        statusTitle.innerText = waiting ? 'Warte auf Freigabe' : 'STATUS Queue';
      }
      if (statusHead) {
        statusHead.classList.toggle('status-waiting-head', waiting);
      }

      const modalTitle = document.getElementById('logModalTitle');
      const modalHead = document.getElementById('logModalHead');
      const modalWaiting = waiting && modalSourceId === 'statusBox';
      if (modalTitle && modalSourceId === 'statusBox') {
        modalTitle.innerText = modalWaiting
          ? `${SITE_TITLE} ${modalVersion || '-'} | Warte auf Freigabe`
          : `${SITE_TITLE} ${modalVersion || '-'} | STATUS Queue`;
      }
      if (modalHead) {
        modalHead.classList.toggle('status-waiting-head', modalWaiting);
      }
    }

    function renderPendingConfirmation() {
      const panel = document.getElementById('confirmPanel');
      const textEl = document.getElementById('confirmText');
      const copyBtn = document.getElementById('confirmCopyBtn');
      const encodeBtn = document.getElementById('confirmEncodeBtn');
      const analyzeBtn = document.getElementById('confirmAnalyzeBtn');
      const cleanBtn = document.getElementById('confirmCleanBtn');
      const editBtn = document.getElementById('confirmEditBtn');
      const exitBtn = document.getElementById('confirmExitBtn');
      const cancelBtn = document.getElementById('confirmCancelBtn');
      const primaryGroup = document.getElementById('confirmPrimaryActions');
      const secondaryGroup = document.getElementById('confirmSecondaryActions');
      if (!panel || !textEl || !copyBtn || !encodeBtn || !analyzeBtn || !cleanBtn || !editBtn || !exitBtn || !cancelBtn) return;

      if (modalSourceId !== 'statusBox') {
        panel.classList.add('hidden');
        updateStatusWaitingTitles();
        return;
      }
      const hasPending = !!pendingConfirmData && !!String(pendingConfirmData.token || '').trim();
      if (!hasPending) {
        panel.classList.add('hidden');
        updateStatusWaitingTitles();
        return;
      }

      const mode = String(pendingConfirmData.mode || '').toLowerCase();
      const isAnalyzeMode = mode === 'analyze';
      const isCopyMode = mode === 'copy';
      const isFfmpegMode = mode === 'ffmpeg';

      if (pendingConfirmNotice) {
        textEl.innerText = `Info: ${pendingConfirmNotice}`;
        textEl.classList.remove('hidden');
      } else {
        textEl.innerText = '';
        textEl.classList.add('hidden');
      }

      copyBtn.classList.toggle('hidden', !isCopyMode);
      encodeBtn.classList.toggle('hidden', !isFfmpegMode);
      analyzeBtn.classList.toggle('hidden', !isAnalyzeMode);
      cleanBtn.classList.toggle('hidden', true);
      editBtn.classList.toggle('hidden', false);
      exitBtn.classList.toggle('hidden', false);
      cancelBtn.classList.toggle('hidden', true);
      if (primaryGroup) {
        const hasPrimary = isCopyMode || isFfmpegMode || isAnalyzeMode;
        primaryGroup.classList.toggle('hidden', !hasPrimary);
      }
      if (secondaryGroup) {
        secondaryGroup.classList.toggle('hidden', false);
      }

      copyBtn.disabled = pendingConfirmInFlight;
      encodeBtn.disabled = pendingConfirmInFlight;
      analyzeBtn.disabled = pendingConfirmInFlight;
      cleanBtn.disabled = true;
      editBtn.disabled = pendingConfirmInFlight;
      exitBtn.disabled = pendingConfirmInFlight;
      cancelBtn.disabled = true;

      panel.classList.remove('hidden');
      updateStatusWaitingTitles();
    }

    function setPendingConfirmation(pending) {
      if (!pending || typeof pending !== 'object') {
        pendingConfirmData = null;
        pendingConfirmToken = '';
        pendingConfirmFilterToken = '';
        pendingConfirmModalToken = '';
        pendingConfirmNotice = '';
        updateStatusWaitingTitles();
        renderPendingConfirmation();
        return;
      }

      const token = String(pending.token || '').trim();
      const mode = String(pending.mode || '').toLowerCase();
      if (!token || (mode !== 'analyze' && mode !== 'copy' && mode !== 'ffmpeg')) {
        pendingConfirmData = null;
        pendingConfirmFilterToken = '';
        pendingConfirmNotice = '';
        updateStatusWaitingTitles();
        renderPendingConfirmation();
        return;
      }

      if (!pendingConfirmData || pendingConfirmData.token !== token) {
        pendingConfirmNotice = '';
      }

      pendingConfirmData = {
        token,
        mode,
        file_count: Number(pending.file_count || 0),
        start_folder: String(pending.start_folder || '').trim(),
      };
      if (pendingConfirmModalToken !== token) {
        openConfirmDecisionWindow(token);
        pendingConfirmModalToken = token;
      }
      pendingConfirmFilterToken = token;

      updateStatusWaitingTitles();
      renderPendingConfirmation();
    }

    async function submitPendingConfirmation(state) {
      if (!pendingConfirmData || pendingConfirmInFlight) return;
      if (state !== 'ok' && state !== 'copy' && state !== 'encode' && state !== 'clean' && state !== 'cancel') return;

      const mode = String(pendingConfirmData.mode || '').toLowerCase();
      if (state === 'ok' && mode !== 'analyze') return;
      if (state === 'copy' && mode !== 'copy') return;
      if (state === 'encode' && mode !== 'ffmpeg') return;

      const token = pendingConfirmData.token;
      const encoderEl = document.getElementById('encoderSetting');
      const encoder = (encoderEl && encoderEl.value) ? encoderEl.value : '';

      pendingConfirmInFlight = true;
      renderPendingConfirmation();

      try {
        if (state === 'clean') {
          const cleanRes = await fetch('/api/confirm/clean', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              token,
              start_folder: pendingConfirmData.start_folder || '',
            }),
          });

          const cleanData = await cleanRes.json().catch(() => ({}));
          if (cleanRes.ok && cleanData && cleanData.ok) {
            const deleted = Number(cleanData.deleted || 0);
            const failed = Number(cleanData.failed || 0);
            pendingConfirmNotice = `Reset "Erledigt" erledigt: gelöscht ${deleted}, Fehler ${failed}`;
            pendingConfirmInFlight = false;
            renderPendingConfirmation();
            await refreshState();
            return;
          }

          pendingConfirmNotice = (cleanData && cleanData.error) ? String(cleanData.error) : 'Reset "Erledigt" fehlgeschlagen';
          pendingConfirmInFlight = false;
          renderPendingConfirmation();
          return;
        }

        const decision = (state === 'cancel') ? 'cancel' : 'start';
        const res = await fetch('/api/confirm', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            token,
            state: decision,
            encoder,
          }),
        });
        const data = await res.json().catch(() => ({}));

        if (res.ok && data && data.ok) {
          pendingConfirmToken = token;
          pendingConfirmData = null;
          pendingConfirmNotice = '';
          pendingConfirmInFlight = false;
          renderPendingConfirmation();
          if (state === 'cancel') {
            await refreshState();
            collapseToHomeLayout();
          } else {
            closeLogModal();
            setCardCollapsed('statusCard', true);
          }
          return;
        }
        pendingConfirmNotice = (data && data.error) ? String(data.error) : 'Freigabe fehlgeschlagen';
      } catch (err) {
        pendingConfirmNotice = `Freigabe fehlgeschlagen: ${err}`;
      }

      pendingConfirmInFlight = false;
      renderPendingConfirmation();
    }

    function formatJob(job) {
      if (!job || !job.exists) return "Kein Job gestartet.";
      const lines = [];
      lines.push(`ID: ${job.job_id}`);
      lines.push(`Mode: ${job.mode}`);
      lines.push(`Folder: ${job.folder}`);
      lines.push(`Encoder: ${job.encoder}`);
      lines.push(`Running: ${job.running}`);
      if (job.exit_code !== null && job.exit_code !== undefined) {
        lines.push(`Exit-Code: ${job.exit_code}`);
      }
      lines.push(`Log: ${job.log_path || '-'}`);
      lines.push(`Release: ${modalVersion || job.release_version || '-'}`);
      return lines.join("\\n");
    }

    function escapeHtml(text) {
      return String(text || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;');
    }

    function normalizedJobFolder(job) {
      const folder = String((job && job.folder) ? job.folder : '').trim();
      if (!folder || folder === '-') return '';
      return folder;
    }

    function formatLeftRunState(running, job) {
      if (!running) {
        if (isInitialSetupLocked()) {
          return '<b>Status:</b> Erststart: Zuerst Einstellungen und API-Keys speichern. Analyze, Copy und Encode sind bis dahin gesperrt.';
        }
        return '<b>Status:</b> Kein laufender Job';
      }
      return '<b>Status:</b> Job läuft';
    }

    function setJobRunIndicator(running) {
      const isRunning = !!running;
      const dots = [
        document.getElementById('jobRunDot'),
        document.getElementById('mainRunDot'),
      ].filter((el) => !!el);
      dots.forEach((dot) => {
        dot.classList.toggle('running', isRunning);
        dot.classList.toggle('stopped', !isRunning);
        dot.title = isRunning ? 'Job läuft' : 'Kein laufender Job';
        dot.setAttribute('aria-label', isRunning ? 'Job läuft' : 'Kein laufender Job');
      });
    }

    function setPreText(id, value, autoScroll = false) {
      const el = document.getElementById(id);
      if (!el) return;
      if (isPreLocked(id) || isSelectionInside(el)) return;

      const nextRaw = (value === null || value === undefined) ? '' : String(value);
      const nextText = normalizeDisplayUmlauts(nextRaw);
      const atBottom = (el.scrollHeight - el.scrollTop - el.clientHeight) < 10;
      const prevTop = el.scrollTop;

      if ((el.textContent || '') !== nextText) {
        el.textContent = nextText;
        if (autoScroll && atBottom) {
          el.scrollTop = el.scrollHeight;
        } else {
          el.scrollTop = prevTop;
        }
      }

      if (modalSourceId === id) {
        syncLogModal();
      }
    }

    document.addEventListener('keydown', (event) => {
      if (event.key === 'Escape') {
        closeLogModal();
        closeSettingsModal();
        resolveInlineConfirm(false);
      }
    });

    function stripAnsi(text) {
      return String(text || '').replace(/\\x1B\\[[0-9;]*[A-Za-z]/g, '');
    }

    function isTableBorderLine(line) {
      const t = (line || '').trim();
      return /^\\+(?:[=+\\-]+\\+)+$/.test(t);
    }

    function splitStatusPanel(raw) {
      const lf = String.fromCharCode(10);
      const cr = String.fromCharCode(13);
      const text = stripAnsi(String(raw || '')).split(cr + lf).join(lf);
      const lines = text.split(lf);
      const tableStart = lines.findIndex((line) => {
        const t = (line || '').trim();
        return t.startsWith('|') || isTableBorderLine(t);
      });

      let metaLines = tableStart >= 0 ? lines.slice(0, tableStart) : lines.slice();
      metaLines = metaLines.filter((line) => {
        const t = (line || '').trim();
        if (!t) return true;
        if (/^#+$/.test(t)) return false;
        if (/^# *ManageMovie/i.test(t)) return false;
        if (isTableBorderLine(t)) return false;
        return true;
      });

      const tableLines = tableStart >= 0 ? lines.slice(tableStart).filter((line) => !isTableBorderLine(line)) : [];
      const meta = metaLines.join(lf).trim();
      const table = tableLines.join(lf).trim();
      return { meta, table };
    }

    function normalizeStatusFraction(value) {
      const m = String(value || '').trim().match(/^(\\d+)\\s*\\/\\s*(\\d+)$/);
      if (!m) return '';
      const left = Number(m[1]);
      const right = Number(m[2]);
      if (!Number.isFinite(left) || !Number.isFinite(right) || right <= 0) return '';
      return `${left}/${right}`;
    }

    function extractActiveStatusKey(metaText) {
      const lf = String.fromCharCode(10);
      const cr = String.fromCharCode(13);
      const lines = String(metaText || '').split(lf).map((line) => String(line || '').split(cr).join(''));
      for (const line of lines) {
        const match = String(line || '').match(/^\\s*Aktiv:\\s*([0-9]+\\s*\\/\\s*[0-9]+)/i);
        if (match) return normalizeStatusFraction(match[1]);
      }
      return '';
    }


    function parseStatusRow(line) {
      const raw = String(line || '').trim();
      if (!raw.startsWith('|')) return [];
      let body = raw;
      if (body.startsWith('|')) body = body.slice(1);
      if (body.endsWith('|')) body = body.slice(0, -1);
      return body.split('|').map((cell) => cell.trim());
    }

    function normHeaderKey(text) {
      return String(text || "").toLowerCase().replace(/[^a-z0-9]/g, "");
    }

    function statusColumnRole(label) {
      const key = normHeaderKey(label);
      if (key === 'quelle' || key.startsWith('quelle')) return 'source';
      if (key === 'ziel' || key.startsWith('ziel')) return 'target';
      return '';
    }

    function splitSourceTargetCell(value) {
      const text = String(value || '').trim();
      if (!text) return { source: '', target: '' };
      const match = text.match(/^(.*?)\\s*->\\s*(.*?)$/);
      if (match) {
        return {
          source: String(match[1] || '').trim(),
          target: String(match[2] || '').trim(),
        };
      }
      return { source: text, target: '' };
    }

    function splitCombinedSourceTargetStatusColumns(headers, rows) {
      const outHeaders = Array.isArray(headers) ? headers.slice() : [];
      const outRows = Array.isArray(rows)
        ? rows.map((row) => ({
            ...(row || {}),
            cells: Array.isArray((row || {}).cells) ? row.cells.slice() : [],
          }))
        : [];
      const combinedIdx = outHeaders.findIndex((label) => {
        const key = normHeaderKey(label);
        return key === 'quelleziel' || key === 'quelletarget' || key.includes('quelleziel');
      });
      if (combinedIdx < 0) return { headers: outHeaders, rows: outRows };

      outHeaders.splice(combinedIdx, 1, 'Quelle', 'Ziel');
      outRows.forEach((row) => {
        const cells = Array.isArray(row.cells) ? row.cells : [];
        const pair = splitSourceTargetCell(cells[combinedIdx] || '');
        cells.splice(combinedIdx, 1, pair.source || '-', pair.target || '-');
        row.cells = cells;
      });
      return { headers: outHeaders, rows: outRows };
    }

    function shouldHideStatusColumnInMain(label) {
      const key = normHeaderKey(label);
      return key === "jahr" || key === "stem";
    }

    function isMissingText(value) {
      const t = String(value || '').trim().toLowerCase();
      return !t || t === 'n/a' || t === 'na' || t === '-' || t === 'none' || t === 'null';
    }

    function isMissingYear(value) {
      const t = String(value || '').trim();
      if (isMissingText(t)) return true;
      return !/\\b(18|19|20)\\d{2}\\b/.test(t);
    }

    function normalizeImdbValue(value) {
      let txt = String(value || '').trim().toLowerCase();
      if (!txt) return '';
      txt = txt.replace(/[\\[\\]\\(\\)\\{\\}]/g, '');
      txt = txt.replace(/[^a-z0-9]/g, '');
      if (/^\\d{7,10}$/.test(txt)) txt = `tt${txt}`;
      return txt;
    }

    function isMissingImdb(value) {
      const imdb = normalizeImdbValue(value);
      if (isMissingText(imdb)) return true;
      if (!/^tt\\d{7,10}$/i.test(imdb)) return true;
      return /^tt0+$/.test(imdb) || imdb === 'tt1234567';
    }

    function findStatusColumnIndex(headers, aliases) {
      const aliasList = aliases.map((a) => String(a || '').toLowerCase());
      for (let i = 0; i < headers.length; i += 1) {
        const key = normHeaderKey(headers[i]);
        for (const alias of aliasList) {
          if (key === alias) return i;
          if (alias.length >= 2 && key.includes(alias)) return i;
        }
      }
      return -1;
    }

    function findAllStatusColumnIndexes(headers, aliases) {
      const aliasList = Array.isArray(aliases) ? aliases.map((a) => String(a || '').toLowerCase()) : [];
      const matches = [];
      (Array.isArray(headers) ? headers : []).forEach((label, idx) => {
        const key = normHeaderKey(label);
        const hit = aliasList.some((alias) => {
          if (!alias) return false;
          if (key === alias) return true;
          return alias.length >= 2 && key.includes(alias);
        });
        if (hit) matches.push(idx);
      });
      return matches;
    }

    function moveStatusColumnValues(values, fromIdx, toIdx) {
      const out = Array.isArray(values) ? values.slice() : [];
      if (fromIdx < 0 || toIdx < 0 || fromIdx >= out.length || toIdx >= out.length || fromIdx === toIdx) {
        return out;
      }
      const moved = out.splice(fromIdx, 1);
      out.splice(toIdx, 0, moved[0]);
      return out;
    }

    function reorderStatusColumns(headers, rows) {
      const safeHeaders = Array.isArray(headers) ? headers.slice() : [];
      const safeRows = Array.isArray(rows) ? rows : [];
      const stIndex = findStatusColumnIndex(safeHeaders, ['stem']);
      const lzeitIndex = findStatusColumnIndex(safeHeaders, ['lzeit']);
      if (stIndex < 0 || lzeitIndex < 0 || lzeitIndex === stIndex + 1) {
        return { headers: safeHeaders, rows: safeRows };
      }

      let targetIndex = stIndex + 1;
      if (lzeitIndex < targetIndex) targetIndex -= 1;

      const orderedHeaders = moveStatusColumnValues(safeHeaders, lzeitIndex, targetIndex);
      const orderedRows = safeRows.map((row) => {
        const r = row || {};
        return {
          ...r,
          cells: moveStatusColumnValues(r.cells || [], lzeitIndex, targetIndex),
        };
      });
      return { headers: orderedHeaders, rows: orderedRows };
    }

    function removeStatusColumns(headers, rows, aliases = []) {
      const aliasList = Array.isArray(aliases) ? aliases : [];
      if (!aliasList.length) {
        return {
          headers: Array.isArray(headers) ? headers.slice() : [],
          rows: Array.isArray(rows) ? rows.slice() : [],
        };
      }
      const safeHeaders = Array.isArray(headers) ? headers.slice() : [];
      const keepIndexes = [];
      safeHeaders.forEach((label, idx) => {
        const key = normHeaderKey(label);
        const drop = aliasList.some((alias) => key === String(alias || '').toLowerCase());
        if (!drop) keepIndexes.push(idx);
      });
      if (keepIndexes.length === safeHeaders.length) {
        return {
          headers: safeHeaders,
          rows: Array.isArray(rows) ? rows.slice() : [],
        };
      }
      const nextHeaders = keepIndexes.map((idx) => safeHeaders[idx]);
      const nextRows = (Array.isArray(rows) ? rows : []).map((row) => {
        const current = row || {};
        const cells = Array.isArray(current.cells) ? current.cells : [];
        return {
          ...current,
          cells: keepIndexes.map((idx) => cells[idx] || ''),
        };
      });
      return { headers: nextHeaders, rows: nextRows };
    }

    function collapseStatusWorkerColumns(headers, rows) {
      const safeHeaders = Array.isArray(headers) ? headers.slice() : [];
      const safeRows = (Array.isArray(rows) ? rows : []).map((row) => ({
        ...(row || {}),
        cells: Array.isArray((row || {}).cells) ? row.cells.slice() : [],
      }));
      const workerIndexes = findAllStatusColumnIndexes(safeHeaders, ['worker']);
      if (workerIndexes.length <= 1) {
        return { headers: safeHeaders, rows: safeRows };
      }
      const keepWorkerIdx = workerIndexes[workerIndexes.length - 1];
      const keepIndexes = safeHeaders
        .map((_, idx) => idx)
        .filter((idx) => !workerIndexes.includes(idx) || idx === keepWorkerIdx);
      return {
        headers: keepIndexes.map((idx) => safeHeaders[idx]),
        rows: safeRows.map((row) => ({
          ...row,
          cells: keepIndexes.map((idx) => (row.cells || [])[idx] || ''),
        })),
      };
    }

    function enrichStatusRowsWithWorkerData(headers, rows) {
      const safeHeaders = Array.isArray(headers) ? headers.slice() : [];
      const safeRows = (Array.isArray(rows) ? rows : []).map((row) => ({
        ...(row || {}),
        cells: Array.isArray((row || {}).cells) ? row.cells.slice() : [],
      }));
      const workerIndexes = findAllStatusColumnIndexes(safeHeaders, ['worker']);
      const workerIdx = workerIndexes.length ? workerIndexes[workerIndexes.length - 1] : -1;
      const nrIdx = findStatusColumnIndex(safeHeaders, ['nr']);
      const sourceIdx = findStatusColumnIndex(safeHeaders, ['quelle', 'source']);
      const targetIdx = findStatusColumnIndex(safeHeaders, ['ziel', 'target']);
      const speedIdx = findStatusColumnIndex(safeHeaders, ['speed']);
      const fpsIdx = findStatusColumnIndex(safeHeaders, ['fps']);
      const etaIdx = findStatusColumnIndex(safeHeaders, ['eta']);
      const zIdx = findStatusColumnIndex(safeHeaders, ['zgb', 'z-gb']);
      const eIdx = findStatusColumnIndex(safeHeaders, ['egb', 'e-gb']);
      const insertWorkerIdx = nrIdx >= 0 ? nrIdx + 1 : 0;
      const nextHeaders = safeHeaders.slice();
      if (workerIdx < 0) {
        nextHeaders.splice(insertWorkerIdx, 0, 'Worker');
      }
      safeRows.forEach((row) => {
        const sourceValue = sourceIdx >= 0 ? String((row.cells || [])[sourceIdx] || '') : '';
        const targetValue = targetIdx >= 0 ? String((row.cells || [])[targetIdx] || '') : '';
        const workerInfo = findWorkerInfoForStatusRow(sourceValue, targetValue);
        const existingWorker = workerIdx >= 0 ? String((row.cells || [])[workerIdx] || '').trim() : '';
        const workerName = workerInfo && workerInfo.worker_name ? statusWorkerLabel(workerInfo.worker_name) : statusWorkerLabel(existingWorker);
        row.worker_name = workerName;
        row.active_worker = !!(workerInfo && workerInfo.running);
        if (workerInfo && speedIdx >= 0) {
          const currentSpeed = String((row.cells || [])[speedIdx] || '').trim();
          if (isMissingUiMetric(currentSpeed) && !isMissingUiMetric(workerInfo.speed || '')) {
            row.cells[speedIdx] = workerInfo.speed;
          }
        }
        if (workerInfo && fpsIdx >= 0) {
          const currentFps = String((row.cells || [])[fpsIdx] || '').trim();
          if (isMissingUiMetric(currentFps) && !isMissingUiMetric(workerInfo.fps || '')) {
            row.cells[fpsIdx] = workerInfo.fps;
          }
        }
        if (workerInfo && etaIdx >= 0) {
          const currentEta = String((row.cells || [])[etaIdx] || '').trim();
          if (isMissingUiMetric(currentEta) && !isMissingUiMetric(workerInfo.eta || '')) {
            row.cells[etaIdx] = workerInfo.eta;
          }
        }
        if (workerInfo && zIdx >= 0) {
          const currentZ = String((row.cells || [])[zIdx] || '').trim();
          if (isMissingUiMetric(currentZ) && !isMissingUiMetric(workerInfo.z_gb || '')) {
            row.cells[zIdx] = workerInfo.z_gb;
          }
        }
        if (workerInfo && eIdx >= 0) {
          const currentE = String((row.cells || [])[eIdx] || '').trim();
          if (isMissingUiMetric(currentE) && !isMissingUiMetric(workerInfo.e_gb || '')) {
            row.cells[eIdx] = workerInfo.e_gb;
          }
        }
        if (workerIdx >= 0) {
          row.cells[workerIdx] = workerName;
        } else {
          row.cells.splice(insertWorkerIdx, 0, workerName);
        }
        if (workerInfo && !workerInfo.running) {
          const hasCompletionMetrics = !isMissingUiMetric(workerInfo.speed || '') || !isMissingUiMetric(workerInfo.eta || '') || !isMissingUiMetric(workerInfo.lzeit || '') || !isMissingUiMetric(workerInfo.z_gb || '');
          if (hasCompletionMetrics) row.completed = true;
        } else if (workerInfo && workerInfo.running) {
          row.completed = false;
        }
      });
      return collapseStatusWorkerColumns(nextHeaders, safeRows);
    }

    function parseStatusTable(rawTable) {
      const lines = String(rawTable || '')
        .split('\\n')
        .map((line) => stripAnsi(line).trim())
        .filter((line) => line.startsWith('|'));
      const parsed = lines.map(parseStatusRow).filter((cells) => cells.length > 0);
      if (!parsed.length) return { headers: [], rows: [] };

      const headers = parsed[0];
      const width = headers.length;
      const yearIndex = findStatusColumnIndex(headers, ['jahr']);
      const imdbIndex = findStatusColumnIndex(headers, ['imdbid', 'imdb']);
      const speedIndex = findStatusColumnIndex(headers, ['speed']);
      const etaIndex = findStatusColumnIndex(headers, ['eta']);

      const rows = parsed.slice(1).map((cells) => {
        const out = [];
        for (let i = 0; i < width; i += 1) {
          out.push((cells[i] || '').trim());
        }

        const yearMissing = yearIndex >= 0 ? isMissingYear(out[yearIndex]) : false;
        const imdbMissing = imdbIndex >= 0 ? isMissingImdb(out[imdbIndex]) : false;
        const speedText = speedIndex >= 0 ? String(out[speedIndex] || '').trim().toLowerCase() : '';
        const etaText = etaIndex >= 0 ? String(out[etaIndex] || '').trim().toLowerCase() : '';
        const completed = (
          speedText.includes('copied')
          || speedText.includes('encoded')
          || speedText.includes('manual')
          || etaText === 'copied'
          || etaText === 'encoded'
          || etaText === 'manual'
          || etaText === '00:00'
        );
        const rowKey = out.length > 0 ? normalizeStatusFraction(out[0]) : '';
        return {
          cells: out,
          missing: yearMissing || imdbMissing,
          completed,
          rowKey,
        };
      });
      const reordered = reorderStatusColumns(headers, rows);
      const split = splitCombinedSourceTargetStatusColumns(reordered.headers, reordered.rows);
      const reduced = removeStatusColumns(split.headers, split.rows, ['lzeit']);
      return enrichStatusRowsWithWorkerData(reduced.headers, reduced.rows);
    }

    function parseDurationSeconds(raw = '') {
      const text = String(raw || '').trim();
      let m = text.match(/^(\d{1,2}):(\d{2}):(\d{2})$/);
      if (m) return (Number(m[1]) * 3600) + (Number(m[2]) * 60) + Number(m[3]);
      m = text.match(/^(\d{1,2}):(\d{2})$/);
      if (m) return (Number(m[1]) * 60) + Number(m[2]);
      return null;
    }

    function estimateDatasetEtaSeconds(rows = []) {
      const safeRows = Array.isArray(rows) ? rows : [];
      const total = safeRows.length;
      if (!total) return 0;
      const completedRows = safeRows.filter((row) => !!(row && row.completed));
      const activeRows = safeRows.filter((row) => row && !row.completed && !isMissingUiMetric(row.speed || '') && !isMissingUiMetric(row.eta || ''));
      const activeEtaLoads = activeRows
        .map((row) => parseDurationSeconds(row.eta || ''))
        .filter((value) => Number.isFinite(value) && value >= 0);
      const completedDurations = completedRows
        .map((row) => parseDurationSeconds(row.eta || ''))
        .filter((value) => Number.isFinite(value) && value > 0);
      const activeCount = activeRows.length;
      const remainingQueued = Math.max(0, total - completedRows.length - activeCount);
      let avgDuration = 0;
      if (completedDurations.length) {
        avgDuration = completedDurations.reduce((sum, value) => sum + value, 0) / completedDurations.length;
      } else if (activeEtaLoads.length) {
        avgDuration = activeEtaLoads.reduce((sum, value) => sum + value, 0) / activeEtaLoads.length;
      }
      const slotCount = Math.max(activeCount, activeEtaLoads.length, 1);
      const loads = activeEtaLoads.length ? activeEtaLoads.slice() : Array(slotCount).fill(avgDuration > 0 ? avgDuration : 0);
      for (let i = 0; i < remainingQueued; i += 1) {
        let minIdx = 0;
        for (let j = 1; j < loads.length; j += 1) {
          if (loads[j] < loads[minIdx]) minIdx = j;
        }
        loads[minIdx] += avgDuration > 0 ? avgDuration : 0;
      }
      return Math.max(0, ...loads);
    }

    function autoScrollStatusWrap(wrapEl, activeRow, isModal = false) {
      if (!wrapEl) return;
      if (!lastJobRunning) {
        if (isModal) statusTableState.lastAutoScrollModalKey = '';
        else statusTableState.lastAutoScrollMainKey = '';
        return;
      }
      const activeKey = normalizeStatusFraction(statusTableState.activeKey || '');
      const stateKey = isModal ? 'lastAutoScrollModalKey' : 'lastAutoScrollMainKey';
      const lockId = isModal ? 'logModalStatusWrap' : 'statusTableWrap';
      if (!activeKey) {
        statusTableState[stateKey] = '';
        return;
      }
      if (!activeRow) {
        statusTableState[stateKey] = '';
        return;
      }
      if (isPreLocked(lockId) || isSelectionInside(wrapEl)) {
        statusTableState[stateKey] = '';
        return;
      }
      if ((statusTableState[stateKey] || '') === activeKey) return;
      activeRow.scrollIntoView({ block: 'nearest', inline: 'nearest' });
      statusTableState[stateKey] = activeKey;
    }

    function parseFractionValue(value) {
      const m = String(value || '').trim().match(/^(\\d+)\\s*\\/\\s*(\\d+)$/);
      if (!m) return null;
      return [Number(m[1]), Number(m[2])];
    }

    function parseNumericValue(value) {
      const txt = String(value || '').trim().replace(',', '.');
      const m = txt.match(/-?\\d+(?:\\.\\d+)?/);
      if (!m) return null;
      const n = Number(m[0]);
      return Number.isFinite(n) ? n : null;
    }

    function compareStatusCells(a, b) {
      const aEmpty = isMissingText(a);
      const bEmpty = isMissingText(b);
      if (aEmpty && !bEmpty) return 1;
      if (!aEmpty && bEmpty) return -1;

      const aFrac = parseFractionValue(a);
      const bFrac = parseFractionValue(b);
      if (aFrac && bFrac) {
        if (aFrac[0] !== bFrac[0]) return aFrac[0] - bFrac[0];
        if (aFrac[1] !== bFrac[1]) return aFrac[1] - bFrac[1];
      }

      const aNum = parseNumericValue(a);
      const bNum = parseNumericValue(b);
      if (aNum !== null && bNum !== null) {
        if (aNum < bNum) return -1;
        if (aNum > bNum) return 1;
        return 0;
      }

      return String(a || '').localeCompare(String(b || ''), 'de', { numeric: true, sensitivity: 'base' });
    }

    function displayStatusCellValue(cell, headerLabel) {
      const key = normHeaderKey(headerLabel || '');
      let text = String(cell || '').trim();
      if (/^n\\/a$/i.test(text)) text = '';
      if (key === 'worker') {
        return normalizeDisplayUmlauts(statusWorkerLabel(text));
      }
      if (key === 'quelle' || key.startsWith('quelle')) {
        text = text.replace(/\\s+/g, ' ').trim();
      }
      if ((key.includes('speed') || key === 'eta') && /^copied$/i.test(text) && statusFilterModeContext === 'ffmpeg') {
        text = 'encoded';
      }
      if (key.includes('speed')) {
        text = formatStatusSpeedText(text);
      } else if (key === 'fps') {
        text = formatStatusFpsText(text);
      }
      return normalizeDisplayUmlauts(text);
    }

    function formatStatusSpeedText(raw = '') {
      const text = String(raw || '').trim();
      if (!text || /^n\\/a$/i.test(text)) return '';
      const match = text.match(/^([0-9]+(?:[.,][0-9]+)?)(?:\\s*(x|mb\\/s|mib\\/s))?$/i);
      if (!match) return text;
      const value = Number(String(match[1] || '').replace(',', '.'));
      if (!Number.isFinite(value)) return text;
      const suffix = String(match[2] || '').trim().toLowerCase();
      if (suffix === 'x') return `${value.toFixed(1)}x`;
      return `${value.toFixed(1)} MB/s`;
    }

    function formatStatusFpsText(raw = '') {
      const text = String(raw || '').trim();
      if (!text || /^n\\/a$/i.test(text)) return '';
      const value = Number(text.replace(',', '.'));
      if (!Number.isFinite(value)) return text;
      return String(Math.round(value));
    }

    function hasPendingStatusApproval() {
      return !!pendingConfirmData && !!String((pendingConfirmData || {}).token || '').trim();
    }

    function updateStatusFilterButton() {
      const btns = [
        document.getElementById('statusFilterBtn'),
        document.getElementById('statusFilterModalBtn'),
      ].filter((el) => !!el);
      const mode = String(statusTableState.filterMode || 'all');
      btns.forEach((btn) => {
        if (mode === 'errors') btn.innerText = 'Fehler';
        else if (mode === 'done') btn.innerText = 'Erledigt';
        else if (mode === 'encode') btn.innerText = 'Encode';
        else if (mode === 'copy') btn.innerText = 'Copy';
        else btn.innerText = 'Alle';
        btn.classList.toggle('active', mode !== 'all');
      });
      const rows = Array.isArray(statusTableState.rows) ? statusTableState.rows : [];
      let errors = 0;
      let done = 0;
      rows.forEach((row) => {
        if (row && row.missing) errors += 1;
        if (row && row.completed) done += 1;
      });
      const infoEls = [
        document.getElementById('statusErrorInfo'),
        document.getElementById('statusErrorModalInfo'),
      ].filter((el) => !!el);
      const hasRows = rows.length > 0;
      const showInfo = hasRows && hasPendingStatusApproval();
      infoEls.forEach((el) => {
        el.classList.toggle('hidden', !showInfo);
        if (showInfo) {
          el.innerText = `Fehler ${errors}/${rows.length} | Erledigt ${done}/${rows.length}`;
        }
      });
    }

    function currentStatusFilterOrder() {
      const order = ['all', 'errors', 'done'];
      if (statusFilterModeContext === 'ffmpeg') {
        order.push('encode', 'copy');
      }
      return order;
    }

    function sortStatusByColumn(index) {
      const idx = Number(index);
      if (!Number.isInteger(idx) || idx < 0) return;

      if (statusTableState.sortIndex === idx) {
        statusTableState.sortDir = statusTableState.sortDir === 'asc' ? 'desc' : 'asc';
      } else {
        statusTableState.sortIndex = idx;
        statusTableState.sortDir = 'asc';
      }
      renderStatusTableFromState();
    }

    function toggleStatusMissingFilter() {
      const order = currentStatusFilterOrder();
      const current = String(statusTableState.filterMode || 'all');
      const idx = order.indexOf(current);
      if (idx < 0) {
        statusTableState.filterMode = order[0] || 'all';
      } else {
        statusTableState.filterMode = order[(idx + 1) % order.length];
      }
      updateStatusFilterButton();
      applyStatusTableFilterVisibility();
    }

    function statusRowMode(row, headers) {
      const cells = row && Array.isArray(row.cells) ? row.cells : [];
      const egbIdx = findStatusColumnIndex(headers || [], ['egb']);
      if (egbIdx < 0) return '';
      const raw = String(cells[egbIdx] || '').trim().toLowerCase();
      if (!raw || raw === '-' || raw === 'n/a' || raw === 'na') return '';
      if (raw.includes('copy')) return 'copy';
      return 'encode';
    }

    function statusFilterText(mode) {
      if (mode === 'done') return 'Keine erledigten Zeilen.';
      if (mode === 'encode') return 'Keine Encode-Zeilen.';
      if (mode === 'copy') return 'Keine Copy-Zeilen.';
      if (mode === 'errors') return 'Keine fehlerhaften Zeilen.';
      return 'Keine Daten.';
    }

    function statusFilterMatchCount(rowsSource, headers, mode) {
      const rows = Array.isArray(rowsSource) ? rowsSource : [];
      if (mode === 'all') return rows.length;
      let count = 0;
      rows.forEach((row) => {
        if (!row) return;
        if (mode === 'errors') {
          if (row.missing) count += 1;
          return;
        }
        if (mode === 'done') {
          if (row.completed) count += 1;
          return;
        }
        if (mode === 'encode' || mode === 'copy') {
          if (statusRowMode(row, headers) === mode) count += 1;
        }
      });
      return count;
    }

    function applyStatusTableFilterVisibility() {
      const table = document.getElementById('statusTable');
      const body = document.getElementById('statusTableBody');
      if (!table || !body) return;
      const mode = String(statusTableState.filterMode || 'all');
      table.classList.toggle('status-filter-errors', mode === 'errors');
      table.classList.toggle('status-filter-done', mode === 'done');
      table.classList.toggle('status-filter-encode', mode === 'encode');
      table.classList.toggle('status-filter-copy', mode === 'copy');

      const existing = body.querySelector('tr.status-filter-empty');
      if (existing) existing.remove();
      const headers = Array.isArray(statusTableState.headers) ? statusTableState.headers : [];
      const rowsSource = Array.isArray(statusTableState.rows) ? statusTableState.rows : [];
      if (mode !== 'all' && headers.length && rowsSource.length) {
        const matches = statusFilterMatchCount(rowsSource, headers, mode);
        if (matches <= 0) {
          const tr = document.createElement('tr');
          tr.className = 'status-filter-empty';
          const td = document.createElement('td');
          td.id = 'statusTableEmpty';
          td.colSpan = headers.length;
          td.innerText = statusFilterText(mode);
          tr.appendChild(td);
          body.appendChild(tr);
        }
      }
      syncLogModal();
    }

    function renderStatusTableFromState() {
      const head = document.getElementById('statusTableHead');
      const body = document.getElementById('statusTableBody');
      const wrap = document.getElementById('statusTableWrap');
      if (!head || !body) return;

      const headers = Array.isArray(statusTableState.headers) ? statusTableState.headers : [];
      const rowsSource = Array.isArray(statusTableState.rows) ? statusTableState.rows : [];
      const activeKey = normalizeStatusFraction(statusTableState.activeKey || '');

      head.innerHTML = '';
      body.innerHTML = '';

      if (!headers.length) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.id = 'statusTableEmpty';
        td.colSpan = 1;
        td.innerText = '';
        tr.appendChild(td);
        body.appendChild(tr);
        syncLogModal();
        return;
      }

      const hr = document.createElement('tr');
      headers.forEach((label, idx) => {
        const th = document.createElement('th');
        let title = String(label || '');
        if (statusTableState.sortIndex === idx) {
          title += statusTableState.sortDir === 'asc' ? ' ▲' : ' ▼';
          th.classList.add(statusTableState.sortDir === 'asc' ? 'sort-asc' : 'sort-desc');
        }
        th.innerText = title;
        th.classList.add('sortable');
        const role = statusColumnRole(label);
        if (role === 'source') th.classList.add('status-col-source');
        if (role === 'target') th.classList.add('status-col-target');
        th.setAttribute("onclick", `sortStatusByColumn(${idx})`);
        if (shouldHideStatusColumnInMain(label)) th.classList.add("status-col-main-hidden");
        hr.appendChild(th);
      });
      head.appendChild(hr);

      let rows = rowsSource.slice();
      if (statusTableState.sortIndex >= 0 && statusTableState.sortIndex < headers.length) {
        const col = statusTableState.sortIndex;
        rows.sort((a, b) => {
          const cmp = compareStatusCells(a.cells[col], b.cells[col]);
          return statusTableState.sortDir === 'asc' ? cmp : -cmp;
        });
      }

      if (!rows.length) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.id = 'statusTableEmpty';
        td.colSpan = headers.length;
        td.innerText = 'Keine Daten.';
        tr.appendChild(td);
        body.appendChild(tr);
        syncLogModal();
        return;
      }

      const hasWorkerActiveRows = rows.some((row) => !!(row && row.active_worker));
      rows.forEach((row) => {
        const tr = document.createElement('tr');
        tr.setAttribute('data-filter-row', '1');
        const rowKey = row ? normalizeStatusFraction(row.rowKey || '') : '';
        const rowIsWorkerActive = !!(row && row.active_worker);
        const rowIsFallbackActive = !hasWorkerActiveRows && !!(activeKey && rowKey && activeKey === rowKey);
        if (rowIsWorkerActive || rowIsFallbackActive) {
          tr.classList.add('status-row-active');
        }
        if (row && row.missing) tr.classList.add('status-row-missing');
        if (row && row.completed && !rowIsWorkerActive && !rowIsFallbackActive) tr.classList.add('status-row-done');
        const rowMode = statusRowMode(row, headers);
        if (rowMode === 'encode') tr.classList.add('status-row-encode');
        if (rowMode === 'copy') tr.classList.add('status-row-copy');
        (row.cells || []).forEach((cell, cellIdx) => {
          const td = document.createElement("td");
          td.innerText = displayStatusCellValue(cell, headers[cellIdx] || '');
          const role = statusColumnRole(headers[cellIdx] || '');
          if (role === 'source') td.classList.add('status-col-source');
          if (role === 'target') td.classList.add('status-col-target');
          if (shouldHideStatusColumnInMain(headers[cellIdx])) td.classList.add("status-col-main-hidden");
          tr.appendChild(td);
        });
        body.appendChild(tr);
      });
      applyStatusTableFilterVisibility();

      const activeRow = body.querySelector('tr.status-row-active');
      autoScrollStatusWrap(wrap, activeRow, false);
      syncLogModal();
    }

    function renderStatusTable(rawTable, forceEmpty = false, activeKey = '') {
      const parsed = parseStatusTable(rawTable);
      const nextHeaders = parsed.headers || [];
      const nextRows = parsed.rows || [];
      const normalizedActiveKey = normalizeStatusFraction(activeKey);

      if (!nextHeaders.length && statusTableState.headers.length > 0 && !forceEmpty) {
        statusTableState.emptyStreak = (statusTableState.emptyStreak || 0) + 1;
        if (statusTableState.emptyStreak < 3) {
          updateStatusFilterButton();
          return;
        }
      } else {
        statusTableState.emptyStreak = 0;
      }

      statusTableState.headers = nextHeaders;
      statusTableState.rows = nextRows;
      statusTableState.activeKey = normalizedActiveKey;
      if (statusTableState.sortIndex >= statusTableState.headers.length) {
        statusTableState.sortIndex = -1;
      }
      if (!normalizedActiveKey) {
        statusTableState.lastAutoScrollMainKey = '';
        statusTableState.lastAutoScrollModalKey = '';
      }
      updateStatusFilterButton();
      renderStatusTableFromState();
    }

    function mainStateApiUrl() {
      const params = new URLSearchParams();
      params.set('full_log', '1');
      params.set('log_max_chars', '2400000');
      return `/api/state?${params.toString()}`;
    }

    async function refreshState() {
      try {
        const res = await fetch(mainStateApiUrl(), { cache: 'no-store' });
        const data = await res.json();
        currentWorkerJobMap = (data && data.worker_job_map && typeof data.worker_job_map === 'object') ? data.worker_job_map : {};
        setPendingConfirmation(data.pending_confirmation);

        const job = (data && data.job) ? data.job : {};
        const running = isJobRunningState(job);
        const previousRunning = lastJobRunning;
        const previousMode = String(lastJobMode || '').trim().toLowerCase();
        const folderNow = normalizedJobFolder(job);
        if (folderNow) lastKnownJobFolder = folderNow;
        if (!folderNow && running) {
          const folderInput = document.getElementById('folder');
          const fallbackFolder = String((folderInput && folderInput.value) ? folderInput.value : '').trim();
          if (fallbackFolder) lastKnownJobFolder = fallbackFolder;
        }
        const settings = (data && data.settings && typeof data.settings === 'object') ? data.settings : {};
        initialSetupRequired = parseBoolSetting(settings.initial_setup_required);
        initialSetupDone = !initialSetupRequired || parseBoolSetting(settings.initial_setup_done);
        statusFilterModeContext = normalizeModeForAmpel(
          running ? (job.mode || '') : (settings.mode || job.mode || '')
        ) || 'analyze';
        if (running) {
          postOptions.sync_nas = !!job.sync_nas;
          postOptions.sync_plex = !!job.sync_plex;
          postOptions.del_out = !!job.del_out;
          postOptions.del_source = !!job.del_source;
          applyPostOptionUI();
        }
        lastJobRunning = running;
        lastJobMode = String((job && job.mode) || settings.mode || selectedMode || '').trim().toLowerCase();
        if (!idlePanelsInitialized) {
          if (!running) {
            applyIdlePanelLayout(true);
          }
          idlePanelsInitialized = true;
        }
        if (running && !previousRunning && String((job && job.mode) || '').trim().toLowerCase() === 'ffmpeg') {
          setCardCollapsed('statusCard', true);
        }
        if (!running && previousRunning && previousMode === 'ffmpeg') {
          setCardCollapsed('statusCard', true);
        }

        const startForm = document.getElementById('startForm');
        const stopForm = document.getElementById('stopForm');
        const folderBrowseBtn = document.getElementById('folderBrowseBtn');
        const folderInput = document.getElementById('folder');
        const startSubmitBtn = document.getElementById('startSubmitBtn');
        const leftRunState = document.getElementById('leftRunState');
        if (startForm && stopForm) {
          startForm.classList.remove('hidden');
          stopForm.classList.toggle('hidden', !running);
        }
        if (folderBrowseBtn) folderBrowseBtn.classList.toggle('hidden', running);
        if (folderInput) folderInput.readOnly = running;
        if (startSubmitBtn) {
          startSubmitBtn.disabled = running;
          startSubmitBtn.classList.toggle('hidden', running);
          if (!running) startSubmitBtn.innerText = 'Start';
        }
        if (leftRunState) {
          leftRunState.innerHTML = formatLeftRunState(running, job);
        }
        setSummaryDetailsVisible(running);
        setJobRunIndicator(running);

        const versionRangeBox = document.getElementById('versionRangeBox');
        const versionCurrentBox = document.getElementById('versionCurrentBox');
        if (versionRangeBox && data && data.versioning && data.versioning.range_text) {
          versionRangeBox.innerText = data.versioning.range_text;
        }
        if (versionCurrentBox && data && data.versioning && data.versioning.current) {
          versionCurrentBox.innerText = data.versioning.current;
          modalVersion = data.versioning.current;
        } else if (job && job.release_version) {
          modalVersion = job.release_version;
        }
        const updateProgressTitle = document.getElementById('updateProgressTitle');
        if (updateProgressTitle) {
          updateProgressTitle.innerText = normalizeDisplayUmlauts(`${SITE_TITLE} ${modalVersion || '-'} | Update`);
        }
        setPreText('jobBox', formatJob(data.job), true);

        const mainTitleText = document.getElementById('mainTitleText');
        if (mainTitleText) {
          mainTitleText.innerText = `${SITE_TITLE} ${modalVersion || '-'}`;
        }

        renderWorkers((data && data.workers) || []);
        currentEncodeWorkerName = String((job && job.worker_name) || currentEncodeWorkerName || '').trim();
        if (!running && selectedMode === 'ffmpeg' && !hasAvailableEncodeWorker()) {
          setModeControls('analyze', false);
        } else {
          applyInitialSetupUi(running);
        }

        const statusParts = splitStatusPanel(data.status_table || '');
        const activeStatusKey = extractActiveStatusKey(statusParts.meta || '');
        const runningPostKey = detectRunningPostOptionKey(data.processing_log || '');
        const activeStatusKeyForHighlight = (running && !runningPostKey) ? activeStatusKey : '';
        const summaryText = buildSummaryDetailsText(
          data,
          statusParts.meta || '',
          statusParts.table || '',
          activeStatusKey || ''
        );
        const selectionLocked = shouldPauseUiRefreshForSelection();
        const settingsEncoder = (((data || {}).settings || {}).encoder || '').trim();
        if (settingsEncoder) {
          setEncoderControls(settingsEncoder);
        }
        const liveMode = normalizeModeForAmpel(running ? (job.mode || '') : (settings.mode || job.mode || selectedMode || ''));
        if (liveMode) {
          setModeControls(liveMode, false);
        }
        const statusSummaryBox = document.getElementById('statusSummaryBox');
        if (statusSummaryBox && !selectionLocked && !isPreLocked('statusSummaryBox')) {
          renderSummaryInlineBox(statusParts.meta || '', summaryText);
        }
        renderSummaryAmpel(data);
        setPreText('summaryBox', summaryText, false);
        if (!selectionLocked) {
          renderStatusTable(statusParts.table, false, activeStatusKeyForHighlight);
        }
        setPreText('statusBox', statusParts.table, running);
        setPreText('procBox', data.processing_log, running);
        setPreText('planBox', data.out_tree, running);
      } catch (err) {
        setPreText('jobBox', `Fehler beim Laden: ${err}`, false);
        setPreText('summaryBox', 'Summary-Daten aktuell nicht erreichbar', false);
        renderStatusTable('', true);
        setSummaryDetailsVisible(false);
        const statusSummaryBox = document.getElementById('statusSummaryBox');
        if (statusSummaryBox) {
          renderSummaryInlineBox('Summary-Daten aktuell nicht erreichbar', '');
        }
        const leftRunState = document.getElementById('leftRunState');
        if (leftRunState) {
          leftRunState.innerHTML = '<b>Status:</b> API aktuell nicht erreichbar';
        }
        setJobRunIndicator(false);
        renderSummaryAmpel();
      }
    }

    clearFlashMessage();
    wireEncoderSetting();
    setModeControls(selectedMode || 'analyze', false);
    wireStartConfirm();
    wirePreInteractions();
    wireButtonTips();
    wireCardCollapseButtons();
    if (pendingTargetNfsSelection || pendingTargetOutSelection || pendingTargetReenqueueSelection) {
      openSettingsModal();
    }
    if (updateModalStoredFlag(MM_UPDATE_MODAL_KEY) === '1') {
      openUpdateProgressModal();
    }
    refreshState();
    setInterval(refreshState, 3000);
    window.addEventListener('storage', (event) => {
      if (event && event.key === 'managemovie.ui.refresh') {
        refreshState();
      }
      if (event && event.key === MM_UPDATE_MODAL_KEY) {
        if (updateModalStoredFlag(MM_UPDATE_MODAL_KEY) === '1') {
          openUpdateProgressModal();
        } else {
          closeUpdateProgressModal();
        }
      }
    });
  </script>
</body>
</html>
"""


if __name__ == "__main__":
    ensure_layout()
    clear_confirmation_file()
    clear_log_windows_data()

    port = int(os.environ.get("MANAGEMOVIE_WEB_PORT", "8126"))
    host = (os.environ.get("MANAGEMOVIE_WEB_BIND", "127.0.0.1") or "").strip() or "127.0.0.1"
    tls_enabled = (os.environ.get("MANAGEMOVIE_WEB_TLS", "0") or "").strip().lower() in {"1", "true", "yes", "y"}
    cert_file = Path(os.environ.get("MANAGEMOVIE_SSL_CERT", str(DATA_DIR / "certs" / "server" / "managemovie-local.crt"))).expanduser()
    key_file = Path(os.environ.get("MANAGEMOVIE_SSL_KEY", str(DATA_DIR / "certs" / "server" / "managemovie-local.key"))).expanduser()

    ssl_context = None
    if tls_enabled:
        if cert_file.exists() and key_file.exists():
            ssl_context = (str(cert_file), str(key_file))
            print(f"[HTTPS] enabled on port {port} with cert={cert_file}")
        else:
            print(f"[HTTPS] requested but certificate/key missing: cert={cert_file} key={key_file}; fallback to HTTP")

    app.run(host=host, port=port, ssl_context=ssl_context)
