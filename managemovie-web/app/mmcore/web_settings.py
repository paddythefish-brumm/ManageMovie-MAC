from __future__ import annotations

from typing import Any

SECRET_PLACEHOLDER = "********"
BOOL_TRUE_SET = {"1", "true", "yes", "y", "on"}


def mask_secret_for_ui(value: str) -> str:
    if str(value or "").strip():
        return SECRET_PLACEHOLDER
    return ""


def apply_secret_update(source: dict[str, Any], key: str, current_value: str) -> str:
    if key not in source:
        return str(current_value or "").strip()
    value = str(source.get(key, "") or "").strip()
    if value == SECRET_PLACEHOLDER:
        return str(current_value or "").strip()
    return value


def parse_bool_flag(value: Any) -> bool:
    return str(value or "").strip().lower() in BOOL_TRUE_SET


def build_public_runtime_settings(
    settings: dict[str, str],
    *,
    mode: str,
    encoder: str,
) -> dict[str, str | bool]:
    plex_api = str(settings.get("plex_api", "") or "").strip()
    tmdb_api = str(settings.get("tmdb_api", "") or "").strip()
    gemini_api = str(settings.get("gemini_api", "") or "").strip()
    ai_query_disabled = parse_bool_flag(settings.get("ai_query_disabled", "1"))
    skip_4k_h265_encode = parse_bool_flag(settings.get("skip_4k_h265_encode", "0"))
    precheck_egb = parse_bool_flag(settings.get("precheck_egb", "1"))
    initial_setup_done = parse_bool_flag(settings.get("initial_setup_done", "1"))
    initial_setup_required = parse_bool_flag(settings.get("initial_setup_required", "0"))
    return {
        "target_nfs_path": str(settings.get("target_nfs_path", "") or "").strip(),
        "target_out_path": str(settings.get("target_out_path", "") or "").strip(),
        "target_reenqueue_path": str(settings.get("target_reenqueue_path", "") or "").strip(),
        "nas_ip": str(settings.get("nas_ip", "") or "").strip(),
        "plex_ip": str(settings.get("plex_ip", "") or "").strip(),
        "plex_api": mask_secret_for_ui(plex_api),
        "tmdb_api": mask_secret_for_ui(tmdb_api),
        "gemini_api": mask_secret_for_ui(gemini_api),
        "has_plex_api": bool(plex_api),
        "has_tmdb_api": bool(tmdb_api),
        "has_gemini_api": bool(gemini_api),
        "ai_query_disabled": ai_query_disabled,
        "skip_4k_h265_encode": skip_4k_h265_encode,
        "precheck_egb": precheck_egb,
        "initial_setup_done": initial_setup_done,
        "initial_setup_required": initial_setup_required,
        "mode": str(mode or "").strip(),
        "encoder": str(encoder or "").strip(),
    }
