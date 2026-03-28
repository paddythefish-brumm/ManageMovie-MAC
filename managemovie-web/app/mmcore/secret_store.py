import base64
import hashlib
import os
from functools import lru_cache

from cryptography.fernet import Fernet

STATE_SECRET_KEYS = frozenset(
    {
        "settings.plex_api",
        "settings.tmdb_api",
        "settings.gemini_api",
    }
)

STATE_ENCRYPTION_PREFIX = "enc:v1:"


def is_secret_state_key(key: str) -> bool:
    return str(key or "").strip() in STATE_SECRET_KEYS


def state_crypto_configured() -> bool:
    return bool(_state_crypto_raw_key())


def is_encrypted_state_value(value: str) -> bool:
    return str(value or "").strip().startswith(STATE_ENCRYPTION_PREFIX)


def encrypt_state_value(key: str, plain_value: str) -> str:
    value = str(plain_value or "").strip()
    if not is_secret_state_key(key) or not value:
        return value
    fernet = _state_fernet()
    token = fernet.encrypt(value.encode("utf-8")).decode("utf-8")
    return f"{STATE_ENCRYPTION_PREFIX}{token}"


def decrypt_state_value(key: str, stored_value: str) -> str:
    value = str(stored_value or "").strip()
    if not value or not is_secret_state_key(key):
        return value
    if not is_encrypted_state_value(value):
        return value
    token = value[len(STATE_ENCRYPTION_PREFIX) :]
    fernet = _state_fernet()
    return fernet.decrypt(token.encode("utf-8")).decode("utf-8").strip()


def _state_crypto_raw_key() -> str:
    return (
        (os.environ.get("MANAGEMOVIE_STATE_CRYPT_KEY", "") or "").strip()
        or (os.environ.get("MANAGEMOVIE_SETTINGS_CRYPT_KEY", "") or "").strip()
    )


@lru_cache(maxsize=1)
def _state_fernet() -> Fernet:
    raw_key = _state_crypto_raw_key()
    if not raw_key:
        raise RuntimeError("MANAGEMOVIE_STATE_CRYPT_KEY fehlt.")
    fernet_key = _normalize_fernet_key(raw_key)
    return Fernet(fernet_key)


def _normalize_fernet_key(raw_key: str) -> bytes:
    key = raw_key.strip()
    candidate = key
    if key.startswith("fernet:"):
        candidate = key.split(":", 1)[1].strip()
    try:
        Fernet(candidate.encode("utf-8"))
        return candidate.encode("utf-8")
    except Exception:
        pass

    digest = hashlib.sha256(key.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)
