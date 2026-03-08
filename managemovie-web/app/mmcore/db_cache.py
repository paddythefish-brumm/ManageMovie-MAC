
from __future__ import annotations

import hashlib
import importlib
import json
import re
import time
from pathlib import Path
from typing import Any, Callable


class GeminiDbStore:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
        retention_days: int,
        connect_timeout_sec: int,
        read_timeout_sec: int,
        write_timeout_sec: int,
    ) -> None:
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.retention_days = self._clamp_retention_days(retention_days)
        self.connect_timeout_sec = connect_timeout_sec
        self.read_timeout_sec = read_timeout_sec
        self.write_timeout_sec = write_timeout_sec
        self._schema_ready = False

    def dsn_text(self) -> str:
        return f"{self.user}@{self.host}:{self.port}/{self.database}"

    @staticmethod
    def prompt_hash(prompt: str) -> str:
        normalized = re.sub(r"\s+", " ", (prompt or "").strip())
        return hashlib.sha256(normalized.encode("utf-8")).hexdigest()

    @staticmethod
    def _clamp_retention_days(value: int) -> int:
        if value < 1:
            return 1
        if value > 3650:
            return 3650
        return value

    def _expiry_utc_text(self) -> str:
        return time.strftime(
            "%Y-%m-%d %H:%M:%S",
            time.gmtime(time.time() + self.retention_days * 86400),
        )

    @staticmethod
    def _import_pymysql():
        return importlib.import_module("pymysql")

    def _connect(self):
        try:
            pymysql = self._import_pymysql()
        except Exception as exc:
            raise RuntimeError(f"PyMySQL Import fehlgeschlagen: {exc}") from exc

        try:
            return pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                charset="utf8mb4",
                autocommit=True,
                connect_timeout=self.connect_timeout_sec,
                read_timeout=self.read_timeout_sec,
                write_timeout=self.write_timeout_sec,
                cursorclass=pymysql.cursors.DictCursor,
            )
        except Exception as exc:
            raise RuntimeError(
                "MariaDB-Verbindung fehlgeschlagen "
                f"({self.dsn_text()}): {exc}. "
                "Bitte ./setup_mariadb.sh ausfuehren oder DB-ENV setzen."
            ) from exc

    def _db_exec(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
        finally:
            conn.close()

    def _db_fetch_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
                if isinstance(row, dict):
                    return row
                return None
        finally:
            conn.close()

    def init_schema(self, info_log: Callable[[str], None] | None = None) -> None:
        if self._schema_ready:
            return

        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS app_state (
                        state_key VARCHAR(128) NOT NULL PRIMARY KEY,
                        state_value LONGTEXT NOT NULL,
                        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                try:
                    cur.execute("ALTER TABLE app_state MODIFY COLUMN state_value LONGTEXT NOT NULL")
                except Exception:
                    # Existing installs can continue even if ALTER is not permitted.
                    pass
                try:
                    cur.execute("ALTER TABLE app_state ADD INDEX idx_app_state_updated_at (updated_at)")
                except Exception:
                    # Index may already exist.
                    pass
                def table_exists(table_name: str) -> bool:
                    cur.execute("SHOW TABLES LIKE %s", (table_name,))
                    return bool(cur.fetchone())

                # Migrate old cache table names once: gemini_* -> tmdb_*.
                try:
                    if table_exists("gemini_cache") and not table_exists("tmdb_cache"):
                        cur.execute("RENAME TABLE gemini_cache TO tmdb_cache")
                    if table_exists("gemini_cache_history") and not table_exists("tmdb_cache_history"):
                        cur.execute("RENAME TABLE gemini_cache_history TO tmdb_cache_history")
                except Exception:
                    # Keep startup resilient; CREATE IF NOT EXISTS below will still recover.
                    pass

                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tmdb_cache (
                        prompt_hash CHAR(64) NOT NULL PRIMARY KEY,
                        prompt_preview VARCHAR(255) NOT NULL,
                        prompt_text MEDIUMTEXT NOT NULL,
                        model VARCHAR(128) NOT NULL,
                        rows_json LONGTEXT NOT NULL,
                        rows_count INT NOT NULL DEFAULT 0,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        last_used_at TIMESTAMP NULL DEFAULT NULL,
                        expires_at TIMESTAMP NOT NULL,
                        KEY idx_tmdb_cache_expires (expires_at),
                        KEY idx_tmdb_cache_last_used (last_used_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS tmdb_cache_history (
                        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
                        prompt_hash CHAR(64) NOT NULL,
                        prompt_preview VARCHAR(255) NOT NULL,
                        prompt_text MEDIUMTEXT NOT NULL,
                        model VARCHAR(128) NOT NULL,
                        rows_json LONGTEXT NOT NULL,
                        rows_count INT NOT NULL DEFAULT 0,
                        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        expires_at TIMESTAMP NOT NULL,
                        KEY idx_tmdb_cache_history_prompt (prompt_hash),
                        KEY idx_tmdb_cache_history_expires (expires_at)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                    """
                )
                cur.execute("DELETE FROM tmdb_cache WHERE expires_at < UTC_TIMESTAMP()")
                cur.execute("DELETE FROM tmdb_cache_history WHERE expires_at < UTC_TIMESTAMP()")
        finally:
            conn.close()

        self._schema_ready = True
        if info_log is not None:
            info_log(
                f"MariaDB bereit: {self.dsn_text()} "
                f"(TMDB-Cache-Retention: {self.retention_days} Tage)"
            )

    def read_state(self, key: str) -> str:
        values = self.read_state_many([key])
        return str(values.get(key, "") or "").strip()

    def write_state(self, key: str, value: str) -> None:
        self.write_state_many([(key, value)])

    def read_state_many(self, keys: list[str], *, chunk_size: int = 400) -> dict[str, str]:
        unique_keys: list[str] = []
        seen: set[str] = set()
        for key in keys:
            text = str(key or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            unique_keys.append(text)
        if not unique_keys:
            return {}

        safe_chunk = max(1, min(int(chunk_size or 1), 1000))
        values: dict[str, str] = {}

        conn = self._connect()
        try:
            with conn.cursor() as cur:
                for i in range(0, len(unique_keys), safe_chunk):
                    chunk = unique_keys[i : i + safe_chunk]
                    placeholders = ",".join(["%s"] * len(chunk))
                    sql = f"SELECT state_key, state_value FROM app_state WHERE state_key IN ({placeholders})"
                    cur.execute(sql, tuple(chunk))
                    rows = cur.fetchall() or []
                    for row in rows:
                        if not isinstance(row, dict):
                            continue
                        state_key = str(row.get("state_key", "") or "").strip()
                        if not state_key:
                            continue
                        values[state_key] = str(row.get("state_value", "") or "")
        finally:
            conn.close()

        return values

    def write_state_many(self, items: list[tuple[str, str]], *, chunk_size: int = 250) -> int:
        deduped: dict[str, str] = {}
        for key, value in items:
            state_key = str(key or "").strip()
            if not state_key:
                continue
            deduped[state_key] = str(value or "")
        if not deduped:
            return 0

        payload = [(key, value) for key, value in deduped.items()]
        safe_chunk = max(1, min(int(chunk_size or 1), 1000))
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                for i in range(0, len(payload), safe_chunk):
                    chunk = payload[i : i + safe_chunk]
                    cur.executemany(
                        """
                        INSERT INTO app_state (state_key, state_value)
                        VALUES (%s, %s)
                        ON DUPLICATE KEY UPDATE state_value = VALUES(state_value)
                        """,
                        chunk,
                    )
        finally:
            conn.close()
        return len(payload)

    def delete_state_many(self, keys: list[str], *, chunk_size: int = 400) -> int:
        unique_keys: list[str] = []
        seen: set[str] = set()
        for key in keys:
            text = str(key or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            unique_keys.append(text)
        if not unique_keys:
            return 0

        safe_chunk = max(1, min(int(chunk_size or 1), 1000))
        deleted = 0
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                for i in range(0, len(unique_keys), safe_chunk):
                    chunk = unique_keys[i : i + safe_chunk]
                    placeholders = ",".join(["%s"] * len(chunk))
                    sql = f"DELETE FROM app_state WHERE state_key IN ({placeholders})"
                    cur.execute(sql, tuple(chunk))
                    deleted += max(0, int(cur.rowcount or 0))
        finally:
            conn.close()
        return deleted

    def read_last_successful_model(self, legacy_model_file: Path | None = None) -> str:
        model = self.read_state("gemini.last_successful_model")
        if model.startswith("gemini-"):
            return model

        if legacy_model_file is not None:
            try:
                if legacy_model_file.exists():
                    legacy = legacy_model_file.read_text(encoding="utf-8").strip()
                    if legacy.startswith("gemini-"):
                        self.write_state("gemini.last_successful_model", legacy)
                        return legacy
            except Exception:
                pass
        return ""

    def write_last_successful_model(self, model: str) -> None:
        if not model.startswith("gemini-"):
            return
        self.write_state("gemini.last_successful_model", model.strip())

    def migrate_legacy_cache_file(self, legacy_cache_file: Path) -> int:
        if self.read_state("gemini.legacy_cache_imported_v1") == "1":
            return 0
        if not legacy_cache_file.exists():
            self.write_state("gemini.legacy_cache_imported_v1", "1")
            return 0
        try:
            raw = legacy_cache_file.read_text(encoding="utf-8")
            data = json.loads(raw)
        except Exception:
            return 0
        if not isinstance(data, dict):
            self.write_state("gemini.legacy_cache_imported_v1", "1")
            return 0

        imported = 0
        for key, entry in data.items():
            if not isinstance(key, str) or not isinstance(entry, dict):
                continue
            rows_any = entry.get("rows")
            if not isinstance(rows_any, list):
                continue
            rows = [item for item in rows_any if isinstance(item, dict)]
            if not rows:
                continue
            model = str(entry.get("model", "legacy-cache") or "legacy-cache")
            prompt_preview = str(entry.get("prompt_preview", "") or "").strip()
            if not prompt_preview:
                prompt_preview = f"legacy:{key[:18]}"
            rows_json = json.dumps(rows, ensure_ascii=False)
            expires_at = self._expiry_utc_text()

            self._db_exec(
                """
                INSERT INTO tmdb_cache (
                    prompt_hash, prompt_preview, prompt_text, model, rows_json, rows_count, created_at, last_used_at, expires_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP(), %s
                )
                ON DUPLICATE KEY UPDATE
                    model = VALUES(model),
                    rows_json = VALUES(rows_json),
                    rows_count = VALUES(rows_count),
                    prompt_preview = VALUES(prompt_preview),
                    expires_at = VALUES(expires_at),
                    last_used_at = UTC_TIMESTAMP()
                """,
                (
                    key,
                    prompt_preview[:255],
                    prompt_preview,
                    model,
                    rows_json,
                    len(rows),
                    expires_at,
                ),
            )
            self._db_exec(
                """
                INSERT INTO tmdb_cache_history (
                    prompt_hash, prompt_preview, prompt_text, model, rows_json, rows_count, created_at, expires_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(), %s
                )
                """,
                (
                    key,
                    prompt_preview[:255],
                    prompt_preview,
                    model,
                    rows_json,
                    len(rows),
                    expires_at,
                ),
            )
            imported += 1

        self.write_state("gemini.legacy_cache_imported_v1", "1")
        return imported

    def get_cached_rows(self, prompt: str) -> tuple[list[dict[str, Any]], str]:
        key = self.prompt_hash(prompt)
        row = self._db_fetch_one(
            """
            SELECT rows_json, model
            FROM tmdb_cache
            WHERE prompt_hash = %s
              AND expires_at >= UTC_TIMESTAMP()
            LIMIT 1
            """,
            (key,),
        )
        if not row:
            return [], ""

        rows_json = str(row.get("rows_json", "") or "")
        model = str(row.get("model", "db-cache") or "db-cache")
        if not rows_json:
            return [], ""

        try:
            rows_any = json.loads(rows_json)
        except Exception:
            return [], ""
        if not isinstance(rows_any, list):
            return [], ""

        rows = [item for item in rows_any if isinstance(item, dict)]
        if not rows:
            return [], ""

        self._db_exec(
            "UPDATE tmdb_cache SET last_used_at = UTC_TIMESTAMP() WHERE prompt_hash = %s",
            (key,),
        )
        return rows, model

    def store_rows(self, prompt: str, rows: list[dict[str, Any]], model: str) -> None:
        if not rows:
            return

        key = self.prompt_hash(prompt)
        rows_json = json.dumps(rows, ensure_ascii=False)
        prompt_text = (prompt or "").strip()
        prompt_preview = prompt_text.replace("\n", " ")[:255]
        expires_at = self._expiry_utc_text()

        self._db_exec(
            """
            INSERT INTO tmdb_cache (
                prompt_hash, prompt_preview, prompt_text, model, rows_json, rows_count, created_at, last_used_at, expires_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(), UTC_TIMESTAMP(), %s
            )
            ON DUPLICATE KEY UPDATE
                prompt_preview = VALUES(prompt_preview),
                prompt_text = VALUES(prompt_text),
                model = VALUES(model),
                rows_json = VALUES(rows_json),
                rows_count = VALUES(rows_count),
                created_at = UTC_TIMESTAMP(),
                last_used_at = UTC_TIMESTAMP(),
                expires_at = VALUES(expires_at)
            """,
            (key, prompt_preview, prompt_text, model, rows_json, len(rows), expires_at),
        )

        self._db_exec(
            """
            INSERT INTO tmdb_cache_history (
                prompt_hash, prompt_preview, prompt_text, model, rows_json, rows_count, created_at, expires_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(), %s
            )
            """,
            (key, prompt_preview, prompt_text, model, rows_json, len(rows), expires_at),
        )

    def read_cache_db_stats(self) -> dict[str, int]:
        self.init_schema()
        stats = {
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
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        SUM(CASE WHEN state_key LIKE %s THEN 1 ELSE 0 END) AS gemini_source_rows,
                        SUM(CASE WHEN state_key LIKE %s THEN 1 ELSE 0 END) AS editor_source_rows,
                        SUM(CASE WHEN state_key LIKE %s THEN 1 ELSE 0 END) AS processed_source_rows,
                        SUM(CASE WHEN state_key LIKE %s THEN 1 ELSE 0 END) AS runtime_gemini_rows,
                        SUM(CASE WHEN state_key LIKE %s THEN 1 ELSE 0 END) AS runtime_rows,
                        SUM(CASE WHEN state_key LIKE %s THEN 1 ELSE 0 END) AS tmdb_state_v1_rows,
                        SUM(CASE WHEN state_key LIKE %s THEN 1 ELSE 0 END) AS tmdb_state_v2_rows,
                        COUNT(*) AS app_state_total_rows
                    FROM app_state
                    """,
                    (
                        "gemini.source.row.%",
                        "editor.source.row.%",
                        "processed.source.row.%",
                        "runtime.gemini_%",
                        "runtime.rows_%",
                        "tmdb.cache.v1.%",
                        "tmdb.cache.v2.%",
                    ),
                )
                row = cur.fetchone() or {}
                for key in (
                    "gemini_source_rows",
                    "editor_source_rows",
                    "processed_source_rows",
                    "runtime_gemini_rows",
                    "runtime_rows",
                    "tmdb_state_v1_rows",
                    "tmdb_state_v2_rows",
                ):
                    try:
                        stats[key] = max(0, int(row.get(key, 0) or 0))
                    except Exception:
                        stats[key] = 0

                try:
                    stats["app_state_cache_rows"] = max(0, int(row.get("app_state_total_rows", 0) or 0))
                except Exception:
                    stats["app_state_cache_rows"] = 0
                try:
                    cur.execute(
                        "SELECT COUNT(*) AS c FROM app_state WHERE state_key LIKE %s OR state_key = %s",
                        ("settings.%", "web.last_encoder"),
                    )
                    settings_row = cur.fetchone() or {}
                    stats["settings_rows"] = max(0, int(settings_row.get("c", 0) or 0))
                except Exception:
                    stats["settings_rows"] = 0

                try:
                    cur.execute(
                        """
                        SELECT COUNT(DISTINCT source_hash) AS c
                        FROM (
                          SELECT SUBSTRING(state_key, %s) AS source_hash
                          FROM app_state
                          WHERE state_key LIKE %s
                          UNION
                          SELECT SUBSTRING(state_key, %s) AS source_hash
                          FROM app_state
                          WHERE state_key LIKE %s
                          UNION
                          SELECT SUBSTRING(state_key, %s) AS source_hash
                          FROM app_state
                          WHERE state_key LIKE %s
                        ) source_keys
                        WHERE source_hash <> ''
                        """,
                        (
                            len("gemini.source.row.") + 1,
                            "gemini.source.row.%",
                            len("editor.source.row.") + 1,
                            "editor.source.row.%",
                            len("processed.source.row.") + 1,
                            "processed.source.row.%",
                        ),
                    )
                    source_row = cur.fetchone() or {}
                    stats["source_file_cache_rows"] = max(0, int(source_row.get("c", 0) or 0))
                except Exception:
                    stats["source_file_cache_rows"] = max(
                        stats["gemini_source_rows"],
                        stats["editor_source_rows"],
                        stats["processed_source_rows"],
                    )

                for table_name, key in (
                    ("tmdb_cache", "tmdb_cache_rows"),
                    ("tmdb_cache_history", "tmdb_cache_history_rows"),
                ):
                    try:
                        cur.execute(f"SELECT COUNT(*) AS c FROM {table_name}")
                        count_row = cur.fetchone() or {}
                        stats[key] = max(0, int(count_row.get("c", 0) or 0))
                    except Exception:
                        stats[key] = 0

        finally:
            conn.close()

        stats["total_cache_rows"] = (
            stats["app_state_cache_rows"]
            + stats["tmdb_cache_rows"]
            + stats["tmdb_cache_history_rows"]
        )
        return stats

    def reset_cache_db_entries(self) -> dict[str, int]:
        self.init_schema()
        cleared = {
            "app_state_cache_rows": 0,
            "tmdb_cache_rows": 0,
            "tmdb_cache_history_rows": 0,
            "legacy_gemini_cache_rows": 0,
            "legacy_gemini_cache_history_rows": 0,
            "app_state_non_settings_remaining": 0,
            "total_cache_rows": 0,
        }
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "DELETE FROM app_state WHERE state_key NOT LIKE %s AND state_key <> %s",
                    ("settings.%", "web.last_encoder"),
                )
                cleared["app_state_cache_rows"] = max(0, int(cur.rowcount or 0))

                for table_name, key in (
                    ("tmdb_cache", "tmdb_cache_rows"),
                    ("tmdb_cache_history", "tmdb_cache_history_rows"),
                    ("gemini_cache", "legacy_gemini_cache_rows"),
                    ("gemini_cache_history", "legacy_gemini_cache_history_rows"),
                ):
                    try:
                        cur.execute(f"SELECT COUNT(*) AS c FROM {table_name}")
                        row = cur.fetchone() or {}
                        existing_rows = max(0, int(row.get("c", 0) or 0))
                        if existing_rows > 0:
                            cur.execute(f"TRUNCATE TABLE {table_name}")
                        cleared[key] = existing_rows
                    except Exception:
                        cleared[key] = 0

                cur.execute(
                    "SELECT COUNT(*) AS c FROM app_state WHERE state_key NOT LIKE %s AND state_key <> %s",
                    ("settings.%", "web.last_encoder"),
                )
                row = cur.fetchone() or {}
                cleared["app_state_non_settings_remaining"] = max(0, int(row.get("c", 0) or 0))
                if cleared["app_state_non_settings_remaining"] > 0:
                    raise RuntimeError(
                        "Cache-Reset unvollstaendig: "
                        f"{cleared['app_state_non_settings_remaining']} Nicht-Settings-Eintraege verblieben."
                    )
        finally:
            conn.close()

        cleared["total_cache_rows"] = (
            cleared["app_state_cache_rows"]
            + cleared["tmdb_cache_rows"]
            + cleared["tmdb_cache_history_rows"]
            + cleared["legacy_gemini_cache_rows"]
            + cleared["legacy_gemini_cache_history_rows"]
        )
        return cleared
