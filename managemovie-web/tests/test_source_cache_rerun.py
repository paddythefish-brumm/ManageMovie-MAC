import importlib.util
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

TESTS_DIR = Path(__file__).resolve().parent
APP_DIR = TESTS_DIR.parent / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
APP_PATH = APP_DIR / "managemovie.py"
SPEC = importlib.util.spec_from_file_location("managemovie_core_app", APP_PATH)
MM = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MM)


class _FakeStateStore:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}

    def read_state_many(self, keys: list[str]) -> dict[str, str]:
        return {key: self.data.get(key, "") for key in keys}

    def write_state_many(self, items: list[tuple[str, str]]) -> int:
        for key, value in items:
            self.data[str(key)] = str(value)
        return len(items)


def _build_row(
    source: str,
    *,
    title: str = "",
    year: str = "",
    runtime: str = "",
    imdb: str = "",
) -> dict[str, str]:
    row = {header: "" for header in MM.CSV_HEADERS}
    row["Quellname"] = source
    row["Name des Film/Serie"] = title
    row["Erscheinungsjahr"] = year
    row["Laufzeit"] = runtime
    row["IMDB-ID"] = imdb
    return row


def _cache_payload(row: dict[str, str]) -> str:
    return json.dumps({"saved_unix": 123456789, "source_name": row["Quellname"], "row": row}, ensure_ascii=False)


class SourceCacheRerunTests(unittest.TestCase):
    def test_rerun_overwrites_weaker_existing_source_cache(self) -> None:
        source = "Serie/Folge01.mkv"
        cache_key = MM.source_row_cache_key(MM.GEMINI_SOURCE_ROW_CACHE_PREFIX, source)
        weak_existing = _build_row(source, title="Titel nur")
        improved = _build_row(source, title="Titel", year="2024", runtime="45", imdb="tt1234567")

        fake_store = _FakeStateStore()
        fake_store.data[cache_key] = _cache_payload(weak_existing)

        with patch.object(MM, "init_mariadb_schema", return_value=None), patch.object(
            MM, "GEMINI_DB_STORE", fake_store
        ), patch.object(MM, "processing_log", return_value=None):
            written = MM.store_source_rows_cache(
                [improved],
                cache_prefix=MM.GEMINI_SOURCE_ROW_CACHE_PREFIX,
                overwrite=True,
                prefer_richer_existing=True,
            )

        self.assertEqual(written, 1)
        saved = json.loads(fake_store.data[cache_key])
        self.assertEqual(saved["row"]["Erscheinungsjahr"], "2024")
        self.assertEqual(saved["row"]["IMDB-ID"], "tt1234567")

    def test_rerun_does_not_downgrade_richer_existing_source_cache(self) -> None:
        source = "Film/Test.mkv"
        cache_key = MM.source_row_cache_key(MM.GEMINI_SOURCE_ROW_CACHE_PREFIX, source)
        rich_existing = _build_row(source, title="Bester Titel", year="2019", runtime="110", imdb="tt7654321")
        weaker_new = _build_row(source, title="Bester Titel")

        fake_store = _FakeStateStore()
        fake_store.data[cache_key] = _cache_payload(rich_existing)

        with patch.object(MM, "init_mariadb_schema", return_value=None), patch.object(
            MM, "GEMINI_DB_STORE", fake_store
        ), patch.object(MM, "processing_log", return_value=None):
            written = MM.store_source_rows_cache(
                [weaker_new],
                cache_prefix=MM.GEMINI_SOURCE_ROW_CACHE_PREFIX,
                overwrite=True,
                prefer_richer_existing=True,
            )

        self.assertEqual(written, 0)
        saved = json.loads(fake_store.data[cache_key])
        self.assertEqual(saved["row"]["Erscheinungsjahr"], "2019")
        self.assertEqual(saved["row"]["IMDB-ID"], "tt7654321")


if __name__ == "__main__":
    unittest.main()
