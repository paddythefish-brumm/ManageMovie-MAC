import importlib.util
import json
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch


TESTS_DIR = Path(__file__).resolve().parent
APP_DIR = TESTS_DIR.parent / "app"
APP_PATH = APP_DIR / "managemovie.py"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))
SPEC = importlib.util.spec_from_file_location("managemovie_core_app", APP_PATH)
MM = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MM)


class TmdbTitleVerifyTests(unittest.TestCase):
    def test_dotted_name_transliterates_german_umlauts(self) -> None:
        self.assertEqual(
            MM.dotted_name("Mädchen über Größe & Spaß"),
            "Maedchen.ueber.Groesse.Spass",
        )

    def test_build_target_rel_path_uses_transliterated_filename(self) -> None:
        row = {
            "Quellname": "Beispiel/folge.s01e01.mkv",
            "Name des Film/Serie": "Mädchen über Größe & Spaß",
            "Erscheinungsjahr": "2025",
            "Staffel": "01",
            "Episode": "01",
            "IMDB-ID": "tt7654321",
            "Aufloesung": "1080p",
        }
        rel = MM.build_target_rel_path(row, MM.Path("__OUT"))
        self.assertIn("Maedchen.ueber.Groesse.Spass.2025.S01.E01.1080p.h264.{tt7654321}.mkv", rel)

    def test_tmdb_db_cache_roundtrip(self) -> None:
        class FakeStore:
            def __init__(self) -> None:
                self.data: dict[str, str] = {}

            def read_state(self, key: str) -> str:
                return self.data.get(key, "")

            def write_state(self, key: str, value: str) -> None:
                self.data[key] = value

        fake_store = FakeStore()
        payload = {"results": [{"id": 123, "name": "Twin Peaks"}]}
        with patch.object(MM, "init_mariadb_schema", return_value=None), patch.object(MM, "GEMINI_DB_STORE", fake_store):
            MM.store_tmdb_response_to_db("/search/tv", {"query": "Twin Peaks", "language": "de-DE"}, payload)
            loaded = MM.load_tmdb_response_from_db("/search/tv", {"query": "Twin Peaks", "language": "de-DE"})

        self.assertEqual(loaded, payload)

    def test_tmdb_db_cache_respects_retention(self) -> None:
        class FakeStore:
            def __init__(self) -> None:
                self.data: dict[str, str] = {}

            def read_state(self, key: str) -> str:
                return self.data.get(key, "")

            def write_state(self, key: str, value: str) -> None:
                self.data[key] = value

        fake_store = FakeStore()
        cache_key = MM.tmdb_state_cache_key("/search/tv", {"query": "Twin Peaks", "language": "de-DE"})
        old_unix = int(time.time()) - ((MM.TMDB_CACHE_RETENTION_DAYS + 1) * 86400)
        fake_store.data[cache_key] = json.dumps({"saved_unix": old_unix, "data": {"results": [{"id": 1}]}})

        with patch.object(MM, "init_mariadb_schema", return_value=None), patch.object(MM, "GEMINI_DB_STORE", fake_store):
            loaded = MM.load_tmdb_response_from_db("/search/tv", {"query": "Twin Peaks", "language": "de-DE"})

        self.assertIsNone(loaded)

    def test_tmdb_client_uses_db_cache_before_http(self) -> None:
        class FakeStore:
            def __init__(self) -> None:
                self.data: dict[str, str] = {}

            def read_state(self, key: str) -> str:
                return self.data.get(key, "")

            def write_state(self, key: str, value: str) -> None:
                self.data[key] = value

        fake_store = FakeStore()
        cached = {"results": [{"id": 1920, "name": "Twin Peaks"}]}
        cache_key = MM.tmdb_state_cache_key("/search/tv", {"query": "Twin Peaks", "language": "de-DE"})
        fake_store.data[cache_key] = json.dumps({"saved_unix": int(time.time()), "data": cached}, ensure_ascii=False)

        with patch.object(MM, "init_mariadb_schema", return_value=None), patch.object(MM, "GEMINI_DB_STORE", fake_store), patch.object(
            MM, "tmdb_get_json", side_effect=AssertionError("HTTP should not be called on DB cache hit")
        ):
            client = MM.TmdbClient("dummy-key")
            row = client.search_tv("Twin Peaks")

        self.assertEqual(row, {"id": 1920, "name": "Twin Peaks"})
        self.assertEqual(client.db_hits, 1)

    def test_tmdb_search_movie_uses_german_language(self) -> None:
        captured: dict[str, object] = {}

        def fake_tmdb_get_json(path: str, _key: str, params: dict | None = None) -> dict:
            captured["path"] = path
            captured["params"] = dict(params or {})
            return {"results": []}

        with patch.object(MM, "tmdb_get_json", side_effect=fake_tmdb_get_json), patch.object(
            MM, "load_tmdb_response_from_db", return_value=None
        ), patch.object(MM, "store_tmdb_response_to_db", return_value=None):
            client = MM.TmdbClient("dummy-key")
            client.search_movie("Momo", "1986")

        self.assertEqual(captured.get("path"), "/search/movie")
        params = captured.get("params")
        self.assertIsInstance(params, dict)
        self.assertEqual(params.get("language"), "de-DE")
        self.assertEqual(params.get("query"), "Momo")
        self.assertEqual(params.get("year"), "1986")

    def test_tmdb_movie_details_uses_german_language(self) -> None:
        captured: dict[str, object] = {}

        def fake_tmdb_get_json(path: str, _key: str, params: dict | None = None) -> dict:
            captured["path"] = path
            captured["params"] = dict(params or {})
            return {}

        with patch.object(MM, "tmdb_get_json", side_effect=fake_tmdb_get_json), patch.object(
            MM, "load_tmdb_response_from_db", return_value=None
        ), patch.object(MM, "store_tmdb_response_to_db", return_value=None):
            client = MM.TmdbClient("dummy-key")
            client.movie_details(601)

        self.assertEqual(captured.get("path"), "/movie/601")
        params = captured.get("params")
        self.assertIsInstance(params, dict)
        self.assertEqual(params.get("language"), "de-DE")

    def test_tmdb_search_tv_prefers_compatible_title_over_first_result(self) -> None:
        results_payload = {
            "results": [
                {
                    "id": 1,
                    "name": "Der Kampf um die Riesenflieger",
                    "first_air_date": "2023-01-01",
                    "vote_count": 100,
                },
                {
                    "id": 2,
                    "name": "Die Nibelungen Kampf der Koenigreiche",
                    "first_air_date": "2024-01-01",
                    "vote_count": 5,
                },
            ]
        }

        with patch.object(MM.TmdbClient, "_cached_get", return_value=results_payload):
            client = MM.TmdbClient("dummy-key")
            picked = client.search_tv("Die Nibelungen Kampf der Koenigreiche")

        self.assertIsNotNone(picked)
        self.assertEqual(int((picked or {}).get("id", 0)), 2)

    def test_tmdb_imdb_verification_updates_series_title(self) -> None:
        rows = [
            {
                "Quellname": "tmsf-snowpiercer-s04e01-1080p.mkv",
                "Name des Film/Serie": "tmsf snowpiercer",
                "Erscheinungsjahr": "2020",
                "Staffel": "04",
                "Episode": "01",
                "IMDB-ID": "tt6156584",
            }
        ]

        class FakeTmdbClient:
            def __init__(self, _key: str):
                pass

            def find_by_imdb(self, _imdb: str) -> dict:
                return {
                    "tv_results": [{"id": 11, "name": "Snowpiercer", "first_air_date": "2020-05-17"}],
                    "movie_results": [],
                }

            def tv_details(self, _tv_id: int) -> dict:
                return {"name": "Snowpiercer", "first_air_date": "2020-05-17"}

            def movie_details(self, _movie_id: int) -> dict:
                return {}

        with patch.object(MM, "TmdbClient", FakeTmdbClient):
            MM.verify_detected_titles_via_tmdb_imdb(rows, "dummy-key")

        self.assertEqual(rows[0]["Name des Film/Serie"], "Snowpiercer")

    def test_tmdb_imdb_verification_blocks_hard_series_title_mismatch(self) -> None:
        rows = [
            {
                "Quellname": (
                    "Die Nibelungen Kampf der Koenigreiche/S01/"
                    "Die.Nibelungen.Kampf.der.Koenigreiche.S01E01.1080p.mkv"
                ),
                "Name des Film/Serie": "Die Nibelungen Kampf der Koenigreiche",
                "Erscheinungsjahr": "",
                "Staffel": "01",
                "Episode": "01",
                "IMDB-ID": "tt26623994",
            }
        ]

        with patch.object(MM, "tmdb_title_year_from_imdb", return_value=("Der Kampf um die Riesenflieger", "2023")):
            MM.verify_detected_titles_via_tmdb_imdb(rows, "dummy-key", tmdb_client=MM.TmdbClient("dummy-key"))

        self.assertEqual(rows[0]["Name des Film/Serie"], "Die Nibelungen Kampf der Koenigreiche")
        self.assertEqual(rows[0]["Erscheinungsjahr"], "")

    def test_clean_title_noise_removes_tmsf_token(self) -> None:
        self.assertEqual(MM.clean_title_noise("tmsf snowpiercer"), "Snowpiercer")

    def test_normalize_title_guess_handles_multi_episode_token(self) -> None:
        guess = MM.normalize_title_guess("twin.peaks.s02e01e02.german.dl.1080p.bluray.mkv")
        self.assertEqual(guess.get("season"), "2")
        self.assertEqual(guess.get("episode"), "1")
        self.assertEqual(guess.get("is_series"), "1")

    def test_normalize_title_guess_avoids_x_pattern_movie_false_positive(self) -> None:
        guess = MM.normalize_title_guess("4x4.2010.german.bluray.mkv")
        self.assertEqual(guess.get("season"), "")
        self.assertEqual(guess.get("episode"), "")
        self.assertEqual(guess.get("is_series"), "0")

    def test_normalize_title_guess_reads_season_folder_and_episode_token(self) -> None:
        guess = MM.normalize_title_guess("Die.Nibelungen/S01/Die.Nibelungen.E05.1080p.mkv")
        self.assertEqual(guess.get("season"), "1")
        self.assertEqual(guess.get("episode"), "5")
        self.assertEqual(guess.get("is_series"), "1")

    def test_normalize_imdb_id_accepts_10_digit_ids(self) -> None:
        self.assertEqual(MM.normalize_imdb_id("tt1234567890"), "tt1234567890")
        self.assertEqual(MM.normalize_imdb_id("1234567890"), "tt1234567890")

    def test_normalize_imdb_id_rejects_non_title_ids(self) -> None:
        self.assertEqual(MM.normalize_imdb_id("nm8630645"), "")
        self.assertEqual(MM.normalize_imdb_id("co1234567"), "")

    def test_ensure_imdb_present_reads_10_digit_id_from_source_name(self) -> None:
        row = {"IMDB-ID": ""}
        MM.ensure_imdb_present(row, "Movie.2025.{tt1234567890}.mkv")
        self.assertEqual(row["IMDB-ID"], "tt1234567890")

    def test_clean_title_noise_removes_10_digit_imdb_tokens(self) -> None:
        self.assertEqual(MM.clean_title_noise("Titel tt1234567890"), "Titel")

    def test_source_looks_series_uses_generic_extraction(self) -> None:
        self.assertTrue(MM.source_looks_series("Smoke/S01/Smoke.E03.1080p.mkv"))
        self.assertFalse(MM.source_looks_series("4x4.2010.german.bluray.mkv"))

    def test_imdb_support_title_year_lookup_uses_support_text_for_non_title_hit(self) -> None:
        def fake_fetch_json(url: str) -> dict:
            if "assasins.json" in url:
                return {
                    "d": [
                        {
                            "id": "nm8630645",
                            "l": "Assasins Creed Showdown",
                            "s": "Visual Effects, Assassin's Creed (2016)",
                        }
                    ]
                }
            if "assassin" in url.lower() and "creed" in url.lower():
                return {
                    "d": [
                        {
                            "id": "tt2094766",
                            "l": "Assassin's Creed",
                            "q": "feature",
                            "y": 2016,
                        }
                    ]
                }
            raise AssertionError(f"unexpected url: {url}")

        with patch.object(MM, "fetch_json", side_effect=fake_fetch_json):
            title, year = MM.imdb_support_title_year_lookup("Assasins", False)

        self.assertEqual(title, "Assassin's Creed")
        self.assertEqual(year, "2016")

    def test_web_backfill_missing_years_imdb_replaces_unresolved_imdb_from_support_lookup(self) -> None:
        rows = [
            {
                "Quellname": "A/Assasins.AC3.1080p.mkv",
                "Name des Film/Serie": "Assasins",
                "Erscheinungsjahr": "0000",
                "Staffel": "",
                "Episode": "",
                "Laufzeit": "",
                "IMDB-ID": "tt8630645",
            }
        ]

        def fake_fetch_json(url: str) -> dict:
            lowered = url.lower()
            if "assasins.json" in lowered:
                return {
                    "d": [
                        {
                            "id": "nm8630645",
                            "l": "Assasins Creed Showdown",
                            "s": "Visual Effects, Assassin's Creed (2016)",
                        }
                    ]
                }
            if "assassin%27s%20creed.json" in lowered or "assassin's%20creed.json" in lowered or "assassin's creed.json" in lowered:
                return {
                    "d": [
                        {
                            "id": "tt2094766",
                            "l": "Assassin's Creed",
                            "q": "feature",
                            "y": 2016,
                        }
                    ]
                }
            raise AssertionError(f"unexpected url: {url}")

        with patch.object(MM, "fetch_json", side_effect=fake_fetch_json):
            MM.web_backfill_missing_years_imdb(rows, tmdb=None)

        self.assertEqual(rows[0]["Erscheinungsjahr"], "2016")
        self.assertEqual(rows[0]["IMDB-ID"], "tt2094766")

    def test_series_title_from_source_prefers_parent_folder(self) -> None:
        source = (
            "Die Nibelungen Kampf der Koenigreiche/S01/"
            "Die.Nibelungen.Kampf.der.Koenigreiche.S01E01.German.DL.1080p.WEB.x264-WvF/"
            "die.nibelungen.kampf.der.koenigreiche.s01e01.german.dl.1080p.web.x264-wvf.mkv"
        )
        self.assertEqual(
            MM.series_title_from_source(source),
            "Die Nibelungen Kampf der Koenigreiche",
        )

    def test_should_probe_runtime_auto_disables_on_network_fs(self) -> None:
        with patch.object(MM, "RUNTIME_PROBE_MODE", "auto"), patch.object(
            MM, "filesystem_type_for_path", return_value="nfs4"
        ):
            MM.RUNTIME_PROBE_REPORTED = False
            self.assertFalse(MM.should_probe_runtime_for_source(MM.Path("/tmp/source.mkv")))

    def test_should_probe_runtime_always_forces_probe(self) -> None:
        with patch.object(MM, "RUNTIME_PROBE_MODE", "always"), patch.object(
            MM, "filesystem_type_for_path", return_value="nfs4"
        ):
            MM.RUNTIME_PROBE_REPORTED = False
            self.assertTrue(MM.should_probe_runtime_for_source(MM.Path("/tmp/source.mkv")))

    def test_harmonize_series_start_year_prefers_source_hint_year(self) -> None:
        rows = [
            {
                "Quellname": "The.Equalizer.2021.S02E01.mkv",
                "Name des Film/Serie": "The Equalizer",
                "Erscheinungsjahr": "1985",
                "Staffel": "02",
                "Episode": "01",
                "IMDB-ID": "tt12345678",
            },
            {
                "Quellname": "The.Equalizer.2021.S02E02.mkv",
                "Name des Film/Serie": "The Equalizer",
                "Erscheinungsjahr": "1985",
                "Staffel": "02",
                "Episode": "02",
                "IMDB-ID": "tt12345678",
            },
        ]
        with patch.object(MM, "imdb_title_lookup", return_value=("1985", "tt12345678")):
            MM.harmonize_series_start_year(rows)
        self.assertEqual(rows[0]["Erscheinungsjahr"], "2021")
        self.assertEqual(rows[1]["Erscheinungsjahr"], "2021")

    def test_harmonize_series_start_year_aligns_wrong_series_imdb(self) -> None:
        rows = [
            {
                "Quellname": "Twin Peaks/S02/twin.peaks.s02e01e02.mkv",
                "Name des Film/Serie": "Twin Peaks",
                "Erscheinungsjahr": "1990",
                "Staffel": "02",
                "Episode": "01",
                "IMDB-ID": "tt27449259",
            },
            {
                "Quellname": "Twin Peaks/S02/twin.peaks.s02e03.mkv",
                "Name des Film/Serie": "Twin Peaks",
                "Erscheinungsjahr": "1990",
                "Staffel": "02",
                "Episode": "03",
                "IMDB-ID": "tt0098936",
            },
        ]
        with patch.object(MM, "imdb_title_lookup", return_value=("1990", "tt0098936")):
            MM.harmonize_series_start_year(rows)
        self.assertEqual(rows[0]["IMDB-ID"], "tt0098936")
        self.assertEqual(rows[1]["IMDB-ID"], "tt0098936")

    def test_web_backfill_missing_years_imdb_fills_series_group(self) -> None:
        rows = [
            {
                "Quellname": "Totenfrau/S01/totenfrau.s01e01.german.1080p.web.x264-wayne.mkv",
                "Name des Film/Serie": "Totenfrau",
                "Erscheinungsjahr": "0000",
                "Staffel": "01",
                "Episode": "01",
                "IMDB-ID": "tt0000000",
            },
            {
                "Quellname": "Totenfrau/S01/totenfrau.s01e02.german.1080p.web.x264-wayne.mkv",
                "Name des Film/Serie": "Totenfrau",
                "Erscheinungsjahr": "0000",
                "Staffel": "01",
                "Episode": "02",
                "IMDB-ID": "tt0000000",
            },
        ]
        with patch.object(MM, "imdb_title_lookup", return_value=("2022", "tt5569404")):
            MM.web_backfill_missing_years_imdb(rows, tmdb=None)
        self.assertEqual(rows[0]["Erscheinungsjahr"], "2022")
        self.assertEqual(rows[1]["Erscheinungsjahr"], "2022")
        self.assertEqual(rows[0]["IMDB-ID"], "tt5569404")
        self.assertEqual(rows[1]["IMDB-ID"], "tt5569404")

    def test_imdb_title_year_from_id_parses_json_ld(self) -> None:
        html = """
        <html><head>
        <script type="application/ld+json">
        {"@context":"https://schema.org","@type":"TVSeries","name":"Interview with the Vampire","datePublished":"2022-10-02"}
        </script>
        </head></html>
        """
        with patch.object(MM, "fetch_text", return_value=html):
            title, year = MM.imdb_title_year_from_id("tt14921986")
        self.assertEqual(title, "Interview with the Vampire")
        self.assertEqual(year, "2022")

    def test_web_backfill_missing_years_imdb_uses_existing_imdb_id_for_year(self) -> None:
        rows = [
            {
                "Quellname": "Test/Smoke.S01E01.mkv",
                "Name des Film/Serie": "Smoke",
                "Erscheinungsjahr": "0000",
                "Staffel": "01",
                "Episode": "01",
                "IMDB-ID": "tt14921986",
            }
        ]
        with patch.object(MM, "imdb_title_lookup", return_value=("", "")):
            with patch.object(MM, "imdb_title_year_from_id", return_value=("Smoke", "2026")):
                MM.web_backfill_missing_years_imdb(rows, tmdb=None)
        self.assertEqual(rows[0]["Erscheinungsjahr"], "2026")
        self.assertEqual(rows[0]["IMDB-ID"], "tt14921986")


if __name__ == "__main__":
    unittest.main()
