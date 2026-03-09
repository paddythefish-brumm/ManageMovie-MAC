import importlib.util
import tempfile
import unittest
from pathlib import Path


TESTS_DIR = Path(__file__).resolve().parent
WEB_DIR = TESTS_DIR.parent / "web"
APP_PATH = WEB_DIR / "app.py"
SPEC = importlib.util.spec_from_file_location("managemovie_web_app", APP_PATH)
APP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(APP)


class ReenqueueMoveTests(unittest.TestCase):
    def test_movie_folder_move_keeps_subfolders_and_extra_videos(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            start = Path(tmp_dir) / 'START'
            movie_dir = start / 'My.Movie.2025'
            extras_dir = movie_dir / 'Extras' / 'Sub'
            extras_dir.mkdir(parents=True)

            source = movie_dir / 'My.Movie.2025.mkv'
            bonus_video = extras_dir / 'bonus-featurette.mp4'
            bonus_text = extras_dir / 'readme.txt'
            source.write_text('main')
            bonus_video.write_text('bonus-video')
            bonus_text.write_text('bonus-text')

            ok, result, err = APP.move_source_to_reenqueue(str(start), str(source.relative_to(start)))

            self.assertTrue(ok, msg=err)
            self.assertEqual('', err)
            self.assertIn('moved_container_dir', result)
            self.assertFalse(source.exists())
            self.assertFalse(bonus_video.exists())
            self.assertFalse(bonus_text.exists())

            target_dir = start / '__RE-ENQUEUE' / 'My.Movie.2025'
            self.assertTrue(target_dir.exists())
            self.assertTrue((target_dir / 'Extras' / 'Sub' / 'bonus-featurette.mp4').exists())
            self.assertTrue((target_dir / 'Extras' / 'Sub' / 'readme.txt').exists())

    def test_file_only_move_when_folder_contains_multiple_top_level_videos(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            start = Path(tmp_dir) / 'START'
            collection_dir = start / 'Collection'
            collection_dir.mkdir(parents=True)

            source = collection_dir / 'Movie.One.2024.mkv'
            sibling = collection_dir / 'Movie.Two.2024.mkv'
            source.write_text('one')
            sibling.write_text('two')

            ok, result, err = APP.move_source_to_reenqueue(str(start), str(source.relative_to(start)))

            self.assertTrue(ok, msg=err)
            self.assertEqual('', err)
            self.assertNotIn('moved_container_dir', result)
            self.assertFalse(source.exists())
            self.assertTrue(sibling.exists())

            moved_file = start / '__RE-ENQUEUE' / 'Collection' / 'Movie.One.2024.mkv'
            self.assertTrue(moved_file.exists())


if __name__ == '__main__':
    unittest.main()
