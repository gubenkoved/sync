import shutil
import tempfile
from unittest import TestCase

from sync.core import Syncer
from sync.provider import ProviderBase
from sync.providers.fs import FSProvider
from tests.common import random_bytes_stream


class FiltersTest(TestCase):
    __test__ = True

    def setUp(self):
        super().setUp()

        src_dir = tempfile.mkdtemp()
        dst_dir = tempfile.mkdtemp()

        src_provider = FSProvider(root_dir=src_dir)
        dst_provider = FSProvider(root_dir=dst_dir)

        self.addCleanup(lambda: shutil.rmtree(src_dir))
        self.addCleanup(lambda: shutil.rmtree(dst_dir))

        self._syncer = Syncer(
            src_provider,
            dst_provider,
        )

        # common setup
        self.write_by_path(src_provider, 'foo.file')
        self.write_by_path(src_provider, 'foo/foo.file')
        self.write_by_path(src_provider, 'foo/bar.file')
        self.write_by_path(src_provider, 'bar.file')
        self.write_by_path(src_provider, 'bar/foo.file')
        self.write_by_path(src_provider, 'bar/bar.file')
        self.write_by_path(src_provider, 'spam.file')
        self.write_by_path(src_provider, 'spam/spam.file')
        self.write_by_path(src_provider, 'spam/foo/file')
        self.write_by_path(src_provider, 'spam/bar/file')

    def write_by_path(self, provider: ProviderBase, path: str):
        with random_bytes_stream() as stream:
            provider.write(path, stream)

    def sync_and_verify_expected_files(self, expected_paths):
        self._syncer.sync()
        state = self._syncer.dst_provider.get_state()
        self.assertCountEqual(expected_paths, state.files.keys())

    def test_single_filter_expression(self):
        self._syncer.filter = 'foo/*'

        self.sync_and_verify_expected_files([
            'foo/foo.file',
            'foo/bar.file',
        ])

    def test_single_filter_in_middle_of_path(self):
        self._syncer.filter = '*foo*'

        self.sync_and_verify_expected_files([
            'foo.file',
            'foo/foo.file',
            'foo/bar.file',
            'bar/foo.file',
            'spam/foo/file',
        ])

    def test_single_negative_filter_expression(self):
        self._syncer.filter = '!spam*'

        self.sync_and_verify_expected_files([
            'foo.file',
            'foo/foo.file',
            'foo/bar.file',
            'bar.file',
            'bar/foo.file',
            'bar/bar.file',
        ])

    def test_multiple_positive_filter_expressions(self):
        self._syncer.filter = 'foo/*, bar/*'

        self.sync_and_verify_expected_files([
            'foo/foo.file',
            'foo/bar.file',
            'bar/foo.file',
            'bar/bar.file',
        ])

    def test_multiple_negative_filter_expressions(self):
        self._syncer.filter = '!spam*, !*foo*'

        self.sync_and_verify_expected_files([
            'bar.file',
            'bar/bar.file',
        ])

    def test_full_path_should_match(self):
        self._syncer.filter = 'foo.file'

        self.sync_and_verify_expected_files([
            'foo.file',
        ])

    def test_full_path_should_match_2(self):
        self._syncer.filter = '*/foo.file'

        self.sync_and_verify_expected_files([
            'foo/foo.file',
            'bar/foo.file',
        ])

    def test_mix_positive_and_negative_filter_expressions(self):
        self._syncer.filter = 'foo/*, !*bar.file, spam.file'

        self.sync_and_verify_expected_files([
            'foo/foo.file',
            'spam.file',
        ])

    @property
    def syncer(self):
        return self._syncer
