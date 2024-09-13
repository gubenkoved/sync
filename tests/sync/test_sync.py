import abc
import os.path
from unittest import TestCase

import pytest

from sync.core import (
    UploadSyncAction, DownloadSyncAction,
    RemoveOnSourceSyncAction, RemoveOnDestinationSyncAction,
    MoveOnSourceSyncAction, MoveOnDestinationSyncAction,
    ResolveConflictSyncAction, NoopSyncAction,
    SyncError,
    Syncer,
)
from tests.common import bytes_as_stream, stream_to_bytes, random_bytes_stream


class SyncTestBase(TestCase):
    __test__ = False

    @property
    @abc.abstractmethod
    def syncer(self) -> Syncer:
        raise NotImplementedError

    def tearDown(self):
        super().tearDown()

        state_file_path = self.syncer.get_state_file_path()

        if os.path.exists(state_file_path):
            os.remove(state_file_path)

    def ensure_same_state(self):
        src_state = self.syncer.src_provider.get_state()
        dst_state = self.syncer.dst_provider.get_state()

        # ensure same file lists
        # note that we can not directly compare different providers StorageState
        # as the content hash is abstract and can mean different things
        self.assertEqual(set(src_state.files), set(dst_state.files))

        for path in src_state.files:
            self.assertTrue(
                self.syncer.compare(path), 'files are different by path %s' % path)

    def do_sync(self, expected_sync_actions=None, ensure_same_state=True):
        sync_actions = self.syncer.sync()
        if expected_sync_actions is not None:
            self.assertCountEqual(expected_sync_actions, sync_actions)
        if ensure_same_state:
            self.ensure_same_state()

    def test_sync_new_files(self):
        src_provider = self.syncer.src_provider
        dst_provider = self.syncer.dst_provider

        self.do_sync(expected_sync_actions=[])

        self.assertEqual(0, len(dst_provider.get_state().files))

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        self.do_sync(expected_sync_actions=[
            UploadSyncAction('foo')
        ])

        self.assertEqual(1, len(dst_provider.get_state().files))

        with bytes_as_stream(b'data') as stream:
            src_provider.write('bar', stream)

        self.do_sync([
            UploadSyncAction('bar')
        ])

        self.assertEqual(2, len(dst_provider.get_state().files))

        # add file on destination
        with bytes_as_stream(b'data') as stream:
            dst_provider.write('baz', stream)

        self.do_sync(expected_sync_actions=[
            DownloadSyncAction('baz')
        ])

        self.assertEqual(3, len(src_provider.get_state().files))

    def test_sync_updated_files(self):
        src_provider = self.syncer.src_provider
        dst_provider = self.syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        self.do_sync([
            UploadSyncAction('foo')
        ])

        self.assertEqual(
            b'data',
            stream_to_bytes(dst_provider.read('foo'))
        )

        with bytes_as_stream(b'updated') as stream:
            src_provider.write('foo', stream)

        self.do_sync([
            UploadSyncAction('foo')
        ])

        self.assertEqual(
            b'updated',
            stream_to_bytes(dst_provider.read('foo'))
        )

    def test_sync_deleted_files(self):
        src_provider = self.syncer.src_provider
        dst_provider = self.syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        with bytes_as_stream(b'data') as stream:
            src_provider.write('bar', stream)

        self.do_sync([
            UploadSyncAction('foo'),
            UploadSyncAction('bar'),
        ])

        self.assertEqual(2, len(dst_provider.get_state().files))

        src_provider.remove('foo')

        self.do_sync([
            RemoveOnDestinationSyncAction('foo'),
        ])

        self.assertEqual(1, len(dst_provider.get_state().files))

        dst_provider.remove('bar')

        self.do_sync([
            RemoveOnSourceSyncAction('bar'),
        ])

        self.assertEqual(0, len(dst_provider.get_state().files))

    def test_sync_moved_files(self):
        src_provider = self.syncer.src_provider
        dst_provider = self.syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        self.do_sync([
            UploadSyncAction('foo'),
        ])

        self.assertEqual(1, len(dst_provider.get_state().files))

        # move the file on source
        src_provider.move('foo', 'bar')

        self.do_sync([
            MoveOnDestinationSyncAction('foo', 'bar')
        ])

        self.assertEqual(1, len(dst_provider.get_state().files))

        # move the file on destination
        dst_provider.move('bar', 'baz')

        self.do_sync([
            MoveOnSourceSyncAction('bar', 'baz')
        ])

        self.assertEqual(1, len(dst_provider.get_state().files))

    def test_conflicting_updates(self):
        src_provider = self.syncer.src_provider
        dst_provider = self.syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo', stream)

        with bytes_as_stream(b'data') as stream:
            dst_provider.write('foo', stream)

        self.do_sync([
            ResolveConflictSyncAction('foo'),
        ])

    def test_limited_depth(self):
        src_provider = self.syncer.src_provider

        with random_bytes_stream() as stream:
            src_provider.write('file1', stream)
            src_provider.write('foo/file1', stream)
            src_provider.write('foo/file2', stream)
            src_provider.write('foo/bar/file1', stream)
            src_provider.write('foo/bar/file2', stream)

        # normally depth is set in the constructor, but it is okay currently
        # to modify it for simplicity
        self.syncer.depth = 1

        self.do_sync([
            UploadSyncAction('file1')
        ], ensure_same_state=False)

        self.syncer.depth = 2

        self.do_sync([
            ResolveConflictSyncAction('file1'),
            UploadSyncAction('foo/file1'),
            UploadSyncAction('foo/file2'),
        ], ensure_same_state=False)

    def test_move_multiple_files_with_same_hash(self):
        src_provider = self.syncer.src_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo/file1', stream)

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo/file2', stream)

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo/file3', stream)

        self.do_sync([
            UploadSyncAction('foo/file1'),
            UploadSyncAction('foo/file2'),
            UploadSyncAction('foo/file3'),
        ])

        # now move files into the new directory
        src_provider.move('foo/file1', 'bar/file1')
        src_provider.move('foo/file2', 'bar/file2')
        src_provider.move('foo/file3', 'bar/file3')

        self.do_sync([
            MoveOnDestinationSyncAction('foo/file1', 'bar/file1'),
            MoveOnDestinationSyncAction('foo/file2', 'bar/file2'),
            MoveOnDestinationSyncAction('foo/file3', 'bar/file3'),
        ])

    def test_move_multiple_files_with_filename_changes(self):
        src_provider = self.syncer.src_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo/file-is-named-like-this', stream)

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo/some-totally-different-naming', stream)

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo/boo', stream)

        self.do_sync([
            UploadSyncAction('foo/file-is-named-like-this'),
            UploadSyncAction('foo/some-totally-different-naming'),
            UploadSyncAction('foo/boo'),
        ])

        # now move files into the new directory and slightly adjust names
        src_provider.move('foo/file-is-named-like-this', 'bar/file_is_named_like_this')
        src_provider.move('foo/some-totally-different-naming', 'bar/some-totally-different-naming-changed')
        src_provider.move('foo/boo', 'bar/boo-new')

        self.do_sync([
            MoveOnDestinationSyncAction(
                'foo/file-is-named-like-this', 'bar/file_is_named_like_this'),
            MoveOnDestinationSyncAction(
                'foo/some-totally-different-naming', 'bar/some-totally-different-naming-changed'),
            MoveOnDestinationSyncAction(
                'foo/boo', 'bar/boo-new'),
        ])

    def test_case_only_filename_change(self):
        src_provider = self.syncer.src_provider
        dst_provider = self.syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo/data', stream)

        self.do_sync([
            UploadSyncAction('foo/data'),
        ])

        src_provider.move('foo/data', 'foo/Data')

        self.do_sync([
            MoveOnDestinationSyncAction('foo/data', 'foo/Data'),
        ])

        dst_provider.move('foo/Data', 'foo/DATA')

        self.do_sync([
            MoveOnSourceSyncAction('foo/Data', 'foo/DATA'),
        ])

    def test_move_same_way_on_both_sides_is_noop(self):
        src_provider = self.syncer.src_provider
        dst_provider = self.syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo/data', stream)

        self.do_sync([
            UploadSyncAction('foo/data'),
        ])

        src_provider.move('foo/data', 'bar/data')
        dst_provider.move('foo/data', 'bar/data')

        self.do_sync([
            NoopSyncAction('foo/data'),
        ])

    def test_move_on_both_sides_to_different_locations_leads_to_error(self):
        src_provider = self.syncer.src_provider
        dst_provider = self.syncer.dst_provider

        with bytes_as_stream(b'data') as stream:
            src_provider.write('foo/data', stream)

        self.do_sync([
            UploadSyncAction('foo/data'),
        ])

        src_provider.move('foo/data', 'bar/data')
        dst_provider.move('foo/data', 'baz/data')

        self.assertRaises(
            SyncError,
            self.syncer.sync
        )

    # TODO: write this tricky test where folder names are changing for case
    #  insensitive providers... there could also be multiple moves which use
    #  different folder casing for the same directory... what do we expect
    #  for these cases after all?
    # def test_case_only_moves_with_folder_name_changes(self):
    #     raise NotImplementedError


if __name__ == '__main__':
    pytest.main()
