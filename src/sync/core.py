import os.path
from enum import StrEnum
from typing import Dict, BinaryIO
import pickle

import logging


LOGGER = logging.getLogger(__name__)


class FileState:
    def __init__(self, content_hash: str):
        self.content_hash = content_hash

    def __repr__(self):
        return '<FileState hash="%s">' % self.content_hash


class StorageState:
    def __init__(self):
        self.files: Dict[str, FileState] = {}

    def save(self, f: BinaryIO):
        pickle.dump(self, f)

    @staticmethod
    def load(f: BinaryIO) -> 'StorageState':
        obj = pickle.load(f)
        assert isinstance(obj, StorageState)
        return obj


class DiffType(StrEnum):
    ADDED = 'added'
    REMOVED = 'removed'
    CHANGED = 'changed'


class StorageStateDiff:
    def __init__(self, changes: Dict[str, DiffType]):
        self.changes: Dict[str, DiffType] = changes

    @staticmethod
    def compute(current: StorageState, baseline: StorageState) -> 'StorageStateDiff':
        changes = {}

        for path, state in current.files.items():
            if path not in baseline.files:
                changes[path] = DiffType.ADDED
            else:
                if state.content_hash != baseline.files[path].content_hash:
                    changes[path] = DiffType.CHANGED

        for path in baseline.files:
            if path not in current.files:
                changes[path] = DiffType.REMOVED

        return StorageStateDiff(changes)


class ProviderBase:
    def construct_state(self) -> StorageState:
        raise NotImplementedError

    def read(self, path: str) -> BinaryIO:
        raise NotImplementedError

    def write(self, path: str, content: BinaryIO):
        raise NotImplementedError

    def remove(self, path: str):
        raise NotImplementedError


class FSProvider(ProviderBase):
    pass


class DropboxProvider(ProviderBase):
    pass


class SyncAction(StrEnum):
    DOWNLOAD = 'download'
    UPLOAD = 'upload'
    REMOVE_SRC = 'remove_src'
    REMOVE_DST = 'remove_dst'
    NOOP = 'noop'


# add selective sync
class Syncer:
    def __init__(self, src_provider: ProviderBase, dst_provider: ProviderBase):
        self.src_provider = src_provider
        self.dst_provider = dst_provider

    def open_state_file(self, path: str):
        if os.path.exists(path):
            with open(path, 'rb') as f:
                return StorageState.load(f)
        return StorageState()

    def sync(self):
        src_state_snapshot = self.open_state_file('src.state')
        dst_state_snapshot = self.open_state_file('dst.state')

        src_state = self.src_provider.construct_state()
        dst_state = self.dst_provider.construct_state()

        src_diff = StorageStateDiff.compute(src_state, src_state_snapshot)
        dst_diff = StorageStateDiff.compute(dst_state, dst_state_snapshot)

        LOGGER.info('SRC CHANGES: %s', src_diff.changes)
        LOGGER.info('DEST CHANGES: %s', dst_diff.changes)

        actions = {}

        # TODO: handle conflicts via hashes comparison
        #  (download remote version first)

        # process remote changes
        for path, diff_type in dst_diff.changes.items():
            if diff_type == DiffType.ADDED:
                assert path not in src_state.files
                actions[path] = SyncAction.DOWNLOAD
            elif diff_type == DiffType.REMOVED:
                if path in src_state.files:
                    actions[path] = SyncAction.REMOVE_SRC
            elif diff_type == DiffType.CHANGED:
                assert path not in src_diff.changes, 'not supported yet'
                actions[path] = SyncAction.DOWNLOAD

        # process local changes
        for path, diff_type in src_diff.changes.items():
            if diff_type == DiffType.ADDED:
                assert path not in dst_state.files
                actions[path] = SyncAction.UPLOAD
            elif diff_type == DiffType.REMOVED:
                if path in dst_state.files:
                    actions[path] = SyncAction.REMOVE_DST
            elif diff_type == DiffType.CHANGED:
                assert path not in dst_diff.changes, 'not supported yet'
                actions[path] = SyncAction.UPLOAD

        # running sync actions
        # TODO: update the state which applying actions, make sure we do not
        #  directly compare/carry content hash between providers
        for path, action in actions.items():
            LOGGER.info('%s %s', action, path)
            if action == SyncAction.UPLOAD:
                stream = self.src_provider.read(path)
                self.dst_provider.write(path, stream)
                dst_state.files[path] = src_state.files[path]
            elif action == SyncAction.DOWNLOAD:
                stream = self.dst_provider.read(path)
                self.src_provider.write(path, stream)
                src_state.files[path] = dst_state.files[path]
            elif action == SyncAction.REMOVE_DST:
                self.dst_provider.remove(path)
                dst_state.files.pop(path)
            elif action == SyncAction.REMOVE_SRC:
                self.src_provider.remove(path)
                src_state.files.pop(path)

        with open('src.state', 'wb') as f:
            src_state.save(f)

        with open('dst.state', 'wb') as f:
            dst_state.save(f)
