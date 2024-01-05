import logging
import os.path
from enum import StrEnum
from typing import Dict, BinaryIO

from sync.state import FileState, StorageState

LOGGER = logging.getLogger(__name__)


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
    def get_handle(self) -> str:
        """
        Returns string that identifies provider along with critical parameters like
        directory for FS provider or other important arguments that identify the
        storage itself.
        """
        raise NotImplementedError

    def construct_state(self) -> StorageState:
        raise NotImplementedError

    def get_file_state(self, path: str) -> FileState:
        raise NotImplementedError

    def read(self, path: str) -> BinaryIO:
        raise NotImplementedError

    def write(self, path: str, content: BinaryIO) -> None:
        raise NotImplementedError

    def remove(self, path: str) -> None:
        raise NotImplementedError

    def compute_content_hash(self, content: BinaryIO) -> str:
        raise NotImplementedError


class SyncAction(StrEnum):
    DOWNLOAD = 'DOWNLOAD'
    UPLOAD = 'UPLOAD'
    REMOVE_SRC = 'REMOVE_SRC'
    REMOVE_DST = 'REMOVE_DST'
    RESOLVE_CONFLICT = 'RESOLVE_CONFLICT'
    NOOP = 'NOOP'


class SyncError(Exception):
    pass


# add selective sync
class Syncer:
    def __init__(self, src_provider: ProviderBase, dst_provider: ProviderBase, state_root_dir: str = '.state'):
        self.src_provider = src_provider
        self.dst_provider = dst_provider
        self.state_root_dir = os.path.abspath(os.path.expanduser(state_root_dir))

        if not os.path.exists(self.state_root_dir):
            LOGGER.warning('state dir does not exist -> create')
            os.makedirs(self.state_root_dir)

    def open_state_file(self, handle: str):
        abs_path = os.path.join(self.state_root_dir, handle)
        if os.path.exists(abs_path):
            with open(abs_path, 'rb') as f:
                return StorageState.load(f)
        LOGGER.warning('state file not found')
        return StorageState()

    def save_state_file(self, handle: str, state: StorageState):
        abs_path = os.path.join(self.state_root_dir, handle)
        with open(abs_path, 'wb') as f:
            return state.save(f)

    def compare(self, path: str) -> bool:
        LOGGER.debug('comparing "%s" source vs. destination...', path)

        src_file_state = self.src_provider.get_file_state(path)

        with self.dst_provider.read(path) as dst_file_stream:
            dst_file_hash = self.src_provider.compute_content_hash(dst_file_stream)

        LOGGER.debug(
            'source hash "%s", destination hash "%s"',
            src_file_state.content_hash, dst_file_hash)

        return src_file_state.content_hash == dst_file_hash

    def sync(self):
        src_provider_handle = self.src_provider.get_handle()
        dst_provider_handle = self.dst_provider.get_handle()

        LOGGER.debug('source provider handle %s', src_provider_handle)
        LOGGER.debug('destination provider handle %s', dst_provider_handle)

        src_state_snapshot = self.open_state_file(src_provider_handle)
        dst_state_snapshot = self.open_state_file(dst_provider_handle)

        src_state = self.src_provider.construct_state()
        dst_state = self.dst_provider.construct_state()

        src_diff = StorageStateDiff.compute(src_state, src_state_snapshot)
        dst_diff = StorageStateDiff.compute(dst_state, dst_state_snapshot)

        LOGGER.info('source changes: %s', {
            path: str(diff) for path, diff in src_diff.changes.items()})
        LOGGER.info('dest changes: %s', {
            path: str(diff) for path, diff in dst_diff.changes.items()})

        # some combinations are not possible w/o corrupted state like
        # ADDED/REMOVED combination means that we saw the file on destination, but
        # why it is ADDED on source then? It means we did not download it
        action_matrix = {
            (None, DiffType.ADDED): SyncAction.DOWNLOAD,
            (None, DiffType.REMOVED): SyncAction.REMOVE_SRC,
            (None, DiffType.CHANGED): SyncAction.DOWNLOAD,
            (DiffType.ADDED, None): SyncAction.UPLOAD,
            (DiffType.REMOVED, None): SyncAction.REMOVE_DST,
            (DiffType.CHANGED, None): SyncAction.UPLOAD,
            (DiffType.ADDED, DiffType.ADDED): SyncAction.RESOLVE_CONFLICT,
            (DiffType.CHANGED, DiffType.CHANGED): SyncAction.RESOLVE_CONFLICT,
            (DiffType.REMOVED, DiffType.REMOVED): SyncAction.NOOP,
        }

        # process remote changes
        changed_files = set(dst_diff.changes) | set(src_diff.changes)

        actions = {}
        for path in changed_files:
            src_diff_type = src_diff.changes.get(path, None)
            dst_diff_type = dst_diff.changes.get(path, None)
            sync_action = action_matrix.get((src_diff_type, dst_diff_type), None)

            if sync_action is None:
                raise SyncError(
                    'undecidable for "%s" source diff %s, destination %s' % (
                    path, src_diff_type, dst_diff_type))

            actions[path] = sync_action

        # running sync actions
        for path, action in actions.items():
            LOGGER.info('%s %s', action, path)
            if action == SyncAction.UPLOAD:
                stream = self.src_provider.read(path)
                self.dst_provider.write(path, stream)
                dst_state.files[path] = self.dst_provider.get_file_state(path)
            elif action == SyncAction.DOWNLOAD:
                stream = self.dst_provider.read(path)
                self.src_provider.write(path, stream)
                src_state.files[path] = self.src_provider.get_file_state(path)
            elif action == SyncAction.REMOVE_DST:
                self.dst_provider.remove(path)
                dst_state.files.pop(path)
            elif action == SyncAction.REMOVE_SRC:
                self.src_provider.remove(path)
                src_state.files.pop(path)
            elif action == SyncAction.RESOLVE_CONFLICT:
                are_equal = self.compare(path)
                if not are_equal:
                    raise SyncError('Unable to resolve conflict for "%s"' % path)
                else:
                    LOGGER.debug(
                        'resolved conflict for "%s" as files identical', path)
            elif action == SyncAction.NOOP:
                pass
            else:
                raise NotImplementedError('action %s' % action)

        LOGGER.info('saving state')

        self.save_state_file(src_provider_handle, src_state)
        self.save_state_file(dst_provider_handle, dst_state)
