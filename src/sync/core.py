import fnmatch
import logging
import os.path
import re
from abc import abstractmethod, ABC
from collections import Counter
from enum import StrEnum
from typing import Dict, BinaryIO, Optional, Callable, List

from sync.hashing import hash_dict, hash_stream, HashType
from sync.state import FileState, StorageState, SyncPairState

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


IGNORE_FILENAMES = set(x.lower() for x in [
    '.DS_Store',
])


def should_ignore(path: str):
    head, filename = os.path.split(path)
    return filename.lower() in IGNORE_FILENAMES


class ProviderError(Exception):
    pass


class FileNotFoundProviderError(ProviderError):
    pass


class ProviderBase(ABC):
    @abstractmethod
    def get_handle(self) -> str:
        """
        Returns string that identifies provider along with critical parameters like
        directory for FS provider or other important arguments that identify the
        storage itself.
        """
        raise NotImplementedError

    @abstractmethod
    def get_state(self) -> StorageState:
        raise NotImplementedError

    @abstractmethod
    def get_file_state(self, path: str) -> FileState:
        raise NotImplementedError

    @abstractmethod
    def read(self, path: str) -> BinaryIO:
        raise NotImplementedError

    @abstractmethod
    def write(self, path: str, content: BinaryIO) -> None:
        raise NotImplementedError

    @abstractmethod
    def remove(self, path: str) -> None:
        raise NotImplementedError

    def supported_hash_types(self) -> List[HashType]:
        return []

    @abstractmethod
    def compute_hash(self, path: str, hash_type: HashType) -> str:
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


def filter_state(state: StorageState, is_match: Callable[[str], bool]):
    return StorageState(files={
        path: state for path, state in state.files.items() if is_match(path)
    })


def make_regex_matcher(pattern: str) -> Callable[[str], bool]:
    regex = re.compile(pattern, re.IGNORECASE)

    def matcher(path: str):
        return bool(regex.match(path))

    return matcher


def make_glob_matcher(glob_pattern: str) -> Callable[[str], bool]:
    regex_pattern = fnmatch.translate(glob_pattern)
    regex = re.compile(regex_pattern, re.IGNORECASE)

    def matcher(path: str):
        return bool(regex.match(path))

    return matcher


# TODO: detect MOVEMENT via DELETE/ADD pair for the file with same content hash
class Syncer:
    def __init__(self,
                 src_provider: ProviderBase,
                 dst_provider: ProviderBase,
                 state_root_dir: str = '.state',
                 filter_glob: Optional[str] = None):
        self.src_provider = src_provider
        self.dst_provider = dst_provider
        self.state_root_dir = os.path.abspath(os.path.expanduser(state_root_dir))
        self.filter_glob = filter_glob

        if not os.path.exists(self.state_root_dir):
            LOGGER.warning('state dir does not exist -> create')
            os.makedirs(self.state_root_dir)

        if getattr(src_provider, 'depth') != getattr(dst_provider, 'depth'):
            raise SyncError('Depth mismatch between providers')

    def get_state_handle(self):
        src_handle = self.src_provider.get_handle()
        dst_handle = self.dst_provider.get_handle()

        pair_handle = hash_dict({
            'src': src_handle,
            'dst': dst_handle,
            'filter_glob': self.filter_glob,
        })
        return pair_handle

    def load_state(self) -> SyncPairState:
        handle = self.get_state_handle()
        abs_path = os.path.join(self.state_root_dir, handle)
        if os.path.exists(abs_path):
            LOGGER.debug('loading state from "%s"', abs_path)
            with open(abs_path, 'rb') as f:
                return SyncPairState.load(f)
        LOGGER.warning('state file not found')
        return SyncPairState(
            StorageState(),
            StorageState(),
        )

    def save_state(self, state: SyncPairState):
        handle = self.get_state_handle()
        abs_path = os.path.join(self.state_root_dir, handle)
        with open(abs_path, 'wb') as f:
            return state.save(f)

    def compare(self, path: str) -> bool:
        LOGGER.debug('comparing "%s" source vs. destination...', path)

        # see if we can compare hashses "remotely"
        shared_hash_types = (
            set(self.src_provider.supported_hash_types()) &
            set(self.dst_provider.supported_hash_types()))

        if shared_hash_types:
            LOGGER.debug(
                'hashes supported by both providers: %s',
                ', '.join(str(t) for t in shared_hash_types))
            hash_type = list(shared_hash_types)[0]
            src_hash = self.src_provider.compute_hash(path, hash_type)
            dst_hash = self.dst_provider.compute_hash(path, hash_type)
        else:  # download and compute locally
            LOGGER.debug('no shared hashes, compare locally: %s', shared_hash_types)
            src_hash = hash_stream(self.src_provider.read(path))
            dst_hash = hash_stream(self.dst_provider.read(path))

        LOGGER.debug(
            'source hash "%s", destination hash "%s"',
            src_hash, dst_hash)

        return src_hash == dst_hash

    def sync(self, dry_run: bool = False):
        pair_state = self.load_state()

        src_state_snapshot = pair_state.source_state
        dst_state_snapshot = pair_state.dest_state

        src_state = self.src_provider.get_state()
        dst_state = self.dst_provider.get_state()

        # get rid of ignored files
        def not_ignored_matcher(path):
            return not should_ignore(path)

        src_state = filter_state(src_state, not_ignored_matcher)
        dst_state = filter_state(dst_state, not_ignored_matcher)

        if self.filter_glob:
            filter_matcher = make_glob_matcher(self.filter_glob)
            src_state = filter_state(src_state, filter_matcher)
            dst_state = filter_state(dst_state, filter_matcher)

        src_diff = StorageStateDiff.compute(src_state, src_state_snapshot)
        dst_diff = StorageStateDiff.compute(dst_state, dst_state_snapshot)

        LOGGER.debug('source changes: %s', {
            path: str(diff) for path, diff in src_diff.changes.items()})
        LOGGER.debug('dest changes: %s', {
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

        if dry_run:
            LOGGER.warning('dry run mode!')

        for path, action in actions.items():
            LOGGER.info('%s %s', action, path)

            if dry_run:
                continue

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
                    # TODO: conflict should not fully stop the sync process, we
                    #  need to process other files and report the sync issues at
                    #  the end; in the state files with conflicts should be
                    #  marked accordingly or probably should not be included into
                    #  set of files, so that they are considered "added" on both
                    #  ends and conflict resolution repeats
                    raise SyncError('Unable to resolve conflict for "%s"' % path)
                else:
                    LOGGER.debug(
                        'resolved conflict for "%s" as files identical', path)
            elif action == SyncAction.NOOP:
                pass
            else:
                raise NotImplementedError('action %s' % action)

        action_count = sum(1 for _, action in actions.items() if action != SyncAction.NOOP)

        if action_count > 0:
            counter = Counter(action for action in actions.values() if action != SyncAction.NOOP)
            LOGGER.info('STATS: ' + ','.join('%s: %s' % (action, count) for action, count in counter.most_common()))
        else:
            LOGGER.info('no changes to sync')

        if not dry_run:
            LOGGER.debug('saving state')
            self.save_state(SyncPairState(
                src_state,
                dst_state,
            ))
