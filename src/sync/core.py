import abc
import concurrent.futures
import fnmatch
import logging
import os.path
import re
from collections import Counter
from typing import Dict, Optional, Callable, BinaryIO, Tuple, List

from sync.diff import (
    DiffType, AddedDiffType, ChangedDiffType, RemovedDiffType, MovedDiffType,
    StorageStateDiff,
)
from sync.hashing import hash_dict, hash_stream
from sync.provider import ProviderBase, SafeUpdateSupportMixin
from sync.state import StorageState, SyncPairState

LOGGER = logging.getLogger(__name__)


IGNORE_FILENAMES = set(x.lower() for x in [
    '.DS_Store',
])


def should_ignore(path: str):
    head, filename = os.path.split(path)
    return filename.lower() in IGNORE_FILENAMES


class SyncAction(abc.ABC):
    TYPE = None

    def __init__(self, path):
        self.path = path

    def __repr__(self):
        return '%s("%s")' % (self.__class__.__name__, self.path)

    def __eq__(self, other):
        if not isinstance(other, SyncAction):
            return False
        return self.path == other.path


class DownloadSyncAction(SyncAction):
    TYPE = 'DOWNLOAD'


class UploadSyncAction(SyncAction):
    TYPE = 'UPLOAD'


class RemoveOnSourceSyncAction(SyncAction):
    TYPE = 'REMOVE_SRC'


class RemoveOnDestinationSyncAction(SyncAction):
    TYPE = 'REMOVE_DST'


class ResolveConflictSyncAction(SyncAction):
    TYPE = 'RESOLVE_CONFLICT'


class MoveOnSourceSyncAction(SyncAction):
    TYPE = 'MOVE_SRC'

    def __init__(self, path, new_path):
        super().__init__(path)
        self.new_path = new_path

    def __repr__(self):
        return '%s("%s", "%s")' % (
            self.__class__.__name__, self.path, self.new_path)

    def __eq__(self, other):
        if not isinstance(other, MoveOnSourceSyncAction):
            return False
        return self.path == other.path and self.new_path == other.new_path


class MoveOnDestinationSyncAction(SyncAction):
    TYPE = 'MOVE_DST'

    def __init__(self, path, new_path):
        super().__init__(path)
        self.new_path = new_path

    def __repr__(self):
        return '%s("%s", "%s")' % (
            self.__class__.__name__, self.path, self.new_path)

    def __eq__(self, other):
        if not isinstance(other, MoveOnDestinationSyncAction):
            return False
        return self.path == other.path and self.new_path == other.new_path


class NoopSyncAction(SyncAction):
    TYPE = 'NOOP'


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


class Syncer:
    def __init__(self,
                 src_provider: ProviderBase,
                 dst_provider: ProviderBase,
                 state_root_dir: str = '.state',
                 filter_glob: Optional[str] = None,
                 depth: int | None = None,
                 threads: int | None = None):
        self.src_provider: ProviderBase = src_provider
        self.dst_provider: ProviderBase = dst_provider
        self.state_root_dir: str = os.path.abspath(os.path.expanduser(state_root_dir))
        self.filter_glob: str | None = filter_glob
        self.depth: int | None = depth
        self.threads: int | None = threads

        if not os.path.exists(self.state_root_dir):
            LOGGER.warning('state dir does not exist -> create')
            os.makedirs(self.state_root_dir)

        if self.depth is not None and self.depth <= 0:
            raise ValueError('Invalid depth')

    def get_state_handle(self):
        src_handle = self.src_provider.get_handle()
        dst_handle = self.dst_provider.get_handle()

        pair_handle = hash_dict({
            'src': src_handle,
            'dst': dst_handle,
            'filter_glob': self.filter_glob,
            'depth': self.depth,
        })
        return pair_handle

    def load_state(self) -> SyncPairState:
        state_path = self.get_state_file_path()
        if os.path.exists(state_path):
            LOGGER.debug('loading state from "%s"', state_path)
            with open(state_path, 'rb') as f:
                return SyncPairState.load(f)
        LOGGER.warning('state file not found')
        return SyncPairState(
            StorageState(),
            StorageState(),
        )

    def get_state_file_path(self):
        handle = self.get_state_handle()
        return os.path.join(self.state_root_dir, handle)

    def save_state(self, state: SyncPairState):
        with open(self.get_state_file_path(), 'wb') as f:
            return state.save(f)

    def compare(self, path: str) -> bool:
        """
        Compares files at given relative path between source and destination
        providers. Note that direct comparison of content hash is not correct
        as different profiles can be using different approaches to computing
        the hash.

        Returns boolean indicating if two files are identical.
        """
        LOGGER.debug('comparing file at "%s" source vs. destination...', path)

        # see if we can compare hashes "remotely"
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

    def sync(self, dry_run: bool = False) -> List[SyncAction]:
        LOGGER.info(
            'syncing %s <---> %s%s',
            self.src_provider.get_label(),
            self.dst_provider.get_label(),
            '; filter: %s' % self.filter_glob if self.filter_glob else '',
        )

        pair_state = self.load_state()

        src_state_snapshot = pair_state.source_state
        dst_state_snapshot = pair_state.dest_state

        src_state = self.src_provider.get_state(self.depth)
        dst_state = self.dst_provider.get_state(self.depth)

        # get rid of ignored files
        def not_ignored_matcher(path):
            return not should_ignore(path)

        src_state = filter_state(src_state, not_ignored_matcher)
        dst_state = filter_state(dst_state, not_ignored_matcher)

        if self.filter_glob:
            filter_matcher = make_glob_matcher(self.filter_glob)
            src_state = filter_state(src_state, filter_matcher)
            dst_state = filter_state(dst_state, filter_matcher)

        src_full_diff = StorageStateDiff.compute(src_state, src_state_snapshot)
        dst_full_diff = StorageStateDiff.compute(dst_state, dst_state_snapshot)

        LOGGER.debug('source changes: %s', {
            path: str(diff) for path, diff in src_full_diff.changes.items()})
        LOGGER.debug('dest changes: %s', {
            path: str(diff) for path, diff in dst_full_diff.changes.items()})

        # some combinations are not possible w/o corrupted state like
        # ADDED/REMOVED combination means that we saw the file on destination, but
        # why it is ADDED on source then? It means we did not download it
        action_matrix: Dict[Tuple[DiffType | None, DiffType | None], Callable[[DiffType, DiffType], SyncAction]] = {
            (None, AddedDiffType): lambda src, dst: DownloadSyncAction(dst.path),
            (None, RemovedDiffType): lambda src, dst: RemoveOnSourceSyncAction(dst.path),
            (None, ChangedDiffType): lambda src, dst: DownloadSyncAction(dst.path),
            (AddedDiffType, None): lambda src, dst: UploadSyncAction(src.path),
            (RemovedDiffType, None): lambda src, dst: RemoveOnDestinationSyncAction(src.path),
            (ChangedDiffType, None): lambda src, dst: UploadSyncAction(src.path),
            (AddedDiffType, AddedDiffType): lambda src, dst: ResolveConflictSyncAction(src.path),
            (ChangedDiffType, ChangedDiffType): lambda src, dst: ResolveConflictSyncAction(src.path),
            (RemovedDiffType, RemovedDiffType): lambda src, dst: NoopSyncAction(src.path),

            # movements handling
            (None, MovedDiffType): lambda src, dst: MoveOnSourceSyncAction(dst.path, dst.new_path),
            (MovedDiffType, None): lambda src, dst: MoveOnDestinationSyncAction(src.path, src.new_path),
        }

        # process both source and destination changes
        changed_files = set(src_full_diff.changes) | set(dst_full_diff.changes)

        actions: Dict[str, SyncAction] = {}
        for path in changed_files:
            src_diff = src_full_diff.changes.get(path, None)
            dst_diff = dst_full_diff.changes.get(path, None)

            src_diff_type = type(src_diff) if src_diff else None
            dst_diff_type = type(dst_diff) if dst_diff else None

            sync_action_fn = action_matrix.get((src_diff_type, dst_diff_type), None)

            if sync_action_fn is None:
                raise SyncError(
                    'undecidable for "%s" source diff %s, destination %s' % (
                    path, src_diff_type, dst_diff_type))

            actions[path] = sync_action_fn(src_diff, dst_diff)

        if dry_run:
            LOGGER.warning('dry run mode!')

        def write(provider: ProviderBase, state: StorageState, path: str, stream: BinaryIO):
            cur_file_state = state.files.get(path)
            safe_update_supported = (
                cur_file_state and
                cur_file_state.revision and
                isinstance(provider, SafeUpdateSupportMixin)
            )
            if safe_update_supported:
                LOGGER.debug(
                    'safe updating "%s" (expected revision "%s")',
                    path, cur_file_state.revision)
                assert isinstance(provider, SafeUpdateSupportMixin)
                provider.update(path, stream, revision=cur_file_state.revision)
            else:  # either file is new or provider does not support concurrency safe update
                LOGGER.debug('writing file at "%s"', path)
                provider.write(path, stream)

        # TODO: store providers as threadLocal -- init when not initialized yet

        def run_action(action: SyncAction):
            LOGGER.info('apply %s', action)

            if isinstance(action, UploadSyncAction):
                stream = self.src_provider.read(action.path)
                write(self.dst_provider, dst_state, action.path, stream)
                dst_state.files[action.path] = self.dst_provider.get_file_state(action.path)
            elif isinstance(action, DownloadSyncAction):
                stream = self.dst_provider.read(action.path)
                write(self.src_provider, src_state, action.path, stream)
                src_state.files[action.path] = self.src_provider.get_file_state(action.path)
            elif isinstance(action, RemoveOnDestinationSyncAction):
                self.dst_provider.remove(action.path)
                dst_state.files.pop(action.path)
            elif isinstance(action, RemoveOnSourceSyncAction):
                self.src_provider.remove(action.path)
                src_state.files.pop(action.path)
            elif isinstance(action, ResolveConflictSyncAction):
                are_equal = self.compare(action.path)
                if not are_equal:
                    # TODO: conflict should not fully stop the sync process, we
                    #  need to process other files and report the sync issues at
                    #  the end; in the state files with conflicts should be
                    #  marked accordingly or probably should not be included into
                    #  set of files, so that they are considered "added" on both
                    #  ends and conflict resolution repeats
                    raise SyncError(
                        'Unable to resolve conflict for "%s"' % action.path)
                else:
                    LOGGER.debug(
                        'resolved conflict for "%s" as files identical',
                        action.path)
            elif isinstance(action, MoveOnSourceSyncAction):
                self.src_provider.move(action.path, action.new_path)
                src_state.files[action.new_path] = src_state.files[action.path]
                src_state.files.pop(action.path)
            elif isinstance(action, MoveOnDestinationSyncAction):
                self.dst_provider.move(action.path, action.new_path)
                dst_state.files[action.new_path] = dst_state.files[action.path]
                dst_state.files.pop(action.path)
            elif isinstance(action, NoopSyncAction):
                # no action is needed
                pass
            else:
                raise NotImplementedError('action %s' % action)

        sync_errors = []

        def run_action_wrapped(action: SyncAction):
            try:
                run_action(action)
            except Exception as exc:
                sync_errors.append(exc)
                LOGGER.error(
                    'Error happened applying action %s: %s',
                    action, exc, exc_info=True)

        # TODO: use separate provider instance for each thread
        # FIXME: Ctrl+C does not work when thread pool executor is running for
        #  some reason
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
            for action in sorted(actions.values(), key=lambda x: x.path):
                if dry_run:
                    LOGGER.info('would apply %s', action)
                    continue
                executor.submit(run_action_wrapped, action)

        if sync_errors:
            raise SyncError(
                '%d sync error occurred (see logs)' % len(sync_errors))

        if len(actions):
            counter = Counter(action.TYPE for action in actions.values())
            LOGGER.info('STATS: ' + ', '.join(
                '%s: %s' % (action, count)
                for action, count in counter.most_common()))
        else:
            LOGGER.info('no changes to sync')

        if not dry_run:
            # correctness check
            missing_on_dst = set(dst_state.files) - set(src_state.files)
            missing_on_src = set(src_state.files) - set(dst_state.files)

            if missing_on_src or missing_on_dst:
                raise SyncError(
                    'Unknown correctness error detected! '
                    'Missing on source: %s, missing on destination: %s' % (
                        missing_on_src, missing_on_dst,
                    ))

            LOGGER.debug('saving state')
            LOGGER.debug('src state: %s', src_state.files)
            LOGGER.debug('dst state: %s', dst_state.files)
            self.save_state(SyncPairState(
                src_state,
                dst_state,
            ))

        return list(actions.values())
