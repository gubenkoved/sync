import abc
import collections
import concurrent.futures
import fnmatch
import logging
import os.path
import re
import threading
import time
import typing
from collections import Counter
from typing import Dict, Optional, Callable, BinaryIO, Tuple, List

from sync.diff import (
    DiffType, AddedDiffType, ChangedDiffType, RemovedDiffType, MovedDiffType,
    StorageStateDiff,
)
from sync.hashing import hash_dict, hash_stream, HashType
from sync.provider import ProviderBase, SafeUpdateSupportMixin
from sync.state import StorageState, SyncPairState, FileState

LOGGER = logging.getLogger(__name__)


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


class RaiseErrorSyncAction(SyncAction):
    TYPE = 'RAISE_ERROR'

    def __init__(self, path, message):
        super().__init__(path)
        self.message = message


class SyncError(Exception):
    pass


def filter_state(state: StorageState, is_match: Callable[[str], bool]):
    return StorageState(files={
        path: state for path, state in state.files.items() if is_match(path)
    })


# TODO: consider actually going back to single regex as more explicit and
#  even powerful option
def make_filter(filter_expr: str) -> Callable[[str], bool]:
    """
    Given filter expression (comma or semicolon separated globs) returns a
    function that allows to figure out if a given path matches the filter or not.
    """

    def make_single_matcher(atomic_expr: str):
        atomic_expr = atomic_expr.strip()
        is_negative = False

        if atomic_expr[0] == '!':
            is_negative = True
            atomic_expr = atomic_expr[1:]

        regex_pattern = fnmatch.translate(atomic_expr)
        regex = re.compile(regex_pattern, re.IGNORECASE)

        LOGGER.debug(
            'translated glob "%s" into "%s" regex',
            atomic_expr, regex_pattern)

        def matcher(path: str):
            return bool(regex.match(path))

        return matcher, is_negative

    matchers = [
        make_single_matcher(expr) for expr
        in filter_expr.replace(';', ',').split(',')
    ]

    def result_matcher(path: str):
        # if the first matcher is positive, we do not match anything by default
        # however, if first is negative, then it is logical to assume that
        # everything is matched
        first_matcher_negative = matchers[0][1]
        result = first_matcher_negative

        for matcher, is_negative in matchers:
            is_match = matcher(path)

            if is_match:
                if not is_negative:
                    result = True
                else:  # is_negative
                    # stop at first negative match
                    return False

        return result

    return result_matcher


class Syncer:
    def __init__(self,
                 src_provider: ProviderBase,
                 dst_provider: ProviderBase,
                 state_root_dir: str = '.state',
                 filter: Optional[str] = None,
                 depth: int | None = None,
                 threads: int | None = None):
        self.src_provider: ProviderBase = src_provider
        self.dst_provider: ProviderBase = dst_provider
        self.state_root_dir: str = os.path.abspath(os.path.expanduser(state_root_dir))
        self.filter: str | None = filter
        self.depth: int | None = depth
        self.threads: int | None = threads

        if not os.path.exists(self.state_root_dir):
            LOGGER.warning('state dir does not exist -> create')
            os.makedirs(self.state_root_dir)

        if self.depth is not None and self.depth <= 0:
            raise ValueError('Invalid depth')

    # TODO: consider ability to reuse sync state when filter changes
    def get_state_handle(self):
        src_handle = self.src_provider.get_handle()
        dst_handle = self.dst_provider.get_handle()

        pair_handle = hash_dict({
            'src': src_handle,
            'dst': dst_handle,
            'filter_glob': self.filter,
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
        src_state = self.src_provider.get_file_state(path)
        dst_state = self.dst_provider.get_file_state(path)
        return self.__compare(
            path, src_state, dst_state, self.src_provider, self.dst_provider)

    @staticmethod
    def __compare(
            path: str,
            src_state: FileState, dst_state: FileState,
            src_provider: ProviderBase, dst_provider: ProviderBase) -> bool:
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
            set(src_provider.supported_hash_types()) &
            set(dst_provider.supported_hash_types()))

        if shared_hash_types:
            LOGGER.debug(
                'hashes supported by both providers: %s',
                ', '.join(str(t) for t in shared_hash_types))

            # if there are multiple shared hash types try to pick one which is
            # already calculated for both or at least for one provider
            def hash_type_preference(hash_type: HashType):
                preference = 0
                if hash_type == src_state.hash_type:
                    preference += 1
                if hash_type == dst_state.hash_type:
                    preference += 1
                return preference

            chosen_hash_type: HashType = sorted(
                shared_hash_types, key=hash_type_preference, reverse=True)[0]

            # try to get hash from the already computed file state when
            # possible to avoid provider invocation
            def compute_hash(file_state: FileState, provider: ProviderBase):
                if file_state.hash_type == chosen_hash_type:
                    return file_state.content_hash
                return provider.compute_hash(path, chosen_hash_type)

            src_hash = compute_hash(src_state, src_provider)
            dst_hash = compute_hash(dst_state, dst_provider)
        else:  # download and compute locally
            LOGGER.debug('no shared hashes, download both and compare locally')
            src_hash = hash_stream(src_provider.read(path))
            dst_hash = hash_stream(dst_provider.read(path))

        LOGGER.debug(
            'source hash "%s", destination hash "%s"',
            src_hash, dst_hash)

        return src_hash == dst_hash

    def __resolve_mutual_movement(self, src: MovedDiffType, dst: MovedDiffType) -> SyncAction:
        assert src.path == dst.path

        if src.new_path == dst.new_path:
            return NoopSyncAction(src.path)

        message = (
            f'File moved on both source and destination in different '
            f'locations; new location on source: {src.new_path}, '
            f'on destination: {dst.new_path};'
        )

        return RaiseErrorSyncAction(src.path, message)

    def sync(self, dry_run: bool = False) -> List[SyncAction]:
        LOGGER.info(
            'syncing %s <---> %s%s',
            self.src_provider.get_label(),
            self.dst_provider.get_label(),
            '; filter: %s' % self.filter if self.filter else '',
        )

        pair_state = self.load_state()

        src_state_snapshot = pair_state.source_state
        dst_state_snapshot = pair_state.dest_state

        src_state = self.src_provider.get_state(self.depth)
        dst_state = self.dst_provider.get_state(self.depth)

        if self.filter:
            filter_matcher = make_filter(self.filter)
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
        DiffProducerType = Callable[[DiffType | None, DiffType | None], SyncAction]
        action_matrix: Dict[Tuple[DiffType | None, DiffType | None], DiffProducerType] = {
            (None, AddedDiffType): lambda src, dst: DownloadSyncAction(dst.path),
            (None, RemovedDiffType): lambda src, dst: RemoveOnSourceSyncAction(dst.path),
            (None, ChangedDiffType): lambda src, dst: DownloadSyncAction(dst.path),
            (AddedDiffType, None): lambda src, dst: UploadSyncAction(src.path),
            (RemovedDiffType, None): lambda src, dst: RemoveOnDestinationSyncAction(src.path),
            (ChangedDiffType, None): lambda src, dst: UploadSyncAction(src.path),
            (AddedDiffType, AddedDiffType): lambda src, dst: ResolveConflictSyncAction(src.path),
            (ChangedDiffType, ChangedDiffType): lambda src, dst: ResolveConflictSyncAction(src.path),
            (RemovedDiffType, RemovedDiffType): lambda src, dst: NoopSyncAction(src.path),

            (ChangedDiffType, RemovedDiffType): lambda src, dst: RaiseErrorSyncAction(
                src.path, 'File changed on source, but removed on destination'),
            (RemovedDiffType, ChangedDiffType): lambda src, dst: RaiseErrorSyncAction(
                src.path, 'File removed on source, but changed on destination'),

            # movements handling
            (None, MovedDiffType): lambda src, dst: MoveOnSourceSyncAction(dst.path, dst.new_path),
            (MovedDiffType, None): lambda src, dst: MoveOnDestinationSyncAction(src.path, src.new_path),
            (MovedDiffType, MovedDiffType): self.__resolve_mutual_movement,
        }

        is_case_sensitive = (
            self.src_provider.is_case_sensitive() and
            self.dst_provider.is_case_sensitive()
        )

        def normalize_dict_keys(dictionary) -> dict[str, typing.Any]:
            normalized_keys_counter = collections.defaultdict(int)

            for key in dictionary:
                normalized_keys_counter[self.__normalize_case(key, is_case_sensitive)] += 1

            case_conflicting_keys = [
                k for k, v in normalized_keys_counter.items() if v > 1]

            if case_conflicting_keys:
                LOGGER.error('conflicting keys: %s', case_conflicting_keys)
                raise SyncError(
                    'Found multiple keys which are same after normalized '
                    'case, can not sync! '
                    'Working in case-insensitive mode because one or two '
                    'providers in sync pair are case-insensitive.')

            return {self.__normalize_case(k, is_case_sensitive): v for k, v in dictionary.items()}

        src_changes = normalize_dict_keys(src_full_diff.changes)
        dst_changes = normalize_dict_keys(dst_full_diff.changes)

        # process both source and destination changes
        changed_files = set(src_changes) | set(dst_changes)

        actions: Dict[str, SyncAction] = {}
        paths_with_errors: List[str] = []

        for path in changed_files:
            src_diff = src_changes.get(path, None)
            dst_diff = dst_changes.get(path, None)

            LOGGER.debug(
                'handling path %s, source diff: %r, destination diff: %r',
                path, src_diff, dst_diff)

            src_diff_type = type(src_diff) if src_diff else None
            dst_diff_type = type(dst_diff) if dst_diff else None

            sync_action_fn = action_matrix.get((src_diff_type, dst_diff_type), None)

            if sync_action_fn is None:
                LOGGER.error(
                    'undecidable for "%s", source diff %r, destination diff %r',
                    path, src_diff, dst_diff)
                paths_with_errors.append(path)
                continue

            actions[path] = sync_action_fn(src_diff, dst_diff)

        if paths_with_errors:
            raise SyncError(
                'Error(s) occurred for %d paths identifying sync '
                'action (see logs)' % len(paths_with_errors))

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

        thread_local = threading.local()

        def get_providers():
            if not hasattr(thread_local, 'providers'):
                LOGGER.debug('create new provider instances...')
                thread_local.providers = self.src_provider.clone(), self.dst_provider.clone()
            return thread_local.providers

        # TODO: separate out sync actions executor for better clarity
        def run_action(action: SyncAction):
            LOGGER.info('apply %s', action)

            src_provider, dst_provider = get_providers()

            if isinstance(action, UploadSyncAction):
                with src_provider.read(action.path) as stream:
                    write(dst_provider, dst_state, action.path, stream)
                dst_state.files[action.path] = dst_provider.get_file_state(action.path)
            elif isinstance(action, DownloadSyncAction):
                with dst_provider.read(action.path) as stream:
                    write(src_provider, src_state, action.path, stream)
                src_state.files[action.path] = src_provider.get_file_state(action.path)
            elif isinstance(action, RemoveOnDestinationSyncAction):
                dst_provider.remove(action.path)
                dst_state.files.pop(action.path)
            elif isinstance(action, RemoveOnSourceSyncAction):
                src_provider.remove(action.path)
                src_state.files.pop(action.path)
            elif isinstance(action, ResolveConflictSyncAction):
                src_file_state = src_state.files.get(action.path)
                dst_file_state = dst_state.files.get(action.path)

                are_equal = self.__compare(
                    action.path, src_file_state, dst_file_state, src_provider, dst_provider)

                if not are_equal:
                    raise SyncError(
                        f'Unable to resolve conflict for "{action.path}" -- files are '
                        'different!')

                LOGGER.debug(
                    'resolved conflict for "%s" as files identical',
                    action.path)
            elif isinstance(action, MoveOnSourceSyncAction):
                src_provider.move(action.path, action.new_path)
                src_state.files[action.new_path] = src_state.files[action.path]
                src_state.files.pop(action.path)
            elif isinstance(action, MoveOnDestinationSyncAction):
                dst_provider.move(action.path, action.new_path)
                dst_state.files[action.new_path] = dst_state.files[action.path]
                dst_state.files.pop(action.path)
            elif isinstance(action, NoopSyncAction):
                # no action is needed
                pass
            elif isinstance(action, RaiseErrorSyncAction):
                raise SyncError(
                    f'error occurred for path "{action.path}": {action.message}'
                )
            else:
                raise NotImplementedError(f'action {action}')

        sync_errors = []

        def run_action_wrapped(action: SyncAction):
            try:
                run_action(action)
            except Exception as exc:
                sync_errors.append(exc)
                LOGGER.error(
                    'Error happened applying action %s: %s',
                    action, exc, exc_info=True)

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=self.threads, thread_name_prefix='worker') as executor:
            futures = []
            for action in sorted(actions.values(), key=lambda x: x.path):
                if dry_run:
                    LOGGER.info('would apply %s', action)
                    continue
                futures.append(executor.submit(run_action_wrapped, action))

            try:
                # wait for all actions to run to completion
                # this explicit wait is needed in order to support the interruption
                # w/o having all futures to be resolved
                wait_round = 1
                while True:
                    all_done = all(future.done() for future in futures)
                    if all_done:
                        break
                    time.sleep(min(5.0, 0.010 * (wait_round ** 1.5)))
                    LOGGER.debug('waiting for all sync actions to complete...')
                    wait_round += 1
            except KeyboardInterrupt:
                LOGGER.warning('interrupted, stop applying sync actions!')
                executor.shutdown(wait=False, cancel_futures=True)
                raise  # reraise the exception

        LOGGER.debug('all sync actions completed')

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
            self.__correctness_check(src_state, dst_state, is_case_sensitive)

            LOGGER.debug('saving state')
            LOGGER.debug('src state: %s', src_state.files)
            LOGGER.debug('dst state: %s', dst_state.files)
            self.save_state(SyncPairState(
                src_state,
                dst_state,
            ))

        return list(actions.values())

    @staticmethod
    def __normalize_case(path: str, is_case_sensitive: bool = False):
        if is_case_sensitive:
            return path
        return path.lower()

    @staticmethod
    def __correctness_check(
            src_state: StorageState, dst_state: StorageState,
            is_case_sensitive: bool):
        src_files = set(
            Syncer.__normalize_case(path, is_case_sensitive)
            for path in src_state.files)
        dst_files = set(
            Syncer.__normalize_case(path, is_case_sensitive)
            for path in dst_state.files)

        missing_on_dst = dst_files - src_files
        missing_on_src = src_files - dst_files

        if missing_on_src or missing_on_dst:
            raise SyncError(
                'Unknown correctness error detected! '
                'Missing on source: %s, missing on destination: %s' % (
                    missing_on_src, missing_on_dst,
                ))
