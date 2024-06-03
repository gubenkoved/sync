import abc
import collections
import logging
from typing import Dict, List

from sync.state import StorageState

LOGGER = logging.getLogger(__name__)


class DiffType(abc.ABC):
    TYPE = None

    def __init__(self, path):
        self.path = path

    def __repr__(self):
        return '%s("%s")' % (
            self.__class__.__name__, self.path)


class AddedDiffType(DiffType):
    TYPE = 'ADDED'


class RemovedDiffType(DiffType):
    TYPE = 'REMOVED'


class ChangedDiffType(DiffType):
    TYPE = 'CHANGED'


class MovedDiffType(DiffType):
    TYPE = 'MOVED'

    def __init__(self, path, new_path):
        super().__init__(path)
        self.new_path = new_path

    def __repr__(self):
        return '%s("%s", "%s")' % (
            self.__class__.__name__, self.path, self.new_path)


class StorageStateDiff:
    def __init__(self, changes: Dict[str, DiffType]):
        self.changes: Dict[str, DiffType] = changes

    @staticmethod
    def compute(current: StorageState, baseline: StorageState) -> 'StorageStateDiff':
        changes = {}

        for path, state in current.files.items():
            if path not in baseline.files:
                changes[path] = AddedDiffType(path)
            else:
                if state.content_hash != baseline.files[path].content_hash:
                    changes[path] = ChangedDiffType(path)

        for path in baseline.files:
            if path not in current.files:
                changes[path] = RemovedDiffType(path)

        LOGGER.debug('raw changes: %s', changes)

        # construct hash to paths for added and removed diff types
        added_removed_by_hash: Dict[str, List[DiffType]] = collections.defaultdict(list)
        for path, diff in changes.items():
            if isinstance(diff, AddedDiffType):
                added_removed_by_hash[current.files[diff.path].content_hash].append(diff)
            elif isinstance(diff, RemovedDiffType):
                added_removed_by_hash[baseline.files[diff.path].content_hash].append(diff)

        # detect file movement
        for content_hash, diffs in added_removed_by_hash.items():
            # TODO: implement more intelligent movement detector when multiple
            #  files with the same content are moved -- there will be as many
            #  added as removed diffs and we can try to do some best match based
            #  on heuristic (exact allocation does not matter too much given
            #  content is the same)
            if len(diffs) == 2:
                diff_types = {type(diff) for diff in diffs}

                if diff_types == {AddedDiffType, RemovedDiffType}:
                    if isinstance(diffs[0], AddedDiffType):
                        added_diff, removed_diff = diffs[0], diffs[1]
                    else:
                        added_diff, removed_diff = diffs[1], diffs[0]

                    LOGGER.info(
                        'detected file movement "%s" --> "%s" (hash %s)',
                        removed_diff.path, added_diff.path, content_hash)

                    del changes[added_diff.path]
                    changes[removed_diff.path] = MovedDiffType(
                        removed_diff.path, added_diff.path)

            if len(diffs) > 2:
                LOGGER.warning(
                    'multiple diff types detected for the same content hash "%s": %s',
                    content_hash, diffs)

        return StorageStateDiff(changes)
