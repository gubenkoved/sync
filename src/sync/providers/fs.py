import logging
import os.path
from typing import BinaryIO, List, Optional

from sync.core import ProviderBase, StorageState, FileState, SyncError
from sync.hashing import (
    hash_stream, hash_dict, HashType,
    sha256_stream, dropbox_hash_stream,
)

LOGGER = logging.getLogger(__name__)


# TODO: provider should be able to have cache store
#  (e.g. to avoid recomputing hashes when there seem to be no reason to do so, like
#  both size and modification time is unchanged), this probably can be united with
#  the other provider state
# TODO: handle empty directories? git does not handle that...
class FSProvider(ProviderBase):
    BUFFER_SIZE = 4096
    SUPPORTED_HASH_TYPES = [HashType.SHA256, HashType.DROPBOX_SHA256]

    def __init__(self, root_dir: str, depth: Optional[int] = None):
        if depth is not None:
            if depth <= 0:
                raise ValueError('invalid depth value')

        LOGGER.debug('init FS provider with root at "%s"', root_dir)
        self.root_dir = os.path.abspath(os.path.expanduser(root_dir))
        self.depth = depth

        if not os.path.exists(self.root_dir):
            raise SyncError('root directory "%s" does not exist' % self.root_dir)

    def get_handle(self) -> str:
        return 'fs-' + hash_dict({
            'root_dif': self.root_dir,
            'depth': self.depth,
        })

    def get_state(self) -> StorageState:
        files = {}

        def walk(dir_path: str, level: int):
            LOGGER.debug('walking "%s"...', dir_path)

            if self.depth is not None and level > self.depth:
                return

            for entry in os.scandir(dir_path):
                if entry.is_file():
                    abs_path = entry.path
                    rel_path = os.path.relpath(abs_path, self.root_dir)
                    files[rel_path] = FileState(
                        content_hash=self._file_hash(abs_path)
                    )
                elif entry.is_dir():
                    walk(entry.path, level + 1)

        walk(self.root_dir, level=1)

        LOGGER.debug('discovered %d files', len(files))
        return StorageState(files)

    def get_file_state(self, path: str):
        abs_path = os.path.join(self.root_dir, path)
        return FileState(
            content_hash=self._file_hash(abs_path)
        )

    def _file_hash(self, path):
        LOGGER.debug('compute hash for "%s"', path)
        abs_path = os.path.join(self.root_dir, path)
        with open(abs_path, 'rb') as f:
            return hash_stream(f)

    def read(self, path: str) -> BinaryIO:
        abs_path = os.path.join(self.root_dir, path)
        return open(abs_path, 'rb')

    # TODO: write to temp file, then swap
    def write(self, path: str, stream: BinaryIO):
        abs_path = os.path.join(self.root_dir, path)
        dir_path = os.path.dirname(abs_path)

        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

        with open(abs_path, 'wb') as f:
            while True:
                buffer = stream.read(self.BUFFER_SIZE)
                if not buffer:
                    break
                f.write(buffer)

    def remove(self, path: str):
        abs_path = os.path.join(self.root_dir, path)
        os.unlink(abs_path)

    def supported_hash_types(self) -> List[HashType]:
        return self.SUPPORTED_HASH_TYPES

    def compute_hash(self, path: str, hash_type: HashType) -> str:
        assert hash_type in self.SUPPORTED_HASH_TYPES

        abs_path = os.path.join(self.root_dir, path)
        with open(abs_path, 'rb') as f:
            if hash_type == HashType.SHA256:
                return sha256_stream(f)
            elif hash_type == HashType.DROPBOX_SHA256:
                return dropbox_hash_stream(f)
            else:
                raise NotImplementedError
