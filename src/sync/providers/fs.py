import logging
import os.path
from typing import BinaryIO

from sync.core import ProviderBase, StorageState, FileState, SyncError
from sync.hashing import hash_stream, hash_dict

LOGGER = logging.getLogger(__name__)


# TODO: provider should be able to have cache store
#  (e.g. to avoid recomputing hashes), this probably can be united with the
#  other provider state
# TODO: handle empty directories? git does not handle that...
class FSProvider(ProviderBase):
    BUFFER_SIZE = 4096

    def __init__(self, root_dir: str):
        LOGGER.debug('init FS provider with root at "%s"', root_dir)
        self.root_dir = os.path.abspath(os.path.expanduser(root_dir))

        if not os.path.exists(self.root_dir):
            raise SyncError('root directory "%s" does not exist' % self.root_dir)

    def get_handle(self) -> str:
        return 'fs-' + hash_dict({
            'root_dif': self.root_dir,
        })

    def get_state(self) -> StorageState:
        state = StorageState()
        LOGGER.debug('walking "%s"...', self.root_dir)
        for parent_dir_name, dir_names, file_names in os.walk(self.root_dir):
            for file_name in file_names:
                abs_path = os.path.join(parent_dir_name, file_name)
                rel_path = os.path.relpath(abs_path, self.root_dir)
                state.files[rel_path] = FileState(
                    content_hash=self._file_hash(abs_path)
                )
        LOGGER.debug('discovered %d files', len(state.files))
        return state

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

    def compute_content_hash(self, content: BinaryIO) -> str:
        return hash_stream(content)
