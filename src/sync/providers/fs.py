import logging
import os.path
import shutil
import tempfile
from typing import BinaryIO, List

from sync.cache import CACHE_MISS, CacheBase, InMemoryCache
from sync.hashing import (
    HashType,
    dropbox_hash_stream,
    hash_dict,
    sha256_stream,
)
from sync.provider import (
    ConflictError,
    FileAlreadyExistsError,
    FileNotFoundProviderError,
    FolderNotFoundProviderError,
    ProviderBase,
    ProviderError,
    SafeUpdateSupportMixin,
)
from sync.providers.common import normalize_unicode, unixify_path
from sync.state import FileState, StorageState

LOGGER = logging.getLogger(__name__)


class FSProvider(ProviderBase, SafeUpdateSupportMixin):
    BUFFER_SIZE = 4096
    SUPPORTED_HASH_TYPES = [HashType.SHA256, HashType.DROPBOX_SHA256]

    def __init__(self, root_dir: str, cache: CacheBase = None):
        LOGGER.debug('init FS provider with root at "%s"', root_dir)
        self.root_dir = os.path.abspath(os.path.expanduser(root_dir))
        self.cache = cache or InMemoryCache()

        # it is not really possible to determine if file system case-sensitive
        # using os.name for instance as both Linux and MacOS will report "posix"
        # but MacOS will not be case-sensitive by default
        self.__is_case_sensitive = self.__determine_if_case_sensitive()

        LOGGER.debug("is case sensitive file system? %s", self.__is_case_sensitive)

    def get_label(self) -> str:
        return "FS(%s)" % self.root_dir

    def get_handle(self) -> str:
        return "fs-" + hash_dict(
            {
                "root_dir": self.root_dir,
            }
        )

    @staticmethod
    def __determine_if_case_sensitive():
        with tempfile.NamedTemporaryFile(suffix="case-test") as tmp_file:
            return not os.path.exists(tmp_file.name.upper())

    def is_case_sensitive(self) -> bool:
        return self.__is_case_sensitive

    def _file_state(self, rel_path: str) -> FileState:
        abs_path = self._abs_path(rel_path)
        return FileState(
            path=rel_path,
            content_hash=self.compute_hash(rel_path, HashType.SHA256),
            hash_type=HashType.SHA256,
            revision=str(os.path.getmtime(abs_path)),
        )

    def get_state(self, depth: int | None = None) -> StorageState:
        files = {}

        def walk(dir_path: str, level: int):
            LOGGER.debug('walking "%s"...', dir_path)

            if depth is not None and level > depth:
                return

            for entry in os.scandir(dir_path):
                if entry.is_file():
                    rel_path = os.path.relpath(entry.path, self.root_dir)
                    rel_path = unixify_path(rel_path)
                    rel_path = normalize_unicode(rel_path)

                    if rel_path in files:
                        raise ProviderError(
                            f"There seem to be multiple files using same name, "
                            f"but in different Unicode normalization forms. "
                            f'This is not supported. File path was "{rel_path}"'
                        )

                    files[rel_path] = self._file_state(rel_path)
                elif entry.is_dir():
                    walk(entry.path, level + 1)

        self._ensure_dir(self.root_dir)
        walk(self.root_dir, level=1)

        LOGGER.debug("discovered %d files", len(files))
        return StorageState(files)

    def _abs_path(self, path: str):
        abs_path = os.path.join(self.root_dir, path)
        abs_path = os.path.abspath(abs_path)
        abs_path = normalize_unicode(abs_path)
        if not abs_path.startswith(self.root_dir):
            raise ProviderError("path outside of root dir")
        return abs_path

    def get_file_state(self, path: str):
        try:
            return self._file_state(path)
        except FileNotFoundError:
            raise FileNotFoundProviderError(f"File not found: {path}")

    def read(self, path: str) -> BinaryIO:
        abs_path = self._abs_path(path)
        try:
            return open(abs_path, "rb")
        except FileNotFoundError:
            raise FileNotFoundProviderError(f"File not found: {path}")

    @staticmethod
    def _ensure_dir(dir_path: str):
        if not os.path.exists(dir_path):
            LOGGER.debug(f"creating directory {dir_path}...")
            # exist_ok allows to handle concurrency induced error that dir
            # is already exists
            os.makedirs(dir_path, exist_ok=True)

    def write(self, path: str, stream: BinaryIO):
        abs_path = self._abs_path(path)
        dir_path = os.path.dirname(abs_path)

        self._ensure_dir(dir_path)

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            while True:
                buffer = stream.read(self.BUFFER_SIZE)
                if not buffer:
                    break
                temp_file.write(buffer)
            temp_file.flush()
            os.fsync(temp_file.fileno())

        # now atomically move temp file
        shutil.move(temp_file.name, abs_path)

    def update(self, path: str, content: BinaryIO, revision: str) -> None:
        # not bullet-proof, but still allows to limit concurrency issues
        current_state = self._file_state(path)

        if current_state.revision != revision:
            raise ConflictError(
                f'Can not update "{path}" due to conflict as revision tag does '
                f"not match current state"
            )

        self.write(path, content)

    def remove_file(self, path: str):
        abs_path = self._abs_path(path)
        try:
            os.unlink(abs_path)
        except FileNotFoundError:
            raise FileNotFoundProviderError(f"File not found: {path}")

    def remove_folder(self, path: str):
        abs_path = self._abs_path(path)
        try:
            shutil.rmtree(abs_path)
        except FileNotFoundError:
            raise FolderNotFoundProviderError(f"Folder not found: {path}")

    def move(self, source_path: str, destination_path: str):
        source_abs_path = self._abs_path(source_path)
        destination_abs_path = self._abs_path(destination_path)
        is_case_only_change = source_path.lower() == destination_path.lower()

        if normalize_unicode(source_path) == normalize_unicode(destination_path):
            LOGGER.warning(
                'suppressing movement for "%s" since source and destination '
                "are the same after unicode normalization to NFC form",
                source_path,
            )
            return

        if self.is_case_sensitive() or not is_case_only_change:
            if os.path.exists(destination_abs_path):
                raise FileAlreadyExistsError(f"File already exists: {destination_path}")
        else:
            LOGGER.warning(
                'case-only change movement requested "%s" -> "%s"',
                source_path,
                destination_path,
            )

        # ensure destination directory if missing
        destination_dir, _ = os.path.split(destination_abs_path)
        self._ensure_dir(destination_dir)

        try:
            shutil.move(source_abs_path, destination_abs_path)
        except FileNotFoundError as err:
            raise FileNotFoundProviderError(f"File not found: {source_path}") from err

    def supported_hash_types(self) -> List[HashType]:
        return self.SUPPORTED_HASH_TYPES

    def compute_hash(self, path: str, hash_type: HashType) -> str:
        assert hash_type in self.SUPPORTED_HASH_TYPES

        abs_path = self._abs_path(path)
        if not os.path.exists(abs_path):
            raise FileNotFoundProviderError(f"File not found: {path}")

        modification_time = os.path.getmtime(abs_path)

        cache_key = "%s__%s" % (hash_type, path)
        cached_value = self.cache.get(cache_key)

        if cached_value is CACHE_MISS or cached_value[0] != modification_time:
            if cached_value is not CACHE_MISS:
                LOGGER.debug(
                    "found previous cache value for modification time %s",
                    cached_value[0],
                )

            LOGGER.debug('compute %s hash for "%s"', hash_type.value, path)

            with open(abs_path, "rb") as f:
                if hash_type == HashType.SHA256:
                    hash_value = sha256_stream(f)
                elif hash_type == HashType.DROPBOX_SHA256:
                    hash_value = dropbox_hash_stream(f)
                else:
                    raise NotImplementedError

            self.cache.set(cache_key, (modification_time, hash_value))
        else:
            _, hash_value = cached_value

        return hash_value

    def clone(self) -> "ProviderBase":
        return FSProvider(self.root_dir, self.cache)

    def close(self):
        # nothing to close
        pass
