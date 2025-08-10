import io
import logging
import time
from typing import BinaryIO, List, Optional
import uuid

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import FileMetadata, FolderMetadata, WriteMode

from sync.hashing import HashType, hash_dict
from sync.provider import (
    ConflictError,
    FileAlreadyExistsError,
    FileNotFoundProviderError,
    ProviderBase,
    ProviderError,
    SafeUpdateSupportMixin,
)
from sync.providers.common import normalize_unicode, path_join, relative_path
from sync.state import FileState, StorageState

LOGGER = logging.getLogger(__name__)
LISTING_LIMIT = 1000


class DropboxProvider(ProviderBase, SafeUpdateSupportMixin):
    SUPPORTED_HASH_TYPES = [HashType.DROPBOX_SHA256]

    def __init__(self, account_id: str, token: str, root_dir: str,
                 is_refresh_token=False, app_key: Optional[str] = None,
                 app_secret: Optional[str] = None):
        self.account_id = account_id
        self.root_dir = root_dir
        self.token = token
        self.is_refresh_token = is_refresh_token
        self.app_key = app_key
        self.app_secret = app_secret
        self._dropbox = None

    def get_label(self) -> str:
        return 'DBX(%s)' % self.root_dir

    def get_handle(self) -> str:
        return 'd-' + hash_dict({
            'account_id': self.account_id,
            'root_dir': self.root_dir,
        })

    def is_case_sensitive(self) -> bool:
        # Dropbox is case-insensitive, it makes efforts to be case-preserving
        # https://www.dropboxforum.com/t5/Dropbox-API-Support-Feedback/Case-Sensitivity-in-API-2/td-p/191279
        return False

    def _get_dropbox(self) -> dropbox.Dropbox:
        if self._dropbox is None:
            if not self.is_refresh_token:
                self._dropbox = dropbox.Dropbox(
                    oauth2_access_token=self.token
                )
            else:
                self._dropbox = dropbox.Dropbox(
                    oauth2_refresh_token=self.token,
                    app_key=self.app_key,
                    app_secret=self.app_secret,
                )
        assert self._dropbox is not None
        return self._dropbox

    def _get_full_path(self, path: str):
        full_path = path_join(self.root_dir, path)
        self.__ensure_inside_root(full_path)
        return full_path

    @staticmethod
    def __dir(dir_path: str) -> str:
        if dir_path == '/':
            return ''  # by Dropbox convention
        return dir_path

    def _ensure_root_dir(self, dbx: dropbox.Dropbox):
        try:
            dbx.files_list_folder(self.__dir(self.root_dir), limit=1)
        except ApiError as err:
            if 'not_found' in str(err):
                LOGGER.info('root directory was not found -> create')
                dbx.files_create_folder_v2(self.root_dir)

    def _list_folder(
            self, dbx: dropbox.Dropbox, path: str, recursive: bool = False):
        LOGGER.debug('listing folder %s (recursive? %s)', path, recursive)
        entries = []
        list_result = dbx.files_list_folder(
            self.__dir(path), recursive=recursive, limit=LISTING_LIMIT)
        LOGGER.debug('retrieved %s entries', len(list_result.entries))
        entries.extend(list_result.entries)
        while list_result.has_more:
            list_result = dbx.files_list_folder_continue(list_result.cursor)
            LOGGER.debug('retrieved %s entries (continuation)', len(list_result.entries))
            entries.extend(list_result.entries)
        return entries

    @staticmethod
    def _file_metadata_to_file_state(entry: FileMetadata):
        return FileState(
            content_hash=entry.content_hash,
            hash_type=HashType.DROPBOX_SHA256,
            revision=entry.rev,
        )

    def __ensure_inside_root(self, full_path: str):
        # for some reason Dropbox can return entries with different casing
        # so when we check we lowercase both even though paths on Unix
        # are case-sensitive and not sensitive on Windows
        assert full_path.lower().startswith(self.root_dir.lower()), \
            'Full path outside of root dir (%s): "%s"' % (
                self.root_dir, full_path)

    def __get_state_walking(self, max_depth: int):
        dbx = self._get_dropbox()
        files = {}

        def walk(path: str, depth: int):
            if depth > max_depth:
                return

            for entry in self._list_folder(dbx, path):
                if isinstance(entry, FileMetadata):
                    full_path = entry.path_display
                    self.__ensure_inside_root(full_path)
                    rel_path = relative_path(full_path, self.root_dir)
                    files[rel_path] = self._file_metadata_to_file_state(entry)
                elif isinstance(entry, FolderMetadata):
                    walk(entry.path_display, depth + 1)

        self._ensure_root_dir(dbx)
        walk(self.root_dir, depth=1)
        return StorageState(files)

    def __get_state(self):
        dbx = self._get_dropbox()
        files = {}
        self._ensure_root_dir(dbx)

        for entry in self._list_folder(dbx, self.root_dir, recursive=True):
            if isinstance(entry, FileMetadata):
                full_path = entry.path_display
                self.__ensure_inside_root(full_path)
                rel_path = relative_path(full_path, self.root_dir)
                files[rel_path] = self._file_metadata_to_file_state(entry)

        return StorageState(files)

    def get_state(self, depth: int | None = None) -> StorageState:
        if depth is not None:
            return self.__get_state_walking(depth)
        return self.__get_state()

    def get_file_state(self, path: str) -> FileState:
        dbx = self._get_dropbox()
        full_path = self._get_full_path(path)

        try:
            entry = dbx.files_get_metadata(full_path)
            assert isinstance(entry, FileMetadata)
            return self._file_metadata_to_file_state(entry)
        except ApiError as err:
            if 'not_found' in str(err):
                raise FileNotFoundProviderError(f'File not found at {full_path}') from err
            raise

    def read(self, path: str) -> BinaryIO:
        dbx = self._get_dropbox()
        full_path = self._get_full_path(path)
        try:
            metadata, response = dbx.files_download(full_path)
            return io.BytesIO(response.content)
        except ApiError as err:
            if 'not_found' in str(err):
                raise FileNotFoundProviderError(f'File not found at {full_path}') from err
            raise

    def write(self, path: str, content: BinaryIO) -> None:
        dbx = self._get_dropbox()
        full_path = self._get_full_path(path)
        file_bytes = content.read()
        dbx.files_upload(file_bytes, full_path, mode=WriteMode.overwrite)

    def update(self, path: str, content: BinaryIO, revision: str) -> None:
        dbx = self._get_dropbox()
        full_path = self._get_full_path(path)
        file_bytes = content.read()
        try:
            dbx.files_upload(file_bytes, full_path, mode=WriteMode.update(revision))
        except ApiError as err:
            raise ConflictError(
                f'Can not update "{path}" due to conflict as revision tag does '
                f'not match current state') from err

    def remove(self, path: str) -> None:
        dbx = self._get_dropbox()
        full_path = self._get_full_path(path)
        try:
            dbx.files_delete_v2(full_path)
        except ApiError as err:
            if 'not_found' in str(err):
                raise FileNotFoundProviderError(f'File not found at {full_path}') from err
            raise

    @staticmethod
    def __move_wrapped(dbx: dropbox.Dropbox, src_path: str, dst_path: str):
        attempt = 0
        while True:
            attempt += 1
            try:
                dbx.files_move_v2(src_path, dst_path)
                break
            except ApiError as err:
                if attempt <= 5 and 'too_many_write_operations' in str(err):
                    back_off_time = 1.0 * (2 ** (attempt - 1))
                    LOGGER.warning(
                        'Got "too_many_write_operations" error, '
                        'attempting retry in %.1f seconds (attempt %d)...',
                        back_off_time, attempt)
                    time.sleep(back_off_time)
                    continue

                # reraise other errors or if attempts exhausted
                raise

    def move(self, source_path: str, destination_path: str) -> None:
        dbx = self._get_dropbox()

        if normalize_unicode(source_path) == normalize_unicode(destination_path):
            LOGGER.warning(
                'suppressing movement for "%s" since source and destination '
                'are the same after unicode normalization to NFC form', source_path)
            return

        source_full_path = self._get_full_path(source_path)
        destination_full_path = self._get_full_path(destination_path)
        is_case_only_change = source_full_path.lower() == destination_full_path.lower()

        try:
            # https://www.dropbox.com/developers/documentation/http/documentation#files-move
            # note that we do not currently support case-only renaming
            if not is_case_only_change:
                self.__move_wrapped(dbx, source_full_path, destination_full_path)
            else:
                LOGGER.warning(
                    'case-only change movement requested "%s" -> "%s", '
                    'will use temporary path', source_path, destination_path)
                uniquifier = '.moving.' + str(uuid.uuid4())[:8]
                intermediary_path = destination_full_path + uniquifier
                self.__move_wrapped(dbx, source_full_path, intermediary_path)
                self.__move_wrapped(dbx, intermediary_path, destination_full_path)
        except ApiError as err:
            if 'not_found' in str(err):
                raise FileNotFoundProviderError(
                    f'File not found at {source_full_path}') from err
            elif 'conflict' in str(err):
                raise FileAlreadyExistsError(
                    f'File already exists at {destination_full_path}') from err
            raise

    def supported_hash_types(self) -> List[HashType]:
        return self.SUPPORTED_HASH_TYPES

    def compute_hash(self, path: str, hash_type: HashType) -> str:
        dbx = self._get_dropbox()
        full_path = self._get_full_path(path)
        result = dbx.files_get_metadata(full_path)
        if not isinstance(result, FileMetadata):
            raise ProviderError('Expected file by path "%s"' % path)
        return result.content_hash

    def clone(self) -> 'ProviderBase':
        return DropboxProvider(
            self.account_id,
            self.token,
            self.root_dir,
            self.is_refresh_token,
            self.app_key,
            self.app_secret,
        )
