import io
import logging
from typing import BinaryIO, Optional, List

import dropbox
from dropbox.exceptions import ApiError
from dropbox.files import FileMetadata, FolderMetadata, WriteMode

from sync.hashing import hash_dict, HashType
from sync.provider import (
    ProviderBase,
    ProviderError,
    FileNotFoundProviderError,
    FileAlreadyExistsError,
    SafeUpdateSupportMixin,
    ConflictError,
)
from sync.providers.common import (
    path_join, relative_path,
)
from sync.state import FileState, StorageState

LOGGER = logging.getLogger(__name__)
LISTING_LIMIT = 1000


class DropboxProvider(ProviderBase, SafeUpdateSupportMixin):
    SUPPORTED_HASH_TYPES = [HashType.DROPBOX_SHA256]

    def __init__(self, account_id: str, token: str, root_dir: str,
                 is_refresh_token=False, app_key: Optional[str] = None,
                 app_secret: Optional[str] = None, depth: Optional[int] = None):
        self.account_id = account_id
        self.root_dir = root_dir
        self.token = token
        self.is_refresh_token = is_refresh_token
        self.app_key = app_key
        self.app_secret = app_secret
        self.depth = depth
        self._dropbox = None

        if depth is not None:
            if depth <= 0:
                raise ValueError('invalid depth value')

    def get_label(self) -> str:
        if self.depth is not None:
            return 'DBX(%s, depth=%s)' % (self.root_dir, self.depth)
        return 'DBX(%s)' % self.root_dir

    def get_handle(self) -> str:
        return 'd-' + hash_dict({
            'account_id': self.account_id,
            'root_dir': self.root_dir,
            'depth': self.depth,
        })

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
        if not full_path.startswith(self.root_dir):
            raise ProviderError('Path outside of the root dir!')
        return full_path

    def _ensure_root_dir(self, dbx: dropbox.Dropbox):
        try:
            dbx.files_list_folder(self.root_dir, limit=1)
        except ApiError as err:
            if 'not_found' in str(err):
                LOGGER.info('root directory was not found -> create')
                dbx.files_create_folder_v2(self.root_dir)

    def _list_folder(
            self, dbx: dropbox.Dropbox, path: str, recursive: bool = False):
        LOGGER.debug('listing folder %s (recursive? %s)', path, recursive)
        entries = []
        list_result = dbx.files_list_folder(
            path, recursive=recursive, limit=LISTING_LIMIT)
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
            revision=entry.rev,
        )

    def __get_state_walking(self):
        dbx = self._get_dropbox()
        files = {}

        def walk(path: str, depth: int):
            if self.depth is not None and depth > self.depth:
                return
            for entry in self._list_folder(dbx, path):
                if isinstance(entry, FileMetadata):
                    full_path = entry.path_display
                    assert full_path.startswith(self.root_dir)
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
                assert full_path.startswith(self.root_dir)
                rel_path = relative_path(full_path, self.root_dir)
                files[rel_path] = self._file_metadata_to_file_state(entry)

        return StorageState(files)

    def get_state(self) -> StorageState:
        if self.depth is not None:
            return self.__get_state_walking()
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

    def move(self, source_path: str, destination_path: str) -> None:
        dbx = self._get_dropbox()

        source_full_path = self._get_full_path(source_path)
        destination_full_path = self._get_full_path(destination_path)

        try:
            dbx.files_move_v2(source_full_path, destination_full_path)
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
