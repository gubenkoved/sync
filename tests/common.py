import io
import logging
import os
from typing import BinaryIO

from sync.provider import FolderNotFoundProviderError
from sync.providers.common import path_split
from sync.providers.dropbox import DropboxProvider
from sync.providers.fs import FSProvider
from sync.providers.sftp import STFPProvider

LOGGER = logging.getLogger(__name__)


def bytes_as_stream(data: bytes) -> BinaryIO:
    return io.BytesIO(data)


def random_bytes_stream(count: int = 1024) -> BinaryIO:
    data = os.urandom(count)
    return bytes_as_stream(data)


def stream_to_bytes(stream: BinaryIO) -> bytes:
    return stream.read()


def cleanup_provider(provider: DropboxProvider | FSProvider | STFPProvider) -> None:
    cleanup_instance = provider.clone()
    parent_dir, subdir = path_split(cleanup_instance.root_dir)
    cleanup_instance.root_dir = parent_dir
    try:
        cleanup_instance.remove_folder(subdir)
    except FolderNotFoundProviderError:
        LOGGER.debug(f"Folder {provider.root_dir} not found")
