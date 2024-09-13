import io
import os
from typing import BinaryIO
from sync.providers.sftp import STFPProvider
import logging
import time


LOGGER = logging.getLogger(__name__)


def bytes_as_stream(data: bytes) -> BinaryIO:
    return io.BytesIO(data)


def random_bytes_stream(count: int = 1024) -> BinaryIO:
    data = os.urandom(count)
    return bytes_as_stream(data)


def stream_to_bytes(stream: BinaryIO) -> bytes:
    return stream.read()


# needed to work around transient issues with DNS on github workers
def create_sftp_provider(*args, **kwargs):
    attempt = 0
    while True:
        try:
            return STFPProvider(*args, **kwargs)
        except Exception as e:
            is_transient = 'Temporary failure in name resolution' in str(e)
            LOGGER.warning(
                'error occurred constructing SFTP provider, '
                'is transient? %s', is_transient, exc_info=True)
            if not is_transient or attempt >= 3:
                raise
            time.sleep(2 ** attempt)
