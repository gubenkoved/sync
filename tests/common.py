import io
import os
from typing import BinaryIO


def bytes_as_stream(data: bytes) -> BinaryIO:
    return io.BytesIO(data)


def random_bytes_stream(count: int = 1024) -> BinaryIO:
    data = os.urandom(count)
    return bytes_as_stream(data)


def stream_to_bytes(stream: BinaryIO) -> bytes:
    return stream.read()
