import io
import json
import logging
from hashlib import sha256
from typing import BinaryIO, Dict


LOGGER = logging.getLogger(__name__)


def sha256_stream(stream: BinaryIO, buffer_size: int = 1024) -> bytes:
    sha = sha256()
    while True:
        buffer = stream.read(buffer_size)
        if not buffer:
            break
        sha.update(buffer)
    return sha.digest()


def hash_stream(stream: BinaryIO) -> str:
    hash_bytes = sha256_stream(stream)
    return hash_bytes.hex()


def hash_dict(data: Dict[str, str]) -> str:
    assert data
    with io.BytesIO() as buffer:
        with io.TextIOWrapper(buffer) as f:
            json.dump(data, f, sort_keys=True)
            f.flush()
            buffer.seek(0)
            return hash_stream(buffer)


def dropbox_hash_stream(stream: BinaryIO) -> str:
    with io.BytesIO() as hash_buffer:
        while True:
            block = stream.read(4 * 1024 * 1024)
            if not block:
                break
            with io.BytesIO(block) as block_stream:
                block_hash = sha256_stream(block_stream)
                hash_buffer.write(block_hash)
        hash_buffer.seek(0)
        result_hash = sha256_stream(hash_buffer)
        return result_hash.hex()
