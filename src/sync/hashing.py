import io
import json
import logging
from hashlib import sha256
from typing import BinaryIO, Dict

LOGGER = logging.getLogger(__name__)


class Hasher:
    BUFFER_SIZE = 4096

    def compute(self, stream: BinaryIO) -> str:
        sha = sha256()
        while True:
            buffer = stream.read(self.BUFFER_SIZE)
            if not buffer:
                break
            sha.update(buffer)
        return sha.hexdigest()


HASHER = Hasher()


def hash_dict(data: Dict[str, str]) -> str:
    assert data
    with io.BytesIO() as buffer:
        with io.TextIOWrapper(buffer) as f:
            json.dump(data, f, sort_keys=True)
            f.flush()
            buffer.seek(0)
            return HASHER.compute(buffer)
