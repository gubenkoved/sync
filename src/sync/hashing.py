import logging
from hashlib import sha256
from typing import BinaryIO


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
