import logging
from hashlib import sha256
from io import BytesIO


LOGGER = logging.getLogger(__name__)


class Hasher:
    BUFFER_SIZE = 4096

    def compute(self, stream: BytesIO) -> str:
        LOGGER.debug('computing hash...')

        sha = sha256()

        while True:
            buffer = stream.read(self.BUFFER_SIZE)

            if not buffer:
                break

            sha.update(buffer)

        return sha.hexdigest()
