from sync.core import Syncer
from sync.providers.fs import FSProvider
import coloredlogs
import logging


LOGGER = logging.getLogger('cli')


if __name__ == '__main__':
    coloredlogs.install(logging.DEBUG)

    syncer = Syncer(
        FSProvider('/tmp/sync/source'),
        FSProvider('/tmp/sync/destination'),
    )
    syncer.sync()

    LOGGER.info('done')
