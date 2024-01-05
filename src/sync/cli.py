#! /usr/bin/env python

import sys

from sync.core import Syncer
from sync.providers.fs import FSProvider
import coloredlogs
import logging


LOGGER = logging.getLogger('cli')


def main():
    syncer = Syncer(
        FSProvider('/tmp/sync/source'),
        FSProvider('/tmp/sync/destination'),
    )
    syncer.sync()


if __name__ == '__main__':
    coloredlogs.install(logging.DEBUG)

    try:
        main()
    except Exception as err:
        LOGGER.fatal('error: %s', err, exc_info=True)
        sys.exit(1)
