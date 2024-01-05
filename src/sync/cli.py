#! /usr/bin/env python

import sys
import argparse

from sync.core import Syncer, ProviderBase
from sync.providers.fs import FSProvider
import coloredlogs
import logging


LOGGER = logging.getLogger('cli')


def main(source_provider: ProviderBase, destination_provider: ProviderBase):
    syncer = Syncer(
        source_provider,
        destination_provider,
    )
    syncer.sync()


def init_provider(arg_list):
    provider_type = arg_list[0]

    if provider_type == 'FS':
        root_dir = arg_list[1]
        return FSProvider(root_dir)
    elif provider_type == 'D':
        raise NotImplementedError
    else:
        raise NotImplementedError


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--log-level', type=str, required=False, default='INFO',
        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'])

    parser.add_argument('-s', '--source', nargs='+', required=True)
    parser.add_argument('-d', '--destination', nargs='+', required=True)

    args = parser.parse_args()

    coloredlogs.install(logging.getLevelName(args.log_level))

    source_provider = init_provider(args.source)
    destination_provider = init_provider(args.destination)

    try:
        main(
            source_provider,
            destination_provider,
        )
    except Exception as err:
        LOGGER.fatal('error: %s', err, exc_info=True)
        sys.exit(1)
