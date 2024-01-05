#! /usr/bin/env python

import sys
import argparse

from sync.core import Syncer, ProviderBase
from sync.providers.fs import FSProvider
from sync.providers.dropbox import DropboxProvider
import coloredlogs
import logging


LOGGER = logging.getLogger('cli')


def main(source_provider: ProviderBase,
         destination_provider: ProviderBase,
         dry_run: bool = False):
    syncer = Syncer(
        source_provider,
        destination_provider,
    )
    syncer.sync(dry_run=dry_run)


def init_provider(arg_list):
    provider_type = arg_list[0]

    if provider_type == 'FS':
        assert len(arg_list) == 'expected DIR'
        root_dir = arg_list[1]
        return FSProvider(root_dir)
    elif provider_type == 'D':
        assert len(arg_list) == 3, 'expected TOKEN DIR'
        token = arg_list[1]
        root_dir = arg_list[2]
        return DropboxProvider(token, root_dir)
    else:
        raise NotImplementedError


def entrypoint():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--log-level', type=str, required=False, default='INFO',
        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'])

    parser.add_argument('-s', '--source', nargs='+', required=True)
    parser.add_argument('-d', '--destination', nargs='+', required=True)
    parser.add_argument('--dry-run', action='store_true', required=False, default=False)

    args = parser.parse_args()

    coloredlogs.install(logging.getLevelName(args.log_level))

    # disable too verbose logging
    logging.getLogger('dropbox').setLevel(logging.WARNING)

    source_provider = init_provider(args.source)
    destination_provider = init_provider(args.destination)

    try:
        main(
            source_provider,
            destination_provider,
            dry_run=args.dry_run,
        )
    except Exception as err:
        LOGGER.fatal('error: %s', err, exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    entrypoint()
