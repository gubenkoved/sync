#! /usr/bin/env python

import sys
import argparse

from typing import List
from sync.core import Syncer, ProviderBase
from sync.providers.fs import FSProvider
from sync.providers.dropbox import DropboxProvider
import coloredlogs
import logging


LOGGER = logging.getLogger('cli')


def main(source_provider: ProviderBase,
         destination_provider: ProviderBase,
         dry_run: bool = False,
         filter: str = None):
    syncer = Syncer(
        source_provider,
        destination_provider,
        filter=filter,
    )
    syncer.sync(dry_run=dry_run)


# TODO: support positional args as well?
def parse_args(args: List[str]):
    result = {}
    for arg in args:
        k, v = arg.split('=', maxsplit=1)
        assert k not in result
        result[k] = v
    return result


def init_provider(args: List[str]):
    provider_type = args[0]
    provider_args = parse_args(args[1:])

    def get(param: str, required=True):
        if required and param not in provider_args:
            raise Exception('expected %s for %s provider' % (param, provider_type))
        return provider_args.pop(param, None)

    if provider_type == 'FS':
        provider = FSProvider(
            root_dir=get('root')
        )
    elif provider_type == 'D':
        provider = DropboxProvider(
            token=get('token'),
            root_dir=get('root'),
        )
    else:
        raise NotImplementedError

    if provider_args:
        raise Exception('unrecognized parameters: %s' % provider_args.keys())

    return provider


def entrypoint():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--log-level', type=str, required=False, default='INFO',
        choices=['DEBUG', 'INFO', 'WARN', 'ERROR'])

    parser.add_argument('-s', '--source', nargs='+', required=True)
    parser.add_argument('-d', '--destination', nargs='+', required=True)
    parser.add_argument('--dry-run', action='store_true', required=False, default=False)
    parser.add_argument('--filter', type=str, default=None, required=False)

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
            filter=args.filter,
        )
    except Exception as err:
        LOGGER.fatal('error: %s', err, exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    entrypoint()
