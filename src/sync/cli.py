#! /usr/bin/env python

import argparse
import logging
import sys
from typing import List, Optional

import coloredlogs

from sync.core import Syncer
from sync.provider import ProviderBase
from sync.providers.dropbox import DropboxProvider
from sync.providers.fs import FSProvider
from sync.providers.sftp import STFPProvider

LOGGER = logging.getLogger('cli')


# TODO: filter as glob expression back + add ability to limit depth instead to
#  be able to implement sync cases where we want couple of files in the root of
#  some dir with a lot of children
def main(source_provider: ProviderBase,
         destination_provider: ProviderBase,
         dry_run: bool = False,
         filter_glob: Optional[str] = None):
    syncer = Syncer(
        source_provider,
        destination_provider,
        filter_glob=filter_glob,
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


def init_provider(args: List[str], depth: Optional[int] = None):
    provider_type = args[0]
    provider_args = parse_args(args[1:])

    def get(param: str, required=True) -> Optional[str]:
        if required and param not in provider_args:
            raise Exception('expected %s for %s provider' % (param, provider_type))
        return provider_args.pop(param, None)

    if provider_type == 'FS':
        provider = FSProvider(
            root_dir=get('root'),
            depth=depth,
        )
    elif provider_type == 'D':
        account_id = get('id')
        access_token = get('access_token', required=False)
        refresh_token = get('refresh_token', required=False)

        if access_token:
            dropbox_args = dict(
                token=access_token,
            )
        elif refresh_token:
            dropbox_args = dict(
                token=refresh_token,
                app_key=get('app_key'),
                app_secret=get('app_secret'),
                is_refresh_token=True,
            )
        else:
            raise Exception('unknown token type')

        provider = DropboxProvider(
            account_id=account_id,
            root_dir=get('root'),
            depth=depth,
            **dropbox_args,
        )
    elif provider_type == 'SFTP':
        provider = STFPProvider(
            host=get('host'),
            username=get('user'),
            root_dir=get('root'),
            key_path=get('key', required=False),
            password=get('pass', required=False),
            port=int(get('port', required=False) or 22),
            depth=depth,
        )
    else:
        raise Exception('unknown provider: "%s"' % provider_type)

    if provider_args:
        raise Exception('unrecognized parameters: %s' % provider_args.keys())

    return provider


def entrypoint():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--log-level', type=str, required=False, default='info')

    parser.add_argument(
        '-s', '--source', nargs='+', required=True)
    parser.add_argument(
        '-d', '--destination', nargs='+', required=True)
    parser.add_argument(
        '--dry-run', action='store_true', required=False, default=False)
    parser.add_argument(
        '--depth', type=int, required=False, default=None)
    parser.add_argument(
        '-f', '--filter-glob', type=str, default=None, required=False)

    args = parser.parse_args()

    coloredlogs.install(logging.getLevelName(args.log_level.upper()))

    # disable too verbose logging
    logging.getLogger('dropbox').setLevel(logging.WARNING)
    logging.getLogger('paramiko').setLevel(logging.WARNING)

    source_provider = init_provider(args.source, depth=args.depth)
    destination_provider = init_provider(args.destination, depth=args.depth)

    try:
        main(
            source_provider,
            destination_provider,
            dry_run=args.dry_run,
            filter_glob=args.filter_glob,
        )
    except Exception as err:
        LOGGER.fatal('error: %s', err, exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    entrypoint()
