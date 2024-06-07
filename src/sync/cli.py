#! /usr/bin/env python

import argparse
from argparse import RawTextHelpFormatter
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


def main(source_provider: ProviderBase,
         destination_provider: ProviderBase,
         dry_run: bool = False,
         filter_glob: Optional[str] = None,
         depth: int | None = None):
    syncer = Syncer(
        source_provider,
        destination_provider,
        filter_glob=filter_glob,
        depth=depth,
    )
    syncer.sync(dry_run=dry_run)


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

    def get(param: str, required=True) -> Optional[str]:
        if required and param not in provider_args:
            raise Exception('expected %s for %s provider' % (param, provider_type))
        return provider_args.pop(param, None)

    if provider_type == 'FS':
        provider = FSProvider(
            root_dir=get('root'),
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
        )
    else:
        raise Exception('unknown provider: "%s"' % provider_type)

    if provider_args:
        raise Exception('unrecognized parameters: %s' % provider_args.keys())

    return provider


def entrypoint():
    parser = argparse.ArgumentParser(
        description="""
Provider options have to be passed in key=value format.

Supported providers:

FS - File system
    root: Path to root directory
D - Dropbox
    root: Path to root directory
    id: User arbitrary ID for account

    auth options:
        access_token
    or
        refresh_token
        app_key
        app_secret
SFTP - SFTP (Linux hosts only)
    host: ip or hostname of target
    root: Path to root directory
    key: Optional path to key file
    pass: Optional pass
    port: Optional port number (22 is default)
""", formatter_class=RawTextHelpFormatter)

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

    source_provider = init_provider(args.source)
    destination_provider = init_provider(args.destination)

    try:
        main(
            source_provider,
            destination_provider,
            dry_run=args.dry_run,
            filter_glob=args.filter_glob,
            depth=args.depth,
        )
    except Exception as err:
        LOGGER.fatal('error: %s', err, exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    entrypoint()
