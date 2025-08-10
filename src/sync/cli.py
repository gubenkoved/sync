#! /usr/bin/env python

import argparse
from argparse import RawTextHelpFormatter
import logging
import os.path
import sys
from typing import List, Optional

import coloredlogs

from sync.cache import InMemoryCacheWithStorage
from sync.core import Syncer
from sync.provider import ProviderBase
from sync.providers.dropbox import DropboxProvider
from sync.providers.fs import FSProvider
from sync.providers.sftp import STFPProvider

LOGGER = logging.getLogger("cli")

CACHES: List[InMemoryCacheWithStorage] = []


def main(
    source_provider: ProviderBase,
    destination_provider: ProviderBase,
    dry_run: bool,
    filter: Optional[str],
    depth: int | None,
    threads: int,
    state_dir: str,
):
    syncer = Syncer(
        source_provider,
        destination_provider,
        filter=filter,
        depth=depth,
        threads=threads,
        state_root_dir=state_dir,
    )
    try:
        syncer.sync(dry_run=dry_run)
    finally:
        # flush caches to disk
        LOGGER.debug("flushing caches...")
        for cache in CACHES:
            cache.try_save()


def parse_args(args: List[str]):
    result = {}
    for arg in args:
        k, v = arg.split("=", maxsplit=1)
        assert k not in result
        result[k] = v
    return result


def init_provider(args: List[str]):
    provider_type = args[0]
    provider_args = parse_args(args[1:])

    def get(param: str, required=True) -> Optional[str]:
        if required and param not in provider_args:
            raise Exception("expected %s for %s provider" % (param, provider_type))
        return provider_args.pop(param, None)

    if provider_type == "FS":
        cache_dir = get("cache_dir", required=False)
        cache_dir = cache_dir or ".cache"

        if not os.path.exists(cache_dir):
            LOGGER.info('creating cache dir for FS provider at "%s"...', cache_dir)
            os.makedirs(cache_dir)

        provider = FSProvider(
            root_dir=get("root"),
        )
        cache_path = os.path.join(cache_dir, provider.get_handle())
        # TODO: is there less clumsy way to pass cache? Should I make "get_handle"
        #  a static method, so that instance is not required?
        cache = InMemoryCacheWithStorage(cache_path)
        provider.cache = cache
        cache.try_load()
        CACHES.append(cache)
    elif provider_type == "D":
        account_id = get("id")
        access_token = get("access_token", required=False)
        refresh_token = get("refresh_token", required=False)

        if access_token:
            dropbox_args = dict(
                token=access_token,
            )
        elif refresh_token:
            dropbox_args = dict(
                token=refresh_token,
                app_key=get("app_key"),
                app_secret=get("app_secret"),
                is_refresh_token=True,
            )
        else:
            raise Exception("unknown token type")

        provider = DropboxProvider(
            account_id=account_id,
            root_dir=get("root"),
            **dropbox_args,
        )
    elif provider_type == "SFTP":
        provider = STFPProvider(
            host=get("host"),
            username=get("user"),
            root_dir=get("root"),
            key_path=get("key", required=False),
            password=get("pass", required=False),
            port=int(get("port", required=False) or 22),
        )
    else:
        raise Exception('unknown provider: "%s"' % provider_type)

    if provider_args:
        raise Exception("unrecognized parameters: %s" % provider_args.keys())

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
SFTP - SFTP (POSIX hosts only)
    host: ip or hostname of target
    root: Path to root directory
    key: Optional path to key file
    pass: Optional pass
    port: Optional port number (22 is default)
""",
        formatter_class=RawTextHelpFormatter,
    )

    parser.add_argument("--log-level", type=str, required=False, default="info")

    parser.add_argument("-s", "--source", nargs="+", required=True)
    parser.add_argument("-d", "--destination", nargs="+", required=True)
    parser.add_argument("--dry-run", action="store_true", required=False, default=False)
    parser.add_argument("--depth", type=int, required=False, default=None)
    parser.add_argument("--threads", type=int, required=False, default=4)
    parser.add_argument(
        "-f",
        "--filter",
        type=str,
        default=None,
        required=False,
        help="""
Comma-separated list of glob file patterns to use as a filter against full path;

When first action is exclusion, then the default action used when no rules matched
    is to include file (e.g. "!foo/*" means to exclude all files except foo dir);

When first action is inclusion, then the default action used when no rules matched
    is to exclude file (e.g. "foo/*" means to include only foo dir);

Examples:
    "foo/*" matches all the items inside foo directory;
    "!.spam*" matches all the items which do not start with .spam;
""",
    )
    parser.add_argument(
        "--state-dir", type=str, default=".state", help="Location of the state files."
    )

    args = parser.parse_args()

    coloredlogs.install(
        level=logging.getLevelName(args.log_level.upper()),
        fmt="%(asctime)s %(hostname)s %(name)s[%(process)d][%(threadName)s] %(levelname)s %(message)s",
    )

    # disable too verbose logging
    logging.getLogger("dropbox").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    source_provider = init_provider(args.source)
    destination_provider = init_provider(args.destination)

    try:
        main(
            source_provider,
            destination_provider,
            dry_run=args.dry_run,
            filter=args.filter,
            depth=args.depth,
            threads=args.threads,
            state_dir=args.state_dir,
        )
    except Exception as err:
        LOGGER.fatal("error: %s", err, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    entrypoint()
