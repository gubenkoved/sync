from typing import Tuple


SEP = '/'


def unixify_path(path):
    return path.replace('\\', SEP)


def path_join(base_path: str, segment: str):
    return base_path.rstrip(SEP) + SEP + segment


def relative_path(full_path: str, base_path: str, case_sensitive: bool = False):
    base_path = base_path.rstrip(SEP) + SEP

    if case_sensitive:
        matching_prefix = full_path.startswith(base_path)
    else:
        matching_prefix = full_path.lower().startswith(base_path.lower())

    if not matching_prefix:
        raise ValueError('Full path "%s" must start with base path "%s"!' % (
            full_path, base_path
        ))

    result = full_path[len(base_path):]
    return result


def path_split(path: str) -> Tuple[str, str]:
    idx = path.rfind(SEP)
    head, tail = path[:idx], path[idx+1:]

    if not head or not tail:
        raise ValueError('Invalid path to split!')

    return head, tail
