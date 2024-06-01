def unixify_path(path):
    return path.replace('\\', '/')


def path_join(base_path: str, segment: str):
    return base_path.rstrip('/') + '/' + segment


def relative_path(full_path: str, base_path: str):
    base_path = base_path.rstrip('/') + '/'
    if not full_path.startswith(base_path):
        raise ValueError('Full path must start with base path!')
    result = full_path[len(base_path):]
    return result
