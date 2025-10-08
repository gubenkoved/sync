from typing import Tuple
import unicodedata

SEP = "/"
UNICODE_NORMAL_FORM = "NFC"


def unixify_path(path):
    return path.replace("\\", SEP)


# TODO: support arbitrary amount of segments
def path_join(base_path: str, segment: str):
    return base_path.rstrip(SEP) + SEP + segment


def relative_path(full_path: str, base_path: str, case_sensitive: bool = False):
    base_path = base_path.rstrip(SEP) + SEP

    if case_sensitive:
        matching_prefix = full_path.startswith(base_path)
    else:
        matching_prefix = full_path.lower().startswith(base_path.lower())

    if not matching_prefix:
        raise ValueError(
            'Full path "%s" must start with base path "%s"!' % (full_path, base_path)
        )

    result = full_path[len(base_path) :]
    return result


def path_split(path: str) -> Tuple[str, str]:
    idx = path.rfind(SEP)
    head, tail = path[:idx], path[idx + 1 :]

    if not head or not tail:
        raise ValueError("Invalid path to split!")

    return head, tail


# different storage systems can behave differently when it comes to the Unicode
# normalization:
# * Dropbox seems to be normalizing to NFC form
# * MacOS seems to be normalizing to NDF form
# * Windows and Linux seems to be preserving whatever form it was
# In order for these different pairs to play together we need a common
# normalization form to be used by providers
def normalize_unicode(string: str) -> str:
    """
    Normalize unicode string to NFC form.
    """
    # Normal form C (NFC) first applies a canonical decomposition,
    # then composes pre-combined characters again
    return unicodedata.normalize(UNICODE_NORMAL_FORM, string)


def normalize_path(path: str, case_insensitive: bool = False) -> str:
    """
    Normalize path string to be used in various comparison to infer if the
    paths are equivalent or not.

    Unicode string are normalized to NFC form.
    In case-insensitive mode, we also lower case the path
    """
    path = normalize_unicode(path)

    if case_insensitive:
        path = path.lower()

    return path
