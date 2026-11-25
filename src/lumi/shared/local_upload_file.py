import os
import stat

from .attachment_payloads import BYTE_LIMIT


def read_temporary(path, *, maximum=BYTE_LIMIT):

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    try:
        initial = path.lstat()
        if not stat.S_ISREG(initial.st_mode) or initial.st_size > maximum:
            raise ValueError("Temporary attachment is unavailable")
        descriptor = os.open(path, flags | getattr(os, "O_BINARY", 0))
        with os.fdopen(descriptor, "rb") as source:
            actual = os.fstat(source.fileno())
            if (initial.st_dev, initial.st_ino) != (actual.st_dev, actual.st_ino):
                raise ValueError("Temporary attachment changed during opening")
            if not stat.S_ISREG(actual.st_mode) or actual.st_size > maximum:
                raise ValueError("Temporary attachment exceeds the delivery limit")
            content = source.read(maximum + 1)
        if len(content) > maximum:
            raise ValueError("Temporary attachment exceeds the delivery limit")
        return content
    except OSError:
        raise ValueError("Temporary attachment is unavailable") from None
