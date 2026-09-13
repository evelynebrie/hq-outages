#!/usr/bin/env python3
"""Print Dropbox's content hash for a file.

Dropbox returns this hash in the metadata of every successful upload, so
comparing it with the local value proves the bytes that arrived are the
bytes we sent. https://www.dropbox.com/developers/reference/content-hash
"""

import hashlib
import sys

BLOCK = 4 * 1024 * 1024


def content_hash(path):
    digests = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            block = f.read(BLOCK)
            if not block:
                break
            digests.update(hashlib.sha256(block).digest())
    return digests.hexdigest()


if __name__ == "__main__":
    print(content_hash(sys.argv[1]))
