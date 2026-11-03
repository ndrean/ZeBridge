#!/usr/bin/env python3
"""The archiver of this example is the zb-archiver library's command (zb-archiver/README.md).

    NATS_URL=… NATS_CREDS=archiver-acme.creds python3 examples/16-archive/archiver.py --tenant acme

is `zb-archiver --tenant acme`, run from the repository without installing the package.
"""
import sys

import common  # noqa: F401  (puts zb-archiver/src on the path when the package is not installed)
from zb_archiver.cli import main

if __name__ == "__main__":
    sys.exit(main())
