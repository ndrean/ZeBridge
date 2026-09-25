#!/usr/bin/env python3
"""libzb's C ABI against libzb/abi.json (libzb/python/abi_check.py): a change to the
exported functions or to the options zb_client_connect reads must bump the version, or
an app embedding an older copy of libzb cannot tell it is stale."""
import pathlib, runpy
runpy.run_path(str(pathlib.Path(__file__).resolve().parents[2] / "libzb" / "python" / "abi_check.py"), run_name="__main__")
