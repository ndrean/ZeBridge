"""libzb's C ABI (libzb/abi.json), declared once. Internal: apps use `zebridge.ZeBridge`.

Every string crosses as UTF-8 bytes; every returned string is JSON the caller owns and
frees with `zb_free` (`take` does both). A NULL result means the call failed, and
`zb_last_error` — per THREAD, like errno — says why; `ZeBridge` reads it on the thread
that made the call.
"""
from __future__ import annotations

import ctypes
import json
import os
import pathlib
import sys

_EXT = {"darwin": ".dylib", "linux": ".so", "win32": ".dll"}


def _find() -> str:
    """ZB_LIB, else a copy bundled in the package, else the repository's own build."""
    if os.environ.get("ZB_LIB"):
        return os.environ["ZB_LIB"]
    name = "libzbcore" + _EXT.get(sys.platform, ".so")
    here = pathlib.Path(__file__).resolve().parent
    for candidate in (here / "lib" / name, here.parents[2] / "libzb" / "zig-out" / "lib" / name):
        if candidate.exists():
            return str(candidate)
    raise OSError(f"zebridge: {name} not found — set ZB_LIB to its path, or build libzb (cd libzb && zig build -Doptimize=ReleaseFast)")


lib = ctypes.CDLL(_find())

_S = ctypes.c_char_p
_P = ctypes.c_void_p  # an owned result: read it, then zb_free it
_H = ctypes.c_uint64
for name, res, args in [
    ("zb_abi_version", ctypes.c_int, []),
    ("zb_free", None, [_P]),
    ("zb_last_error", _S, []),
    ("zb_grammar_hash", _P, []),
    ("zb_create_user", _P, []),
    ("zb_creds_file_text", _P, [_S, _S]),
    ("zb_client_connect", _H, [_S]),
    ("zb_client_close", ctypes.c_int, [_H]),
    ("zb_client_wipe", ctypes.c_int, [_H]),
    ("zb_client_revoked", ctypes.c_int, [_H]),
    ("zb_client_sync", _P, [_H]),
    ("zb_client_poll", _P, [_H, ctypes.c_uint64]),
    ("zb_client_flush_outbox", _P, [_H, ctypes.c_uint64]),
    ("zb_client_query", _P, [_H, _S, _S]),
    ("zb_client_mutate", _P, [_H, _S, _S, _S, _S]),
    ("zb_client_mutate_at", _P, [_H, _S, _S, _S, _S, _S]),
    ("zb_client_join", _P, [_H, _S]),
    ("zb_client_stamp", _P, [_H]),
    ("zb_client_wake", ctypes.c_int, [_H]),
    ("zb_client_leave", _P, [_H, _S]),
    ("zb_client_request", _P, [_H, _S, _S, ctypes.c_uint64]),
    ("zb_client_reply", _P, [_H, ctypes.c_uint64, _S]),
    ("zb_client_serve", _P, [_H, _S]),
    ("zb_client_ingest", _P, [_H, _S, _S, _S]),
]:
    fn = getattr(lib, name)
    fn.restype, fn.argtypes = res, args


def last_error() -> str | None:
    e = lib.zb_last_error()
    return e.decode() if e else None


def take(p: int | None) -> dict:
    """An owned JSON result → a dict, freed. NULL raises with libzb's reason."""
    if not p:
        from . import ZeBridgeError
        raise ZeBridgeError(last_error() or "libzb returned nothing")
    try:
        return json.loads(ctypes.string_at(p).decode())
    finally:
        lib.zb_free(p)


def enc(v) -> bytes | None:
    """A str as UTF-8, a dict/list as JSON UTF-8, None as NULL."""
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        return bytes(v)
    if isinstance(v, str):
        return v.encode()
    return json.dumps(v).encode()
