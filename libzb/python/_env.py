"""Shared environment resolution for the libzb Python proofs.

Every proof needs three things: the Zig-built shared library, a psql binary, and
the repo root (for grammar.json and the .creds files). Resolve them once, here,
with clear errors — never a hardcoded /Users path, never a bare ctypes OSError.

    ZB_PGBIN   directory holding psql (default: Homebrew postgresql@18, else PATH)
"""
import ctypes
import os
import shutil
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
LIBZB = os.path.dirname(HERE)                 # libzb/
REPO = os.path.dirname(LIBZB)                 # the repo root
CREDS_DIR = os.path.join(REPO, "scripts", "native", "creds")
GRAMMAR = os.path.join(REPO, "grammar.json")

_EXT = {"darwin": ".dylib", "linux": ".so", "win32": ".dll"}
_HOMEBREW_PG = "/opt/homebrew/opt/postgresql@18/bin"


def lib_path() -> str:
    ext = _EXT.get(sys.platform)
    if ext is None:
        sys.exit(f"unsupported platform {sys.platform!r} (known: {', '.join(_EXT)})")
    return os.path.join(LIBZB, "zig-out", "lib", f"libzbcore{ext}")


def load_lib() -> ctypes.CDLL:
    path = lib_path()
    if not os.path.exists(path):
        sys.exit(f"{path} is missing — build it first:  cd libzb && zig build")
    try:
        return ctypes.CDLL(path)
    except OSError as e:
        sys.exit(f"cannot load {path}: {e}")


def psql_bin() -> str:
    d = os.environ.get("ZB_PGBIN")
    if d:
        p = os.path.join(d, "psql")
        if not os.path.exists(p):
            sys.exit(f"ZB_PGBIN={d} has no psql")
        return p
    p = os.path.join(_HOMEBREW_PG, "psql")
    if os.path.exists(p):
        return p
    p = shutil.which("psql")
    if p:
        return p
    sys.exit("no psql found — set ZB_PGBIN=<dir containing psql>")


def psql_cmd(*extra: str) -> list:
    """The native-stack psql invocation: 127.0.0.1:5432, postgres, trust auth."""
    return [psql_bin(), "-X", "-q", "-h", "127.0.0.1", "-p", "5432",
            "-U", "postgres", "-d", "postgres", *extra]


def creds(principal: str) -> str:
    return os.path.join(CREDS_DIR, f"{principal}.creds")


def rm_sqlite(db: str) -> None:
    for f in (db, db + "-wal", db + "-shm"):
        try:
            os.unlink(f)
        except FileNotFoundError:
            pass
