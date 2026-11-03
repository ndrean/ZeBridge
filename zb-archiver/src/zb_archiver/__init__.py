"""zb-archiver: ZeBridge sensor readings, from a NATS stream into Parquet files in S3.

The archiver (`run`, or the `zb-archiver` command) writes the archive; `layout` is its
contract (stream, manifest, file names, what a reading is), which readers of the archive
import too; `env` holds the connections (NATS, S3) and `fetch_files` for readers.
"""
from .archiver import ArchiverConfig, ExitCode, run
from .env import connect, fetch_files, s3, stream_when_there
from .layout import read_manifest, reading, write_manifest

__version__ = "0.2.11"
__all__ = ["ArchiverConfig", "ExitCode", "run", "connect", "fetch_files", "s3", "stream_when_there",
           "read_manifest", "reading", "write_manifest"]
