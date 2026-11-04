"""zb-archiver: one tenant's readings, from the stream into Parquet files in S3.

    NATS_URL=… NATS_CREDS=archiver-acme.creds zb-archiver --tenant acme

Its credential: `bridge --mint-archiver --name archiver-acme --tenant acme` — it may read
READINGS_acme and write acme's manifests, nothing else. S3 settings: S3_ENDPOINT, S3_BUCKET,
S3_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.

Exit codes: 0 stopped, 1 a reader failed, 2 retention too short, 3 another archiver moved the
manifest, 4 readings lost from the stream, 5 an upload of the wrong size, 9 killed on purpose
(--crash-after-upload, tests).
"""
from __future__ import annotations

import argparse, asyncio, signal, sys

from .archiver import ArchiverConfig, run


def parse(argv=None) -> ArchiverConfig:
    ap = argparse.ArgumentParser(prog="zb-archiver", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tenant", required=True)
    ap.add_argument("--flush-s", type=float, default=60, help="flush at least this often while readings arrive")
    ap.add_argument("--batch", type=int, default=100_000, help="flush when this many readings are waiting")
    ap.add_argument("--outage-s", type=float, default=3600, help="the S3 outage the stream must outlast")
    ap.add_argument("--exit-when-idle", type=float, default=0, help="stop after this long with nothing to flush (tests)")
    ap.add_argument("--accept-loss", action="store_true", help="start even if the stream lost readings")
    ap.add_argument("--fetch-bin", help="bridge_readings_fetch executable (default: ZB_READINGS_FETCH, else on the PATH)")
    ap.add_argument("--crash-after-upload", action="store_true", help=argparse.SUPPRESS)
    a = ap.parse_args(argv)
    return ArchiverConfig(tenant=a.tenant, flush_s=a.flush_s, batch=a.batch, outage_s=a.outage_s,
                          exit_when_idle=a.exit_when_idle, accept_loss=a.accept_loss, fetch_bin=a.fetch_bin,
                          crash_after_upload=a.crash_after_upload)


async def amain(argv=None) -> int:
    cfg = parse(argv)
    stop = asyncio.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(sig, stop.set)
    return int(await run(cfg, stop))


def main(argv=None) -> int:
    return asyncio.run(amain(argv))


if __name__ == "__main__":
    sys.exit(main())
