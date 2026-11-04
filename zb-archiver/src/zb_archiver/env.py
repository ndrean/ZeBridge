"""Connections, from arguments or else the environment: NATS, S3 and the tenant's stream.

NATS: NATS_URL, NATS_CREDS, and NATS_TLS_CA for a server whose certificate a private CA signed.

S3 is any S3 API: SeaweedFS locally, Cloudflare R2 on a server. Settings from the environment:
S3_ENDPOINT (http://127.0.0.1:8333), S3_BUCKET (zb-events), S3_REGION (auto), and the keys from
AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY, or else the file S3_KEYS (~/.config/zebridge/s3.json:
SeaweedFS's identity file, or {"access_key_id", "secret_access_key"}) for a local endpoint only.
"""
from __future__ import annotations

import asyncio, base64, json, os, pathlib

from .layout import stream_for

DEFAULT_BUCKET = "zb-events"
DEFAULT_ENDPOINT = "http://127.0.0.1:8333"


def bucket() -> str:
    return os.environ.get("S3_BUCKET", DEFAULT_BUCKET)


def principal_of(creds_path: str | None) -> str | None:
    """The user name in a creds file's JWT: its own inbox is `_INBOX.<name>.>`."""
    if not creds_path:
        return None
    text = pathlib.Path(creds_path).read_text()
    jwt = text.split("-----BEGIN NATS USER JWT-----", 1)[1].split("------END", 1)[0].strip()
    body = jwt.split(".")[1]
    return json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4))).get("name")


def nats_url(url: str | None = None) -> str:
    """`url`, else NATS_URL; unset, the dev server. The images set it EMPTY: a container must name its server."""
    url = url if url is not None else os.environ.get("NATS_URL")
    if url == "":
        raise SystemExit("🔴 NATS_URL is not set: pass the NATS server, e.g. -e NATS_URL=tls://nats.example.com:4222")
    return url or "nats://127.0.0.1:4222"


async def connect(creds: str | None = None, url: str | None = None):
    """NATS with the given creds (or NATS_CREDS), and the principal's own inbox prefix."""
    import nats
    url = nats_url(url)
    creds = creds or os.environ.get("NATS_CREDS")
    if creds and not os.path.exists(creds):
        raise SystemExit(f"🔴 no creds file at {creds}: mount the archiver's (bridge --mint-archiver), e.g. -v archiver-acme.creds:{creds}:ro")
    name = principal_of(creds)
    kw = {"user_credentials": creds} if creds else {}
    if name:
        kw["inbox_prefix"] = f"_INBOX.{name}".encode()
    if os.environ.get("NATS_TLS_CA"):   # a server whose certificate a private CA signed
        import ssl
        kw["tls"] = ssl.create_default_context(cafile=os.environ["NATS_TLS_CA"])
    return await nats.connect(url, **kw)


def s3(endpoint: str | None = None):
    """A boto3 S3 client for `endpoint` (else S3_ENDPOINT), path-style addressing."""
    import boto3
    from botocore.config import Config
    endpoint = endpoint or os.environ.get("S3_ENDPOINT", DEFAULT_ENDPOINT)
    local = any(h in endpoint for h in ("://127.0.0.1", "://localhost", "://[::1]"))
    kw = {}
    if not os.environ.get("AWS_ACCESS_KEY_ID") and not local:
        # The dev keys file is for the local S3 only: a misspelled variable once sent them to
        # R2, which answered "400 Bad Request" with no word of why.
        raise SystemExit(f"🔴 AWS_ACCESS_KEY_ID is not set for {endpoint}: set AWS_ACCESS_KEY_ID and "
                         "AWS_SECRET_ACCESS_KEY (the S3 key pair; with R2, the API token's access key)")
    if not os.environ.get("AWS_ACCESS_KEY_ID"):
        path = pathlib.Path(os.environ.get("S3_KEYS", pathlib.Path.home() / ".config" / "zebridge" / "s3.json"))
        if path.exists():
            d = json.loads(path.read_text())
            c = d["identities"][0]["credentials"][0] if "identities" in d else d
            kw = {"aws_access_key_id": c.get("accessKey") or c["access_key_id"],
                  "aws_secret_access_key": c.get("secretKey") or c["secret_access_key"]}
    return boto3.client("s3", endpoint_url=endpoint,
                        region_name=os.environ.get("S3_REGION", "auto"),
                        config=Config(s3={"addressing_style": "path"}), **kw)


async def stream_when_there(js, tenant: str, every_s: float = 10, say=print, stop: asyncio.Event | None = None):
    """The tenant's stream, waited for: the bridge makes it when the tenant has its first row
    in zebridge_user_tenants. Said once, then checked every `every_s` seconds. None when
    `stop` is set while it waits: a stopped archiver returns, it does not wait for a stream
    that may never come."""
    from nats.js.errors import NotFoundError
    stream, said = stream_for(tenant), False
    while not (stop and stop.is_set()):
        try:
            return await js.stream_info(stream)
        except NotFoundError:
            if not said:
                say(f"⏳ waiting for the stream {stream}: the bridge makes it when the tenant has a row in "
                    f"zebridge_user_tenants (INSERT INTO zebridge_user_tenants (principal, tenant_id) "
                    f"VALUES ('<principal>', '{tenant}')). Checking every {every_s:g} s.", flush=True)
                said = True
            if stop is None:
                await asyncio.sleep(every_s)
            else:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=every_s)
                except asyncio.TimeoutError:
                    pass
    return None


def fetch_files(files: list[dict], cache: pathlib.Path, bucket_name: str | None = None, client=None) -> list[pathlib.Path]:
    """The manifest's files in a local cache, each once: Parquet never changes, so a file in
    the cache with the manifest's size is the file. For readers of the archive."""
    out, bucket_name = [], bucket_name or bucket()
    for f in files:
        local = cache / f["key"]
        if not local.exists() or local.stat().st_size != f["bytes"]:
            client = client or s3()
            local.parent.mkdir(parents=True, exist_ok=True)
            client.download_file(bucket_name, f["key"], str(local) + ".part")
            os.replace(str(local) + ".part", local)
        out.append(local)
    return out
