#!/usr/bin/env python3
"""Field-run evidence helpers; no server configuration or product code is changed.

`logged` tags native stderr without trying to attribute a process-wide file descriptor
to Python threads. `observe` uses a device identity for read-only JetStream queries.
"""
import argparse
import asyncio
from contextlib import closing
import datetime
import hashlib
import json
import os
import pathlib
import shutil
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import time


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds")


def value_frame(note):
    return b"N" if note is None else f"S{len(note.encode('utf-8'))}:".encode() + note.encode("utf-8")


def frame(key, note):
    return str(key).encode("utf-8") + b":" + value_frame(note)


def version_instant(value):
    """Compare aware timestamps at microsecond precision; retain unknown spellings."""
    try:
        instant = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if instant.tzinfo is not None:
            return instant.astimezone(datetime.timezone.utc)
    except (ValueError, AttributeError, TypeError):
        pass
    return value


def atomic_json(path, value):
    path = pathlib.Path(path)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False) + "\n")
    tmp.replace(path)


def truth_sql(table, tenant):
    # Both the grading digest and the diagnostic rows use ONE materialized snapshot.
    # Hex hashes use the existing NULL/UTF-8 byte framing, not JSON serialization.
    if not table.replace("_", "").isalnum():
        raise ValueError("invalid fixture table")
    tenant = tenant.replace("'", "''")
    return f"""WITH frozen AS MATERIALIZED (
      SELECT id, updated_at::text AS version,
        convert_to(CASE WHEN note IS NULL THEN 'N'
          ELSE 'S' || octet_length(note) || ':' || note END, 'UTF8') AS value_bytes
      FROM public.\"{table}\" WHERE tenant_id = '{tenant}' AND deleted_at IS NULL
    ), entries AS (
      SELECT 0 AS ord, ''::text AS key, json_build_object('kind', 'summary',
        'snapshot', pg_current_snapshot()::text, 'captured_at', statement_timestamp(),
        'table', '{table}', 'tenant', '{tenant}', 'rows', count(*),
        'digest', encode(sha256(coalesce(string_agg(convert_to(id || ':', 'UTF8') || value_bytes,
          ''::bytea ORDER BY id COLLATE "C"), ''::bytea)), 'hex')) AS value FROM frozen
      UNION ALL
      SELECT 1, id, json_build_object('kind', 'row', 'id', id, 'version', version,
        'value_digest', encode(sha256(value_bytes), 'hex')) FROM frozen
    ) SELECT value::text FROM entries ORDER BY ord, key COLLATE "C";"""


def read_truth(path, expected_hash=None):
    raw = pathlib.Path(path).read_bytes()
    if expected_hash and hashlib.sha256(raw).hexdigest() != expected_hash:
        raise ValueError("truth file checksum mismatch")
    records = [json.loads(line) for line in raw.splitlines() if line.strip()]
    if not records or records[0].get("kind") != "summary":
        raise ValueError("truth has no snapshot summary")
    summary, rows = records[0], {}
    for row in records[1:]:
        if row.get("kind") != "row" or not isinstance(row.get("id"), str) or row["id"] in rows:
            raise ValueError("invalid or duplicate truth row")
        if not isinstance(row.get("version"), str) or len(row.get("value_digest", "")) != 64:
            raise ValueError("invalid truth version/hash")
        bytes.fromhex(row["value_digest"])
        rows[row["id"]] = row
    if summary.get("rows") != len(rows) or len(summary.get("digest", "")) != 64:
        raise ValueError("truth row count/digest mismatch")
    bytes.fromhex(summary["digest"])
    return summary, rows


def freeze_truth(env, path, table, tenant):
    result = subprocess.run(["psql", env["SB_ADMIN_URL"], "-X", "-q", "-At", "-v", "ON_ERROR_STOP=1",
                             "-c", truth_sql(table, tenant)], capture_output=True, text=True, env=env)
    if result.returncode:
        raise RuntimeError(f"truth export failed: {result.stderr[-500:]}")
    path = pathlib.Path(path)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(result.stdout)
    summary, _ = read_truth(tmp)
    tmp.replace(path)
    return {**summary, "truth_file": path.name, "truth_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def replica_digest(db, tenant, table="storm_items"):
    try:
        with closing(sqlite3.connect(pathlib.Path(db).resolve().as_uri() + "?mode=ro", uri=True, timeout=5)) as con:
            h, n = hashlib.sha256(), 0
            for key, note in con.execute(f'SELECT id, note FROM "{table}" WHERE tenant_id = ? ORDER BY id', (tenant,)):
                h.update(frame(key, note))
                n += 1
            return {"digest": h.hexdigest(), "rows": n}
    except sqlite3.Error as exc:
        return {"error": str(exc)}


def positions(db):
    try:
        with closing(sqlite3.connect(pathlib.Path(db).resolve().as_uri() + "?mode=ro", uri=True, timeout=0.1)) as con:
            con.row_factory = sqlite3.Row
            result = {}
            for table in ("_zbz_stream_seq", "_zbz_generations"):
                try:
                    result[table] = [dict(row) for row in con.execute(f'SELECT * FROM "{table}"')]
                except sqlite3.Error as exc:
                    result[table] = {"error": str(exc)}
            return result
    except sqlite3.Error as exc:
        return {"error": str(exc)}


def diff_replica(db, tenant, truth_rows, out, table="storm_items"):
    """Write every differing key; versions are evidence, not a substitute for values."""
    out = pathlib.Path(out)
    counts = dict(missing=0, extra=0, differing=0, version_only=0)
    seen = set()
    with (out / "missing.jsonl").open("w") as missing, (out / "extra.jsonl").open("w") as extra, \
            (out / "differing.jsonl").open("w") as differing, (out / "version-only.jsonl").open("w") as versions:
        with closing(sqlite3.connect(pathlib.Path(db).resolve().as_uri() + "?mode=ro", uri=True, timeout=5)) as con:
            for key, note, version in con.execute(
                    f'SELECT id, note, updated_at FROM "{table}" WHERE tenant_id = ? ORDER BY id', (tenant,)):
                seen.add(key)
                actual = {"id": key, "version": version, "value_digest": hashlib.sha256(value_frame(note)).hexdigest()}
                expected = truth_rows.get(key)
                if expected is None:
                    counts["extra"] += 1
                    extra.write(json.dumps(actual, ensure_ascii=False) + "\n")
                elif actual["value_digest"] != expected["value_digest"]:
                    counts["differing"] += 1
                    differing.write(json.dumps({"id": key, "expected": expected, "actual": actual}, ensure_ascii=False) + "\n")
                elif version_instant(actual["version"]) != version_instant(expected["version"]):
                    # Keep the original strings in evidence, but compare instants so
                    # PostgreSQL's space/+00 and the wire's T/Z do not swamp real drift.
                    counts["version_only"] += 1
                    versions.write(json.dumps({"id": key, "expected": expected, "actual": actual}, ensure_ascii=False) + "\n")
        for key in sorted(truth_rows.keys() - seen):
            counts["missing"] += 1
            missing.write(json.dumps(truth_rows[key], ensure_ascii=False) + "\n")
    return counts


def finalize_replica(db, tenant, done, client):
    """Call ONLY after libzb closes. Exact replicas go; all other evidence survives."""
    db = pathlib.Path(db)
    have = replica_digest(db, tenant)
    exact = have.get("digest") == done["digest"] and have.get("rows") == done["rows"]
    result = {"exact": exact, "rows": have.get("rows"), "closed_at": utc_now()}
    if exact:
        for suffix in ("", "-wal", "-shm"):
            pathlib.Path(str(db) + suffix).unlink(missing_ok=True)
        return result
    out = db.parent / "evidence" / client
    out.mkdir(parents=True, exist_ok=True)
    result["positions"] = positions(db)
    try:
        if not db.exists():
            raise FileNotFoundError(db.name)
        with closing(sqlite3.connect(db.resolve().as_uri() + "?mode=rw", uri=True, timeout=5)) as con:
            busy, wal, checkpointed = con.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            result["checkpoint"] = {"busy": busy, "wal_frames": wal, "checkpointed_frames": checkpointed}
            if busy:
                result["checkpoint"]["error"] = "busy; original database and WAL retained"
    except (sqlite3.Error, OSError) as exc:
        result["checkpoint"] = {"error": str(exc), "original_files_retained": True}
    try:
        _, rows = read_truth(db.parent / done["truth_file"], done["truth_sha256"])
        result["diff"] = diff_replica(db, tenant, rows, out)
    except (ValueError, KeyError, OSError, sqlite3.Error) as exc:
        result["diff_error"] = str(exc)
    # Do not copy a live database. This handle is closed, and the trio is moved
    # together even when checkpointing failed. No other client's files are touched.
    for suffix in ("", "-wal", "-shm"):
        source = pathlib.Path(str(db) + suffix)
        if source.exists():
            source.replace(out / ("replica.sqlite3" + suffix))
    result["replica"] = str(out / "replica.sqlite3")
    result["truth_sha256"] = done.get("truth_sha256")
    atomic_json(out / "result.json", result)
    return result


SIGNALS = ("gap stays open", "gap on ", "refusing the full", "chain predates", "seeding failed",
           "delivery lost in transit", "drain idle with deliveries unacknowledged")


def log_signal(line):
    return next((signal for signal in SIGNALS if signal in line), None)


def logged(a):
    """One stderr reader per PROCESS; never pretend a batch log names its client."""
    command = a.command[1:] if a.command[:1] == ["--"] else a.command
    child = subprocess.Popen(command, stderr=subprocess.PIPE)
    with pathlib.Path(a.log).open("w") as log, closing(child.stderr):
        for raw in iter(child.stderr.readline, b""):
            line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
            stamp = utc_now()
            tag = f"client={a.client}" if a.client else f"worker={a.worker} client=unattributed"
            log.write(f"[{stamp} {tag}] {line}\n")
            log.flush()
            if signal := log_signal(line):
                payload = json.dumps({"at": stamp, "at_ms": round(time.time() * 1000), "client": a.client,
                                      "worker": a.worker, "reason": signal, "line": line[:1000]}) + "\n"
                fd = os.open(a.signals, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
                try:
                    os.write(fd, payload.encode())
                finally:
                    os.close(fd)
    status = child.wait()
    atomic_json(pathlib.Path(a.log).with_suffix(".logger.done"), {"returncode": status, "closed_at": utc_now()})
    return status


async def sample(js, streams, bucket, keys):
    result = {"started_at": utc_now(), "streams": {}, "manifests": {}}
    for name in streams:
        try:
            info = await js.stream_info(name)
            state = info.state
            result["streams"][name] = {"at": utc_now(), "created": str(info.created),
                "first_seq": state.first_seq, "last_seq": state.last_seq, "bytes": state.bytes,
                "messages": state.messages, "max_bytes": info.config.max_bytes,
                "max_msgs": info.config.max_msgs, "max_age": info.config.max_age}
        except Exception as exc:
            result["streams"][name] = {"at": utc_now(), "error": type(exc).__name__}
    for key in keys:
        try:
            # A device may read its exact keys, but deliberately cannot STREAM.INFO
            # KV_generations (it would expose tenant names). key_value() needs that
            # forbidden lookup; use the same scoped DIRECT.GET as libzb instead.
            entry = await js.get_msg(f"KV_{bucket}", subject=f"$KV.{bucket}.{key}", direct=True)
            operation = (entry.headers or {}).get("KV-Operation")
            result["manifests"][key] = {"at": utc_now(), "revision": entry.seq,
                **({"error": "KeyDeleted", "operation": operation} if operation in ("DEL", "PURGE")
                   else {"manifest": json.loads(entry.data)})}
        except Exception as exc:
            result["manifests"][key] = {"at": utc_now(), "error": type(exc).__name__,
                                        "err_code": getattr(exc, "err_code", None)}
    result["ended_at"] = utc_now()
    return result


async def observe(a):
    import nats
    root = pathlib.Path(a.run_dir)
    nc, temporary = None, None
    interval, next_periodic, last_sample = a.interval, time.monotonic(), 0.0
    offset, pending, total, signals_seen, stream_samples = 0, [], 0, 0, 0
    config = json.loads((root / "run-config.json").read_text())
    clean_stop = False
    try:
        with (root / "retention.jsonl").open("w") as output:
            while not (root / "observer.stop").exists():
                # A single append per trigger prevents interleaving between loggers.
                signal_path = root / "signals.jsonl"
                if signal_path.exists():
                    with signal_path.open() as source:
                        source.seek(offset)
                        for line in source:
                            pending.append(json.loads(line))
                            signals_seen += 1
                        offset = source.tell()
                now = time.monotonic()
                periodic_due = now >= next_periodic
                if periodic_due or (a.triggered and pending and now - last_sample >= 1):
                    if nc is not None and nc.is_closed:
                        await nc.close()
                        nc = None
                        if temporary is not None:
                            pathlib.Path(temporary.name).unlink(missing_ok=True)
                            temporary = None
                    if nc is None:
                        try:
                            ident = json.loads(pathlib.Path(a.identity).read_text())
                            temporary = tempfile.NamedTemporaryFile("w", suffix=".creds", delete=False)
                            temporary.write(ident["creds"])
                            temporary.close()
                            nc = await asyncio.wait_for(nats.connect(a.nats, user_credentials=temporary.name, connect_timeout=3,
                                inbox_prefix=f"_INBOX.{ident['principal']}", allow_reconnect=True,
                                max_reconnect_attempts=2, reconnect_time_wait=1), timeout=6)
                            js = nc.jetstream(domain=ident.get("js_domain") or a.js_domain or None, timeout=2)
                        except Exception as exc:
                            if nc is not None:
                                await nc.close()
                                nc = None
                            if temporary is not None:
                                pathlib.Path(temporary.name).unlink(missing_ok=True)
                                temporary = None
                            output.write(json.dumps({"at": utc_now(), "error": type(exc).__name__,
                                                     "phase": "observer_connect"}) + "\n")
                            output.flush()
                            await asyncio.sleep(1)
                            continue
                    triggers, pending = pending, []
                    record = await sample(js, config["streams"], config["manifest_bucket"], config["manifest_keys"])
                    record["reason"] = "periodic" if periodic_due else "client_signal"
                    record["triggers"] = triggers
                    record["elapsed_s"] = round(time.time() - a.t0, 3)
                    named = {t["client"] for t in triggers if t.get("client")}
                    groups = {t["worker"] for t in triggers if not t.get("client")}
                    named.update(c for group in groups for c in config["worker_clients"].get(group, []))
                    record["replica_positions"] = {c: positions(root / f"{c}.sqlite3") for c in sorted(named)}
                    output.write(json.dumps(record, ensure_ascii=False) + "\n")
                    output.flush()
                    total += 1
                    stream_samples += int(all("error" not in s for s in record["streams"].values()))
                    last_sample = time.monotonic()
                    if periodic_due:
                        # Keep the schedule anchored; slow requests may skip a tick,
                        # but never create a burst of catch-up sampling requests.
                        while next_periodic <= last_sample:
                            next_periodic += interval
                await asyncio.sleep(0.2)
            clean_stop = True
    finally:
        if nc is not None:
            await nc.close()
        if temporary is not None:
            pathlib.Path(temporary.name).unlink(missing_ok=True)
        atomic_json(root / "observer.done", {"clean_stop": clean_stop, "samples": total, "stream_samples": stream_samples, "signals_seen": signals_seen,
                                             "pending_signals": len(pending), "stopped_at": utc_now()})


def file_sha256(path):
    digest = hashlib.sha256()
    with pathlib.Path(path).open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def archive_run(root, wait_s=20):
    root = pathlib.Path(root)
    config_path = root / "run-config.json"
    workers = json.loads(config_path.read_text()).get("worker_clients", {}) if config_path.exists() else {}
    def stopped():
        return (root / "observer.done").exists() and all((root / f"{w}.logger.done").exists() for w in workers)
    deadline = time.monotonic() + wait_s
    while not stopped() and time.monotonic() < deadline:
        time.sleep(0.2)
    # A worker without a final event may still own its database. Never delete it or
    # package a torn copy; record where its originals remain for a later collection.
    unfinished = [{"database": str(p.resolve()), "positions": positions(p)} for p in root.glob("*.sqlite3")]
    atomic_json(root / "uncollected-replicas.json", unfinished)
    entries = [p for p in sorted(root.rglob("*")) if p.is_file() and p.name != "collection.json"
               and not (p.parent == root and ".sqlite3" in p.name)]
    manifest = {"stopped": stopped(), "files": {str(p.relative_to(root)): file_sha256(p) for p in entries}}
    atomic_json(root / "collection.json", manifest)
    path = root.with_name(root.name + ".tar.gz")
    with tarfile.open(path, "w:gz") as archive:
        for entry in entries + [root / "collection.json"]:
            archive.add(entry, arcname=str(entry.relative_to(root)), recursive=False)
    return path


def extract_collection(archive, destination, expected_hash):
    """Verify transport and every extracted file before authorizing source cleanup."""
    if file_sha256(archive) != expected_hash:
        raise ValueError("collection archive checksum mismatch")
    destination = pathlib.Path(destination)
    with tarfile.open(archive) as saved:
        saved.extractall(destination, filter="data")
    manifest = json.loads((destination / "collection.json").read_text())
    for name, digest in manifest["files"].items():
        if file_sha256(destination / name) != digest:
            raise ValueError(f"collected file checksum mismatch: {name}")
    return manifest


def cleanup_collected(root, expected_hash):
    """Only a verified, unchanged, fully stopped collection can lose its source copy."""
    root = pathlib.Path(root)
    archive = root.with_name(root.name + ".tar.gz")
    if file_sha256(archive) != expected_hash:
        raise ValueError("source archive checksum mismatch; originals retained")
    with tarfile.open(archive) as saved:
        manifest = json.load(saved.extractfile("collection.json"))
    if json.loads((root / "collection.json").read_text()) != manifest:
        return {"removed": False, "reason": "source collection manifest changed; originals retained"}
    if not manifest["stopped"] or list(root.glob("*.sqlite3*")):
        return {"removed": False, "reason": "unfinished workers or replicas; originals retained"}
    if json.loads((root / "uncollected-replicas.json").read_text()):
        return {"removed": False, "reason": "uncollected replicas; originals retained"}
    names = {str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()}
    if names != set(manifest["files"]) | {"collection.json"}:
        return {"removed": False, "reason": "source files changed since collection; originals retained"}
    for name, digest in manifest["files"].items():
        if file_sha256(root / name) != digest:
            return {"removed": False, "reason": f"source changed: {name}; originals retained"}
    shutil.rmtree(root)
    archive.unlink()
    return {"removed": True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    log = sub.add_parser("logged")
    log.add_argument("--client")
    log.add_argument("--worker", required=True)
    log.add_argument("--log", required=True)
    log.add_argument("--signals", required=True)
    log.add_argument("command", nargs=argparse.REMAINDER)
    obs = sub.add_parser("observe")
    obs.add_argument("--run-dir", required=True)
    obs.add_argument("--identity", required=True)
    obs.add_argument("--nats", required=True)
    obs.add_argument("--js-domain", default="")
    obs.add_argument("--t0", type=float, required=True)
    obs.add_argument("--interval", type=float, default=10)
    obs.add_argument("--triggered", action="store_true", help="extra samples on recovery warnings; diagnostic runs only")
    archive = sub.add_parser("archive")
    archive.add_argument("--run-dir", required=True)
    cleanup = sub.add_parser("cleanup")
    cleanup.add_argument("--run-dir", required=True)
    cleanup.add_argument("--archive-sha256", required=True)
    args = parser.parse_args()
    if args.action == "logged":
        return logged(args)
    if args.action == "archive":
        path = archive_run(args.run_dir)
        print(json.dumps({"sha256": file_sha256(path)}))
        return 0
    if args.action == "cleanup":
        print(json.dumps(cleanup_collected(args.run_dir, args.archive_sha256)))
        return 0
    if args.interval <= 0:
        parser.error("--interval must be positive")
    return asyncio.run(observe(args))


if __name__ == "__main__":
    sys.exit(main())
