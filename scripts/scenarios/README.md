# Scenario scripts

Runnable counterparts to `TEST_SCENARIOS.md`. Each prints the evidence the scenario
claims, so a group in that document is a command rather than a paragraph someone has to
re-derive at 1am.

Deliberately **not** in `python-consumer/`: that is a reference *client*, meant to show
an SDK author what a correct consumer looks like. These are a test harness. They rot for
different reasons and should read as different things.

Python here because these grew out of live debugging and the decode side needs
MessagePack. The natural alternative is `emitter/` — it already has `gnat` and a
Postgres pool, so it would not need the `psql` shell-out below. Worth moving if this
harness grows.

## Setup

```bash
python3 -m venv scripts/scenarios/.venv
scripts/scenarios/.venv/bin/pip install -r scripts/scenarios/requirements.txt

set -a && . ./.env.bridge && set +a   # DATABASE_URL, NATS_URL, BRIDGE_PORT
export NATS_NKEY_SEED="SU..."         # kept out of the file on purpose

scripts/scenarios/.venv/bin/python scripts/scenarios/stampede.py
```

`.env.bridge` and not `.env.admin`: the probes drive the bridge, and the bridge has no
business seeing admin credentials — that separation is one of the things `endpoint.py`
exists to keep true.

`nats-py` needs `nkeys` alongside it for seed authentication — it imports the package
lazily, so a missing dependency surfaces at connect time, not at install time.

| variable | default |
| --- | --- |
| `NATS_URL` | `nats://127.0.0.1:4222` |
| `NATS_NKEY_SEED` | *required*, from `.env` |
| `ZB_PSQL` | `docker exec -i postgres-primary psql -U postgres` |
| `DATABASE_URL` | *required by the bridge-spawning probes* — they start a real bridge, and it no longer has a PG_HOST/PG_USER fallback |
| `ZB_BRIDGE_ARGS` | `--slot zb_probe --pub my_pub --port 9096` — the probes get their own slot and port so they never disturb a running bridge |

Subject and stream names are read from `topology.json`, never hardcoded: one rename must
move the bridge, `nats-init` and this harness together.

## Scripts

| script | scenario | asserts |
| --- | --- | --- |
| `snapshot.py [table] [seed_rows]` | B, C | chunks replay, no duplicate keys, every chunk fits `max_payload`, and the key sequence is **identical to PostgreSQL's own `ORDER BY`** — which is what proves composite keyset pagination. Pass `25000` to force ~3 chunks and exercise a real boundary |
| `stampede.py [table] [n]` | C1 | N concurrent requests → exactly **one** enters the window; the rest are refused by the broker |
| `burst.py [table] [stmts] [rows]` | I | generates a burst large enough to build a backlog, then names the LOOP numbers that mean healthy versus "the reader cannot drain its socket" |
| `invalidate.py` | schema | migrations run against a **live** bridge, and the four caches that must notice: the KV schema (DDL trigger → WAL → KV), the replication thread's relation cache (⚠️ decoded positionally — staleness shifts values rather than erroring), the refusal registry (a keyless table refused, then **fixed without a restart**, then dropped), and the write path's catalog cache — both halves, since a *dropped* column heals through PostgreSQL's `42703` while an *added* one never reaches PostgreSQL at all |
| `mutate.py [table] [id]` | ingress | the last-write-wins round trip: a stale version must lose even when it arrives last |
| `poison.py [table]` | ingress | four unprocessable messages are dead-lettered **once each**, not retried forever |
| `endpoint.py` | config | one process, one NATS address — `NATS_HOST=nats-server` beside a working `NATS_URL` must not leave ingress dialling somewhere else |
| `envcheck.py` | config | offline: the port in `.env.bridge`'s URLs matches `PG_PUBLISH_PORT` in `.env.admin`, and no admin variable has crept back into the bridge's file. Nothing else can check this — the bridge does not know what compose published, and giving it `.env.admin` would undo the split |
| `credentials.py` | config | the bridge cannot connect as the admin: no `DATABASE_URL` → refuses to start even with full `PG_*` credentials present; no `DATABASE_WRITER_URL` → ingress off rather than falling back to the read role |
| `faults.py` | config | `REQUESTS` missing → the bridge stops **non-zero** instead of spinning; the consumer deleted underneath it → a status frame is recycled, not acked into a null unwrap |

Exit code is 0 when the assertion holds, so these can be chained in CI later. `burst.py`
and `poison.py` report rather than assert — their evidence is in the bridge's log, which
this harness does not read.

`endpoint.py` and `faults.py` start bridges of their own, and `faults.py` deletes the
`REQUESTS` stream: it saves the config first, recreates it byte-identically afterwards,
and **diffs the result** so a silent restore failure is a test failure rather than a
surprise next week. If it ever aborts between the two, it prints the exact
`nats stream add … --config` line to run.

## Prerequisites for the ingress scripts

The write path is off unless a role and a table are opened for it:

```sql
SELECT zebridge_grant_edge_writes('public.test_types');
```

```bash
POSTGRES_WRITER_USER=bridge_writer POSTGRES_WRITER_PASSWORD=… \
SYNC_RULES="test_types:updated_at,deleted_at" \
./zig-out/bin/bridge --slot my_slot --pub my_pub
```

Without writer credentials the mutation listener does not start at all — deliberately,
rather than falling back to the read role, which has REPLICATION.
