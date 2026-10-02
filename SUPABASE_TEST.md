# ZeBridge on Supabase, step by step

From an empty Supabase project to a client that follows two tables and writes back through the bridge. Every command here was run as written, on a free Supabase project (PostgreSQL 17), with the bridge and NATS on a laptop.

What runs where:

* **Supabase**: PostgreSQL, the ZeBridge objects (tables, functions, event triggers, two roles), the application tables.
* **The bridge's host** (a laptop here, a VPS later): `nats-server` and the bridge.
* **A client**: here the Python package over libzb, on the same laptop.

## 0. Before you start

* A Supabase project, and its database password.
* IPv6 on the bridge's host. Replication needs Supabase's **direct** connection, which is IPv6 only (unless you buy Supabase's IPv4 add-on), and never its pooler:

  ```sh
  curl -6 https://ifconfig.co     # prints an IPv6 address: good
  ```

* The bridge built (`zig build -Doptimize=ReleaseFast`), `nats-server` and `psql` on the PATH.

## 1. The connection file

In Supabase: **Connect** → **Direct connection**: a URL of the form:

`postgresql://postgres:<password>@db.<project>.supabase.co:5432/postgres`.

Create `.env.supabase` (git-ignored, mode 600). It holds the admin URL, and the two roles the init SQL will create for the bridge, with passwords you generate:

```sh
host=db.<project>.supabase.co
umask 077
cat > .env.supabase <<EOF
SB_ADMIN_URL=postgresql://postgres:<password>@$host:5432/postgres
DATABASE_READER_URL=postgresql://zb_reader:$(openssl rand -hex 16)@$host:5432/postgres?sslmode=require
DATABASE_WRITER_URL=postgresql://zb_writer:$(openssl rand -hex 16)@$host:5432/postgres?sslmode=require
BRIDGE_CDC_PUBLICATION=my_pub
EOF
```

## 2. Check the project

```sh
set -a; . ./.env.supabase; set +a
psql "$SB_ADMIN_URL" -X <<'SQL'
select version();
show wal_level;                 -- logical
show max_replication_slots;     -- 5 on the free tier
show max_slot_wal_keep_size;    -- 512MB: a stopped bridge holds at most this much WAL
select rolsuper, rolreplication, rolcreaterole from pg_roles where rolname = current_user;
-- event triggers: created, then rolled back
begin;
create function zb_probe() returns event_trigger language plpgsql as $$ begin end $$;
create event trigger zb_probe on ddl_command_end execute function zb_probe();
rollback;
SQL
```

Expected: `wal_level` is `logical`; `postgres` is not a superuser but has replication and createrole; the event trigger is created.

## 3. Install ZeBridge in the database

The bridge renders the init SQL from the connection file: the two roles, the `zebridge_*` tables and functions, the event triggers, the publication. Render it in a clean environment, so no other settings leak in, and apply it as `postgres`:

```sh
umask 077
env -i PATH="$PATH" HOME="$HOME" sh -c 'set -a; . ./.env.supabase; set +a; ./zig-out/bin/bridge --init-sql' > zebridge_init.sql
set -a; . ./.env.supabase; set +a
psql "$SB_ADMIN_URL" -X -q -f zebridge_init.sql
```

`zebridge_init.sql` contains the two roles' passwords: delete it afterwards.

Check:

```sql
select count(*) from pg_tables where schemaname = 'public' and tablename like 'zebridge%';   -- 10
select evtname from pg_event_trigger where evtname like 'zebridge%';                        -- 4
select tablename from pg_publication_tables where pubname = 'my_pub';                       -- the 4 internal tables
select rolname, rolreplication, rolbypassrls from pg_roles where rolname in ('zb_reader', 'zb_writer');
```

## 4. NATS

The bridge generates the whole NATS setup (operator, account, signing keys, the bridge's credentials) into `./zb-nats/` (git-ignored):

```sh
./zig-out/bin/bridge --init-nats operator
nats-server -c zb-nats/nats-server.conf
```

Here the ports were moved off the defaults (client 4232, monitoring 8232, WebSocket 8082) so another NATS could run beside it: edit `port`, `http_port` and the websocket `port` in `zb-nats/nats-server.conf` before starting it.

`zb-nats/operator.store` holds every seed: keep it off the server.

## 5. The bridge

The generated `zb-nats/.env.bridge` names NATS and the enrollment keys; `.env.supabase` names the database. Load both, the database last:

```sh
#!/bin/sh
# zb-nats/run-bridge.sh
cd "$(dirname "$0")/.."
set -a
. zb-nats/.env.bridge
. ./.env.supabase
NATS_URL=nats://127.0.0.1:4232
set +a
exec ./zig-out/bin/bridge
```

On start, the log shows the slot created on Supabase, the publication verified, the streams created on NATS, and `enrollment endpoint armed`.

## 6. The tables

Create the tables as plain tables, and let `zebridge_enable` add the rest (version trigger, tenant guard, RLS, replica identity, the publication). The bridge follows the change without a restart.

```sql
CREATE TABLE public.counter_public (
    uid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    value integer DEFAULT 0 NOT NULL,
    last_writer varchar(255),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE public.counter_tenant (
    uid uuid DEFAULT gen_random_uuid() PRIMARY KEY,
    value integer DEFAULT 0 NOT NULL,
    tenant_id varchar(255) NOT NULL,
    last_writer varchar(255),
    inserted_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- the same rows for every tenant
SELECT step, status FROM zebridge_enable('public.counter_public',
  public_reason => 'demo counter — identical content for every tenant',
  writable => true, version_col => 'updated_at', tiebreak_col => 'last_writer',
  generations => true, allow_physical_deletes => true,
  publication => 'my_pub', dry_run => false);

-- rows divided by tenant_id
SELECT step, status FROM zebridge_enable('public.counter_tenant',
  tenant_col => 'tenant_id',
  writable => true, version_col => 'updated_at', tiebreak_col => 'last_writer',
  generations => true, allow_physical_deletes => true,
  publication => 'my_pub', dry_run => false);

INSERT INTO public.counter_public (value) VALUES (0);
INSERT INTO public.counter_tenant (value, tenant_id) VALUES (0, 'acme'), (0, 'globex');
```

`allow_physical_deletes => true` says these counters are never deleted. A table whose rows are deleted gets a tombstone column instead (`deleted_at timestamptz`, `tombstone_col => 'deleted_at'`).

The last step of each `zebridge_enable` reads `T3 bridge=LIVE T4 nats=LIVE`.

## 7. An invite

Your application's backend issues one per user, once it has signed them in. The code is 16 characters or more:

```sql
INSERT INTO public.zebridge_invites (code, principal, tenant_id)
VALUES ('<a random code>', 'alice', 'acme');
```

## 8. A client

The first run redeems the invite at the bridge, stores its identity next to the replica, seeds both tables and follows them. Later runs need neither the invite nor the NATS URL, and the JWT renews itself.

```python
from zebridge import ZeBridge

with ZeBridge(bridge_url="http://127.0.0.1:27434", invite="<the code>",
              nats_url="nats://127.0.0.1:4232", db_path="demo.sqlite3",
              tables=["counter_public", "counter_tenant"]) as zb:
    print(zb.query("SELECT value FROM counter_public"))
    print(zb.query("SELECT tenant_id, value FROM counter_tenant"))   # acme only
    uid = zb.query("SELECT uid FROM counter_public")[0]["uid"]
    zb.mutate("counter_public", "UPDATE", {"uid": uid}, {"value": 1})
```

## 9. What to check, and what was measured

| check | measured |
| --- | --- |
| enroll and seed both tables | 2.1 s |
| the tenant table holds only `acme` | yes |
| `UPDATE` in Supabase (psql) reaching the client | 20 ms |
| a client write, send → PostgreSQL's verdict | median 42 ms (40–127) over 20 writes |
| `ALTER TABLE … ADD COLUMN` reaching the client's table | about 1 s |
| a new `psql` connection to Supabase, for scale | about 200 ms |

The bridge was on a laptop in France; on a VPS in Supabase's region (London, UK), every number above shrinks with the distance.

## 10. Tearing down

```sh
# stop the bridge, then drop its slot (or Supabase keeps WAL for it, up to 512 MB)
ADMIN_DATABASE_URL="$SB_ADMIN_URL" ./zig-out/bin/bridge --drop-slot zb_slot
```

The tables, roles and ZeBridge objects stay in the database until you drop them, or delete the project.
