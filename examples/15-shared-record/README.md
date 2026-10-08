# 15 — a shared record: three editors, two offline, one merge

A "site_survey" table with five independently editable fields — access, hazard, contact, rating, notes. Three people edit it, two of them are offline, all in the same tenant. One user is in another tenant, never sees these edits.

For simplicity, four tabs in the browser:

| editor | how offline is simulated |
| -- | -- |
| Alice | online the whole time |
| Bob | off then on |
| Carol | off then on |
| Omar | tenant globex online, but sees nothing |

“Offline” in the browser is a network condition the page toggles:  `zb.close()` / `zb.connect()`. The library keeps working: writes go to the outbox, the replica keeps rendering, register stamps use the HLC, and nothing is sent.

> **The point is to observe merge**: who wins each field, who loses, and why the loser is told, with two scenarios.

PostgreSQL judges every write. The register format `{v, t, w}` decides which field survives.

> PostgreSQL decides which write wins the row, and merges the registers of every write it accepts. It works like Cassandra's per-cell timestamps or Figma's per-property last-writer-wins: each field keeps its newest edit, whoever wrote the row last.

## One table, two flavours

One table, two flavours, merged in one table for the demo: a real table picks one.

* Part A: five plain columns, one per field — the library's rebase (column granularity) on `stale`. A write whose columns are disjoint from the winner’s is reapplied and lands; one whose columns overlap is dropped and the loser is told. No page-side logic.
* Part B: one jsonb column holding the five registers, the format `{t,w,v}` as in [COOPERATIVE_EDITING.md](https://github.com/ndrean/zebridge/blob/main/COOPERATIVE_EDITING.md), merged register by register. Every field edit changes `doc`, so for the row every write to it is a write to the same column: a stale one is refused, and a late one that wins the row carries whatever `doc` its writer last saw. PostgreSQL merges that `doc` into the stored one (`register_cols => ARRAY['doc']`), so each register keeps its newest stamp; the page merges its own registers into what it sees before writing, and again after a refusal.

**The columns**:

|field|what it holds|
|--|--|
|access|how to get on site: "gate code 4421"|
|hazard|a known hazard "asbestos roof"`|
|contact|the site contact: "M. Dupont, 06…"`|
|rating|a 1–5 condition score: 3|
|notes|free text: "a new warehouse..."`|
|||
|doc|the jsonb version|

The DBA runs firstly:

```sql
CREATE TABLE site_survey (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),

  -- part A: one field per column, no register format inside.
  access       text,
  hazard       text,
  contact      text,
  rating       integer,
  notes        text,

  -- part B: the register format, one register per field, inside one column 'doc'
  doc          jsonb NOT NULL DEFAULT '{}'::jsonb,   -- the registers
  
  -- writable table needed columns
  tenant_id    text NOT NULL,
  updated_at   timestamptz NOT NULL DEFAULT now(),   -- the version (LWW on the row)
  deleted_at   timestamptz,                          -- the tombstone
  last_writer  text                                  -- the tiebreak
);
```

The table is tenant-scoped, not public.

Three editors are in tenant "acme"; Omar is in "globex".

RLS scopes the rows, the NATS JWT scopes the subjects, and neither is a claim in a payload.

The DBA then runs:

```sql
SELECT * FROM zebridge_enable(
  'public.site_survey'::regclass,
  writable      => true,
  tenant_col    => 'tenant_id',
  version_col   => 'updated_at',
  tombstone_col => 'deleted_at',
  tiebreak_col  => 'last_writer',
  register_cols => ARRAY['doc'],   -- part B: PostgreSQL merges the registers of each accepted write
  publication   => 'my_pub',
  dry_run       => false
);
```

`dry_run => true` first if you want to see every rule before anything is written.

The schema declares the sync rules in one call, as every writable table does.

The DBA creates a seed, one row, for tenant_id = 'acme', with `doc = '{}'::jsonb`.

```sql
INSERT INTO site_survey (id, tenant_id, access, hazard, contact, rating, notes, doc)
VALUES (
  '11111111-1111-4111-8111-111111111111',
  'acme',
  NULL, NULL, NULL, NULL, NULL,
  '{}'::jsonb
);
```

The three editors (Alice, Bob, Carol) in the tenant "acme" share this id; Omar in the tenant "globex" never sees it.

Every register is empty at t = 0, so the first write to any of them wins uncontested.

### Part A: five columns

|field|what it will do|who typically writes it|
|--|--|--|
|access|`INTO (access) VALUES "gate code 4421"`|Alice (office)|
|hazard|`INTO (hazard) VALUES "asbestos roof"`|Bob (on site)|
|contact|`INTO (contact) VALUES "M. Safety"`|Carol (phone)|
|rating|`INTO (rating) VALUES 3`|any of the three|
|notes|`INTO (notes) VALUES "the new warehouse..."`|any of the three|

### Part B: one column, five registers

A site's editable fields are one jsonb column, `doc`, holding one register per field - see [COOPERATIVE_EDITING](https://github.com/ndrean/zebridge/blob/main/COOPERATIVE_EDITING.md): `{v, t, w}`: a value, a stamp, and its writer.

```js
doc = {access: {t,v,w}, hazard: {t,v,w},...}
```

|register|what it will do|who typically writes it|
|--|--|--|
|access|`SET doc = doc  \|\| "access": {v: "gate code 4421", t, w}::jsonb`|Alice|
|hazard|`SET doc = doc  \|\|  "hazard": {v: "asbestos roof", t, w}::jsonb`|Bob|
|contact|`SET doc = doc  \|\|  "contact": {v: "M. Dupont, 06…", t, w}::jsonb`|Carol|
|rating|`SET doc = doc  \|\| "rating": {v: 3, t, w}::jsonb`|any of the three|
|notes|`SET doc = doc  \|\|  "notes": {v: "the new...", t, w}::jsonb`|any of the three|

* `t` is RFC 3339, UTC, ending in Z, with exactly six fractional digits (2026-09-20T06:20:26.474000Z).
* `w` is stable per editor: laptop-alice, phone-bob, tablet-carol.
* The library compares `t` as text, which is time order only at that fixed width, and breaks ties on `w`.

The row itself carries the LWW machinery.

### Scenario

#### Part A

| t(s) | Alice | Bob | Carol | what it shows |
| -- | -- | -- | -- | -- |
| 0 | opens, seeds | opens, seeds | opens, seeds | all three converge on the same record |
| 5 | edits "access" | — | — | a plain write, echoed |
| 10 | — | goes offline | — | Bob's replica stops at the row as it is now |
| 15 | edits "notes" | — | — | a plain write |
| 20 | edits "rating" to 2 | — | goes offline, edits "rating" to 1 | two writers on the same register, one offline |
| 25 | edits "contact" | — | — | a plain write |
| 28 | — | edits "hazard" | — | a write queues, stamped after Alice's last write |
| 30 | — | comes back, outbox flushes | — | hazard lands: the write is the newest, and it sets only `hazard` |
| 35 | edits "rating" to 5 | - | - | the register moves again |
| 40 | — | — | comes back, outbox flushes | rating loses again; UI shows “Carol’s edit LOST” |
| 45 | — | — | edits "contact" | a plain write; contact changes |
| 50 | — | — | — | everyone re-reads; the merge is stable |

#### Part B

| t(s) | Alice | Bob | Carol | what it shows |
| -- | -- | -- | -- | -- |
| 0 | opens, seeds | opens, seeds | opens, seeds | all three converge on the same record |
| 5 | edits "doc.access" | — | — | a plain write, echoed |
| 10 | — | goes offline | — | Bob's replica stops at the row as it is now: `doc` holds access only |
| 15 | edits "doc.notes" | — | — | a plain write |
| 20 | edits "doc.rating" to 4 | — | goes offline, edits "doc.rating" to 2 | two writers on the same register, one offline |
| 25 | edits "doc.contact" | — | — | a plain write |
| 28 | — | edits "doc.hazard" | — | a write queues: Bob's `doc` is the one he saw at t = 10, plus hazard |
| 30 | — | comes back, outbox flushes | — | Bob's write is the newest, so PostgreSQL accepts it — and merges it: hazard is added, Alice's notes, rating and contact keep their newer stamps |
| 35 | edits "doc.rating" to 5 | - | - | the register moves again |
| 40 | — | — | comes back, outbox flushes | rating loses again; UI shows “Carol’s edit LOST” |
| 45 | — | — | edits "doc.contact" | a plain write; contact changes |
| 50 | — | — | — | everyone re-reads; the merge is stable |

Without `register_cols`, t = 30 is a silent loss: Bob's write is accepted whole, his `doc` from t = 10 replaces the row's, no verdict says anything went wrong, and Alice's three registers are gone. `scripts/scenarios/registers.py` plays this sequence with and without the merge. Part A has no such moment, because each write sets only its own column.

#### Results

| | A: five columns | B: one doc |
| -- | -- | -- |
| rebase unit | the column (library) | the register (page) |
| hazard after Bob’s offline edit | lands | lands |
| Alice's fields after Bob's late write | untouched | kept: PostgreSQL merges the registers |
| rating after Carol’s offline edit | LOST, right outcome | page re-merges, loses, right outcome |
| schema changes for a new field | one ADD COLUMN | nothing |
| page-side merge logic | none | merge before writing, once more after a `stale` |
| matches COOPERATIVE_EDITING.md | yes, one register per column | yes, registers inside one doc |

## The app

### Setup Postgres & NATS

Three steps, from the repository's root, with the bridge built (`zig build -Doptimize=ReleaseFast`).
The parentheses keep each env file's values inside that one command.

**1. NATS** — the whole stack, generated: operator, account, signing keys, `nats-server.conf`
(client port 4222, WebSocket 8080 for the browser) and `zb-nats/.env.nats`. `operator` is the
mode with enrollment, which the invites need:

```sh
./zig-out/bin/bridge --init-nats operator --dir ./zb-nats   # add --js-domain <name> for clients behind a leaf node
nats-server -c zb-nats/nats-server.conf
```

For a quick try on a laptop, `--init-nats dev` instead: an open server, no authentication, no
enrollment, so no invites. Map each editor to its tenant (`INSERT INTO zebridge_user_tenants
(principal, tenant_id) VALUES ('alice', 'acme'), …`) and open the page with `?as=alice`: the
name is the principal. Anyone can claim any name on such a server — never on a reachable host.

**2. PostgreSQL** — the bridge's functions, grants and the publication, from `.env.bridge`
(`DATABASE_READER_URL`, `DATABASE_WRITER_URL`, `BRIDGE_CDC_PUBLICATION`):

```sh
( set -a; . ./.env.bridge; set +a; ./zig-out/bin/bridge --init-sql ) | psql "$ADMIN_URL"
```

Then the table, `zebridge_enable` and the seed row (above).

**3. The bridge** — both env files; its first start creates the replication slot:

```sh
( set -a; . zb-nats/.env.nats; . ./.env.bridge; set +a; exec ./zig-out/bin/bridge )
```

### Generate the invites

The bridge is agnostic, so the DBA generates invites.

> If you want to automate this, you need a backend that can receive a demand from a client, then connect to the PostgreSQL database (WRITER profile), and can send the OTP code back to the client (eg via email).

```sql
INSERT INTO public.zebridge_invites (principal, tenant_id) VALUES
  ('alice', 'acme'),
  ('bob', 'acme'),
  ('carol', 'acme'),
  ('omar', 'globex')
RETURNING code, principal, tenant_id, expires_at;
```

Use the returned invite code per tab:

```txt
http://localhost:5179/?invite=<code>&as=alice
```

### The page

Two flows meet on the page:

* Data, replicated into each browser: the site_survey table, one row per site, its editable fields kept as one register each in a jsonb column. Every editor's replica converges, offline or not.

* Verdicts, coming back from PostgreSQL on each write (`onVerdict`): applied, rebased, lost, deleted, rejected. The page lists them, so the merge is watched, not inferred.

Each window has:

* Two forms, side by side: **A · a column** writes one of the five plain columns; **B · a register in `doc`** writes one register, merged into the `doc` this replica holds.
* The record, rendered from that editor's replica: the five columns (A), then the five registers of `doc` (B), each with its writer `w` and the time of its stamp `t`.
* A chip, from `pending()`: online, or offline with the number of queued writes. Going offline dims the record.
* Activity: each write sent, and what became of it (`onVerdict`) — applied, rebased, the row moved first and the page merged into it, or LOST with the value that stands.

### Played by a script

`scripts/scenarios/shared_record.py` plays the timeline above with three zb-client-ts clients (alice, bob, carol) and an outsider, on part A and on part B, and checks the outcome mechanically: PostgreSQL holds access, notes and rating 5 by alice, hazard by bob, contact by carol; carol is told she lost rating; every replica equals PostgreSQL; the outsider changes nothing and sees nothing.

```txt
scripts/scenarios/run.py owns -k shared_record
```

### The TS client survival kit

The page uses `zb-client-ts` and nothing else from ZeBridge. The whole client side of a cooperative app fits in these calls:

```js
import { ZeBridge, NotEnrolled, mergeRegisters } from 'zb-client-ts';

// 1. who am I — the first visit enrolls with an invite; later visits need nothing
const zb = new ZeBridge({ 
  bridgeUrl: 'https://bridge.mydomain.org:27434',  // the bridge daemon
  invite: <code>,                 // the invite code
  tables: ['site_survey'] // the tables you follow  
});
// zb.natsUrl is populated by the identity when `zb.connect()`

try { 
  await zb.connect(); // the connection dance to get an auto-rotating JWT
} catch (e) { 
  if (e instanceof NotEnrolled) 
    askForAnInvite(); // your own UI call
}

console.log(zb.principal);   // eg 'alice': the writer of every register

// 2. read — SQL on the local replica, offline too
// example:
const [row] = await zb.query('SELECT * FROM site_survey WHERE deleted_at IS NULL LIMIT 1');
const row_id = row.id; 

// or `const id = crypto.randomUUID()` minted locally

// 3. write into a column — applied here at once, queued, sent, judged by PostgreSQL
const { version } = await zb.mutate(
  'site_survey',                // the table
  'UPDATE',                     // the SQL verb, INSERT | UPDATE | DELETE
  { id: row_id},                // the row_id
  { hazard: 'asbestos roof' }   // the column:value
);

// 4. reactive primitive — the row moved, by me or by anyone
// and trigger your UI change
zb.onChange(
  'site_survey',    // the table
  redraw            // the callback
);

// 5. write by merge into the jsonb column 'doc': merge — one jsonb doc of registers {v, t, w}, edited by many
mine.hazard = { 
  v: 'asbestos roof',   // the value, any
  // zb primitive to stamp with the right clock to be able to compare
  t: zb.stamp(),        
  w: zb.principal       // the writer id
;

await zb.mutate(
  'site_survey',        // the table
  'UPDATE',             // the SQL verb
  { id: row_id },
    doc: mergeRegisters(row.doc, mine) 
    // the primitive takes 
  }
);

// 6. offline and back — writes wait in the outbox, then go
await zb.close();         // can be triggered by the UI
await zb.pending();       // 1: one write waiting
await zb.connect();       // can be triggered by the UI

// 7. what became of each write — matched to mutate() by its version
zb.onVerdict(({ version, outcome, columns }) => show(version, outcome, columns));
```

| call | what it gives |
| -- | -- |
| `new ZeBridge(opts)`, `connect()`, `NotEnrolled` | an identity on this device, enrolled once with an invite |
| `principal` | who this editor is: the `w` of each register it writes |
| `query(sql, ...params)` | the replica, read with SQL; it answers offline |
| `mutate(table, op, key, values)` | a write, applied locally at once; returns its `version` |
| `onChange(table, cb)` | a call each time the table's rows change, from any writer |
| `stamp()` | a register's `t`, on the bridge's clock, never behind what this replica saw |
| `mergeRegisters(a, b)` | the merge of two docs, register by register: the later `t` wins, then `w` |
| `close()`, `connect()`, `pending()` | offline and back; the writes still waiting in the outbox |
| `onVerdict(cb)` | each write's outcome, once: `applied`, `rebased`, `lost`, `deleted` or `rejected` |

`onLog(cb)` says the same things in words, for people reading a console; an app does not need it.

**The merge (part B).** Merge your register into the `doc` you see (`mergeRegisters`) and write it. PostgreSQL merges what it accepts into the stored `doc`, register by register, so a write built on an old `doc` cannot roll anyone back. A write refused as `stale` comes back to you: merge your registers into the newer row and write again; a register whose stamp lost to a newer one is LOST, and the page says so.

Serve it, from `examples/15-shared-record/web`. On a local stack, the dev server's proxy carries NATS's WebSocket and the bridge's `/enroll` on the page's own origin; tell it where they are:

```sh
ZB_NATS_WS_ORIGIN=ws://127.0.0.1:8080 \
ZB_BRIDGE_ORIGIN=http://127.0.0.1:27434 \
pnpm dev   # http://localhost:5179
```

Against a deployed bridge, build the page for it instead: `VITE_ZB_BRIDGE_URL=https://bridge.example.com pnpm build`.

## What the demo deliberately does not do

No CRDTs, no causal tracking, no ordered lists, no text collaboration. The register format is per-field, by design; notes is one register, and two editors who both edit it lose one. That is the same tradeoff as in COOPERATIVE_EDITING.md.

No multi-master. PostgreSQL judges every write.

No map, no fleet, no routing. Those are examples 08, 13, 14. This one is about the merge.

No offline seeding. All four editors seed at t = 0, online. The offline part is writes, which is the part the register model addresses.

No server-side merge. The rebase is done by the library, following the rules the server stated. The server never sees a rebase as a new edit; it sees a fresh write.

## What it makes visible

Four panels, one per editor, and the timeline:

* The register format (t, w, v), on a real record.
* The difference between LWW on the row (who must merge) and LWW on the register
(which value survives).
* What rebased means, in a case where it changes the outcome.
* What LOST means, and why it is not a bug.
* What tenant isolation looks like at the client.
* What the outbox is, and why the verdict is the right shape for it.
* That the merge is deterministic: same inputs, same golden file.
