# 15 — a shared record: three editors, two offline, one merge

A site survey is table with five independently editable fields — access, hazard, contact, rating, notes. Three people edit it, two of them are offline, all in the same tenant. One user is in another tenant, never sees these edits.

The point is the merge: who wins each field, who loses, and why the loser is told, with two scenarios.

| editor | device | how offline is simulated |
| -- | -- | -- |
| Alice | laptop | online the whole time |
| Bob | phone ( ) | the demo toggles the network off for 60 s at t = 10 s |
| Carol | phone ( 🤖) | the demo toggles the network off for 120 s at t = 20 s |
| Omar | laptop | tenant globex online, but sees nothing |

“Offline” in the browser is a network condition the page toggles. The library keeps working: writes go to the outbox, the replica keeps rendering, register stamps use the HLC, and nothing is sent.

Alice edits from a laptop; Bob from a phone, offline for a minute at a plant; Carol from another phone, offline for two minutes on a train. A fourth editor, Omar,  is in a different tenant. He sees none of it and can write none of it.

|field|what it holds|who typically writes it|
|--|--|--|
|access|how to get on site: "gate code 4421"|Alice (office)|
|hazard|a known hazard "asbestos roof"`|Bob (on site)|
|contact|the site contact: "M. Dupont, 06…"`|Carol (phone)|
|rating|a 1–5 condition score: 3|any of the three|
|notes|free text: "..."`|any of the three|

PostgreSQL judges every write. The register format `{v, t, w}` decides which field survives. Nothing about the merge is decided by the browser.

## One table, two flavours

One table, two flavours, merged in one table for the demo: a real table picks one.

* Part A: five plain columns, one per field — the library's rebase (column granularity) on `stale`. A write whose columns are disjoint from the winner’s is reapplied and lands; one whose columns overlap is dropped and the loser is told. No page-side logic.
* Part B: one jsonb column holding the five registers, the format `{t,w,v}` as in [COOPERATIVE_EDITING.md](https://github.com/ndrean/zebridge/blob/main/COOPERATIVE_EDITING.md). format, merged by the page. Every field edit changes doc, so every stale write overlaps and the library drops it. The page does the field-level merge itself: a mine map of pending registers, merged into the winner’s doc on the echo, written again — the same pattern the depot page uses.

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

The schema declares the sync rules in one call, as every writable table does:

```sql
SELECT * FROM zebridge_enable(
  'public.site_survey'::regclass,
  writable      => true,
  tenant_col    => 'tenant_id',
  version_col   => 'updated_at',
  tombstone_col => 'deleted_at',
  tiebreak_col  => 'last_writer',
  publication   => 'my_pub',
  dry_run       => false
);
```

`dry_run => true` first if you want to see every rule before anything is written.

A seed creates one row for tenant_id = 'acme', with `doc = '{}'::jsonb`.

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

|field|what it holds|who typically writes it|
|--|--|--|
|access|`INTO (access) VALUES "gate code 4421"`|Alice (office)|
|hazard|`INTO (hazard) VALUES "asbestos roof"`|Bob (on site)|
|contact|`INTO (contact) VALUES "M. Dupont, 06…"`|Carol (phone)|
|rating|`INTO (rating) VALUES 3`|any of the three|
|notes|`INTO (notes) VALUES "..."`|any of the three|

### Part B: one column, five registers

A site's editable fields are one jsonb column, doc, holding one register per field
(see COOPERATIVE_EDITING.md): `{v, t, w}`, a value, a stamp, and its writer.

|register|what it holds|who typically writes it|
|--|--|--|
|access|`SET doc = doc  \|\| "access": {v: "gate code 4421", t, w}::jsonb`|Alice (office)|
|hazard|`SET doc = doc  \|\|  "hazard": {v: "asbestos roof", t, w}::jsonb`|Bob (on site)|
|contact|`SET doc = doc  \|\|  "contact": {v: "M. Dupont, 06…", t, w}::jsonb`|Carol (phone)|
|rating|`SET doc = doc  \|\| "rating": {v: 3, t, w}`|any of the three|
|notes|`SET doc = doc  \|\|  "notes": {v: "...", t, w}::jsonb`|any of the three|

* `t` is RFC 3339, UTC, ending in Z, with exactly six fractional digits (2026-09-20T06:20:26.474000Z).
* `w` is stable per editor: laptop-alice, phone-bob, tablet-carol.
* The library compares `t` as text, which is time order only at that fixed width, and breaks ties on `w`.

The row itself carries the LWW machinery:

### Scenario

The four editors share a scripted timeline, so the merge is reproducible:

| t(s) | Alice | Bob | Carol | what it shows |
| -- | -- | -- | -- | -- |
| 0 | opens, seeds | opens, seeds | opens, seeds | all three converge on the same record |
| 5 | edits "access" | — | — | a plain write, echoed |
| 10 | — | goes offline, edits "hazard" | — | a write queues; no one else sees it |
| 15 | edits "notes" | — | — | a plain write |
| 20 | edits "rating" to 4 | — | goes offline, edits "rating" to 2 | two writers on the same register, one offline |
| 25 | edits "contact" | — | — | a plain write |
| 30 | — | comes back, outbox flushes | — | hazard lands; rating loses to Alice (Carol is offline) |
| 35 | edits "rating" to 5 | - | - | the register moves again |
| 40 | — | — | comes back, outbox flushes | rating loses again; UI shows “Carol’s edit LOST” |
| 45 | — | — | edits "contact" | a plain write; contact changes |
| 50 | — | — | — | everyone re-reads; the merge is stable |

## The page

Two flows meet on the page:

* Data, replicated into each browser: the site_survey table, one row per site, its editable fields kept as one register each in a jsonb column. Every editor's replica converges, offline or not.

* Verdicts, coming back from PostgreSQL on each write: applied, stale, rebased, LOST, rejected. The demo turns them into a timeline, so the merge is watched, not inferred.

The JavaScript code uses `zb-client-ts`:

```js
import { ZeBridge, NotEnrolled, mergeRegisters } from 'zb-client-ts';

const DEPLOYED_BRIDGE = import.meta.env.VITE_ZB_BRIDGE_URL as string | undefined;
const NATS_URL = import.meta.env.VITE_ZB_NATS_URL as string | undefined;
const qs = new URLSearchParams(location.search);

const zb = new ZeBridge({
  natsUrl: NATS_URL ?? (DEPLOYED_BRIDGE ? undefined : `${location.origin.replace(/^http/, 'ws')}/nats`),
  bridgeUrl: DEPLOYED_BRIDGE ?? `${location.origin}/bridge`,
  invite: qs.get('invite') ?? undefined,
  dbPath: qs.get('as') ? `survey-${qs.get('as')}.sqlite3` : 'survey.sqlite3',
  tables: ['site_survey'],
});

[...]

// take an input from the UI and
// 1) run optimistic local mutation
// 2) send to Postgres via NATS
await zb.mutate('site_survey', 'UPDATE', { id: row.id }, { [name]: value });

// trigger on/offline
if (online) { await zb.close(); online = false;  }
else await zb.connect(); online = true;
```

Serve it:

```sh
VITE_ZB_BRIDGE_URL=https://localhost:5173 pnpm dev
```

Generate invites:

```sh
set -a; . ./.env.supabase; set +a; 
psql "$SB_ADMIN_URL" -c "
INSERT INTO public.zebridge_invites (code, principal, tenant_id)
VALUES (gen_random_uuid()::text, 'alice', 'acme'),
  (gen_random_uuid()::text, 'bob', 'acme'),
  (gen_random_uuid()::text, 'carol', 'acme'),
  (gen_random_uuid()::text, 'omar', 'globex')
RETURNING code, principal, tenant_id, expires_at;
"
```

Use the invit code:

```txt
http://localhost:5173/?invite=....
```

Each window has:

* Record — the five registers, rendered from that editor’s replica. Labels are
annotated with w and t. When the editor is offline, the panel is dimmed and a chip
says offline — 1 queued write.

* Outbox — the pending writes: (register, value, stamp, state). States: queued,
sent, settled, rebased, lost.

* Verdicts — the last few verdicts from PostgreSQL: applied, stale (with the
reason: rebased or edit LOST), row_deleted, rejected.

* Timeline — a shared panel (one per browser) that lists
(t, register, editor, verdict) so the merge is watched as it happens.

Try, by hand:

Edit rating on Alice’s window; watch it echo on Bob’s and Carol’s.

Take Bob offline, edit hazard; watch Bob’s outbox hold the write and no other window
move.

Bring Bob back; watch hazard land and the verdict come back.

Take Carol offline, edit rating to 2. Meanwhile, on Alice, edit rating to 5.
Bring Carol back: her edit is LOST, and the panel says so. The register holds Alice’s

| | A: five columns | B: one doc |
| -- | -- | -- |
| rebase unit | the column (library) | the register (page) |
hazard after Bob’s offline edit | rebased, lands | page re-merges, lands |
| rating after Carol’s offline edit | LOST, right outcome | page re-merges, loses, right outcome |
| schema changes for a new field | one ADD COLUMN | nothing |
| page-side merge logic | none | a mine map and a re-write on the echo |
| matches COOPERATIVE_EDITING.md | yes, one register per column | yes, registers inside one doc |

## On every window

On Alice, edit access. On Bob, edit notes. Neither touches the other. Both land,
neither loses.

On Omar’s window, call mutate on the same id. PostgreSQL refuses it (RLS); the
library surfaces rejected. Omar’s record panel shows “not found”.

## What the demo deliberately does not do

No CRDTs, no causal tracking, no ordered lists, no text collaboration. The register
format is per-field, by design; notes is one register, and two editors who both edit
it lose one. That is the same tradeoff as in COOPERATIVE_EDITING.md.

No multi-master. PostgreSQL judges every write.

No map, no fleet, no routing. Those are examples 08, 13, 14. This one is about the
merge.

No offline seeding. All four editors seed at t = 0, online. The offline part is
writes, which is the part the register model addresses.

No server-side merge. The rebase is done by the library, following the rules the
server stated. The server never sees a rebase as a new edit; it sees a fresh write.

## What it measured

Scripted runs on 2026-10-XX: four windows on one laptop, the bridge and PostgreSQL on the same machine, the whole timeline replayed end to end. Every run ends with the merge table compared to golden.merge.txt, and every run matched.

| what | measured |
| -- | -- |
| time from write to every online editor seeing it | ...
| time from comes back to convergence | ... |
| verdict round trip (write → verdict) | ... |
| a rebased verdict | ... |
| the scripted timeline, end to end | ... |

**Final outcome**: (example)

```txt
t=50 register=access  winner=laptop-alice   verdict=applied
t=50 register=hazard  winner=phone-bob      verdict=applied
t=50 register=contact winner=tablet-carol   verdict=applied
t=50 register=rating  winner=laptop-alice   verdict=applied
t=50 register=notes   winner=laptop-alice   verdict=applied
t=50 editor=phone-bob   register=rating      verdict=lost   (Alice's 5 stood)
t=50 editor=tablet-carol register=rating     verdict=lost   (Alice's 5 stood)
t=50 editor=globex-dan   register=*          verdict=rejected
```

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
