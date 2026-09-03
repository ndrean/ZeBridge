import re

with open("PROTOCOL.md", "r") as f:
    text = f.read()

new_section = """## 6. Seeding — generation chains ✅

Seeding provides a fresh or fallen-behind edge client with the complete, current state of a table so it can safely begin consuming live CDC events.

Because querying Postgres directly from thousands of edge clients would overwhelm the database, ZeBridge uses a decoupled, pre-computed approach called **Generation Chains**. 

A dedicated background thread (`generation_producer.zig`) periodically queries the database and builds "generations". Edge clients simply download these pre-built generations directly from NATS Object Storage, meaning Postgres is never burdened by edge client reads.

### The Storage Architecture

| Component | Storage Location | Purpose |
| --- | --- | --- |
| **Data Objects** | NATS Object Store (`gen-<tenant>`) | The actual table data (MessagePack format, wrapped in zstd compression). These are 128KB chunked files, allowing clients to download massive tables without hitting NATS message size limits. |
| **Manifests** | NATS KV Store (`$KV.manifests.<table>`) | A tiny JSON document pointing to the most recent generation chain (like a symlink). Clients read this *first* to know exactly which Data Objects to download. |
| **State Tracker** | Postgres (`zebridge_generations`) | Internal bookkeeping table where the producer tracks which generations it has built and when. This is strictly internal and never exposed to clients. |

### Fulls vs. Deltas (The Chain)

Building a complete snapshot of a massive table every 5 minutes is too expensive. Instead, ZeBridge builds a **Chain**:

1. **Full Generation**: A complete snapshot of the entire table. Built rarely (e.g., every 6th generation, dictated by `chain_depth`).
2. **Delta Generations**: Small snapshots containing *only* the rows that have changed since the previous generation. Built frequently (e.g., every 5 minutes).

When an edge client needs to seed, it reads the Manifest. The Manifest gives it a recipe:
> *"To get the current state of this table, download Full Object #1, then apply Delta Object #2, then Delta Object #3. When you are done, start consuming the live CDC stream starting from `cutoff_lsn` X."*

### The Seeding Workflow (Step-by-Step)

1. **The Producer (Server-side)**:
   - Every `GENERATION_CADENCE_SECONDS` (e.g., 300s), the generation producer wakes up.
   - It records the current Postgres WAL position (`pg_current_wal_lsn`).
   - It queries the table for changes (building a Delta) or queries the whole table (building a Full).
   - It compresses the result into MessagePack/zstd and uploads it to NATS Object Store.
   - It updates the Manifest in NATS KV to point to this new link in the chain, including the `cutoff_lsn`.
   - It prunes old generations that exceed the `chain_depth` limit.

2. **The Client (Edge-side)**:
   - Client boots up and realizes it needs data for a table.
   - Client reads the `$KV.manifests.<table>` Manifest from NATS.
   - Client downloads the specified Full and Delta objects from the NATS Object Store and merges them locally.
   - Client subscribes to the live CDC event stream (`cdc.<tenant>.<table>.>`), but **ignores all events older than the Manifest's `cutoff_lsn`**.
   - Client is now fully synchronized.

### Safety Guarantees

* **No Data Loss / No Duplication**: Because the producer records the exact WAL position (`cutoff_lsn`) *before* taking the REPEATABLE READ snapshot, the client knows exactly where the snapshot ends and the CDC stream begins. 
* **Invisible Bookkeeping**: The `zebridge_generations` table is entirely internal. It is shielded from the CDC publisher, meaning the act of creating a generation does not emit noise onto the CDC event stream.
* **Append-Only by Privilege**: The `bridge_reader` role only holds INSERT and DELETE grants for the generations table. History cannot be maliciously rewritten by an injected UPDATE.

"""

text = re.sub(r'## 6\. Seeding — generation chains ✅.*?(?=## 7\. Offline window)', new_section, text, flags=re.DOTALL)

with open("PROTOCOL.md", "w") as f:
    f.write(text)
