with open("NOTES.md", "a") as f:
    f.write("""
## § The Generation Chain Architecture

The generation chain is ZeBridge's mechanism for seeding new clients or recovering clients that have fallen hopelessly behind the WAL. It acts as a bulk-catchup bridge, avoiding the cost of replaying thousands of single-row CDC events.

### 1. The PostgreSQL Source (`generation_producer.zig`)
Every `GENERATION_CADENCE_SECONDS` (e.g., 5 minutes), the bridge spins up a fresh Postgres connection and checks for new mutations per tenant. It strictly enters a `REPEATABLE READ` transaction and sets `zb.principal = <tenant>` to enforce Row-Level Security (RLS). 

It looks up the state of the last generation for this `(tenant, table)` pair in the **`zebridge_generations`** tracking table:
- **No changes?** It skips building anything.
- **Normal changes?** It builds a **Delta** (`SELECT * FROM table WHERE updated_at > last_cutoff`).
- **Hard deletes, or `GENERATION_CHAIN_DEPTH` reached?** It forces a **Full** generation (`SELECT * FROM table`). 

*Example:* A tenant with 10,000 rows modifies 50 rows. A Delta is built containing just those 50 rows. 

### 2. NATS Object Storage (The Payload)
The SQL result is encoded into MessagePack, compressed with Zstd, and uploaded to the NATS JetStream Object Store as a discrete file.
- Example Full: `gen-tenant.test_types-g1-full` (Contains all 10,000 rows)
- Example Delta: `gen-tenant.test_types-g2-delta` (Contains only the 50 modified rows)

Because Deltas are limited by `GENERATION_CHAIN_DEPTH` (e.g., max 6 objects before forcing a new Full), the Object Store is tightly bounded. When `g7` is built, `g1` is permanently deleted. NATS holds almost exactly one physical copy of the database, partitioned by tenant.

### 3. The NATS KV Manifest (The Menu)
A tiny JSON manifest is written to the NATS KV bucket under the key `<tenant>.<table>`. It lists the cutoff timestamp for the Full and every available Delta in the rolling window.

```json
{
  "gen": 6,
  "cutoff_version": "2026-09-02T19:00:00Z",
  "full": { "gen": 1, "object": "test_types-g1-full", "cutoff": "2026-09-02T18:35:00Z" },
  "deltas": [
    { "gen": 2, "object": "test_types-g2-delta", "cutoff": "2026-09-02T18:40:00Z" },
    ...
  ]
}
```

### 4. Client Consumption & The CDC Complement
When an edge client connects, it evaluates its local SQLite `watermark` against the KV Manifest (`planFromManifest`):
- **Brand New Client (Empty DB):** Downloads the Full and all subsequent Deltas, bulk-upserting them into SQLite to rapidly build the replica.
- **Short Disconnect (e.g., 10 minutes offline):** The client's watermark falls within the Delta window! It completely skips the Full, cherry-picks only the 2 Deltas it missed, bulk-upserts them, and resumes.
- **Connected (Live):** The client uses the NATS **CDC Stream** (`cdc.*`). The CDC stream is configured with a very short retention (e.g., 2 ticks) because historical events are already bulk-compressed in the Deltas. NATS avoids storing millions of single-row CDC events, acting solely as an ephemeral, low-latency live pipe!
""")
