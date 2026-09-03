import re

with open("README.md", "r") as f:
    text = f.read()

old_text = r"\*\*Seeding: generation chains\.\*\*(.*?)(?=\n### Inside)"

new_text = """### Bulk Catch-Up & Edge Optimization (Generations)

Most sync engines rely on holding massive WAL logs or long CDC queues for disconnected clients. ZeBridge takes a radically different approach optimized for storage, edge bandwidth, and database load: **Short CDC, Long Deltas**.

**The Flow:**
1. **Short CDC Stream:** The live JetStream `cdc.*` queue is kept intentionally short (e.g., retaining only the last 15 minutes of events). This prevents NATS from bloating with millions of single-row historical events.
2. **Generation Chains (The Fallback):** The bridge periodically captures bulk snapshots (**Fulls**) and incremental changes (**Deltas**) per tenant/table, and stores them directly in the NATS Object Store. A tiny JSON Manifest in NATS KV tracks this rolling window.
3. **Smart Catch-Up:** When a client goes offline and misses the CDC window, it doesn't do a full wipe. The client reads the Manifest, discovers the missing Deltas, and cherry-picks only what it needs. It bulk-upserts these Deltas into local SQLite (vastly outperforming single-row replays) and gracefully resumes tailing the live CDC stream. The manifest's `cutoff_seq` acts as the precise splice point.

**Why this architecture wins:**
* **No connection storm on Postgres.** An on-demand dump is served _per request_, queued against the database. A generation is built once by a background job and pushed to NATS object storage. A fleet of 10,000 edge clients reconnecting simultaneously hits Object Storage (which fans out cheaply), completely shielding PostgreSQL.
* **One copy, partitioned.** The chain slices the database by tenant and table, so NATS holds a single partitioned copy of the database, not a fresh full dump per consumer request.
* **Massive Edge Compression (Zstd Dictionaries):** To optimize edge bandwidth, ZeBridge trains a **Zstd Dictionary** on every Full generation. It then uses this specific dictionary to compress subsequent Deltas in that era. This allows tiny 50-row JSON Deltas to compress at massive ratios (often saving 80%+ bandwidth on mobile networks), saving battery and data for your edge users.
"""

text = re.sub(old_text, new_text, text, flags=re.DOTALL)

with open("README.md", "w") as f:
    f.write(text)
