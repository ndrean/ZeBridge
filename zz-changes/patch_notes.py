import re

with open("NOTES.md", "r") as f:
    text = f.read()

replacement = """### 1. The PostgreSQL Source & The Tracking Table (`generation_producer.zig`)
Every `GENERATION_CADENCE_SECONDS` (e.g., 5 minutes), the bridge spins up a fresh Postgres connection and checks for new mutations per tenant. It strictly enters a `REPEATABLE READ` transaction and sets `zb.principal = <tenant>` to enforce Row-Level Security (RLS). 

To know exactly where it stands, the producer relies completely on its control-plane memory: the **`zebridge_generations`** table. 

**Table fields:**
- `tenant`, `tbl`: The exact partition being built.
- `gen` (bigint): The absolute tick number for this chain (e.g., `124`).
- `has_full` (boolean): Whether this specific tick included a Full generation.
- `cutoff_version`, `cutoff_lsn`: The exact timestamp and WAL position of the snapshot.
- `row_count`, `del_count`: The table state at this tick (used to detect hard deletes).

**Determining the Tick & Rolling Window:**
1. The producer queries the highest `gen` for this table/tenant to find `last_gen`. The new tick will be `gen = last_gen + 1`.
2. It queries the highest `gen` where `has_full = true` to find `last_full_gen`. 
3. It subtracts: `(gen - last_full_gen)`. If this distance is `>= chain_depth - 1`, it forces a **Full** generation.
4. If the distance is smaller, and no hard deletes occurred, it builds a **Delta** (`SELECT * FROM table WHERE updated_at > last_cutoff`).
5. Finally, it enforces the rolling window by aggressively pruning history: `DELETE FROM zebridge_generations WHERE gen <= (gen - chain_depth)`, deleting the corresponding objects from NATS."""

text = re.sub(r'### 1\. The PostgreSQL Source \(`generation_producer\.zig`\).*?(?=\n### 2\.)', replacement, text, flags=re.DOTALL)

with open("NOTES.md", "w") as f:
    f.write(text)
