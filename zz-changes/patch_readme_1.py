import re

with open("README.md", "r") as f:
    text = f.read()

# 1. Fix Sweeper text
old_sweeper = r"\*\*Sweeper\*\*: because consumers can apply _soft-deletion_, we have a companion garbage collector\. The executable `bridge_sweeper` prunes Postgres' rows marked for deletion every tick, so these are echoed via CDCs to consumers who applied soft-deletion\.\nWe capture this event to emit simple telemetry about the garbadged rows\."
new_sweeper = "**Sweeper**: because consumers can apply _soft-deletion_, we have a companion garbage collector. The executable `bridge_sweeper` prunes Postgres' rows marked for deletion every tick. Because edge databases (SQLite) have limited storage, the Sweeper turns your logical soft-deletes into physical hard-deletes, triggering a final CDC event that permanently frees up disk space on the edge devices.\nWe capture this event to emit simple telemetry about the garbage-collected rows."
text = re.sub(old_sweeper, new_sweeper, text)

# 2. Fix Diagram (PGREP)
text = text.replace("PGREP[(PG StandBy<br>Replica<br>:5433)]:::secure", "PGREP[(PG StandBy<br>Replica<br>Planned)]:::secure")

# 3. Fix Typos in the Opinionated table
text = text.replace("| Action | colmun | type | note |", "| Action | column | type | note |")
text = text.replace("| Write  | version | **timestampz** | no bigserial, `updated_at` |", "| Write  | version | **timestamptz** | no bigserial, `updated_at` |")
text = text.replace("| Write  | tombstone | **timestampz** | soft-deleted, `deleted_at` |", "| Write  | tombstone | **timestamptz** | soft-deleted, `deleted_at` |")
text = text.replace("annotate in `zb_enable()`", "annotate in `zebridge_enable()`")

with open("README.md", "w") as f:
    f.write(text)
