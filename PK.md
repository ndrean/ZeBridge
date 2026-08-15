▎ A table needs a primary key to be replicated, and a timestamp column your ORM already writes to be editable from the edge.

┌───────────────────────────┬───────────────────────────────────────────────┐
│         table has         │                    can do                     │
├───────────────────────────┼───────────────────────────────────────────────┤
│ PK                        │ outbound CDC + snapshots                      │
├───────────────────────────┼───────────────────────────────────────────────┤
│ PK + version column       │ edge writes with LWW                          │
├───────────────────────────┼───────────────────────────────────────────────┤
│ PK + version + deleted_at │ edge writes, deletes, and resurrection safety │
└───────────────────────────┴───────────────────────────────────────────────┘
