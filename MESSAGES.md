## Message Formats

Payloads use `MessagePack` binary encoding by default.

JSON can be enabled with the flag `--json`.

The examples below show the logical structure for illustration.

### Schema (KV Store: `schemas.{table}`)

Published at bridge startup. Consumers fetch before requesting snapshots.

```json
{
  "table": "users",
  "schema": "public.users",
  "timestamp": 1765201228,
  "columns": [
    {
      "name": "id",
      "position": 1,
      "data_type": "integer",
      "is_nullable": false,
      "column_default": "nextval('users_id_seq'::regclass)"
    },
    {
      "name": "name",
      "position": 2,
      "data_type": "text",
      "is_nullable": false,
      "column_default": null
    },
    {
      "name": "email",
      "position": 3,
      "data_type": "text",
      "is_nullable": true,
      "column_default": null
    }
  ]
}
```

### Snapshot Metadata (INIT Stream: `init.meta.{table}`)

Published after all chunks. Tells consumer how many chunks to expect.

```json
{
  "snapshot_id": "snap-1765208480",
  "lsn": "0/191BFD0",
  "timestamp": 1765208480,
  "batch_count": 4,
  "row_count": 4000,
  "table": "users"
}
```

### Snapshot Chunk (INIT Stream: `init.snap.{table}.{snapshot_id}.{chunk}`)

Chunked by **bytes**, not rows: the bridge asks Postgres for the longest prefix of rows
whose cumulative size fits one NATS message (`max_payload` minus envelope). `chunk_size`
in `config.zig` is only a row _ceiling_ — a narrow table hits it, a table of 256 KiB rows
gets three rows a chunk. A row too large to publish at all suspends the table rather than
being split.

```json
{
  "table": "users",
  "operation": "snapshot",
  "snapshot_id": "snap-1765208480",
  "chunk": 3,
  "lsn": "0/191BFD0",
  "data": [
    {
      "id": "3001",
      "name": "User-3001",
      "email": "user3001@example.com",
      "created_at": "2025-12-08 13:45:21.719719+00"
    },
    {
      "id": "3002",
      "name": "User-3002",
      "email": "user3002@example.com",
      "created_at": "2025-12-08 13:45:22.123456+00"
    }
    // ... as many rows as fit one message
  ]
}
```

### CDC Event (CDC Stream: `cdc.{table}.{operation}.{batch}`)

Real-time INSERT/UPDATE/DELETE events.

**Subject pattern:** `cdc.{table}.{operation}` with the test table 'test_types':

* `cdc.test_types.insert`
* `cdc.test_types.update`
* `cdc.test_types.delete`

**Message ID (for deduplication):** `{lsn}-{table}-{operation}`

Example: `"1851208-test_types-insert"`

**INSERT event:**

```json
{
  "data" => {
    "age" => 30,
    "created_at" => "2025-12-12T12:00:34.338547Z",
    "id" => 12,
    "is_true" => true,
    "matrix" => "{{1,2},{3,4}}",
    "metadata" => {
      "key_1" => "value_1",
      "key_2" => [[1, 2], [3, 4], [5, 6]],
      "key_3" => {"key_4" => "value_4", "key_5" => "value_5"}
    },
    "price" => "123.4500",
    "some_text" => "Sample text",
    "tags" => "{\"tag1\",\"tag2\"}",
    "temperature" => 36.6,
    "uid" => "f4b0611f-7258-47f8-bceb-0eba9ac5195a"
  },
  "msg_id" => "1851208-test_types-insert",
  "operation" => "INSERT",
  "relation_id" => 16392,
  "subject" => "cdc.test_types.insert",
  "table" => "test_types"
}
```

**UPDATE event:**

```json
{
  "data" => {
    "age" => 31,
    "created_at" => "2025-12-12T12:00:34.338547Z",
    "id" => 12,
    "is_true" => false,
    "matrix" => "{{1,2},{3,4}}",
    "metadata" => {
      "key_1" => "value_1",
      "key_2" => [[1, 2], [3, 4], [5, 6]],
      "key_3" => {"key_4" => "value_4", "key_5" => "value_5"}
    },
    "price" => "122.9905",
    "some_text" => "Sample text",
    "tags" => "{\"tag1\",\"tag2\"}",
    "temperature" => 37,
    "uid" => "f4b0611f-7258-47f8-bceb-0eba9ac5195a"
  },
  "msg_id" => "18513a8-test_types-update",
  "operation" => "UPDATE",
  "relation_id" => 16392,
  "subject" => "cdc.test_types.update",
  "table" => "test_types"
}
```

**DELETE event:**

```json
{
  "data" => {
    "age" => nil,
    "created_at" => nil,
    "id" => 12,
    "is_true" => nil,
    "matrix" => nil,
    "metadata" => nil,
    "price" => nil,
    "some_text" => nil,
    "tags" => nil,
    "temperature" => nil,
    "uid" => nil
  },
  "msg_id" => "1851518-test_types-delete",
  "operation" => "DELETE",
  "relation_id" => 16392,
  "subject" => "cdc.test_types.delete",
  "table" => "test_types"
}
```

---
