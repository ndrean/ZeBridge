# 06-large-table / web — one big table, seeded in a browser

One table, one progress bar, one clock. The page connects as `bob`, follows
`test_types` (3,055,002 rows on tenant globex, the firehose fixture of NOTES §10iw),
seeds it from the generation chain into OPFS-SQLite, and prints three facts at the end:

```
rows / distinct uid / sum(age)
```

the same three the Node and libzb seeds are checked with against PostgreSQL:

```sh
psql postgres://bridge_reader:…@127.0.0.1:5432/postgres \
  -c "select count(*), count(distinct uid), sum(age) from test_types where tenant_id = 'globex'"
```

so a run here is a measurement, not a demo. No mutation, no SQL console.

## Run

```sh
pnpm install
pnpm dev          # http://localhost:5175 — the stack's ports live in vite.config.ts
```

The replica is ONE OPFS file, `zebridge_bob.sqlite3`, kept across reloads: a second
load finds the table seeded and only tails. "wipe & reload" seeds again. Do not make a
fresh database per load here — a 1 GB replica per attempt is how 05-tables blew its
storage.

## What the page shows

* the four startup phases (as in 05-tables);
* the bar: `onSeedProgress` events, one per 50,000-row window — `applied / total`,
  the step (`full test_types-g1-full`), and `done` only once the rows have landed;
* the clock from `connect()` to "CDC active", and the seed span alone with its rows/s;
* after the seed: the three facts, the OPFS bytes, and the JS heap peak (heap only —
  wasm-sqlite and OPFS are not in it; Chrome's task manager has the process figure).

`window.zb` is the client, for `zb.query('PRAGMA journal_mode')` and the like.

`?table=<name>` follows another table (`test_types_v7`, NOTES §10ja); `?spill=0` forces
the one-window-at-a-time path instead of staging, to measure what arrival order does.

## How the seed runs in a browser

`seedStreaming: true` with fzstd as `zstdDecompressStream`: object chunks are inflated
and decoded as they arrive, never the whole document. The browser storage sets
`PRAGMA temp_store = FILE` and declares `spillsTemp: true` — sqlite-wasm's OPFS VFS
spills TEMP tables and the sorter to OPFS files — so the client takes the staged path:
every window is appended to a TEMP table, and the real table is filled once by
`INSERT … SELECT … ORDER BY pk`, SQLite sorting on disk and building the b-tree
sequentially. The bar reaches 100% when the stage is full; the sort and the insert
run after that with `done` still false, ~1.5 min for this table.

The alternative — one sorted window at a time into the real table — is bounded too,
but each window scatters across the whole b-tree, and over OPFS every page miss is a
round trip to the OPFS worker: it fell from 20k to 2k rows/s by 750k rows and was
aborted (NOTES §10ix).

## ⚠️ Chrome and "delete site data on close"

With `chrome://settings/content/siteData` set to delete data when all windows close,
Chrome enforces a fixed **256 MiB per origin** on OPFS writes while
`navigator.storage.estimate()` still advertises the full quota. The seed then dies
around 400k rows with `SQLITE_IOERR_WRITE` (sqlite-wasm's translation of
`QuotaExceededError: No space available for this operation`). Add `http://localhost`
under "Allowed to save data", or turn the setting off. Measured 2026-09-24: a raw
OPFS write on a fresh origin stops at exactly 268,435,456 bytes; with the exception
in place, 1.5 GB in 1 s.

## Measured (2026-09-24, Chrome 153, Mac)

| | |
| --- | --- |
| stage filled | ~60 s (~65k rows/s) |
| sort + insert | ~95 s |
| seed, first window → done | **155.8 s → 19,613 rows/s** |
| connect → CDC active | 188.6 s |
| replica | 988 MB |
| OPFS peak during the seed | ~2.3 GB (replica + TEMP database + sorter spill) |
| JS heap peak | 264 MB |
| rows / distinct uid / sum(age) | 3,055,002 / 3,055,002 / 138,916,285 — exact |

The temp files the stage and the sort leave in OPFS (~1.3 GB here) are swept by the
client once the seed lands, and orphans of an interrupted seed at the next open —
watch the log line "reclaimed … MB of temp files". Details and the runs that failed
first: NOTES §10ix, "in Chrome".
