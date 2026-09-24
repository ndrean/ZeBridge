# 06-large-table — one big table, every host

The other examples follow a handful of small tables. This one follows exactly one,
`test_types` — 3,055,002 rows on tenant globex, the firehose fixture of NOTES §10iw —
and does nothing but seed it: a progress bar, a clock, and at the end the three facts
that are checked against PostgreSQL. Each host is a measurement of the same seed.

| host | client | path | status |
| --- | --- | --- | --- |
| `web/` | zb-client-ts, OPFS-SQLite in Chrome | staged (TEMP on OPFS) | 156 s, 988 MB — NOTES §10ix "in Chrome" |
| `react-native/` | zb-client-ts, expo-sqlite | staged | iOS simulator 364 s, Android emulator 475 s, **iPhone 12: 725 s** — NOTES §10iy |
| `flutter/` | libzb (C ABI), dart:ffi | streamed, sorted per window in Zig | **iPhone 12: 70.8 s**, simulator 30.2 s — NOTES §10iy |
| `python/` | libzb (C ABI) | streamed | planned |

The two TypeScript hosts share the streaming pipeline (`seedStreaming`, fzstd as the
decompressor) and differ only in the storage adapter; both take the staged path because
both storages can spill a TEMP table and a sort to disk. libzb streams and sorts per
window in C, and reports nothing while it does — a clock instead of a bar until a
`seeding` field in the poll report (the same six names as `SeedProgress`) exists. On the
same iPhone 12 the native seed is 10× the JavaScript one: 70.8 s against 725 s.

If PostgreSQL ever loses the fixture, the numbers change: compute them with

```sh
psql postgres://bridge_reader:…@127.0.0.1:5432/postgres \
  -c "select count(*), count(distinct uid), sum(age) from test_types where tenant_id = 'globex'"
```
