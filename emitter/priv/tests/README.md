# Retired migrations

Not run by `mix ecto.migrate` — Ecto only scans `priv/repo/migrations/` by
convention, so these sit outside it deliberately.

Both are upgrade paths for a database that ran an *older* shape of a migration
still in `priv/repo/migrations/`, before that migration was corrected to
produce the right shape from the start. Their own docstrings say so directly —
each is `IF EXISTS`-guarded and a confirmed no-op against a freshly reset
database (verified twice this session, native and Docker, zero effect either
time). Kept for reference, not for replay: this project resets from empty
routinely enough that there is no database left that still needs them.

- `20260817200000_test_types_uid_primary_key.exs` — promotes `uid` to primary
  key on a `test_types` created before `20260810120000_setup_tables.exs` was
  corrected to do that from the start.
- `20260818070000_timestamptz_version_columns.exs` — converts version/tombstone
  columns to `timestamptz` on a database created before the same migration was
  corrected to use `timestamptz` from the start.
