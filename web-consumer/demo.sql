-- The web consumer's tables, as plain SQL. Run once against the database the bridge
-- replicates, after init.core (zebridge_enable must exist):
--
--   psql "$DATABASE_URL" -v pub=<publication> -f web-consumer/demo.sql
--
-- `pub` is the publication the bridge is started with (--pub / BRIDGE_CDC_PUBLICATION).
-- zebridge_enable has no default for it on purpose: that name decides what a bridge
-- replicates. Everything below is idempotent.
--
-- Four tables, two lessons.
--
--   counter_public, counter_tenant   one row, one integer column, never deleted.
--                                    No tombstone: `allow_physical_deletes` is the
--                                    explicit opt-out, and a counter never needs one.
--                                    `tiebreak_col` names who won the last write —
--                                    the page shows it next to the value.
--   app_users, app_orders            a parent and a child on a real foreign key,
--                                    tenant-scoped, writable, both with tombstones.
--                                    The key does NOT cascade: a cascade into a
--                                    tombstone table is refused by zebridge_enable,
--                                    and a parent's tombstone is refused while a
--                                    live child references it (NOTES §10dn).
--
-- Every writable table carries the four columns the bridge needs: a `timestamptz`
-- version (`updated_at`), `last_writer`, `tenant_id` where the table is tenant-scoped,
-- and `deleted_at timestamptz` where rows can be deleted. Timestamps are timestamptz
-- throughout — the DDL guard refuses `timestamp without time zone`.

\set ON_ERROR_STOP on

-- ── the counters ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.counter_public (
  uid          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  value        integer NOT NULL DEFAULT 0,
  last_writer  varchar(255),
  inserted_at  timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.counter_tenant (
  uid          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  value        integer NOT NULL DEFAULT 0,
  tenant_id    varchar(255) NOT NULL,
  last_writer  varchar(255),
  inserted_at  timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- ── users ⟶ orders ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.app_users (
  uid          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name         varchar(255) NOT NULL,
  tenant_id    varchar(255) NOT NULL,
  last_writer  varchar(255),
  deleted_at   timestamptz,
  inserted_at  timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now()
);

-- A composite, natural key: an order IS (who, what). No surrogate. The page addresses
-- rows by whatever the key is, read from the table state, so this shape can also be
-- reached live from a `uid` key (the re-key ladder, MIGRATIONS.md) — which is how
-- it was built the first time.
CREATE TABLE IF NOT EXISTS public.app_orders (
  user_id      uuid NOT NULL REFERENCES public.app_users (uid),   -- no ON DELETE CASCADE
  item         varchar(255) NOT NULL,          -- the order's text: half of its identity
  count        integer NOT NULL DEFAULT 1,     -- the playground, with note: what two tabs edit
  note         varchar(255),
  tenant_id    varchar(255) NOT NULL,
  last_writer  varchar(255),
  deleted_at   timestamptz,                    -- a tombstone keeps its key until reaped
  inserted_at  timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (user_id, item)
);
CREATE INDEX IF NOT EXISTS app_orders_user_id_index ON public.app_orders (user_id);

-- ── hand them to the bridge ────────────────────────────────────────────────
-- Each call installs the grants, the write guards, row-level security for a
-- tenant-scoped table, the width guard, the catalogue row and the publication
-- membership, and reports one row per step. Re-running is safe.
SELECT step, status, detail FROM public.zebridge_enable(
  'public.counter_public'::regclass,
  writable               => true,
  version_col            => 'updated_at',
  tiebreak_col           => 'last_writer',
  allow_physical_deletes => true,
  public_reason          => 'demo counter — identical content for every tenant',
  publication            => :'pub',
  dry_run                => false);

SELECT step, status, detail FROM public.zebridge_enable(
  'public.counter_tenant'::regclass,
  tenant_col             => 'tenant_id',
  writable               => true,
  version_col            => 'updated_at',
  tiebreak_col           => 'last_writer',
  allow_physical_deletes => true,
  publication            => :'pub',
  dry_run                => false);

SELECT step, status, detail FROM public.zebridge_enable(
  'public.app_users'::regclass,
  tenant_col    => 'tenant_id',
  writable      => true,
  version_col   => 'updated_at',
  tombstone_col => 'deleted_at',
  tiebreak_col  => 'last_writer',
  publication   => :'pub',
  dry_run       => false);

SELECT step, status, detail FROM public.zebridge_enable(
  'public.app_orders'::regclass,
  tenant_col    => 'tenant_id',
  writable      => true,
  version_col   => 'updated_at',
  tombstone_col => 'deleted_at',
  tiebreak_col  => 'last_writer',
  publication   => :'pub',
  dry_run       => false);
