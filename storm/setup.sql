-- The storm's table: 10,000 rows in tenant `storm` (about 200 bytes each), writable, so
-- one client can write the markers the others time. Idempotent.
--   psql … -v pub=<publication> -f setup.sql
CREATE TABLE IF NOT EXISTS public.storm_items (
  id          text PRIMARY KEY,
  tenant_id   text NOT NULL,
  note        text,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz
);
SELECT step FROM public.zebridge_enable(
  'public.storm_items'::regclass,
  writable      => true,
  tenant_col    => 'tenant_id',
  version_col   => 'updated_at',
  tombstone_col => 'deleted_at',
  publication   => :'pub',
  dry_run       => false
);
INSERT INTO public.storm_items (id, tenant_id, note)
SELECT 'row-' || lpad(i::text, 5, '0'), 'storm', repeat(md5(i::text), 6)
FROM generate_series(1, 10000) AS i
ON CONFLICT (id) DO NOTHING;
