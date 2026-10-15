-- One tenant-scoped, writable table, and one row the clients must seed.
CREATE TABLE IF NOT EXISTS public.smoke_items (
  id          text PRIMARY KEY,
  tenant_id   text NOT NULL,
  note        text,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz
);
SELECT step FROM public.zebridge_enable(
  'public.smoke_items'::regclass,
  writable      => true,
  tenant_col    => 'tenant_id',
  version_col   => 'updated_at',
  tombstone_col => 'deleted_at',
  publication   => :'pub',
  dry_run       => false
);
INSERT INTO public.smoke_items (id, tenant_id, note) VALUES ('seed', 'smoke', 'seeded by setup')
ON CONFLICT (id) DO NOTHING;
