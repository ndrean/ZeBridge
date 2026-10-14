-- The quickstart's second page, examples/15-shared-record: one survey record that three
-- editors in tenant acme change, two of them offline, and that an editor in globex never
-- sees. Idempotent: init.sh runs it on every `up`.
--
--   psql "$DATABASE_URL" -v pub=<publication> -f survey.sql
CREATE TABLE IF NOT EXISTS public.site_survey (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  -- part A: one field per column
  access       text,
  hazard       text,
  contact      text,
  rating       integer,
  notes        text,
  -- part B: one register per field, in one column
  doc          jsonb NOT NULL DEFAULT '{}'::jsonb,
  tenant_id    text NOT NULL,
  updated_at   timestamptz NOT NULL DEFAULT now(),
  deleted_at   timestamptz,
  last_writer  text
);

SELECT step FROM public.zebridge_enable(
  'public.site_survey'::regclass,
  writable      => true,
  tenant_col    => 'tenant_id',
  version_col   => 'updated_at',
  tombstone_col => 'deleted_at',
  tiebreak_col  => 'last_writer',
  register_cols => ARRAY['doc'],
  publication   => :'pub',
  dry_run       => false
);

-- The one record the acme editors share.
INSERT INTO public.site_survey (id, tenant_id, doc)
VALUES ('11111111-1111-4111-8111-111111111111', 'acme', '{}'::jsonb)
ON CONFLICT (id) DO NOTHING;
