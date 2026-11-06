-- Read-side comparison fixture: 50,000 rows in one tenant, about 200 bytes each.
-- Run after the ZeBridge quickstart has initialized Postgres and the bridge.
--   docker compose -f docker-compose.quickstart.yml \
--     -f scripts/compare/electric/compose.overlay.yml exec -T postgres \
--     psql -U postgres -d app -v ON_ERROR_STOP=1 -f - < scripts/compare/electric/fixture.sql
-- Reset both Compose volumes before re-running so each system starts from the same state.

CREATE TABLE IF NOT EXISTS public.storm_items (
  id          text PRIMARY KEY,
  tenant_id   text NOT NULL,
  note        text,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz
);

SELECT step, status, detail
FROM public.zebridge_enable(
  'public.storm_items'::regclass,
  writable      => false,
  tenant_col    => 'tenant_id',
  version_col   => 'updated_at',
  tombstone_col => 'deleted_at',
  publication   => 'quickstart',
  dry_run       => false
);

INSERT INTO public.storm_items (id, tenant_id, note)
SELECT 'row-' || lpad(i::text, 5, '0'), 'cmp', repeat(md5(i::text), 6)
FROM generate_series(1, 50000) AS i
ON CONFLICT (id) DO NOTHING;

-- ZeBridge's read grants and per-tenant stream are keyed by this authoritative roster.
-- It is seeded by the setup/admin path; no benchmark client writes this table.
INSERT INTO public.zebridge_user_tenants (principal, tenant_id)
VALUES ('compare-reader', 'cmp')
ON CONFLICT DO NOTHING;

-- One ZeBridge invite per parallel test client. These identities all receive the
-- same tenant grant, while Electric's proxy uses one signed test identity.
INSERT INTO public.zebridge_user_tenants (principal, tenant_id)
SELECT 'compare-reader-' || lpad(i::text, 3, '0'), 'cmp'
FROM generate_series(0, 99) AS i
ON CONFLICT DO NOTHING;

INSERT INTO public.zebridge_invites (code, principal, tenant_id, expires_at)
VALUES ('electric-read-compare-invite', 'compare-reader', 'cmp', now() + interval '1 day')
ON CONFLICT (code) DO NOTHING;

INSERT INTO public.zebridge_invites (code, principal, tenant_id, expires_at)
SELECT 'zb-read-compare-' || lpad(i::text, 3, '0'),
       'compare-reader-' || lpad(i::text, 3, '0'),
       'cmp', now() + interval '1 day'
FROM generate_series(0, 99) AS i
ON CONFLICT (code) DO NOTHING;
