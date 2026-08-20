-- Two principals, two tenants, one bridge — the setup the two-window demo needs.
--
--   psql "$DATABASE_URL" -f scripts/demo_two_tenants.sql
--
-- ⚠️ This is **model B**. Do not use `zebridge_scope_publication_to_one_tenant` for it: that adds a
-- publication row filter (`WHERE tenant_id = 'acme'`), which pins the publication — and so
-- the bridge reading it — to a single tenant. One bridge per tenant is a legitimate
-- deployment, but it is not this one.
--
-- Model B keeps one publication carrying every tenant and splits them on the **subject**:
--   * RLS bounds what each principal may WRITE          (this file)
--   * TENANT_RULES routes CDC to cdc.<tenant>.<table>.<op>   (.env.bridge)
--   * NATS permissions bound what each client may READ  (nats-server.conf.template)
--
-- ⚠️ All three are required. RLS alone gives airtight writes and **no read isolation**:
-- a client granted `cdc.>` sees every tenant's rows however perfect the policies are.

INSERT INTO public.zebridge_user_tenants (principal, tenant_id) VALUES
    ('alice', 'acme'),
    ('bob',   'globex'),
    -- ⚠️ The sweeper needs a row per tenant or its reaps see nothing: it runs under RLS
    -- like every other writer, and an unmapped principal matches no rows at all — so
    -- tombstones would accumulate forever with the sweeper reporting success.
    -- `SELECT * FROM zebridge_audit_sweeper();` lists tenants it cannot reach.
    ('zb_sweeper', 'acme'),
    ('zb_sweeper', 'globex')
ON CONFLICT DO NOTHING;

SELECT public.zebridge_scope_writes_by_tenant('public.test_types'::regclass, 'tenant_id');

-- What each principal may now write, as the database sees it.
SELECT principal, string_agg(tenant_id, ', ' ORDER BY tenant_id) AS tenants
FROM public.zebridge_user_tenants GROUP BY principal ORDER BY principal;
