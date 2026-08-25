-- ─────────────────────────────────────────────────────────────────────────────
-- init.write.sql — the write half: ingress, guards, and optional tenant scoping.
--
-- ⚠️ **Appended to `init.core.template.sql`, never run alone.** It needs the reader role,
-- the publication and the DDL triggers that file creates:
--
--     cat init.core.template.sql init.write.template.sql | envsubst | psql -v ON_ERROR_STOP=1 …
--
-- Everything below is **defined, not applied**. These are functions a DBA calls per
-- table; loading this file grants nobody anything beyond the writer role's CONNECT and
-- USAGE. Making a table writable is a deliberate act:
--
--     SELECT zebridge_grant_edge_writes('public.orders');
--     SELECT zebridge_install_write_guards('public.orders', 'updated_at', 'deleted_at');
--
-- Multi-tenant is not a build flag, it is two more calls plus one bridge setting:
--
--     INSERT INTO zebridge_user_tenants (principal, tenant_id) VALUES ('alice', 'acme');
--     SELECT zebridge_scope_writes_by_tenant('public.orders', 'tenant_id');
--     -- and in the bridge's environment, which is what routes the *subject*:
--     TENANT_RULES=orders:tenant_id        ← needs a bridge restart; the two above do not
--
-- ⚠️ `zebridge_scope_publication_to_one_tenant()` is a different shape entirely — it pins
-- a publication to ONE tenant value, for one-bridge-per-tenant deployments. It is not the
-- multi-tenant path above.
-- ─────────────────────────────────────────────────────────────────────────────

-- ---------------------------------------------------------
-- The WRITE role — separate from the read role on purpose
-- ---------------------------------------------------------
--
-- ${POSTGRES_BRIDGE_USER} stays exactly what it is: SELECT + REPLICATION, physically
-- unable to write. The ingress path (NATS -> bridge -> PostgreSQL) uses a *second*
-- connection under this role, so adding the write feature never widens the privileges
-- of the read path.
--
-- It is created with **no table privileges at all**. That is the backstop that makes
-- "the bridge is a dumb dispatcher" safe rather than merely simple: even a bug in
-- subject parsing cannot reach a table the DBA has not explicitly opened, because the
-- role has no path to one. Grants are per table, via
-- zebridge_grant_edge_writes() below — never ALL TABLES, and never by default.
--
-- Two attributes are spelled out even though they are the defaults, because each fails
-- **open** if it is ever wrong:
--   NOBYPASSRLS  - with BYPASSRLS every row-level policy becomes decorative
--   NOREPLICATION- the write role must not be able to read the WAL
--
-- A third rule cannot be expressed here and belongs in review: this role must **not own
-- any table**. Table owners are exempt from row-level security unless the table is set
-- to FORCE ROW LEVEL SECURITY, so an owning writer silently bypasses every policy.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${POSTGRES_WRITER_USER}') THEN
        CREATE USER ${POSTGRES_WRITER_USER} WITH PASSWORD '${POSTGRES_WRITER_PASSWORD}'
            NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
    END IF;
END;
$$;

GRANT CONNECT ON DATABASE ${TARGET_DB} TO ${POSTGRES_WRITER_USER};
GRANT USAGE ON SCHEMA public TO ${POSTGRES_WRITER_USER};

-- Deliberately NOT granted: SELECT/INSERT/UPDATE on any table, and no ALTER DEFAULT
-- PRIVILEGES. A new table is not edge-writable until someone says so.


-- Open one table to edge writes. Idempotent; call it once per table.
--
-- SELECT is required alongside INSERT/UPDATE because the last-write-wins guard
-- (`WHERE stored.version < EXCLUDED.version`) reads the existing row.
--
-- zebridge_ddl_events is refused by name: it is the bridge's own DDL tracker, and a
-- client able to insert into it could forge a schema, a suspension or a tombstone for
-- every other client. That is the one table where "can write my own rows" becomes
-- "can control every replica".
-- opens one table to edge writes — see PROTOCOL.md §7.4
CREATE OR REPLACE FUNCTION public.zebridge_grant_edge_writes(tbl regclass)
RETURNS void AS $$
DECLARE
    tbl_name text := tbl::text;
BEGIN
    IF tbl_name IN ('zebridge_ddl_events', 'public.zebridge_ddl_events') THEN
        RAISE EXCEPTION 'refusing to grant edge writes on %: forging rows here would '
                        'let a client publish a fabricated schema to every client', tbl_name;
    END IF;

    -- DELETE as well as SELECT/INSERT/UPDATE, for two paths that both need it:
    --
    --   * a table with **no tombstone column** deletes physically — that is the documented
    --     weaker guarantee, and it is an actual DELETE;
    --   * the **tombstone sweeper** (src/gc.zig) reaps soft-deleted rows past
    --     GC_THRESHOLD_MS, and runs as this role precisely so it is not the admin.
    --
    -- Marginal risk is small: this role can already UPDATE any row it may write, and RLS
    -- bounds which rows those are. Withholding DELETE did not prevent data loss, it only
    -- made the sweeper fail with `permission denied` — silently, in a sidecar nobody reads.
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON %s TO ${POSTGRES_WRITER_USER}', tbl);
    RAISE NOTICE 'edge writes enabled on % for ${POSTGRES_WRITER_USER}', tbl;
END;
$$ LANGUAGE plpgsql;


-- ---------------------------------------------------------
-- Write guards — making the version column true for EVERY writer
-- ---------------------------------------------------------
--
-- Last-write-wins compares `stored.version < incoming.version`, and the bridge stamps the
-- version on every write it applies. The bridge is not the only writer. A cron job, a psql
-- session, another service, or an ORM path that forgets can run
--
--     UPDATE users SET name = 'x' WHERE id = 1;      -- updated_at untouched
--
-- and the row changes while the version does not. The stored version then no longer
-- describes the row, so a client's OLDER edit beats a NEWER server write — silently, and
-- "correctly" by the rule. See PROTOCOL.md §7.3.
--
-- A trigger fixes it without a migration: it changes no shape and breaks no query, which
-- is what makes it usable on a database you do not control.

-- Stamp the version column when the statement did not — see PROTOCOL.md §7.3
CREATE OR REPLACE FUNCTION public.zebridge_bump_version()
RETURNS trigger AS $$
DECLARE
    col text := TG_ARGV[0];
BEGIN
    -- Only when this statement left the version alone. A writer that DID set it — the
    -- bridge, which sends the client's value and clamps it (§7.3), or a well-behaved ORM —
    -- keeps what it wrote. Overwriting unconditionally would make the version
    -- server-assigned and quietly discard the value the client was told to send back.
    IF (to_jsonb(OLD) -> col) IS NOT DISTINCT FROM (to_jsonb(NEW) -> col) THEN
        -- jsonb round trip because plpgsql cannot assign to a column named at runtime.
        -- One conversion per updated row, on tables the operator opted in.
        NEW := jsonb_populate_record(NEW, jsonb_build_object(col, now()));
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Turn a physical DELETE into a tombstone — see PROTOCOL.md §7.5
CREATE OR REPLACE FUNCTION public.zebridge_soft_delete()
RETURNS trigger AS $$
DECLARE
    tombstone text := TG_ARGV[0];
    version   text := TG_ARGV[1];
BEGIN
    -- The sweeper MUST get through, or tombstones accumulate forever and the reaping this
    -- whole design depends on can never happen. It identifies itself the same way every
    -- other writer does — `zb.principal`, the setting the RLS policies read — so no new
    -- mechanism is needed here.
    IF coalesce(current_setting('zb.principal', true), '') = 'zb_sweeper' THEN
        RETURN OLD;
    END IF;

    -- Located by `ctid`, not by primary key: the key columns are not known here without a
    -- catalog lookup per row, and the tuple's physical address is stable for the statement
    -- that is deleting it.
    --
    -- The version is stamped too, not just the tombstone. A soft delete that did not move
    -- the version would be refused by the next edge write's LWW guard — the row would read
    -- as deleted while still accepting edits.
    EXECUTE format(
        'UPDATE %I.%I SET %I = now(), %I = now() WHERE ctid = $1',
        TG_TABLE_SCHEMA, TG_TABLE_NAME, tombstone, version
    ) USING OLD.ctid;

    -- Suppress the DELETE. The UPDATE above is what reaches CDC, so clients see the
    -- tombstone rather than a physical removal they cannot overrule.
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Attach both to one table — see PROTOCOL.md §7.3
--
-- The column names must match this table's SYNC_RULES entry in the bridge's environment.
-- They are the same contract stated in two places and nothing cross-checks them: name a
-- different column here and the bridge and the database disagree about what "version"
-- means, with no error. `zebridge_audit_write_guards()` reports what is actually attached
-- so the two can be compared.
-- Tenant guard: every row leaves with a legal tenant token, or the write is refused.
--
-- Attached per table by `zebridge_install_write_guards`, which passes the column name as a
-- trigger argument. It fills in a missing tenant and refuses an unusable one — because the
-- bridge copies the column straight into `cdc.<tenant>.<table>.<op>` without interpreting it.
--
--   1. **absence** — omitted, NULL, or empty → **the writer's own tenant**, looked up from
--      `zebridge_user_tenants` by `zb.principal`. This is the correction that matters: a
--      tenanted client that forgets the column must NOT have its (sensitive) row silently
--      published to everyone. Omission means "mine", never "open". N-1 (one tenant per
--      client principal) makes the lookup unambiguous.
--
--      ⚠️ **Fail-CLOSED on no derivable tenant.** If `zb.principal` is unset or unmapped,
--      there is no tenant to stamp and the write is REJECTED — a row on a tenant-routed
--      table with no tenant cannot be routed, so producing one is worse than refusing. An
--      admin seeding such a table must state the tenant, exactly as a client does.
--
--      The OPEN tenant (`${OPEN_TENANT}`) is no longer a coalesce target — it is simply the
--      tenant a principal *mapped to it* carries, so its rows land in `CDC_PUBLIC` and are
--      read by everyone. "Open" is a mapping, not a default: to write open data you are a
--      member of the open tenant, audited in the one table like every other writer.
--
--   2. **malformed** — a real value with a NATS subject metacharacter (`.`, space, `*`, `>`)
--      → RAISES. A dot splits the token and the row routes to a subject no consumer
--      receives. Not corrected: a caller that sent `evil.acme` meant something, and
--      rerouting it silently is worse than refusing.
--
--   3. **valid** — passed through untouched; `zb_tenant_write` (RLS `WITH CHECK`) is what
--      confirms the writer may actually write that tenant, so a supplied value is validated,
--      not trusted.
--
-- ⚠️ `set_config('zb.principal', …, true)` MUST share the write's transaction (the bridge's
-- pipeline does). In autocommit each statement is its own transaction and the setting is
-- gone by the INSERT — the lookup then finds nothing and this fails closed.
--
-- ⚠️ Runs BEFORE the NOT NULL check (Postgres fires BEFORE-row triggers first), so the
-- column stays NOT NULL with no DEFAULT and this still satisfies it.
--
-- ⚠️ The jsonb round trip is how a dynamically-named column is written — plain plpgsql
-- cannot assign `NEW.<variable>`. Same trick as `zebridge_bump_version()`; no extension, and
-- a generated per-table function would need nested dollar-quoting, which `envsubst`
-- (the bridge-init pipe) destroys.
CREATE OR REPLACE FUNCTION public.zebridge_guard_tenant()
RETURNS trigger AS $$
DECLARE
    col  text := TG_ARGV[0];
    val  text;
    mine text;
    who  text := current_setting('zb.principal', true);
BEGIN
    EXECUTE format('SELECT ($1).%I::text', col) USING NEW INTO val;

    IF val IS NULL OR val = '' THEN
        -- Derive from identity. N-1: one tenant per client principal, so LIMIT 1 is exact.
        SELECT tenant_id INTO mine FROM public.zebridge_user_tenants
            WHERE principal = who LIMIT 1;
        IF mine IS NULL THEN
            RAISE EXCEPTION 'no tenant for a write to %: principal % is unset or not in '
                'zebridge_user_tenants, and a tenant-routed row cannot be left unrouted. '
                'Set zb.principal in the transaction, or supply % explicitly.',
                TG_TABLE_NAME, coalesce(quote_literal(who), 'NULL'), col
                USING ERRCODE = 'check_violation';
        END IF;
        NEW := jsonb_populate_record(NEW, jsonb_build_object(col, mine));
        RETURN NEW;
    END IF;

    IF val ~ '[.*> ]' THEN
        RAISE EXCEPTION 'tenant %=% on % contains a NATS subject metacharacter '
            '(. * > or space); it would route to a subject no consumer receives',
            col, quote_literal(val), TG_TABLE_NAME USING ERRCODE = 'check_violation';
    END IF;

    RETURN NEW;
END $$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.zebridge_install_write_guards(
    tbl regclass,
    version_col text,
    tombstone_col text DEFAULT NULL,
    tenant_col text DEFAULT NULL
)
RETURNS void AS $$
DECLARE
    t text := tbl::text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = tbl
                   AND attname = version_col AND attnum > 0 AND NOT attisdropped) THEN
        RAISE EXCEPTION '% has no column %', t, version_col;
    END IF;

    EXECUTE format('DROP TRIGGER IF EXISTS zebridge_bump_version_t ON %s', t);
    EXECUTE format(
        'CREATE TRIGGER zebridge_bump_version_t BEFORE UPDATE ON %s '
        'FOR EACH ROW EXECUTE FUNCTION public.zebridge_bump_version(%L)', t, version_col
    );
    RAISE NOTICE 'version guard on %: % is stamped even when a writer forgets', t, version_col;

    EXECUTE format('DROP TRIGGER IF EXISTS zebridge_soft_delete_t ON %s', t);
    IF tombstone_col IS NOT NULL THEN
        IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = tbl
                       AND attname = tombstone_col AND attnum > 0 AND NOT attisdropped) THEN
            RAISE EXCEPTION '% has no column %', t, tombstone_col;
        END IF;
        EXECUTE format(
            'CREATE TRIGGER zebridge_soft_delete_t BEFORE DELETE ON %s '
            'FOR EACH ROW EXECUTE FUNCTION public.zebridge_soft_delete(%L, %L)',
            t, tombstone_col, version_col
        );
        RAISE NOTICE 'delete guard on %: DELETE writes % instead of removing the row', t, tombstone_col;
    ELSE
        -- No tombstone column means deletes are physical for this table — the documented
        -- weaker guarantee, not an oversight. Saying so beats silence.
        RAISE NOTICE 'no tombstone for %: deletes stay physical, so an offline client can resurrect a row', t;
    END IF;

    -- The tenant guard, when the table is tenant-routed. One shared function,
    -- `zebridge_guard_tenant()`, with the column name passed as a trigger argument.
    EXECUTE format('DROP TRIGGER IF EXISTS zebridge_guard_tenant_t ON %s', t);
    IF tenant_col IS NOT NULL THEN
        IF NOT EXISTS (SELECT 1 FROM pg_attribute WHERE attrelid = tbl
                       AND attname = tenant_col AND attnum > 0 AND NOT attisdropped) THEN
            RAISE EXCEPTION '% has no column %', t, tenant_col;
        END IF;
        EXECUTE format(
            'CREATE TRIGGER zebridge_guard_tenant_t BEFORE INSERT OR UPDATE ON %s '
            'FOR EACH ROW EXECUTE FUNCTION public.zebridge_guard_tenant(%L)', t, tenant_col
        );
        RAISE NOTICE 'tenant guard on %: %  filled from the writer''s tenant when omitted (fail-closed if unmapped), malformed refused', t, tenant_col;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Take the guards off again — the counterpart, and needed more often than it looks
--
-- ⚠️ Once the delete guard is on, **nobody but the sweeper can physically delete from this
-- table**. An admin running `DELETE FROM t WHERE …` in psql gets a tombstone, not a
-- removal. That is the whole point, and it is also a genuine surprise the first time it
-- happens during a cleanup. Two ways out, in order of preference:
--
--   SELECT set_config('zb.principal', 'zb_sweeper', false);   -- for this session only
--   SELECT zebridge_remove_write_guards('public.t');          -- permanently
CREATE OR REPLACE FUNCTION public.zebridge_remove_write_guards(tbl regclass)
RETURNS void AS $$
DECLARE
    t text := tbl::text;
BEGIN
    EXECUTE format('DROP TRIGGER IF EXISTS zebridge_bump_version_t ON %s', t);
    EXECUTE format('DROP TRIGGER IF EXISTS zebridge_soft_delete_t ON %s', t);
    RAISE NOTICE 'write guards removed from %: versions are only as true as their writers again', t;
END;
$$ LANGUAGE plpgsql;
-- Scope WRITES by tenant, without pinning the publication — see PROTOCOL.md §7.4
--
-- ⚠️ The companion to `zebridge_scope_publication_to_one_tenant`, and the difference is the whole
-- point. That one adds a publication row filter (`WHERE tenant = 'acme'`), which pins the
-- publication — and therefore the bridge — to **one** tenant. This one does not touch the
-- publication at all, so a single bridge can carry many tenants and route them by subject
-- (`TENANT_RULES` → `cdc.<tenant>.<table>.<op>`).
--
-- ⚠️ **RLS does not bound reads.** It bounds what the writer role may write and what a
-- snapshot may select. What stops one tenant *reading* another's rows is the CDC subject
-- plus the NATS permission on it. Enabling this and granting clients `cdc.>` gives you
-- airtight writes and no read isolation whatsoever.
CREATE OR REPLACE FUNCTION public.zebridge_scope_writes_by_tenant(tbl regclass, tenant_col name)
RETURNS void AS $$
DECLARE
    ri_kind  "char";
    covered  boolean;
    pk_cols  name[];
    idx_name name;
    col_list text;
BEGIN
    -- NOT NULL because a replica-identity index requires it, and because a nullable tenant
    -- is a row no policy matches and no subject can carry.
    IF NOT EXISTS (SELECT 1 FROM pg_attribute
                   WHERE attrelid = tbl AND attname = tenant_col
                     AND attnum > 0 AND NOT attisdropped AND attnotnull) THEN
        RAISE EXCEPTION 'tenant column %.% must exist and be NOT NULL', tbl, tenant_col;
    END IF;

    -- ⚠️ The tenant column must be inside the REPLICA IDENTITY, or the table is refused
    -- with `tenant_not_in_replica_identity` and nothing replicates.
    --
    -- A DELETE carries only the replica-identity columns — under the default that is the
    -- primary key alone. Inserts and updates would route to the right tenant subject and
    -- deletes could not route at all, so a deleted row would stay in every replica that
    -- held it, with no later event able to remove it. Silent, permanent divergence.
    SELECT relreplident INTO ri_kind FROM pg_class WHERE oid = tbl;
    IF ri_kind = 'f' THEN
        covered := true;                       -- FULL carries every column
    ELSE
        SELECT coalesce(bool_or(a.attname = tenant_col), false) INTO covered
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = tbl
          AND ((ri_kind = 'd' AND i.indisprimary) OR (ri_kind = 'i' AND i.indisreplident));
    END IF;

    IF NOT covered THEN
        SELECT array_agg(a.attname ORDER BY k.ord) INTO pk_cols
        FROM pg_index i
        JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
        WHERE i.indrelid = tbl AND i.indisprimary;

        IF pk_cols IS NULL THEN
            RAISE EXCEPTION '% has no primary key, so no replica identity can cover %',
                            tbl, tenant_col;
        END IF;

        idx_name := replace(tbl::text, '.', '_') || '_zb_ri';
        col_list := quote_ident(tenant_col) || ', ' ||
                    array_to_string(ARRAY(SELECT quote_ident(c) FROM unnest(pk_cols) c), ', ');
        EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %s (%s)', idx_name, tbl, col_list);
        EXECUTE format('ALTER TABLE %s REPLICA IDENTITY USING INDEX %I', tbl, idx_name);
        RAISE NOTICE 'replica identity of % moved to (%) so deletes can be routed by tenant',
                     tbl, col_list;
    END IF;

    EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);

    -- The writer may touch only rows whose tenant is mapped to the principal it declared.
    -- `USING` bounds what it can see and change; `WITH CHECK` bounds what it can leave
    -- behind — both are needed, or a writer can move a row into another tenant.
    --
    -- ⚠️ **Open bar on the OPEN tenant (`${OPEN_TENANT}`): every principal may write it**, in
    -- both clauses, even one mapped to no tenant but its own. This is the write half of the
    -- open-data model — the open tenant is a shared pool anyone may post to and anyone may
    -- edit (it is also read by everyone via CDC_PUBLIC). Integrity on *private* tenants is
    -- untouched: alice still cannot write or delete a `globex` row. The carve-out only widens
    -- reach to the tenant that is public by definition, so it grants nothing that reading the
    -- open feed did not already expose.
    --
    -- ⚠️ It is in USING as well as WITH CHECK on purpose. Edge writes are upserts
    -- (INSERT … ON CONFLICT DO UPDATE); updating an existing open row needs USING to see it,
    -- so a WITH-CHECK-only carve-out would let a restricted user CREATE an open row but fail
    -- to UPDATE one — a half-open state worse than either choice.
    EXECUTE format('DROP POLICY IF EXISTS zb_tenant_write ON %s', tbl);
    EXECUTE format(
        'CREATE POLICY zb_tenant_write ON %s FOR ALL TO %I'
        ' USING (%I IN (SELECT tenant_id FROM public.zebridge_user_tenants'
        '   WHERE principal = current_setting(''zb.principal'', true)) OR %I = ''${OPEN_TENANT}'')'
        ' WITH CHECK (%I IN (SELECT tenant_id FROM public.zebridge_user_tenants'
        '   WHERE principal = current_setting(''zb.principal'', true)) OR %I = ''${OPEN_TENANT}'')',
        tbl, '${POSTGRES_WRITER_USER}', tenant_col, tenant_col, tenant_col, tenant_col);

    -- Reads: CDC stays subject-scoped regardless (RLS cannot see it — see
    -- zebridge_scope_reads_by_tenant's own comment, init.core.template.sql). Snapshots
    -- now ARE RLS-scoped, via the same function a read-only deployment would call
    -- directly — defined once in core so it needs nothing from this file.
    PERFORM public.zebridge_scope_reads_by_tenant(tbl, tenant_col);

    RAISE NOTICE 'writes and snapshot reads on % are now scoped by %; CDC stays scoped by the subject, not RLS — zebridge_enable records %.% in zebridge_catalogue, and the bridge routes it after one restart',
                 tbl, tenant_col, tbl, tenant_col;
END;
$$ LANGUAGE plpgsql;

-- Answers "which tables are guarded, and on which columns?" — compare against SYNC_RULES
CREATE OR REPLACE FUNCTION public.zebridge_audit_write_guards()
RETURNS TABLE (tbl text, version_guard boolean, delete_guard boolean, detail text) AS $$
    SELECT c.relname::text,
           EXISTS (SELECT 1 FROM pg_trigger g WHERE g.tgrelid = c.oid
                   AND g.tgname = 'zebridge_bump_version_t'),
           EXISTS (SELECT 1 FROM pg_trigger g WHERE g.tgrelid = c.oid
                   AND g.tgname = 'zebridge_soft_delete_t'),
           coalesce((SELECT string_agg(pg_get_triggerdef(g.oid), ' | ')
                     FROM pg_trigger g WHERE g.tgrelid = c.oid
                       AND g.tgname LIKE 'zebridge_%'), '')
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public' AND c.relkind = 'r'
      -- Prefix test rather than `zebridge_is_internal_table()`: a LANGUAGE sql body is
      -- validated when it is CREATEd, so calling a function declared later in this file
      -- fails the whole script. Caught by scripts/scenarios/render.py, which applies the
      -- rendered SQL to a scratch database rather than trusting that it parses.
      AND c.relname NOT LIKE 'zebridge%'
    ORDER BY c.relname;
$$ LANGUAGE sql STABLE;


-- ---------------------------------------------------------
-- Per-tenant scoping — ONE entry point, on purpose
-- ---------------------------------------------------------
--
-- ⚠️ **Use `zebridge_scope_publication_to_one_tenant()` and nothing else** to put a tenant-scoped
-- table into a publication. A bare `ALTER PUBLICATION ... ADD TABLE` is the failure this
-- exists to prevent: it publishes the table with no row filter and no policy, and every
-- subscriber of that publication then receives every tenant's rows. Nothing errors, and
-- nothing in the bridge can detect it — the bridge is a pass-through and is meant to stay
-- one.
--
-- The function does the four things that must not drift apart, in the order they must
-- happen:
--
--   1. refuses a nullable or absent tenant column (a NULL tenant belongs to nobody, and a
--      nullable column cannot carry a replica identity)
--   2. moves the REPLICA IDENTITY to (tenant_col, <pk>) if the tenant is not already in it
--      — without this PostgreSQL refuses every UPDATE and DELETE on the table once the
--      publication carries a filter, including the application's own
--   3. enables RLS and creates two policies: writes bounded by the principal→tenant
--      mapping, reads left wide for `bridge_reader` (one session serves all tenants, so it
--      has no principal to resolve — read scoping is the publication's job)
--   4. only then adds the table to the publication, with the filter
--
-- Called once per (table, tenant): the per-table work is idempotent, the publication entry
-- is per-tenant. Each publication needs its own replication slot and its own bridge
-- instance.
--
-- ⚠️ `bridge_reader` must NOT be given BYPASSRLS to make step 3 work. That is a *role*
-- attribute covering every table in the database, and this file already grants the reader
-- SELECT on all of them — including tables added later by someone who never heard of the
-- bridge. The per-table `zb_reader_all` policy is the same capability, scoped, and it
-- fails closed for anything nobody considered.

-- Which principal may act for which tenant. Referenced by the write policy above; for
-- *writes* the bridge never reads it and never learns what a tenant is — it sets
-- `zb.principal` from the subject token it already trusts, and PostgreSQL resolves the
-- rest.
--
-- For *reads* it is a second source: this table is replicated (below) so the bridge can
-- mirror it live into `$KV.tenants.<principal>`, letting a fresh consumer holding only a
-- principal ID ask NATS for its own tenant (PROTOCOL.md "The Connection Flow", Step 0;
-- NOTES.md §1.12 part 3) instead of guessing or hardcoding it. The bridge still never
-- resolves a tenant *for routing purposes* from this table — that stays the subject
-- token's job — this is purely a client-facing lookup, mirrored from the same source of
-- truth as the write policy above so the two cannot drift apart.
-- principal → tenant, the mapping RLS resolves against — see PROTOCOL.md §7.4
CREATE TABLE IF NOT EXISTS public.zebridge_user_tenants (
    principal text NOT NULL,
    tenant_id text NOT NULL,
    PRIMARY KEY (principal, tenant_id)
);
GRANT SELECT ON public.zebridge_user_tenants TO ${POSTGRES_BRIDGE_USER}, ${POSTGRES_WRITER_USER};

-- ⚠️ Never published as ordinary CDC — see the matching exemption in
-- `zebridge_publication_guard()` (init.core.template.sql). The bridge special-cases this
-- relation's name in its WAL loop and diverts every row into a `$KV.tenants.<principal>`
-- put instead, exactly as it already does for `zebridge_ddl_events` → `$KV.schemas.*`.
-- Publishing it as `cdc.zebridge_user_tenants.*` would hand every client with a broad CDC
-- grant the full principal→tenant roster — the disclosure the KV bucket's principal-first
-- key order exists to prevent.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = '${BRIDGE_CDC_PUBLICATION}' AND tablename = 'zebridge_user_tenants'
    ) THEN
        ALTER PUBLICATION ${BRIDGE_CDC_PUBLICATION} ADD TABLE public.zebridge_user_tenants;
    END IF;
END $$;

-- ⚠️ Dollar-quote with a bare double-dollar ONLY — never a named tag (dollar, letters,
-- dollar). This file is rendered by `envsubst`, which reads such a tag as a variable
-- reference and substitutes it away, breaking the quoting and truncating every definition
-- after it. Measured: the whole file rendered down to one surviving function, and psql
-- reported no error. A bare double-dollar is not a valid identifier, so it survives.
--
-- The same rule forces the nested DDL below to use doubled single quotes rather than a
-- second dollar-quoted string — this comment cannot even show you the syntax to avoid.
-- the only supported way to publish a tenant-scoped table — see PROTOCOL.md §7.4, NOTES.md §1.8
CREATE OR REPLACE FUNCTION public.zebridge_scope_publication_to_one_tenant(
    tbl          regclass,
    tenant_col   name,
    tenant_value text,
    publication  name
) RETURNS void AS $$
DECLARE
    ri_kind  "char";
    covered  boolean;
    pk_cols  name[];
    idx_name name;
    col_list text;
BEGIN
    IF tbl::text IN ('zebridge_ddl_events','public.zebridge_ddl_events') THEN
        RAISE EXCEPTION 'refusing to publish %: it is the bridge''s own DDL tracker', tbl;
    END IF;

    -- 1. A NULL tenant belongs to nobody, and a nullable column cannot carry a replica
    --    identity.
    IF NOT EXISTS (SELECT 1 FROM pg_attribute
                   WHERE attrelid = tbl AND attname = tenant_col
                     AND attnum > 0 AND NOT attisdropped AND attnotnull) THEN
        RAISE EXCEPTION 'tenant column %.% must exist and be NOT NULL', tbl, tenant_col;
    END IF;

    -- 2. The tenant must be inside the replica identity, or PostgreSQL refuses every
    --    UPDATE and DELETE once the publication carries a filter on it — including the
    --    application's own writes.
    SELECT relreplident INTO ri_kind FROM pg_class WHERE oid = tbl;
    IF ri_kind = 'f' THEN
        covered := true;
    ELSE
        SELECT coalesce(bool_or(a.attname = tenant_col), false) INTO covered
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = tbl
          AND ((ri_kind = 'd' AND i.indisprimary) OR (ri_kind = 'i' AND i.indisreplident));
    END IF;

    IF NOT covered THEN
        SELECT array_agg(a.attname ORDER BY k.ord) INTO pk_cols
        FROM pg_index i
        JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
        WHERE i.indrelid = tbl AND i.indisprimary;
        IF pk_cols IS NULL THEN
            RAISE EXCEPTION '% has no primary key; the bridge refuses such a table anyway', tbl;
        END IF;
        idx_name := (SELECT relname FROM pg_class WHERE oid = tbl) || '_zb_ri';
        col_list := quote_ident(tenant_col) || ', ' ||
                    (SELECT string_agg(quote_ident(c), ', ') FROM unnest(pk_cols) c);
        EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %s (%s)', idx_name, tbl, col_list);
        EXECUTE format('ALTER TABLE %s REPLICA IDENTITY USING INDEX %I', tbl, idx_name);
        RAISE NOTICE 'replica identity of % moved to (%) so the tenant filter is legal', tbl, col_list;
    END IF;

    -- 3. Per-TABLE, idempotent: this runs once per (table, tenant) and the policies do not
    --    vary by tenant — the mapping table does.
    EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);

    IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = tbl AND polname = 'zb_tenant_write') THEN
        EXECUTE format(
            'CREATE POLICY zb_tenant_write ON %s FOR ALL TO %I'
            ' USING (%I IN (SELECT tenant_id FROM public.zebridge_user_tenants'
            '   WHERE principal = current_setting(''zb.principal'', true)))'
            ' WITH CHECK (%I IN (SELECT tenant_id FROM public.zebridge_user_tenants'
            '   WHERE principal = current_setting(''zb.principal'', true)))',
            tbl, 'bridge_writer', tenant_col, tenant_col);
    END IF;

    -- The reader sees every row of the tables it replicates: one session serves all
    -- tenants and has no principal to resolve. Read scoping is the publication's job.
    IF NOT EXISTS (SELECT 1 FROM pg_policy WHERE polrelid = tbl AND polname = 'zb_reader_all') THEN
        EXECUTE format('CREATE POLICY zb_reader_all ON %s FOR SELECT TO %I USING (true)',
                       tbl, 'bridge_reader');
    END IF;

    -- 4. Per-TENANT, and only now: the publication entry is what makes it a tenant stream.
    IF EXISTS (SELECT 1 FROM pg_publication_rel pr JOIN pg_publication p ON p.oid = pr.prpubid
               WHERE pr.prrelid = tbl AND p.pubname = publication) THEN
        EXECUTE format('ALTER PUBLICATION %I SET TABLE %s WHERE (%I = %L)',
                       publication, tbl, tenant_col, tenant_value);
    ELSE
        EXECUTE format('ALTER PUBLICATION %I ADD TABLE %s WHERE (%I = %L)',
                       publication, tbl, tenant_col, tenant_value);
    END IF;

    RAISE NOTICE 'published % to % for tenant %', tbl, publication, tenant_value;
END;
$$ LANGUAGE plpgsql;

-- Answers "which tenants will never have their tombstones reaped?"
--
-- The sweeper acts as a named principal (`zb_sweeper` by default) and is therefore bounded
-- by `zebridge_user_tenants` like any other writer — which is the point: forgetting to map
-- a tenant fails **closed**, and its reach is auditable in the same table as everyone
-- else's. The earlier design gave it a policy exempting principal-less sessions, which
-- opened the whole table to anything that reached that connection without setting a
-- principal. Failing open on an omission is the wrong default for a process that deletes.
--
-- ⚠️ The sweeper cannot run this check itself. RLS hides the unmapped rows from a warning
-- query exactly as it hides them from the DELETE — measured, the same query returns 0 as
-- the sweeper and 1 as the admin. A blind spot cannot survey itself, so the audit lives
-- here, with the grant.
--
--     INSERT INTO zebridge_user_tenants (principal, tenant_id) VALUES ('zb_sweeper', '<tenant>');
-- tenants whose tombstones will never be reaped — see PROTOCOL.md §7.5
CREATE OR REPLACE FUNCTION public.zebridge_audit_sweeper(sweeper_principal text DEFAULT 'zb_sweeper'::text)
 RETURNS TABLE(tbl text, tenant_id text, verdict text)
 LANGUAGE plpgsql
AS $$
DECLARE
    r record;
BEGIN
    -- Every published table that has BOTH a tenant column and a tombstone column: those
    -- are the ones the sweeper is expected to reach. A table with no tombstone is never
    -- swept (its deletes are physical), so an unmapped tenant there costs nothing.
    FOR r IN
        SELECT c.oid::regclass::text AS relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
        WHERE c.relkind = 'r'
          AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid
                        AND a.attname = 'tenant_id' AND a.attnum > 0 AND NOT a.attisdropped)
          AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid
                        AND a.attname = 'deleted_at' AND a.attnum > 0 AND NOT a.attisdropped)
    LOOP
        -- WARNING: doubled quotes, not a nested dollar-quote. A `$q` + `$` tag here is
        -- eaten by envsubst (it reads it as a variable) and truncates every definition
        -- after this point — caught by scripts/scenarios/render.py, which reported only
        -- 3 of 9 functions surviving the render.
        RETURN QUERY EXECUTE format(
            'SELECT %L::text, t.tenant_id::text, ''UNREAPED - tombstones for this tenant' || ' accumulate; %I is not mapped to it'' FROM (SELECT DISTINCT tenant_id FROM %s'
            || ' WHERE deleted_at IS NOT NULL) t WHERE NOT EXISTS (SELECT 1 FROM'
            || ' public.zebridge_user_tenants m WHERE m.principal = %L AND m.tenant_id = t.tenant_id)',
            r.relname, sweeper_principal, r.relname, sweeper_principal);
    END LOOP;
END $$;

GRANT SELECT, INSERT, UPDATE ON public.zebridge_gc_watermark TO ${POSTGRES_WRITER_USER};
