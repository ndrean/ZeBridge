-- ─────────────────────────────────────────────────────────────────────────────
-- init.core.sql — the read-only half: CDC and snapshots, and nothing that writes.
--
-- Runnable on its own. That is the point: a deployment that only *reads* PostgreSQL
-- should never meet RLS, tombstone guards or tenant scoping, and a newcomer reading this
-- file should not have to skip past them to find out how a change becomes an event.
--
--     envsubst < init.core.template.sql | psql -v ON_ERROR_STOP=1 …
--
-- The write path is `init.write.template.sql`, appended after this one:
--
--     cat init.core.template.sql init.write.template.sql | envsubst | psql …
--
-- ⚠️ It is appended, never merged. The read-only setup is this file *unmodified*, so the
-- two profiles cannot drift: there is no commented-out block here that the other profile
-- uncomments, and no second copy of the publication or the DDL triggers.
--
-- ⚠️ Nothing here may reference ${POSTGRES_WRITER_USER}. That role is created by the
-- write half, so a reference from this file breaks the read-only profile — which is the
-- one invariant that keeps the split honest.
-- ─────────────────────────────────────────────────────────────────────────────
-- Every object below carries a `-- <what it is> — see <doc> §<n>` line above it.
--
-- The reference points at the section that explains *why* the object exists, because this
-- file is where someone is standing when they need that: reading a trigger and wondering
-- whether it can be dropped. The docs describe behaviour a DBA has to preserve;
-- `zebridge_gc_watermark` in particular carries a 🔴 DBA callout in PROTOCOL.md §7.5,
-- because dropping it or unpublishing it removes a client guarantee without failing
-- anything.
--
-- Keep the reference when moving or rewriting an object. A comment that says what the code
-- already says is noise; one that says where the reasoning lives is the only link between
-- the two files.
--
-- The link runs both ways: **PROTOCOL.md §8b** lists every object created here, what it is,
-- and what breaks if it is removed. Read this file and you can find the reasoning; read the
-- docs and you can find the code. Adding an object means adding a row there too — a
-- one-directional link is the one that goes stale.
-- ─────────────────────────────────────────────────────────────────────────────

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${POSTGRES_READER_USER}') THEN
        CREATE USER ${POSTGRES_READER_USER} WITH PASSWORD '${POSTGRES_READER_PASSWORD}';
        ALTER USER ${POSTGRES_READER_USER} WITH REPLICATION;
    END IF;
    -- The reader's connection BUDGET, DB-enforced (outside the IF: applies to
    -- pre-existing roles on re-apply). Steady state is ~4 backends (producer,
    -- snapshot worker, boot passes) — the replication stream rides a walsender
    -- slot, not a regular backend. 10 leaves boot-burst headroom while making it
    -- impossible for a bridge bug to eat the cluster's default 100.
    ALTER ROLE ${POSTGRES_READER_USER} CONNECTION LIMIT 10;
    IF true THEN
    END IF;
END;
$$;

GRANT CONNECT ON DATABASE ${TARGET_DB} TO ${POSTGRES_READER_USER};

GRANT USAGE ON SCHEMA public TO ${POSTGRES_READER_USER};

-- Grant SELECT on existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO ${POSTGRES_READER_USER};

-- Ensure bridge user gets SELECT permissions on all FUTURE tables created by the DB owner
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ${POSTGRES_READER_USER};

-- Scope the reader's SELECT by tenant, for the snapshot path — see PROTOCOL.md "The
-- Connection Flow" Step 0, NOTES.md §1.12 part 1
--
-- ⚠️ RLS is evaluated against a query, and logical decoding has no query — structurally
-- invisible to CDC (§1.8: a policy returned 1 row to a SELECT while the WAL still carried
-- 2). CDC's tenant correctness is the subject's job and stays that way; this function
-- only ever affects a real SELECT, which today means the snapshot listener's own
-- connection setting `zb.tenant` before it queries.
--
-- Lives in core, not write: it needs nothing from `zebridge_user_tenants` (that table
-- resolves a WRITER's own tenant when omitted from a write — a write-path concern only)
-- and nothing from the writer role. A read-only deployment (`init.core.template.sql`
-- alone, `ZB_PROFILE=readonly`) can scope its snapshots by tenant with no write profile
-- installed at all.
--
-- ⚠️ `zb_reader_all` cannot simply gain a second, more restrictive policy alongside it:
-- PostgreSQL ORs every applicable permissive policy for one role/command together, so a
-- second policy next to an existing `USING (true)` changes nothing — the permissive one
-- always wins. The fix is making the ONE policy conditional instead: wholesale when
-- `zb.tenant` is unset, filtered when it is. The replication connection never calls
-- `set_config('zb.tenant', ...)` — and RLS does not apply to it regardless — so this is a
-- no-op for CDC and preserves exactly what `zb_reader_all USING (true)` already gave every
-- caller that never sets it: the reader sees everything by default, and only narrows on
-- an explicit opt-in.
--
-- ⚠️ `coalesce(..., '') = ''`, NOT `IS NULL` — the same trap `zb_sweeper`'s policy already
-- documents for `zb.principal`, measured again building this one: `current_setting(...,
-- true)` returns an empty string, not SQL NULL, for a GUC that has never been set on this
-- role/session (not only after a `SET LOCAL` resets at COMMIT). `IS NULL` never matched —
-- the "wholesale when unset" branch was dead code, and every reader saw zero rows instead
-- of everything until this was measured directly against the running database.
--
-- ⚠️ The bridge sets `zb.tenant` from the AUTHENTICATED REQUEST SUBJECT, never resolves it
-- through `zebridge_user_tenants` for this purpose — two derivations of one fact can
-- disagree (subject says acme, mapping says globex), and only one of them cannot disagree
-- with itself.
CREATE OR REPLACE FUNCTION public.zebridge_scope_reads_by_tenant(tbl regclass, tenant_col name)
RETURNS void AS $$
DECLARE
    t text := tbl::text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_attribute
                   WHERE attrelid = tbl AND attname = tenant_col
                     AND attnum > 0 AND NOT attisdropped AND attnotnull) THEN
        RAISE EXCEPTION 'tenant column %.% must exist and be NOT NULL', tbl, tenant_col;
    END IF;

    EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);

    EXECUTE format('DROP POLICY IF EXISTS zb_reader_all ON %s', tbl);
    EXECUTE format(
        'CREATE POLICY zb_reader_all ON %s FOR SELECT TO %I'
        -- Same open-tenant carve-out zb_tenant_write already has (init.write.template.sql):
        -- a row mapped to the OPEN tenant is readable by every principal, not just whoever
        -- has zb.tenant set to it. Without this, CDC (subject-routed, RLS-blind) delivers
        -- an open row to every client while a snapshot never grants the SELECT that would
        -- have put it there in the first place — audience says everyone, contents says only
        -- the open tenant itself, and a client's local copy of the row depends on whether it
        -- happened to be connected for a live CDC write, not on what its snapshot returned.
        ' USING (coalesce(current_setting(''zb.tenant'', true), '''') = '''' OR %I::text = current_setting(''zb.tenant'', true) OR %I::text = ''${OPEN_TENANT}'')',
        tbl, '${POSTGRES_READER_USER}', tenant_col, tenant_col);

    RAISE NOTICE 'reads on % now filtered by % when zb.tenant is set — CDC is unaffected (RLS does not apply to replication); the snapshot connection sets zb.tenant to scope it',
                 tbl, tenant_col;
END;
$$ LANGUAGE plpgsql;


-- The widest row actually stored, in the units CDC packs — see PROTOCOL.md §9
--
-- ⚠️ `octet_length`, never `pg_column_size`. The latter reports the *stored, compressed*
-- size: a 40 KB run of one character compresses to almost nothing, so a check built on it
-- reports a table as safe while CDC cannot carry its widest row. Wrong direction for a
-- safety bound.
--
-- ⚠️ Cheap for the types that matter: `octet_length` on text/varchar/bytea reads the length
-- out of the TOAST pointer without fetching the value — measured at 2 shared buffers for a
-- 50 MB table. `jsonb`/arrays need `::text`, which does materialise; they are usually small.
CREATE OR REPLACE FUNCTION public.zebridge_widest_row(tbl regclass)
RETURNS bigint AS $$
DECLARE
    expr   text;
    result bigint;
BEGIN
    SELECT coalesce(string_agg(
        CASE
            WHEN a.atttypid IN ('text'::regtype, 'bytea'::regtype, 'varchar'::regtype)
                THEN format('coalesce(octet_length(%I),0)', a.attname)
            WHEN t.typcategory = 'A'
              OR a.atttypid IN ('json'::regtype, 'jsonb'::regtype, 'xml'::regtype)
                THEN format('coalesce(octet_length(%I::text),0)', a.attname)
            -- Fixed-width columns: a flat allowance rather than a per-type table. The
            -- result is a floor on the row's size, which is what a guard wants.
            ELSE '8'
        END, ' + '), '0')
    INTO expr
    FROM pg_attribute a
    JOIN pg_type t ON t.oid = a.atttypid
    WHERE a.attrelid = tbl AND a.attnum > 0 AND NOT a.attisdropped;

    EXECUTE format('SELECT coalesce(max(%s),0)::bigint FROM %s', expr, tbl) INTO result;
    RETURN result;
END;
$$ LANGUAGE plpgsql STABLE;

-- Column DEFAULTs that alone exceed a byte budget — see PROTOCOL.md §9
--
-- ⚠️ The case `zebridge_widest_row` cannot see: a table with a huge column DEFAULT and no
-- rows yet measures as perfectly safe, and then the *first* insert — a 258-byte mutation
-- that passed every ingress guard — stores a 50 KB row and suspends the table for every
-- client. Measured. The default is the hazard; the row is only where it shows up.
--
-- ⚠️ Two guards make evaluating a default safe:
--
--   * only **variable-width** columns are evaluated. A `nextval()` default lives on an
--     integer column, so it is never reached and no sequence is burned;
--   * the function is **STABLE**, which makes PostgreSQL refuse any write the expression
--     attempts — a side-effecting default raises, and the handler below skips it rather
--     than failing the check.
CREATE OR REPLACE FUNCTION public.zebridge_oversized_defaults(tbl regclass, budget bigint)
RETURNS TABLE(col text, bytes bigint) AS $$
DECLARE
    r record;
    n bigint;
BEGIN
    FOR r IN
        SELECT a.attname AS name, pg_get_expr(ad.adbin, ad.adrelid) AS expr
        FROM pg_attribute a
        JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        JOIN pg_type t ON t.oid = a.atttypid
        WHERE a.attrelid = tbl AND a.attnum > 0 AND NOT a.attisdropped
          AND (a.atttypid IN ('text'::regtype, 'bytea'::regtype, 'varchar'::regtype)
               OR t.typcategory = 'A'
               OR a.atttypid IN ('json'::regtype, 'jsonb'::regtype, 'xml'::regtype))
    LOOP
        BEGIN
            EXECUTE format('SELECT coalesce(octet_length((%s)::text),0)::bigint', r.expr) INTO n;
        EXCEPTION WHEN OTHERS THEN
            -- A default that cannot be evaluated read-only is not a finding. Saying
            -- nothing beats refusing to start over an expression we declined to run.
            CONTINUE;
        END;
        IF n >= budget THEN
            col := r.name;
            bytes := n;
            RETURN NEXT;
        END IF;
    END LOOP;
END;
$$ LANGUAGE plpgsql STABLE;


-- THE CATALOGUE comes first: the guard below and every registration in this file
-- read it. Public tables used to live in a separate `zebridge_public_tables`; that
-- was a strict projection of this relation (`tenant_col IS NULL`) and is gone — the
-- deliberate "published to everyone" decision is the `public_reason` column, which
-- the CHECK makes mandatory exactly when a row is public.
-- THE CATALOGUE — one row per replicated table, the disjoint union of public and
-- tenant-scoped as one nullable column (NOTES.md, the static-residue endgame).
-- Written by `zebridge_enable` in the SAME transaction as the guards it installs
-- with these very columns, so nothing can disagree: TENANT_RULES and SYNC_RULES
-- become boot-time reads of this table (env demotes to override), the generation
-- producer reads it per tick, and the T3 "transcribe into .env.bridge" era ends.
-- The CHECK absorbs the publication guard's core law into the schema itself: a
-- row is tenant-scoped or publicly justified — never silently neither.
CREATE TABLE IF NOT EXISTS public.zebridge_catalogue (
    tbl           text PRIMARY KEY,
    tenant_col    name,            -- NULL = public; NOT NULL = tenant-scoped
    public_reason text,
    version_col   name NOT NULL DEFAULT 'updated_at',
    tombstone_col name,
    tiebreak_col  name,
    generations   boolean NOT NULL DEFAULT true,
    CHECK ((tenant_col IS NULL) <> (public_reason IS NULL))
);
GRANT SELECT ON public.zebridge_catalogue TO ${POSTGRES_READER_USER};
-- The writer's grant lives in init.write.template.sql, NOT here: roles are
-- cluster-wide, so this line passed silently on any cluster where the write half
-- had ever run — and failed with `role does not exist` only on a fresh cluster
-- running the read-only profile, which is precisely the profile it breaks. See
-- this file's header: nothing here may reference the writer role.

-- Refuses `ALTER PUBLICATION ... ADD TABLE` for a table that is neither tenant-scoped nor
-- recorded as public. The bridge cannot check this for itself — it is a pass-through, and
-- an unscoped table looks exactly like a scoped one from the WAL — so the check belongs
-- here, where the mistake is actually made.
--
-- ⚠️ Only the table the command touched is checked. Validating the whole publication would
-- make a pre-existing unscoped table block every unrelated change, and a guard that blocks
-- unrelated work is a guard people drop.
--
-- `zebridge_scope_publication_to_one_tenant()` passes this because it creates the policies and the
-- row filter *before* adding to the publication. That ordering is load-bearing, not
-- incidental.
-- refuses an unscoped ALTER PUBLICATION — see NOTES.md §1.8
CREATE OR REPLACE FUNCTION public.zebridge_publication_guard()
 RETURNS event_trigger
 LANGUAGE plpgsql
AS $$
DECLARE
    r       record;
    relid   oid;
    scoped  boolean;
    rls     boolean;
BEGIN
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands()
             WHERE command_tag = 'ALTER PUBLICATION' AND object_type = 'publication relation'
    LOOP
        -- Only the table this command touched: a pre-existing unscoped table must not
        -- block an unrelated change, or the guard becomes something people disable.
        SELECT pr.prrelid, pr.prqual IS NOT NULL, c.relrowsecurity
          INTO relid, scoped, rls
        FROM pg_publication_rel pr JOIN pg_class c ON c.oid = pr.prrelid
        WHERE pr.oid = r.objid;

        CONTINUE WHEN relid IS NULL;
        CONTINUE WHEN relid::regclass::text LIKE '%zebridge_ddl_events';
        -- Same reasoning as zebridge_ddl_events: this table's rows are never published as
        -- ordinary CDC (the bridge special-cases its relation name and diverts every row
        -- into $KV.tenants.<principal> instead — NOTES.md §1.12 part 3). It carries the
        -- full principal→tenant roster, which the whole point of that KV bucket's
        -- principal-first key order is to avoid ever exposing wholesale; publishing it as
        -- `cdc.zebridge_user_tenants.*` would hand every client with a broad CDC grant
        -- exactly that roster.
        CONTINUE WHEN relid::regclass::text LIKE '%zebridge_user_tenants';
        CONTINUE WHEN EXISTS (SELECT 1 FROM public.zebridge_catalogue cat
                              WHERE cat.tbl = (SELECT relname FROM pg_class WHERE oid = relid)
                                AND cat.tenant_col IS NULL);
        -- Either model counts as scoped:
        --   A  a publication row filter  → PostgreSQL bounds CDC itself
        --   B  RLS enabled               → RLS bounds writes and snapshots, and the
        --                                  bridge bounds CDC via TENANT_RULES, which is
        --                                  not visible from here
        -- Requiring both would refuse every model-B table; requiring neither would let an
        -- unscoped table through. So: at least one deliberate act of scoping.
        CONTINUE WHEN scoped OR rls;

        RAISE EXCEPTION
            'refusing to publish % unscoped: row filter=%, RLS=%',
            relid::regclass, scoped, rls
        USING HINT =
            'Use SELECT zebridge_scope_publication_to_one_tenant(''' || relid::regclass ||
            ''', ''<tenant_col>'', ''<tenant>'', ''<publication>''), or record the decision '
            'with SELECT zebridge_enable(''' || relid::regclass ||
            ''', public_reason => ''why everyone may read this'', dry_run => false).';
    END LOOP;
END $$;

DROP EVENT TRIGGER IF EXISTS zebridge_publication_guard_t;
CREATE EVENT TRIGGER zebridge_publication_guard_t ON ddl_command_end
    WHEN TAG IN ('ALTER PUBLICATION') EXECUTE FUNCTION public.zebridge_publication_guard();
-- Answers "is anything published without being scoped?" — the invariant a pass-through
-- bridge cannot check for itself. Run it after any publication change.
-- is anything published without being scoped? — see NOTES.md §1.8
CREATE OR REPLACE FUNCTION public.zebridge_audit_publications()
RETURNS TABLE (publication name, tbl text, row_filter text, rls boolean, verdict text) AS $$
    SELECT p.pubname,
           pr.prrelid::regclass::text,
           coalesce(pg_get_expr(pr.prqual, pr.prrelid), '(none)'),
           c.relrowsecurity,
           CASE
             WHEN pr.prrelid::regclass::text LIKE '%zebridge_ddl_events' THEN 'internal — expected'
             WHEN pr.prqual IS NULL AND NOT c.relrowsecurity THEN
               'PASS-THROUGH — every row reaches every subscriber of this publication'
             WHEN pr.prqual IS NULL AND c.relrowsecurity THEN
               'CDC UNSCOPED — RLS bounds the snapshot, pgoutput ignores it'
             WHEN pr.prqual IS NOT NULL AND NOT c.relrowsecurity THEN
               'reads scoped, WRITES UNSCOPED — no policy bounds what a principal may write'
             ELSE 'scoped'
           END
    FROM pg_publication_rel pr
    JOIN pg_publication p ON p.oid = pr.prpubid
    JOIN pg_class c ON c.oid = pr.prrelid
    ORDER BY p.pubname, 2;
$$ LANGUAGE sql;

-- ── making a publication is an OPERATION, not a line in a template ──────────
--
-- Nothing in these files creates a publication any more. This one used to:
-- init.core rendered `CREATE PUBLICATION` with BRIDGE_CDC_PUBLICATION substituted in from the
-- environment, so the name a bridge streams was spelled in two places — the
-- template's env and the bridge's own `--pub` — with nothing to make them agree.
--
-- And that block was not the only reader of the variable. THREE internal tables
-- get attached to the publication, and every consuming bridge needs all three:
--
--   zebridge_ddl_events    the DDL transport. Without it a bridge never learns a
--                          schema changed.
--   zebridge_gc_watermark  the offline-window watermark every client reads.
--   zebridge_user_tenants  the principal->tenant roster, diverted to $KV.tenants.
--                          Without it PROTOCOL Step 0 fails for every client of
--                          that bridge. (Write profile only.)
--
-- So the obvious recipe for a second bridge — `CREATE PUBLICATION pub_x` by hand,
-- then enables — produced a QUIET CRIPPLE: user tables replicate, DDL
-- propagation, the GC watermark and tenant resolution silently do not, and every
-- check stays green because the publication is not empty.
--
-- One fix for both: the publication is created HERE, by name, as a deliberate
-- act, and this is the only supported way to make one — so the three cannot be
-- forgotten. Idempotent, and worth re-running after init.write.template.sql to
-- pick up zebridge_user_tenants.
CREATE OR REPLACE FUNCTION public.zebridge_create_publication(p_name name)
RETURNS TABLE (step text, status text, detail text) AS $$
DECLARE
    t        text;
    -- Existence-checked one by one rather than assumed: zebridge_user_tenants
    -- lives in init.write, which a readonly-profile database never applies.
    internal text[] := ARRAY['zebridge_ddl_events', 'zebridge_gc_watermark', 'zebridge_user_tenants'];
BEGIN
    IF p_name IS NULL OR p_name = '' THEN
        RAISE EXCEPTION 'zebridge_create_publication: no name given'
            USING HINT = 'Name it: SELECT * FROM zebridge_create_publication(''my_pub''). '
                         'The same name the bridge is given as --pub / BRIDGE_CDC_PUBLICATION.';
    END IF;

    IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = p_name) THEN
        RETURN QUERY SELECT 'publication', 'already', format('%I exists', p_name);
    ELSE
        EXECUTE format('CREATE PUBLICATION %I', p_name);
        RETURN QUERY SELECT 'publication', 'created',
            format('CREATE PUBLICATION %I — carries only the internal tables until the '
                   'first zebridge_enable', p_name);
    END IF;

    FOREACH t IN ARRAY internal LOOP
        IF to_regclass('public.' || quote_ident(t)) IS NULL THEN
            RETURN QUERY SELECT 'internal', 'ABSENT',
                format('%I does not exist in this database. If this is a write-enabled '
                       'deployment, apply init.write.template.sql and re-run this function — '
                       'a bridge on %I loses what that table carries.', t, p_name);
        ELSIF EXISTS (SELECT 1 FROM pg_publication_tables
                      WHERE pubname = p_name AND schemaname = 'public' AND tablename = t) THEN
            RETURN QUERY SELECT 'internal', 'already', format('%I already carries %I', p_name, t);
        ELSE
            EXECUTE format('ALTER PUBLICATION %I ADD TABLE public.%I', p_name, t);
            RETURN QUERY SELECT 'internal', 'added',
                format('ALTER PUBLICATION %I ADD TABLE %I', p_name, t);
        END IF;
    END LOOP;
END $$ LANGUAGE plpgsql;

-- ---------------------------------------------------------
-- ZeBridge DDL Event Tracking
-- Guarantees strict ordering of schema changes with CDC data
-- ---------------------------------------------------------

-- DDL transport: the INSERT is what reaches the bridge — see PROTOCOL.md §5
CREATE TABLE IF NOT EXISTS public.zebridge_ddl_events (
    id SERIAL PRIMARY KEY,
    schema_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    command_tag TEXT NOT NULL,
    -- Schema captured INSIDE the DDL transaction, so it is the schema as of this
    -- LSN. The bridge must not re-query the catalog at processing time: that reads
    -- "now" rather than "then", which reorders bursts and corrupts WAL replay after
    -- a restart. Shape: {"columns":[{"name":..,"type":..}], "pk":".."|null}
    schema_def JSONB,
    emitted_at TIMESTAMPTZ DEFAULT NOW()
);

-- Backfill for databases initialised before schema_def existed
ALTER TABLE public.zebridge_ddl_events ADD COLUMN IF NOT EXISTS schema_def JSONB;

-- Tables whose DDL is infrastructure, not application data. Publishing their schemas
-- would tell every client to build a local replica of bookkeeping it has no use for.
-- Defined once and used by BOTH triggers: keeping two copies of this list in sync by
-- hand is exactly how one of them ends up stale.
--
-- Add your migration tool's table here if it differs: flyway_schema_history,
-- alembic_version, __diesel_schema_migrations, goose_db_version, ...
-- ---------------------------------------------------------
-- GC watermark — the oldest tombstone still standing
-- ---------------------------------------------------------
--
-- `GC_THRESHOLD_MS` is a promise to clients: the maximum offline window this deployment
-- supports. A client offline for longer can resurrect a deleted row, because the tombstone
-- that would have overruled its queued edit has been reaped. Until now a client had no way
-- to know where that line sits — it could only assume the sweeper was running on schedule.
--
-- ⚠️ Carried as a **replicated table**, not published to KV by the sweeper. The sweeper is
-- a PostgreSQL client and nothing else: giving it a NATS identity would widen what a
-- compromised sweeper can reach, in order to publish one number the existing CDC pipeline
-- already carries for free. It writes a row; every client that consumes CDC gets it.
--
-- A singleton (`id = 1`, CHECKed), because there is one watermark per deployment and a
-- table that can grow to two rows will eventually have two disagreeing answers.
--
-- ⚠️ Deliberately NOT in `zebridge_is_internal_table`: internal tables are hidden from
-- clients, and this one exists precisely to reach them.
-- the GC watermark clients read before flushing an outbox — see PROTOCOL.md §7.5
CREATE TABLE IF NOT EXISTS public.zebridge_gc_watermark (
    id            smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    -- Nothing soft-deleted before this is guaranteed to still exist. A client whose oldest
    -- queued write predates it must re-snapshot rather than flush.
    watermark     timestamptz NOT NULL,
    -- What the sweeper was configured with, so a client can tell a *moving* watermark from
    -- a sweeper that has stopped: if `swept_at` is old, the watermark is stale whatever it
    -- says.
    threshold_ms  bigint NOT NULL,
    swept_at      timestamptz NOT NULL DEFAULT now(),
    -- Rows removed in the last pass. Zero is the normal steady state; a large number after
    -- a long quiet period usually means the sweeper was down.
    reaped        bigint NOT NULL DEFAULT 0,
    -- The version column, so this table satisfies the same contract as any other CDC
    -- table. It is never written from the edge — see the grant below.
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- The sweeper writes it; everyone else reads it through CDC. ⚠️ SELECT for the reader is
-- what puts it in snapshots; UPDATE/INSERT for the writer is what lets the sweeper stamp
-- it. No DELETE: a singleton is never removed, and the row must survive a sweeper bug.
GRANT SELECT ON public.zebridge_gc_watermark TO ${POSTGRES_READER_USER};

INSERT INTO public.zebridge_gc_watermark (id, watermark, threshold_ms)
VALUES (1, now(), 0)
ON CONFLICT (id) DO NOTHING;

-- ⚠️ Published, or the whole design is inert: the row reaches clients through CDC and
-- nothing else, so a table nobody publishes is a watermark nobody can read.
--
-- Published **here**, hardcoded, rather than through `zebridge_scope_publication_to_one_tenant()`.
-- That is the rule for bridge-owned tables and it is worth stating, because there are now
-- two publication paths and the difference is not obvious:
--
--   this file          the bridge's own tables (zebridge_ddl_events, zebridge_gc_watermark).
--                      They exist before any migration runs, they have no tenant, and their
--                      shape is fixed by the bridge rather than chosen by a DBA. Publishing
--                      them anywhere else would mean the bridge could boot without them.
--   a migration        application tables. Tenant-scoped ones go through
--                      `zebridge_scope_publication_to_one_tenant()`; genuinely public ones are recorded
--                      in `zebridge_catalogue` (public_reason) and added directly.
--
-- Both paths still pass through `zebridge_publication_guard`, which is why the registration
-- below is not optional even here: the guard refuses an unscoped ADD TABLE regardless of who
-- issues it, and this table genuinely is public — one row, no tenant, identical for every
-- consumer.
DO $$
BEGIN
    INSERT INTO public.zebridge_catalogue (tbl, public_reason)
    VALUES ('zebridge_gc_watermark',
            'GC watermark: one row, no tenant, every client needs it')
    ON CONFLICT (tbl) DO NOTHING;

    -- The catalogue row is written here and the publication attachment is NOT:
    -- `zebridge_create_publication` attaches this table to whichever publication
    -- is being made. The row must exist first either way — it is what carries this
    -- table past zebridge_publication_guard (tenant_col IS NULL == public), and
    -- the guard fires on the ALTER, not here.
END $$;

-- ---------------------------------------------------------
-- Generation bookkeeping — the producer's own memory (NOTES.md §1.13, delta generations)
-- ---------------------------------------------------------
--
-- One row per built generation of a (tenant, table) pair: which cutoff it used and at
-- which LSN it was taken. The generation producer reads its LAST row to compute the
-- next delta and appends its new one — from PostgreSQL, never from NATS, because "the
-- bridge never reads its own output back" is a founding invariant. Append-only with
-- pruning past the chain depth; the rows double as the audit trail ("what did G17
-- cover and when").
--
-- ⚠️ `cutoff_lsn` is stamped by the producer itself: `SELECT pg_current_wal_lsn()`
-- taken BEFORE the REPEATABLE READ snapshot that reads the content, and this row
-- committed in that same transaction — overlap-never-gap; see NOTES.md §1.13 for why
-- the reverse order loses rows.
--
-- ⚠️ NOT published and listed in `zebridge_is_internal_table`: clients must never build
-- a local replica of producer bookkeeping, and its DDL must not reach `$KV.schemas`.
CREATE TABLE IF NOT EXISTS public.zebridge_generations (
    tenant         text NOT NULL,
    tbl            text NOT NULL,
    gen            bigint NOT NULL,
    -- The version watermark this generation's content query cut at. The next delta
    -- selects `version > cutoff_version - clamp_tolerance` (the margin rule).
    cutoff_version timestamptz NOT NULL,
    -- Where CDC resumes after applying this generation's chain: clients skip events
    -- with lsn <= cutoff_lsn (every CDC event carries lsn).
    cutoff_lsn     pg_lsn NOT NULL,
    -- The previous generation's cutoff — the delta's lower bound (minus the margin).
    -- Stored, not derived: after pruning, the oldest kept delta's lower bound refers
    -- to a generation whose row is gone. NULL on a chain's first generation.
    prev_cutoff    timestamptz,
    -- Whether this generation also shipped a full object (`<tbl>-g<N>-full`): the
    -- chain's jump-in point, refreshed before it can age out of the kept window.
    has_full       boolean NOT NULL DEFAULT false,
    built_at       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant, tbl, gen)
);
-- §10x: the compression dictionary is a chain member. `dict` holds the trained
-- dictionary on the row that carried the full (the producer's memory — read back
-- from HERE for the era's deltas, never from NATS); `dict_object` names the
-- dictionary object this row's payload was compressed with (NULL = plain zstd).
ALTER TABLE public.zebridge_generations ADD COLUMN IF NOT EXISTS dict bytea;
ALTER TABLE public.zebridge_generations ADD COLUMN IF NOT EXISTS dict_object text;
-- Databases created before the delta milestone: same columns, idempotently.
ALTER TABLE public.zebridge_generations ADD COLUMN IF NOT EXISTS prev_cutoff timestamptz;
ALTER TABLE public.zebridge_generations ADD COLUMN IF NOT EXISTS has_full boolean NOT NULL DEFAULT false;

-- ⚠️ The ONE deliberate write grant the read role holds, and why it is not a breach of
-- "the read role is physically unable to write": the generation's content query MUST
-- run as the reader (SELECT-everywhere + `zb.tenant` RLS scoping belong to it), and
-- the bookkeeping row MUST commit in that same transaction — so the same connection
-- writes it. The grant is INSERT + DELETE (pruning) on THIS table only; no UPDATE, so
-- history is append-only by privilege, not by convention.
GRANT SELECT, INSERT, DELETE ON public.zebridge_generations TO ${POSTGRES_READER_USER};

-- keeps the tracker's own rows out of the DDL feed — see PROTOCOL.md §5
CREATE OR REPLACE FUNCTION public.zebridge_is_internal_table(tbl text)
RETURNS boolean AS $$
    SELECT tbl = ANY (ARRAY[
        'zebridge_ddl_events',   -- our own DDL tracker
        'zebridge_generations',  -- generation producer bookkeeping (NOTES.md §1.13)
        'zebridge_limits',       -- the change-feed width budget (one row)
        'zebridge_generation_overrides',  -- superseded by zebridge_catalogue.generations
        'zebridge_catalogue',    -- THE catalogue: one row per replicated table
        'zebridge_invites',      -- enrollment codes: bridge infrastructure, never replicated
        -- ⚠️ the principal→tenant roster: BRIDGE INPUT (its CDC feeds $KV.tenants),
        -- never client-replicated — replicating it would disclose the roster the
        -- per-key $KV.tenants grant exists to protect (NOTES.md §1.12 part 3).
        'zebridge_user_tenants',
        'schema_migrations'      -- Ecto / ActiveRecord migration bookkeeping
    ]);
$$ LANGUAGE sql IMMUTABLE;

-- Retention. The bridge reads these events from the WAL, never from the table, so
-- rows are disposable the moment they are committed — pruning cannot cause a missed
-- or mis-replayed schema event even if the bridge is down and later replays old WAL.
-- The table is kept only as a human-readable audit trail, so a short window is
-- plenty and unbounded growth is pure cost.
--
-- Pruning runs inside the DDL triggers rather than on a schedule: DDL is rare, so
-- the cost is negligible and there is no external cron to deploy or forget.
-- Deliberately a constant, not a setting. PostgreSQL has no table-level TTL, so a
-- function is the only way to express this at all — but the *value* governs nothing
-- beyond disk.
--
-- What bounds it: **nothing functional.** The bridge never reads this table, it reads the
-- WAL, so a row is disposable the moment it commits. The pruning DELETE even runs in the
-- same transaction as the INSERT that carried the schema, so no consumer can be waiting on
-- a row this removes. The only reader is a human answering "what changed on Tuesday"
-- during an incident.
--
-- 7 days was arbitrary. 2 days is arbitrary too, and smaller: a schema change nobody
-- noticed within two days is one the audit trail was not going to explain anyway, and the
-- table grows with every DDL forever otherwise. Raise it freely — nothing breaks either
-- way, which is exactly why it is not a tuneable.
-- retention for the DDL audit trail — see PROTOCOL.md §5
CREATE OR REPLACE FUNCTION public.zebridge_prune_ddl_events()
RETURNS void AS $$
BEGIN
    DELETE FROM public.zebridge_ddl_events
    WHERE emitted_at < NOW() - INTERVAL '2 days';
END;
$$ LANGUAGE plpgsql;

-- Full replica identity ensures ZeBridge gets all columns in the WAL
ALTER TABLE public.zebridge_ddl_events REPLICA IDENTITY FULL;
GRANT SELECT ON TABLE public.zebridge_ddl_events TO ${POSTGRES_READER_USER};

-- on ddl_command_end: writes the schema-change row — see PROTOCOL.md §5
CREATE OR REPLACE FUNCTION public.zebridge_ddl_trigger_fn()
RETURNS event_trigger AS $$
DECLARE
    obj         record;
    tbl         text;
    tables      text[] := '{}';
    cmd_tag     text   := 'UNKNOWN';
    schema_json jsonb;
    last_def    jsonb;
    renames     jsonb;
BEGIN
    -- Pass 1: collect the distinct tables this command touched.
    -- object_identity is 'public.users' for a table but 'public.users.email' for a
    -- column, so split_part(..., 2) normalises both to the table name. ALTER TABLE
    -- ADD COLUMN emits a 'table' row AND a 'table column' row; deduping here stops
    -- the same schema being published twice.
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        IF obj.object_type IN ('table', 'table column') AND obj.schema_name = 'public' THEN
            tbl := split_part(obj.object_identity, '.', 2);
            cmd_tag := obj.command_tag;

            -- Skip infrastructure tables (our tracker, migration bookkeeping): their
            -- schemas are meaningless to clients and would provoke pointless migrations.
            IF NOT public.zebridge_is_internal_table(tbl) AND NOT (tbl = ANY(tables)) THEN
                tables := array_append(tables, tbl);
            END IF;
        END IF;
    END LOOP;

    -- Pass 2: snapshot each table's schema while we are still inside the DDL
    -- transaction, so catalog state and LSN are bound together atomically.
    FOREACH tbl IN ARRAY tables
    LOOP
        SELECT jsonb_build_object(
                 -- `oid` and `typtype` are for the BRIDGE, not for clients: its CDC
                 -- decoder switches on numeric OIDs and cannot know a per-database one
                 -- (extension and user-defined types), so it needs `typtype` to tell an
                 -- enum (safe to pass through as text) from anything else (must refuse).
                 -- Produced here because the event trigger already runs inside Postgres
                 -- with the catalog open — the alternative was a blocking pg_type query
                 -- on the replication hot path. `type` stays information_schema's
                 -- data_type verbatim, which is what PROTOCOL.md promises clients.
                 'columns', COALESCE((
                     SELECT jsonb_agg(jsonb_build_object(
                              'name', c.column_name,
                              'type', c.data_type,
                              -- Required-ness, so a client can build a valid INSERT.
                              -- Without these the descriptor said which columns exist and
                              -- not which ones a write must carry: an omitted NOT NULL
                              -- column with no DEFAULT is refused by PostgreSQL (23502),
                              -- and the only way a client could learn that was the
                              -- rejection. `required` is the single fact it needs —
                              -- `nullable`/`has_default` are kept because "why" matters
                              -- when a DBA is reading the descriptor.
                              'nullable', (c.is_nullable = 'YES'),
                              'has_default', (c.column_default IS NOT NULL),
                              'required', (c.is_nullable = 'NO' AND c.column_default IS NULL),
                              'oid', a.atttypid::int,
                              'typtype', t.typtype,
                              -- `attnum` is what survives a RENAME COLUMN: Postgres
                              -- renames by updating pg_attribute.attname alone, never
                              -- attnum (which is also never reused — a dropped column
                              -- keeps its attnum with attisdropped). Same-attnum,
                              -- different-name between two events IS the rename, with
                              -- no need to parse ALTER TABLE's command tag. For the
                              -- BRIDGE like oid/typtype; never forwarded to clients.
                              'attnum', a.attnum
                            ) ORDER BY c.ordinal_position)
                     FROM information_schema.columns c
                     JOIN pg_attribute a
                       ON a.attrelid = to_regclass(format('%I.%I', 'public', tbl))
                      AND a.attname = c.column_name
                      AND NOT a.attisdropped
                     JOIN pg_type t ON t.oid = a.atttypid
                     WHERE c.table_schema = 'public'
                       AND c.table_name   = tbl
                 ), '[]'::jsonb),
                 -- Every primary key column, in key order — NOT just single-column
                 -- keys. This used to filter on array_length(indkey,1) = 1, which made
                 -- a composite key report NULL, indistinguishable from having no key at
                 -- all. Those two cases need opposite treatment: a composite key
                 -- identifies rows exactly (CDC is fine, only snapshot pagination is
                 -- unimplemented), whereas no key at all makes DELETE ambiguous.
                 -- NULL here now means genuinely no primary key.
                 'pk', (
                     SELECT jsonb_agg(a.attname ORDER BY k.ord)
                     FROM pg_index i
                     JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                       ON true
                     JOIN pg_attribute a
                       ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                     -- to_regclass, not ::regclass: returns NULL instead of raising
                     -- if the table is gone (e.g. dropped later in the same tx)
                     WHERE i.indrelid = to_regclass(format('%I.%I', 'public', tbl))
                       AND i.indisprimary
                 ),
                 -- Secondary indexes, so a replica can be QUERIED rather than only
                 -- scanned. "Query your replica directly — joins, aggregates, offline"
                 -- is the product's central promise, and without these a 100k-row table
                 -- answers every predicate with a sequential scan.
                 --
                 -- Dialect-NEUTRAL and at the root, like 'pk': CREATE [UNIQUE] INDEX
                 -- name ON tbl (cols) is the same statement in PostgreSQL/PGlite and in
                 -- SQLite, so one list serves both consumer shapes.
                 --
                 -- Only what actually translates:
                 --   NOT indisprimary — the PK is created inline with the table
                 --   indpred IS NULL  — partial indexes carry a PG WHERE expression
                 --   indexprs IS NULL — expression indexes carry PG expressions
                 --   amname = 'btree' — gin/gist/brin have no SQLite equivalent
                 -- Anything excluded is a performance loss on the replica, never a
                 -- correctness one: an index is never the reason a row is right.
                 'indexes', COALESCE((
                     SELECT jsonb_agg(jsonb_build_object(
                              'name', ci.relname,
                              'unique', i.indisunique,
                              'columns', (
                                  SELECT jsonb_agg(a.attname ORDER BY k.ord)
                                  FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                                  JOIN pg_attribute a
                                    ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                              )
                            ) ORDER BY ci.relname)
                     FROM pg_index i
                     JOIN pg_class ci ON ci.oid = i.indexrelid
                     JOIN pg_am am ON am.oid = ci.relam
                     WHERE i.indrelid = to_regclass(format('%I.%I', 'public', tbl))
                       AND NOT i.indisprimary
                       AND i.indpred IS NULL
                       AND i.indexprs IS NULL
                       AND am.amname = 'btree'
                 ), '[]'::jsonb),
                 -- Foreign keys, so a replica can enforce referential integrity
                 -- rather than merely resemble it (PROTOCOL §4; NOTES §10d).
                 --
                 -- ⚠️ Filtered to what SQLite will ACCEPT, which is stricter than what
                 -- PostgreSQL allows. SQLite requires the parent key to be the parent's
                 -- PRIMARY KEY or to carry a UNIQUE INDEX, and refuses anything else at
                 -- DML time with `foreign key mismatch` — a failure no amount of waiting
                 -- resolves. Measured, both directions. So a constraint travels only if
                 -- its parent columns are the parent's PK, or are covered by a unique
                 -- index we ACTUALLY PORT (btree, non-partial, non-expression): a parent
                 -- key unique only under a partial or expression index is unique to
                 -- PostgreSQL and unsatisfiable on the replica.
                 --
                 -- Composite keys included: confrelid/conkey/confkey are arrays, and the
                 -- child and parent column lists must stay in matching order.
                 'foreign_keys', COALESCE((
                     SELECT jsonb_agg(jsonb_build_object(
                              'name', con.conname,
                              'columns', child_cols.cols,
                              'references', pc.relname,
                              'parent_columns', parent_cols.cols
                            ) ORDER BY con.conname)
                     FROM pg_constraint con
                     JOIN pg_class pc ON pc.oid = con.confrelid
                     JOIN LATERAL (
                         SELECT jsonb_agg(a.attname ORDER BY k.ord) AS cols
                         FROM unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
                         JOIN pg_attribute a ON a.attrelid = con.conrelid AND a.attnum = k.attnum
                     ) child_cols ON true
                     JOIN LATERAL (
                         SELECT jsonb_agg(a.attname ORDER BY k.ord) AS cols,
                                array_agg(k.attnum ORDER BY k.ord) AS attnums
                         FROM unnest(con.confkey) WITH ORDINALITY AS k(attnum, ord)
                         JOIN pg_attribute a ON a.attrelid = con.confrelid AND a.attnum = k.attnum
                     ) parent_cols ON true
                     WHERE con.conrelid = to_regclass(format('%I.%I', 'public', tbl))
                       AND con.contype = 'f'
                       -- the parent table must itself be replicated, or the child
                       -- references something the replica will never have
                       -- ⚠️ catalogue.tbl is TEXT (the bare relname), not regclass —
                       -- comparing it to an oid raises `operator does not exist:
                       -- text = oid`, which inside this trigger would fail the DDL
                       -- itself. Same comparison the generation producer uses.
                       AND EXISTS (SELECT 1 FROM public.zebridge_catalogue cat
                                    WHERE cat.tbl = pc.relname)
                       -- parent key is the PK, or has a unique index we port
                       AND EXISTS (
                           SELECT 1 FROM pg_index i
                           JOIN pg_class ci ON ci.oid = i.indexrelid
                           JOIN pg_am am ON am.oid = ci.relam
                           WHERE i.indrelid = con.confrelid
                             AND i.indisunique
                             AND i.indpred IS NULL AND i.indexprs IS NULL
                             AND am.amname = 'btree'
                             AND i.indkey::int2[] @> parent_cols.attnums::int2[]
                             AND parent_cols.attnums::int2[] @> i.indkey::int2[]
                       )
                 ), '[]'::jsonb),
                 -- Replica identity governs what UPDATE/DELETE actually carry, so both
                 -- the bridge and its clients need it: without a PK, DEFAULT means
                 -- PostgreSQL rejects writes outright, and only FULL yields old.* values
                 -- (and therefore working transition rules). Neither is discoverable
                 -- from the column list alone.
                 'replica_identity', (
                     SELECT CASE c.relreplident
                              WHEN 'd' THEN 'default'
                              WHEN 'f' THEN 'full'
                              WHEN 'n' THEN 'nothing'
                              WHEN 'i' THEN 'index'
                            END
                     FROM pg_class c
                     WHERE c.oid = to_regclass(format('%I.%I', 'public', tbl))
                 )
               )
        INTO schema_json;

        -- No columns means the table did not survive the transaction; nothing useful
        -- to publish, and an empty schema would tell clients to drop every column.
        IF schema_json -> 'columns' <> '[]'::jsonb THEN
            -- One migration commonly fires several ddl_command_end events for the same
            -- table (CREATE TABLE, then ALTER TABLE ... REPLICA IDENTITY, then a GRANT).
            -- Emitting an identical schema each time makes clients re-run a migration
            -- that changes nothing. Compare against the newest row for this table and
            -- skip if nothing actually changed — replica_identity is part of schema_def,
            -- so a genuine identity change still produces an event.
            SELECT e.schema_def INTO last_def
            FROM public.zebridge_ddl_events e
            WHERE e.table_name = tbl
            ORDER BY e.id DESC
            LIMIT 1;

            -- RENAME COLUMN detection (§1.2): same attnum, different name between this
            -- schema and the previous event's. Without this hint a rename reads as
            -- drop+add and the client nulls every existing value of the renamed column.
            -- Emitted as {"new_name": "old_name"}. An older last_def without attnum
            -- joins nothing and yields NULL — renames are simply not detected across
            -- the trigger upgrade, which is the pre-existing behaviour.
            renames := NULL;
            IF last_def IS NOT NULL THEN
                SELECT jsonb_object_agg(n.name, o.name) INTO renames
                FROM jsonb_to_recordset(schema_json -> 'columns') AS n(name text, attnum int)
                JOIN jsonb_to_recordset(last_def -> 'columns') AS o(name text, attnum int)
                  ON n.attnum = o.attnum AND n.name <> o.name;
            END IF;
            IF renames IS NOT NULL THEN
                schema_json := schema_json || jsonb_build_object('renamed', renames);
            END IF;

            -- Compare with 'renamed' stripped from BOTH sides: it describes the
            -- transition, not the shape. Comparing it too would emit one spurious
            -- schema event on the first unrelated DDL after a rename.
            IF (last_def - 'renamed') IS DISTINCT FROM (schema_json - 'renamed') THEN
                INSERT INTO public.zebridge_ddl_events
                       (schema_name, table_name, command_tag, schema_def)
                VALUES ('public', tbl, cmd_tag, schema_json);
            END IF;
        END IF;
    END LOOP;

    PERFORM public.zebridge_prune_ddl_events();
END;
$$ LANGUAGE plpgsql;

-- ⚠️ This trigger is the ONLY way a schema change reaches a client — PROTOCOL.md §0: the bridge never
-- polls the catalog for shape, so a table whose DDL does not land here keeps serving its
-- old schema forever. Tested end to end by scripts/scenarios/invalidate.py §1-2.
DROP EVENT TRIGGER IF EXISTS zebridge_ddl_trigger;
CREATE EVENT TRIGGER zebridge_ddl_trigger ON ddl_command_end
EXECUTE FUNCTION public.zebridge_ddl_trigger_fn();

-- DROP TABLE never reaches ddl_command_end — by the time it fires the object is
-- gone and pg_event_trigger_ddl_commands() does not report it. Drops therefore
-- need their own trigger on sql_drop, or clients never learn a table disappeared
-- and keep a local replica of something that no longer exists upstream.
-- on sql_drop: drops never reach ddl_command_end — see PROTOCOL.md §5
CREATE OR REPLACE FUNCTION public.zebridge_drop_trigger_fn()
RETURNS event_trigger AS $$
DECLARE
    obj record;
    tbl text;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_dropped_objects()
    LOOP
        IF obj.object_type = 'table' AND obj.schema_name = 'public' THEN
            tbl := split_part(obj.object_identity, '.', 2);

            IF NOT public.zebridge_is_internal_table(tbl) THEN
                -- schema_def is NULL rather than '{"columns":[]}': the table is gone,
                -- so there is no schema to state. The bridge treats a DROP TABLE tag
                -- as "forget this table" and never reads schema_def for it.
                INSERT INTO public.zebridge_ddl_events
                       (schema_name, table_name, command_tag, schema_def)
                VALUES ('public', tbl, 'DROP TABLE', NULL);

                -- Reap the table's generated width guard. `zebridge_install_width_guard`
                -- creates ONE FUNCTION PER TABLE, and DROP TABLE ... CASCADE removes the
                -- trigger (which belongs to the table) but NOT the function (which does
                -- not) — so every dropped table used to leave a dead
                -- zebridge_width_guard_<tbl>() behind, permanently. Measured: two were
                -- found in a running database, and one still carried the pre-2026-08-26
                -- `WHERE id = 1` budget lookup against a column that no longer exists,
                -- so calling it raised. Dead code that is also a landmine.
                --
                -- Here rather than in the bridge because it is DDL about the database's
                -- own objects, it runs in the same transaction as the DROP, and it needs
                -- no connection the bridge would have to open. `IF EXISTS` because a
                -- table with no unbounded columns never had a guard installed.
                EXECUTE format('DROP FUNCTION IF EXISTS public.%I()',
                               'zebridge_width_guard_' || tbl);
            END IF;
        END IF;
    END LOOP;

    PERFORM public.zebridge_prune_ddl_events();
END;
$$ LANGUAGE plpgsql;

DROP EVENT TRIGGER IF EXISTS zebridge_drop_trigger;
CREATE EVENT TRIGGER zebridge_drop_trigger ON sql_drop
EXECUTE FUNCTION public.zebridge_drop_trigger_fn();

-- No publication attachment here: `zebridge_create_publication` adds this table
-- to every publication it makes. Attaching it to one name rendered from the
-- environment is what made a second, hand-made publication a quiet cripple —
-- it replicated user tables and never carried a schema change.

-- Refuses CREATE/ALTER TABLE that introduces a `timestamp without time zone` column.
-- The version protocol depends on it: version columns travel in §7.2's wire format
-- (UTC, trailing Z), are compared and clamped as absolute instants, and a naive
-- timestamp makes two writers in different zones disagree about which write is newer —
-- silently, per row. The rule lived in prose ("use timestamptz") until a migration
-- forgot it; this is the mechanical form, same pattern as zebridge_publication_guard:
-- refuse at the source, inside the DDL transaction, so the migration fails whole.
--
-- ⚠️ `zebridge_is_internal_table` names are exempt — Ecto's own `schema_migrations`
-- uses naive timestamps by design, and blocking it would stop `mix ecto.migrate`'s
-- very first statement on a fresh database. Scoped to ordinary tables in `public`;
-- a deliberate exception is a DBA act:
--   ALTER EVENT TRIGGER zebridge_timestamp_guard_t DISABLE;  -- migrate; then ENABLE
-- refuses naive-timestamp columns — see SECURITY.md §1.3
CREATE OR REPLACE FUNCTION public.zebridge_timestamp_guard()
RETURNS event_trigger AS $$
DECLARE
    r       record;
    bad_col text;
    rel     text;
BEGIN
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands()
             WHERE command_tag IN ('CREATE TABLE', 'CREATE TABLE AS', 'ALTER TABLE')
               AND object_type IN ('table', 'table column')
    LOOP
        SELECT c.relname INTO rel
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.oid = r.objid AND n.nspname = 'public' AND c.relkind IN ('r', 'p');
        CONTINUE WHEN rel IS NULL;
        CONTINUE WHEN public.zebridge_is_internal_table(rel);

        SELECT a.attname INTO bad_col
        FROM pg_attribute a
        JOIN pg_type t ON t.oid = a.atttypid
        WHERE a.attrelid = r.objid
          AND t.typname = 'timestamp'
          AND a.attnum > 0 AND NOT a.attisdropped
        LIMIT 1;

        IF FOUND THEN
            RAISE EXCEPTION
                'migration rejected: column "%" on table "%" is "timestamp without time zone" — use timestamptz (Ecto: timestamps(type: :timestamptz)). Version comparison and the §7.2 wire format need absolute instants.',
                bad_col, rel;
        END IF;
        rel := NULL;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

DROP EVENT TRIGGER IF EXISTS zebridge_timestamp_guard_t;
CREATE EVENT TRIGGER zebridge_timestamp_guard_t ON ddl_command_end
    WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS', 'ALTER TABLE')
    EXECUTE FUNCTION public.zebridge_timestamp_guard();

-- ─────────────────────────────────────────────────────────────────────────────
-- The T2 entry point — one call instead of six remembered ones (NOTES.md §4.5)
-- ─────────────────────────────────────────────────────────────────────────────
--
-- Activation is spread over four places and two times: the templates *define* functions
-- at bootstrap, the DBA *invokes* them per table after the application's migrations, and
-- two more settings live outside the database entirely. Nothing here can set those last
-- two — so this composes what it can and **prints the rest**, which is the difference
-- between a workflow you can follow and one you have to remember.
--
-- ⚠️ `dry_run` defaults to TRUE. This grants, guards and enables RLS in one statement;
-- showing the plan first is the cheap half of that trade. Pass `dry_run => false` to apply.
--
--     SELECT * FROM zebridge_enable('public.orders');                       -- plan only
--     SELECT * FROM zebridge_enable('public.orders',
--                                   tenant_col => 'tenant_id',
--                                   writable   => true,
--                                   version_col => 'updated_at',
--                                   tombstone_col => 'deleted_at',
--                                   dry_run    => false);
--
-- ⚠️ Lives in the core half but dispatches the write-path calls through EXECUTE, so a
-- read-only install can still use it for the one step it needs (publishing a table) and
-- fails with a *named* reason rather than "function does not exist" if asked for writes.
-- ─────────────────────────────────────────────────────────────────────────────
-- The change-feed row-width budget, and the guard that enforces it AT THE ROW.
--
-- Two doors write rows: the edge (mutations through the bridge) and psql/backend
-- apps. The bridge's ingress check measures only the mutation PAYLOAD, so a small
-- edit to an already-wide row — or any direct backend write — could commit a row
-- the change feed cannot carry, and the first CDC touch would then suspend the
-- table for every reader ("accepted, then everybody freezes" — NOTES.md §1.13,
-- case C). A BEFORE trigger closes BOTH doors at the only place they meet: the
-- row, inside the writer's own transaction. The edge write becomes a verdict
-- (`rejected`, SQLSTATE 23514 — class 23 is already classified permanent by the
-- mutation listener); the psql write becomes an ordinary ERROR. Rules that only
-- live in prose are the ones that bite.
--
-- ⚠️ The budget is PER BRIDGE INSTANCE, not per database. `BASE_BUF` is a
-- per-process memory setting, so two bridges on the same database can legitimately
-- carry different row ceilings; a single global limit was simply wrong. It was also
-- a coupling maintained BY HAND — SECURITY.md said so, and it was forgotten the
-- first time BASE_BUF changed (measured: buffer 4 KB, table still 16384, so
-- PostgreSQL would accept rows the feed could not carry and suspend the table).
--
-- So the bridge WRITES this table at boot from its own resolved BASE_BUF. Nobody
-- maintains it; there is nothing to forget.
--
--   slot          — the instance. It is the identity PostgreSQL already keeps
--                   (`pg_replication_slots`), so a retired bridge's row is detectable
--                   and GC'd on the next boot of any bridge. Keyed by publication
--                   instead, a decommissioned 4 KB instance would pin the budget low
--                   forever with nothing able to tell.
--   publication   — what it carries. `pg_publication_tables` expands it to tables.
--   max_row_bytes — that instance's `2^BASE_BUF`.
--
-- ⚠️ This table is the INPUT, not what the guard reads. Each table's guard carries its
-- budget as a LITERAL, baked at boot by `zebridge_register_limits` from
--
--     zebridge_limits (slot -> budget)  x  pg_publication_tables (publication -> tables)
--
-- taking MIN per table: a row must fit the NARROWEST instance carrying it, or it is
-- accepted here and suspends the feed over there. Measured, and the reason the join is
-- not in the trigger: a per-row publication join cost 22 us against 2.2 for a plain
-- lookup, and a plain lookup still cost +4.81 us/row against a baked literal, which is
-- free. The cold path resolves; the hot path reads nothing at all.
CREATE TABLE IF NOT EXISTS public.zebridge_limits (
    slot          text PRIMARY KEY,
    publication   name    NOT NULL,
    max_row_bytes integer NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);
GRANT SELECT ON public.zebridge_limits TO ${POSTGRES_READER_USER};

-- The bridge registers its OWN budget here at boot. One call, over the connection
-- that always exists (the reader's), so this works identically in the read-only and
-- read/write profiles — a read-only deployment still has a width guard, and it is
-- still the truth about that instance's buffer.
--
-- ⚠️ SECURITY DEFINER, and that is the whole point: ${POSTGRES_READER_USER} keeps
-- SELECT + REPLICATION and NO write privilege on any table. "The reader is
-- physically unable to write" stays literally true and testable in
-- `role_table_grants` — the reader's only power here is to call this one function,
-- which writes rows describing itself and nothing else. Granting the reader
-- INSERT/UPDATE on the table instead would have bought the same behaviour by
-- weakening the one invariant the whole read path leans on.
--
-- Three steps, in this order:
--   1. GC instances PostgreSQL no longer knows — a retired bridge must stop
--      constraining everyone else, and its slot vanishing is the signal. Any
--      bridge's boot cleans up after every dead one.
--   2. drop this slot's rows for tables it no longer carries.
--   3. upsert one row per table currently in its publication.
--
-- ⚠️ Call it AFTER the replication slot exists, or step 1 deletes what step 3
-- writes on the next boot.
CREATE OR REPLACE FUNCTION public.zebridge_register_limits(
    p_slot          text,
    p_publication   name,
    p_max_row_bytes integer
) RETURNS integer AS $$
DECLARE
    r       record;
    n_guard integer := 0;
BEGIN
    IF p_max_row_bytes IS NULL OR p_max_row_bytes < 1024 THEN
        RAISE EXCEPTION 'refusing a row-width budget of %: below any sane floor', p_max_row_bytes
            USING ERRCODE = 'check_violation';
    END IF;

    -- 1. GC instances PostgreSQL no longer knows. A retired bridge must stop
    --    constraining everyone else, and its slot vanishing is the signal. An
    --    INACTIVE slot is NOT gone — that bridge is down, its WAL is retained, and
    --    it will replay; its budget still binds.
    DELETE FROM public.zebridge_limits l
     WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots s WHERE s.slot_name = l.slot);

    -- 2. this instance's own row.
    INSERT INTO public.zebridge_limits (slot, publication, max_row_bytes, updated_at)
    VALUES (p_slot, p_publication, p_max_row_bytes, now())
    ON CONFLICT (slot) DO UPDATE
       SET publication   = EXCLUDED.publication,
           max_row_bytes = EXCLUDED.max_row_bytes,
           updated_at    = now();

    -- 3. bake the literal into every guard this instance carries.
    --
    -- MIN over the instances that carry the table: a row must fit the NARROWEST of
    -- them. Taking MAX would admit rows the smallest instance cannot encode, and it
    -- would suspend the table for every reader on ITS feed while the wider instance
    -- carried on — a partial, per-feed outage that reads as a mystery.
    FOR r IN
        SELECT format('%I.%I', pt.schemaname, pt.tablename)::regclass AS tbl,
               MIN(l.max_row_bytes) AS budget
          FROM public.zebridge_limits l
          JOIN pg_publication_tables pt ON pt.pubname = l.publication
         WHERE format('%I.%I', pt.schemaname, pt.tablename)::regclass IN (
                   SELECT format('%I.%I', p2.schemaname, p2.tablename)::regclass
                     FROM pg_publication_tables p2 WHERE p2.pubname = p_publication)
         GROUP BY 1
    LOOP
        IF public.zebridge_rebudget_width_guard(r.tbl, r.budget) THEN
            n_guard := n_guard + 1;
        END IF;
    END LOOP;

    RETURN n_guard;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_catalog;

-- Rewrite ONE guard's budget, body only. Returns true if a guard was rewritten.
--
-- ⚠️ **Never touches the TRIGGER**, and that is the whole point of it existing
-- separately from `zebridge_install_width_guard`. Measured against a table with one
-- open writer: `CREATE OR REPLACE FUNCTION` completed in 203 ms, while `DROP TRIGGER`
-- blocked past a 6 s timeout waiting for ACCESS EXCLUSIVE. A pending exclusive lock
-- also queues AHEAD of new writers, so doing that at every bridge boot would freeze a
-- busy table for the length of someone else's transaction. Replacing the body needs no
-- table lock at all: the trigger keeps pointing at the same function, and PostgreSQL
-- invalidates the cached plan, so even a pool connection open for months uses the new
-- literal on its very next write (measured both ways).
--
-- The width EXPRESSION is regenerated too, since the column set may have changed; only
-- the trigger creation is skipped, and only when the trigger is already there.
CREATE OR REPLACE FUNCTION public.zebridge_rebudget_width_guard(tbl regclass, p_budget integer)
RETURNS boolean AS $$
DECLARE
    short       text := (SELECT relname FROM pg_class WHERE oid = tbl);
    expr        text := '';
    n_cols      int  := 0;
    n_unbounded int  := 0;
    col         record;
    fn_body     text;
BEGIN
    -- A table with no guard installed has none to rebudget. Installing one here would
    -- need the trigger DDL this function exists to avoid.
    IF NOT EXISTS (SELECT 1 FROM pg_trigger g
                    WHERE g.tgrelid = tbl AND g.tgname = 'zebridge_width_guard'
                      AND NOT g.tgisinternal) THEN
        RETURN false;
    END IF;

    FOR col IN
        SELECT a.attname, t.typname, a.atttypmod, t.typcategory
        FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
        WHERE a.attrelid = tbl AND a.attnum > 0 AND NOT a.attisdropped
    LOOP
        n_cols := n_cols + 1;
        IF col.typname = 'bytea' THEN
            n_unbounded := n_unbounded + 1;
            expr := expr || format(' + coalesce(octet_length(NEW.%I) * 2 + 2, 0)', col.attname);
        ELSIF col.typname IN ('text', 'json', 'jsonb', 'xml')
              OR (col.typname = 'varchar' AND col.atttypmod = -1)
              OR col.typcategory = 'A' THEN
            n_unbounded := n_unbounded + 1;
            expr := expr || format(' + coalesce(octet_length(NEW.%I::text), 0)', col.attname);
        END IF;
    END LOOP;

    IF n_unbounded = 0 THEN
        RETURN false;
    END IF;

    fn_body := format(
        'DECLARE '
        '  budget integer := %s; '
        '  width  integer := %s + %s; '
        'BEGIN '
        '  IF width >= budget THEN '
        '    RAISE EXCEPTION ''row width %% bytes is at or over the %%-byte change-feed budget: the feed could not carry this row (zebridge_limits, narrowest instance carrying this table; store a reference, not the blob)'', width, budget '
        '      USING ERRCODE = ''check_violation''; '
        '  END IF; '
        '  RETURN NEW; '
        'END',
        p_budget, substr(expr, 4), 32 * n_cols + 256);

    EXECUTE 'CREATE OR REPLACE FUNCTION public.' || quote_ident('zebridge_width_guard_' || short)
         || '() RETURNS trigger AS ' || quote_literal(fn_body)
         || ' LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_catalog';
    RETURN true;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_catalog;

GRANT EXECUTE ON FUNCTION public.zebridge_register_limits(text, name, integer)
    TO ${POSTGRES_READER_USER};



-- The generation producer's per-tenant chain set: tenants found in the DATA,
-- UNIONed with every tenant the system KNOWS (zebridge_user_tenants).
--
-- Data alone was the original rule (dyntenant-correct: a new tenant's first row
-- creates its chain on the next tick) — but data alone made "no chain" AMBIGUOUS.
-- Measured (NOTES §10h): test_types holds rows for acme/dynten only, so no
-- kilo chain was ever built, and a kilo client could not tell "producer has not
-- built yet" from "genuinely empty for me" — the retired snapshot path used to
-- answer that with a 0-row RLS dump. Including known tenants means an EMPTY
-- MANIFEST (a 0-row full) is built for them, which is the explicit "nothing to
-- seed": the client seeds zero rows, anchors its watermark, and follows CDC.
--
-- A tenant that exists ONLY as data (dyntenant, not yet registered) still gets
-- its chain from the data half of the union — neither source is dropped.
--
-- ⚠️ `zebridge_user_tenants` is created by init.write, and THIS file must stay
-- runnable (and its functions callable) in the read-only profile — hence the
-- to_regclass guard rather than a bare reference: plpgsql only resolves tables
-- at call time, so an unguarded reference would pass the apply and then fail
-- every producer tick on a read-only deployment.
--
-- SECURITY DEFINER because the reader's own RLS would scope the DISTINCT to one
-- tenant — PG functions hold the rules.
CREATE OR REPLACE FUNCTION public.zebridge_tenants_of(tbl regclass, tenant_col name)
RETURNS SETOF text AS $$
BEGIN
    IF to_regclass('public.zebridge_user_tenants') IS NOT NULL THEN
        RETURN QUERY EXECUTE format(
            'SELECT DISTINCT %I::text FROM %s WHERE %I IS NOT NULL '
            'UNION SELECT DISTINCT tenant_id::text FROM public.zebridge_user_tenants',
            tenant_col, tbl, tenant_col);
    ELSE
        RETURN QUERY EXECUTE format(
            'SELECT DISTINCT %I::text FROM %s WHERE %I IS NOT NULL', tenant_col, tbl, tenant_col);
    END IF;
END;
$$ LANGUAGE plpgsql STABLE SECURITY DEFINER SET search_path = public, pg_catalog;

-- Install (or refresh) the per-table width guard. Only unbounded columns are
-- measured — text, unbounded varchar, bytea (×2: the feed renders it as hex),
-- json/jsonb/xml, and arrays of any type — because a table with none of them is
-- statically inside every budget and gets no trigger at all (hot-path cost only
-- where overflow is possible). The generated trigger body is STATIC per table:
-- explicit column references, no runtime catalog reads, one SELECT on the one-row
-- limits table.
CREATE OR REPLACE FUNCTION public.zebridge_install_width_guard(tbl regclass)
RETURNS text AS $$
DECLARE
    short       text := (SELECT relname FROM pg_class WHERE oid = tbl);
    expr        text := '';
    n_cols      int  := 0;
    n_unbounded int  := 0;
    col         record;
    fn_body     text;
    budget_lit  integer;
BEGIN
    FOR col IN
        SELECT a.attname, t.typname, a.atttypmod, t.typcategory
        FROM pg_attribute a JOIN pg_type t ON t.oid = a.atttypid
        WHERE a.attrelid = tbl AND a.attnum > 0 AND NOT a.attisdropped
    LOOP
        n_cols := n_cols + 1;
        IF col.typname = 'bytea' THEN
            n_unbounded := n_unbounded + 1;
            expr := expr || format(' + coalesce(octet_length(NEW.%I) * 2 + 2, 0)', col.attname);
        ELSIF col.typname IN ('text', 'json', 'jsonb', 'xml')
              OR (col.typname = 'varchar' AND col.atttypmod = -1)
              OR col.typcategory = 'A' THEN
            n_unbounded := n_unbounded + 1;
            expr := expr || format(' + coalesce(octet_length(NEW.%I::text), 0)', col.attname);
        END IF;
    END LOOP;

    IF n_unbounded = 0 THEN
        RETURN format('%s: no unbounded columns — statically inside every budget, no guard installed', short);
    END IF;

    -- ⚠️ The generated body is assembled as a STRING and attached via quote_literal,
    -- never via nested dollar-quote tags (dollar-f-dollar and the like): the
    -- templates pass through envsubst at init, which eats any `$word` as an (empty)
    -- shell variable and leaves an unterminated quote that swallows every statement
    -- after it. Measured on the first fresh boot: this function AND zebridge_enable
    -- simply did not exist. Only the plain double-dollar delimiter survives envsubst
    -- — and it must never be SPELLED inside a body either, comments included: the
    -- lexer ends the body at the first occurrence, wherever it sits. (This very
    -- comment broke two boots by naming it literally.)
    --
    -- SECURITY DEFINER: the guard reads zebridge_limits as whoever writes the row
    -- (bridge_writer, a backend app), and none of them should need a grant on bridge
    -- internals just to be told no. search_path pinned, as every SECURITY DEFINER must.
    -- The budget is baked as a LITERAL, resolved HERE, once. MIN over the instances
    -- carrying this table: a row must fit the NARROWEST of them, or it is accepted
    -- here and suspends the feed over there. COALESCE covers "no bridge has
    -- registered yet" — nothing replicates the table, so there is no feed to
    -- overflow. `zebridge_register_limits` re-bakes it at every bridge boot.
    --
    -- Measured, and the reason it is a literal rather than a lookup: per row, a
    -- publication join cost 22 us, a plain indexed lookup 3.0 us, and a literal is
    -- free (within noise of no guard at all). This trigger fires on every insert and
    -- update of the table.
    SELECT COALESCE(MIN(l.max_row_bytes), 16384) INTO budget_lit
      FROM public.zebridge_limits l
      JOIN pg_publication_tables pt ON pt.pubname = l.publication
     WHERE format('%I.%I', pt.schemaname, pt.tablename)::regclass = tbl;

    fn_body := format(
        'DECLARE '
        '  budget integer := %s; '
        '  width  integer := %s + %s; '
        'BEGIN '
        '  IF width >= budget THEN '
        '    RAISE EXCEPTION ''row width %% bytes is at or over the %%-byte change-feed budget: the feed could not carry this row (zebridge_limits, narrowest instance carrying this table; store a reference, not the blob)'', width, budget '
        '      USING ERRCODE = ''check_violation''; '
        '  END IF; '
        '  RETURN NEW; '
        'END',
        budget_lit, substr(expr, 4), 32 * n_cols + 256);

    EXECUTE 'CREATE OR REPLACE FUNCTION public.' || quote_ident('zebridge_width_guard_' || short)
         || '() RETURNS trigger AS ' || quote_literal(fn_body)
         || ' LANGUAGE plpgsql SECURITY DEFINER SET search_path = public, pg_catalog';

    EXECUTE format('DROP TRIGGER IF EXISTS zebridge_width_guard ON %s', tbl);
    EXECUTE format('CREATE TRIGGER zebridge_width_guard BEFORE INSERT OR UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION public.%I()',
                   tbl, 'zebridge_width_guard_' || short);
    RETURN format('%s: width guard installed over %s unbounded column(s)', short, n_unbounded);
END;
$$ LANGUAGE plpgsql;

-- Adding a parameter changes the function's identity: without this DROP the old
-- signature would survive CREATE OR REPLACE and every named-argument call would
-- be ambiguous between the two.
DROP FUNCTION IF EXISTS public.zebridge_enable(regclass, name, boolean, name[], name, name, boolean, text, name, boolean);
DROP FUNCTION IF EXISTS public.zebridge_enable(regclass, name, boolean, name[], name, name, name, boolean, text, name, boolean);
DROP FUNCTION IF EXISTS public.zebridge_enable(regclass, name, boolean, name[], name, name, name, boolean, boolean, text, name, boolean);
CREATE OR REPLACE FUNCTION public.zebridge_enable(
    tbl           regclass,
    tenant_col    name    DEFAULT NULL,
    writable      boolean DEFAULT false,
    columns       name[]  DEFAULT NULL,
    version_col   name    DEFAULT NULL,
    tombstone_col name    DEFAULT NULL,
    tiebreak_col  name    DEFAULT NULL,  -- where the winning writer's id is stored; makes equal versions resolvable
    -- ⚠️ The conscious opt-OUT of the tombstone requirement (see the gate below).
    -- Physical deletes are INEXPRESSIBLE in a generation delta (deltas are
    -- upsert-only), and generations are the only seed path — so a hard-deleted row
    -- RESURRECTS on any fresh or re-seeding client until the next full (measured,
    -- NOTES §10i/§10j). Setting this true says the operator accepts that.
    allow_physical_deletes boolean DEFAULT false,
    generations   boolean DEFAULT true,
    public_reason text    DEFAULT NULL,
    -- ⚠️ No default. It used to render BRIDGE_CDC_PUBLICATION here, which made
    -- this name a SECOND source of truth against the bridge's own `--pub` —
    -- two spellings that had to agree, with nothing checking that they did.
    -- Naming it is one word, and it decides which tables get replicated.
    publication   name    DEFAULT NULL,
    -- Opt-in, same shape as allow_physical_deletes: a typo must not manufacture
    -- a publication (that would be a silent, empty, perfectly healthy-looking
    -- second feed), so an unknown name is refused unless this says otherwise.
    create_publication boolean DEFAULT false,
    dry_run       boolean DEFAULT true
) RETURNS TABLE (step text, status text, detail text) AS $$
DECLARE
    short      text := (SELECT relname FROM pg_class WHERE oid = tbl);
    have_write boolean := to_regprocedure('public.zebridge_grant_edge_writes(regclass)') IS NOT NULL;
    published  boolean;
    scoped     boolean;
    col_list   text;
    verb       text := CASE WHEN dry_run THEN 'would' ELSE 'done' END;
    r           record;
BEGIN
    -- ── the publication must be NAMED, and may only be created on request ─────
    --
    -- A RAISE, not one of this function's ERROR rows: an enable that does not name
    -- its publication is a malformed CALL, not a table failing a rule — and the
    -- rows are easy to not read (`PERFORM * FROM zebridge_enable(...)` in a
    -- migration discards every one of them). Same reasoning as the bridge's own
    -- refusal to invent a publication name: this argument decides WHICH TABLES get
    -- replicated, so there is nothing sensible to guess.
    IF publication IS NULL THEN
        RAISE EXCEPTION 'zebridge_enable(%): no publication named', tbl
            USING HINT = 'Pass publication => ''my_pub'' — the same name the bridge is given as '
                         '--pub / BRIDGE_CDC_PUBLICATION. There is no default. Add '
                         'create_publication => true if it does not exist yet.';
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = publication) THEN
        IF NOT create_publication THEN
            RAISE EXCEPTION 'zebridge_enable(%): publication % does not exist', tbl, publication
                USING HINT = 'Refused by default so a typo cannot manufacture a silent, empty, '
                             'healthy-looking second feed. If you meant to create it: '
                             'create_publication => true, or SELECT * FROM '
                             'zebridge_create_publication(''<name>'') first.';
        END IF;
        -- Reported through the same rows as everything else, so the operator sees
        -- the three internal tables land in the enable's own output.
        IF dry_run THEN
            RETURN QUERY SELECT 'publication', 'would',
                format('CREATE PUBLICATION %I, with zebridge_ddl_events / _gc_watermark / '
                       '_user_tenants attached', publication);
        ELSE
            RETURN QUERY SELECT * FROM public.zebridge_create_publication(publication);
        END IF;
    END IF;

    IF writable AND NOT have_write THEN
        RETURN QUERY SELECT 'preflight', 'ERROR',
            'writable => true, but the write half is not installed — this database was '
            'initialised with ZB_PROFILE=readonly (init.core.template.sql alone).';
        RETURN;
    END IF;

    -- ── the tombstone gate ────────────────────────────────────────────────────
    -- A writable table without a tombstone column deletes PHYSICALLY, and a
    -- physical delete cannot travel in a generation delta (deltas are upsert-only).
    -- With snapshot-on-demand retired, chains are the ONLY seed path — so a
    -- hard-deleted row silently RESURRECTS on every fresh or re-seeding client
    -- until the next full rebuild. That stopped being a "documented weaker
    -- guarantee" and became a divergence generator the day snapshots retired
    -- (measured: NOTES §10i, trigger-B). Refused mechanically, same pattern as the
    -- timestamp and publication guards; `allow_physical_deletes => true` is the
    -- recorded, deliberate opt-out.
    IF writable AND tombstone_col IS NULL AND NOT allow_physical_deletes THEN
        RETURN QUERY SELECT 'preflight', 'ERROR',
            format('%s is writable but has no tombstone_col: DELETEs would be physical, '
                   'and a physical delete cannot travel in a generation delta — hard-deleted '
                   'rows RESURRECT on fresh clients until the next full (NOTES §10i). '
                   'Add a tombstone column (e.g. deleted_at timestamptz) and pass '
                   'tombstone_col => %L, or state the acceptance explicitly with '
                   'allow_physical_deletes => true.', tbl, 'deleted_at');
        RETURN;
    END IF;
    IF writable AND tombstone_col IS NULL AND allow_physical_deletes THEN
        RETURN QUERY SELECT 'tombstone', 'WARNING',
            format('%s: physical deletes ACCEPTED by the operator — hard-deleted rows '
                   'resurrect on fresh seeds until the next full rebuild.', tbl);
    END IF;

    IF tenant_col IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM pg_attribute WHERE attrelid = tbl AND attname = tenant_col
          AND attnum > 0 AND NOT attisdropped
    ) THEN
        RETURN QUERY SELECT 'preflight', 'ERROR', format('%s has no column %I', tbl, tenant_col);
        RETURN;
    END IF;

    -- ⚠️ **Scoping must exist BEFORE the table is published**, and that ordering is not a
    -- preference — `zebridge_publication_guard` is an event trigger that refuses
    -- `ALTER PUBLICATION ... ADD TABLE` for a table with neither a row filter nor RLS.
    -- Publishing first and scoping second leaves a window in which the change feed carries
    -- every tenant's rows, so the guard closes it at the source. Predicted here rather than
    -- discovered halfway through: an ERROR row costs nothing, a half-applied activation does.
    scoped := (SELECT relrowsecurity FROM pg_class WHERE oid = tbl)
              OR EXISTS (SELECT 1 FROM pg_publication_rel WHERE prrelid = tbl AND prqual IS NOT NULL)
              -- ⚠️ Aliased and qualified: the parameter is also named `tbl`, and this
              -- table has a column of that name, so the unqualified form is ambiguous.
              OR EXISTS (SELECT 1 FROM public.zebridge_catalogue cat
                         WHERE cat.tbl = short AND cat.tenant_col IS NULL)
              OR public_reason IS NOT NULL
              OR (writable AND tenant_col IS NOT NULL);   -- scope_writes_by_tenant enables RLS
    IF NOT scoped THEN
        RETURN QUERY SELECT 'preflight', 'ERROR',
            format('%s would be published unscoped, which zebridge_publication_guard refuses. '
                   'Pick one: (a) writable => true with tenant_col, which enables RLS; '
                   '(b) public_reason => ''why everyone may read this'', recorded in '
                   'zebridge_catalogue; (c) call zebridge_scope_publication_to_one_tenant() '
                   'first for a one-bridge-per-tenant deployment.', tbl);
        RETURN;
    END IF;

    -- ── writes, guards and RLS come FIRST, because the guard demands it ───────
    IF writable THEN
        IF NOT dry_run THEN EXECUTE format('SELECT public.zebridge_grant_edge_writes(%L::regclass)', tbl); END IF;
        RETURN QUERY SELECT 'grants', verb, format('zebridge_grant_edge_writes(%L)', tbl::text);

        IF version_col IS NULL THEN
            RETURN QUERY SELECT 'guards', 'skipped',
                'no version_col given — writes are granted but unguarded: a client that omits '
                'the version is not corrected, and a direct DELETE is not tombstoned';
        ELSE
            IF NOT dry_run THEN
                -- tenant_col passed through: when the table is tenant-routed, the same call
                -- installs the tenant guard (absent -> the open tenant, OPEN_TENANT; malformed -> rejected).
                EXECUTE format('SELECT public.zebridge_install_write_guards(%L::regclass, %L, %L, %L)',
                               tbl, version_col, tombstone_col, tenant_col);
            END IF;
            RETURN QUERY SELECT 'guards', verb,
                format('zebridge_install_write_guards(%L, %L, %L, %L)', tbl::text, version_col, tombstone_col, tenant_col);
        END IF;

        IF tenant_col IS NOT NULL THEN
            IF NOT dry_run THEN
                EXECUTE format('SELECT public.zebridge_scope_writes_by_tenant(%L::regclass, %L)', tbl, tenant_col);
            END IF;
            RETURN QUERY SELECT 'rls', verb,
                format('zebridge_scope_writes_by_tenant(%L, %L) — writes AND snapshot reads scoped; '
                       'CDC stays scoped by the subject, not RLS (RLS cannot see it)',
                       tbl::text, tenant_col);
        END IF;
    ELSIF tenant_col IS NOT NULL THEN
        -- writable => false: nothing to scope on the WRITE side, but the READ side still
        -- can be — zebridge_scope_reads_by_tenant needs nothing this branch doesn't
        -- already have (init.core.template.sql, no write profile required).
        IF NOT dry_run THEN
            EXECUTE format('SELECT public.zebridge_scope_reads_by_tenant(%L::regclass, %L)', tbl, tenant_col);
        END IF;
        RETURN QUERY SELECT 'rls', verb,
            format('zebridge_scope_reads_by_tenant(%L, %L) — snapshot reads scoped; '
                   'CDC stays scoped by the subject, not RLS (RLS cannot see it)',
                   tbl::text, tenant_col);
    END IF;

    -- ── the width guard: both doors, one trigger ──────────────────────────────
    IF NOT dry_run THEN
        EXECUTE format('SELECT public.zebridge_install_width_guard(%L::regclass)', tbl);
    END IF;
    RETURN QUERY SELECT 'width guard', verb,
        format('zebridge_install_width_guard(%L) — a row the change feed cannot carry is '
               'refused at write time: edge writes get a rejected verdict (SQLSTATE 23514), '
               'psql gets an ordinary ERROR. No-op on tables without unbounded columns.', tbl::text);

    -- ── THE CATALOGUE ROW: the declaration itself, atomically with the guards ──
    IF NOT dry_run THEN
        EXECUTE format(
            'INSERT INTO public.zebridge_catalogue (tbl, tenant_col, public_reason, version_col, tombstone_col, tiebreak_col, generations) '
            'VALUES (%L, %L, %L, COALESCE(%L, ''updated_at''), %L, %L, %L) '
            'ON CONFLICT (tbl) DO UPDATE SET tenant_col = EXCLUDED.tenant_col, '
            'public_reason = EXCLUDED.public_reason, version_col = EXCLUDED.version_col, '
            'tombstone_col = EXCLUDED.tombstone_col, tiebreak_col = EXCLUDED.tiebreak_col, '
            'generations = EXCLUDED.generations',
            short, tenant_col, public_reason, version_col, tombstone_col, tiebreak_col, generations);
    END IF;
    RETURN QUERY SELECT 'catalogue', verb,
        format('zebridge_catalogue[%s]: tenant_col=%s version_col=%s tombstone=%s tiebreak=%s generations=%s '
               '— the bridge reads this at boot (env rules become overrides) and the '
               'generation producer per tick', short,
               COALESCE(tenant_col::text, 'NULL(public)'), COALESCE(version_col::text, 'updated_at'),
               COALESCE(tombstone_col::text, '-'), COALESCE(tiebreak_col::text, '-'), generations);

    -- ── publication LAST ──────────────────────────────────────────────────────
    SELECT EXISTS (SELECT 1 FROM pg_publication_tables
                   WHERE pubname = publication AND tablename = short) INTO published;
    IF published THEN
        RETURN QUERY SELECT 'publication', 'already', format('%I already carries %s', publication, tbl);
    ELSE
        col_list := CASE WHEN columns IS NULL THEN ''
                    ELSE ' (' || array_to_string(
                         ARRAY(SELECT quote_ident(c) FROM unnest(columns) c), ', ') || ')' END;
        IF NOT dry_run THEN
            EXECUTE format('ALTER PUBLICATION %I ADD TABLE %s%s', publication, tbl, col_list);
        END IF;
        RETURN QUERY SELECT 'publication', verb,
            format('ALTER PUBLICATION %I ADD TABLE %s%s', publication, tbl, col_list);
    END IF;

    -- ── the same table in a SECOND publication: warn, never refuse ────────────
    -- Legitimate (two NATS deployments, a staged migration), and dangerous in one
    -- specific way: the width guard bakes MIN(max_row_bytes) across every instance
    -- whose publication carries this table, so the NARROWEST bridge sets the ceiling
    -- for everyone. Rows already stored above that ceiling stay valid in Postgres and
    -- on the wider feed — but the narrow bridge cannot encode them, so IT quarantines
    -- the table (preflight at boot, or on the next touch) while the other carries on.
    -- A per-feed outage that reads as a mystery unless someone says this out loud.
    FOR r IN
        SELECT p.pubname
          FROM pg_publication_tables p
         WHERE format('%I.%I', p.schemaname, p.tablename)::regclass = tbl
           AND p.pubname <> publication
    LOOP
        RETURN QUERY SELECT 'publication'::text, 'WARNING'::text,
            format('%s is also in publication %I. The width guard takes the MINIMUM '
                   'budget across every instance carrying it, so the narrowest bridge '
                   'sets the ceiling for ALL writers — and any bridge whose buffer is '
                   'smaller than an existing row quarantines this table on its own feed. '
                   'Keep their BASE_BUF equal, or keep the publications disjoint.',
                   tbl::text, r.pubname);
    END LOOP;

    -- ── re-bake the width guard NOW THAT THE TABLE IS PUBLISHED ───────────────
    -- The guard is installed BEFORE the publication step (its own required
    -- ordering), but its baked budget derives from a join through
    -- pg_publication_tables — which at install time contains NOTHING for this
    -- table, so a newborn's guard silently baked the 16384 default and ignored
    -- every registered instance budget until the next bridge restart. Measured:
    -- livebirth's 4096-budget probe stored a 4096-byte row the guard should have
    -- refused (case C, the exact failure the guard exists for). Re-derive with
    -- the publication row in place; rebudget is body-only, so no table lock.
    IF NOT dry_run THEN
        DECLARE
            eff_budget integer;
        BEGIN
            -- Schema-qualified regclass comparison, IDENTICAL to
            -- zebridge_install_width_guard's own derivation: `pt.tablename = short`
            -- matches by bare name across every schema in the publication, so two
            -- same-named tables in different schemas would share a budget.
            SELECT COALESCE((SELECT MIN(l.max_row_bytes)
                             FROM public.zebridge_limits l
                             JOIN pg_publication_tables pt ON pt.pubname = l.publication
                             WHERE format('%I.%I', pt.schemaname, pt.tablename)::regclass = tbl), 16384)
              INTO eff_budget;
            IF public.zebridge_rebudget_width_guard(tbl, eff_budget) THEN
                RETURN QUERY SELECT 'width guard', 'done',
                    format('budget re-baked at %s bytes now the publication carries %s', eff_budget, tbl);
            END IF;
        END;
    END IF;

    IF columns IS NOT NULL AND tenant_col IS NOT NULL THEN
        RETURN QUERY SELECT 'publication', 'NOTE',
            format('column list + tenant scoping: put %I inside the replica identity, or '
                   'UPDATE/DELETE are refused at the source (NOTES.md §1.8)', tenant_col);
    END IF;

    -- ── T3 and T4 live outside the database and always will ───────────────────
    RETURN QUERY SELECT 'T3 bridge', 'MANUAL',
        CASE WHEN tenant_col IS NULL
             THEN 'restart the bridge — boot reconciles CDC_PUBLIC''s subject filter from the '
                  'catalogue, so a NEW public table needs one restart to gain its subject. '
                  'No env transcription, no stream edit by hand.'
             ELSE 'restart the bridge — it reads zebridge_catalogue at boot, so the routing for a '
                  'NEW tenant-scoped table needs one restart (no env transcription: SYNC_RULES/'
                  'TENANT_RULES are legacy overrides now). The generation chain needs nothing — '
                  'the producer reads the catalogue per tick.' END;
    RETURN QUERY SELECT 'T4 nats conf', 'MANUAL',
        CASE WHEN tenant_col IS NULL
             THEN format('grant subscribe on cdc.%s.> — and init.snap.%s.> to match, because a '
                         'client must not be able to dump what it cannot subscribe to', short, short)
             ELSE 'grant subscribe on cdc.<tenant>.>, init.snap.<tenant>.> and '
                  '$KV.snapshots.<tenant>.> per principal — one stream (INIT_<TENANT>) per '
                  'tenant, same reason as CDC_<TENANT> (a JetStream filter_subject is '
                  'reader-chosen, not ACL-checked); reload NATS, not the bridge' END;

    IF dry_run THEN
        RETURN QUERY SELECT 'summary', 'DRY RUN', 'nothing was applied — re-run with dry_run => false';
    END IF;
END;
$$ LANGUAGE plpgsql;
