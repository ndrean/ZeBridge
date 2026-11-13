-- Included by setup.sql. Upgrade an existing example with:
-- psql "$ADMIN_DATABASE_URL" -1 -v ON_ERROR_STOP=1 -f examples/17-yjs/maintenance.sql
ALTER TABLE public.yjs_updates DROP CONSTRAINT IF EXISTS yjs_updates_deleted_at_check;
-- A client's compaction names the rows it merged here, as a JSON array of IDs. It is an input
-- of the INSERT only: yjs_updates_replace() retires those rows and stores NULL, so the list never
-- replicates. A text column, so every client stores it without array support.
ALTER TABLE public.yjs_updates ADD COLUMN IF NOT EXISTS replaces text;

CREATE OR REPLACE FUNCTION public.yjs_updates_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.deleted_at IS NOT NULL THEN
            RAISE EXCEPTION 'New Yjs updates must be active' USING ERRCODE = '23514';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        -- The existing sweeper owns physical cleanup, after its normal retention.
        -- Its identity is the same one zebridge_soft_delete already recognizes.
        IF OLD.deleted_at IS NOT NULL
           AND coalesce(current_setting('zb.principal', true), '') = 'zb_sweeper' THEN
            RETURN OLD;
        END IF;
        RAISE EXCEPTION 'Yjs updates are immutable; compact them before sweeper cleanup'
            USING ERRCODE = '23514';
    END IF;

    IF ROW(NEW.id, NEW.document_id, NEW.tenant_id, NEW.update_data)
       IS DISTINCT FROM ROW(OLD.id, OLD.document_id, OLD.tenant_id, OLD.update_data)
       OR NEW.replaces IS NOT NULL THEN
        RAISE EXCEPTION 'A Yjs update ID must always name the same document and bytes'
            USING ERRCODE = '23514';
    END IF;
    IF NEW.deleted_at IS DISTINCT FROM OLD.deleted_at THEN
        -- An invoker-rights trigger: only the table owner (or a role inheriting its
        -- privileges) can retire rows. Edge writes run as the separate writer role;
        -- no caller-controlled session flag grants compaction permission.
        IF OLD.deleted_at IS NOT NULL OR NEW.deleted_at IS NULL
           OR NOT pg_has_role((SELECT relowner FROM pg_class WHERE oid = TG_RELID), 'USAGE') THEN
            RAISE EXCEPTION 'Only the Yjs table owner may retire active updates'
                USING ERRCODE = '42501';
        END IF;
        -- Retention starts at retirement, never at the old edit's timestamp.
        NEW.deleted_at := clock_timestamp();
        NEW.updated_at := greatest(NEW.deleted_at, OLD.updated_at + interval '1 microsecond');
        NEW.last_writer := 'yjs-compactor';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS yjs_updates_immutable_t ON public.yjs_updates;
CREATE TRIGGER yjs_updates_immutable_t BEFORE INSERT OR UPDATE OR DELETE ON public.yjs_updates
    FOR EACH ROW EXECUTE FUNCTION public.yjs_updates_immutable();

-- Client compaction: an ordinary INSERT through mutate, whose `replaces` lists the confirmed
-- rows it merged. Every authorized writer of a tenant is trusted to merge correctly (PostgreSQL
-- does not decode Yjs); these triggers enforce only the bookkeeping. Any refusal is a check
-- violation: a permanent verdict.
--
-- Two steps, because the bridge writes INSERT … ON CONFLICT (id) DO UPDATE … WHERE <version
-- guard>: BEFORE INSERT triggers run before the conflict is found, so a replacement whose ID is
-- taken by a newer row would otherwise retire its inputs while inserting nothing. The BEFORE
-- trigger only sets the list aside, for this transaction, under the new row's ID, and stores
-- NULL; the AFTER trigger, which fires only for a row actually inserted, retires the inputs.
CREATE OR REPLACE FUNCTION public.yjs_updates_replace_take() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF NEW.replaces IS NOT NULL THEN
        -- An empty value would be set aside as '' and read back as "no list": refuse it here.
        IF btrim(NEW.replaces) = '' THEN
            RAISE EXCEPTION 'replaces must be a JSON array of update IDs' USING ERRCODE = '23514';
        END IF;
        PERFORM set_config('yjs_replaces.r' || replace(NEW.id::text, '-', ''), NEW.replaces, true);
        NEW.replaces := NULL;
    END IF;
    RETURN NEW;
END;
$$;

-- SECURITY DEFINER, owned by the table owner, so the retirement passes yjs_updates_immutable's
-- owner check; it therefore checks tenant and document itself.
CREATE OR REPLACE FUNCTION public.yjs_updates_replace() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog SET lock_timeout = '5s' AS $$
DECLARE
    stash constant text := 'yjs_replaces.r' || replace(NEW.id::text, '-', '');
    list text := nullif(current_setting(stash, true), '');
    ids uuid[];
    n integer;
BEGIN
    IF list IS NULL THEN RETURN NULL; END IF;
    PERFORM set_config(stash, '', true);
    BEGIN
        IF jsonb_typeof(list::jsonb) <> 'array' THEN RAISE EXCEPTION 'not an array'; END IF;
        ids := ARRAY(SELECT e::uuid FROM jsonb_array_elements_text(list::jsonb) e);
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'replaces must be a JSON array of update IDs' USING ERRCODE = '23514';
    END;
    n := cardinality(ids);
    IF n < 1 OR n > 256 THEN
        RAISE EXCEPTION 'A compaction replaces 1 to 256 updates, not %', n USING ERRCODE = '23514';
    END IF;
    IF (SELECT count(DISTINCT x) FROM unnest(ids) x) <> n OR NEW.id = ANY (ids) THEN
        RAISE EXCEPTION 'Compaction inputs must be distinct and exclude the replacement' USING ERRCODE = '23514';
    END IF;
    -- One compaction of a document at a time, client or maintenance (scripts/compaction.ts takes
    -- the same key): the second sees the first's retirements and is refused, never a deadlock.
    PERFORM pg_advisory_xact_lock(hashtextextended('yjs_updates/' || NEW.tenant_id || '/' || NEW.document_id, 0));
    IF (SELECT count(*) FROM public.yjs_updates
            WHERE id = ANY (ids) AND tenant_id = NEW.tenant_id AND document_id = NEW.document_id
              AND deleted_at IS NULL) <> n THEN
        RAISE EXCEPTION 'Compaction inputs are missing, already retired, or outside this document'
            USING ERRCODE = '23514';
    END IF;
    UPDATE public.yjs_updates SET deleted_at = clock_timestamp() WHERE id = ANY (ids);
    RETURN NULL;
END;
$$;
REVOKE ALL ON FUNCTION public.yjs_updates_replace_take(), public.yjs_updates_replace() FROM PUBLIC;

-- The BEFORE trigger fires after yjs_updates_immutable_t and before ZeBridge's width guard
-- (triggers run in name order), so the guard measures the row as stored, without the list.
DROP TRIGGER IF EXISTS yjs_updates_replace_t ON public.yjs_updates;
DROP TRIGGER IF EXISTS yjs_updates_replace_take_t ON public.yjs_updates;
CREATE TRIGGER yjs_updates_replace_take_t BEFORE INSERT ON public.yjs_updates
    FOR EACH ROW EXECUTE FUNCTION public.yjs_updates_replace_take();
CREATE TRIGGER yjs_updates_replace_t AFTER INSERT ON public.yjs_updates
    FOR EACH ROW EXECUTE FUNCTION public.yjs_updates_replace();
