-- Included by setup.sql. Upgrade an existing example with:
-- psql "$ADMIN_DATABASE_URL" -1 -v ON_ERROR_STOP=1 -f examples/17-yjs/maintenance.sql
ALTER TABLE public.yjs_updates DROP CONSTRAINT IF EXISTS yjs_updates_deleted_at_check;

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
       IS DISTINCT FROM ROW(OLD.id, OLD.document_id, OLD.tenant_id, OLD.update_data) THEN
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
