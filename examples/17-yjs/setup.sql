-- Run as the DBA: psql "$ADMIN_DATABASE_URL" -v ON_ERROR_STOP=1 \
--   -v publication=my_pub -f examples/17-yjs/setup.sql
BEGIN;

CREATE TABLE public.yjs_updates (
    id           uuid PRIMARY KEY,
    document_id  uuid NOT NULL,
    update_data  bytea NOT NULL CHECK (octet_length(update_data) > 0),
    tenant_id    text NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    last_writer  text,
    deleted_at   timestamptz
);
CREATE INDEX yjs_updates_document ON public.yjs_updates (tenant_id, document_id);

\ir maintenance.sql

SELECT * FROM zebridge_enable('public.yjs_updates'::regclass,
    writable      => true,
    tenant_col    => 'tenant_id',
    version_col   => 'updated_at',
    tombstone_col => 'deleted_at',
    tiebreak_col  => 'last_writer',
    publication   => :'publication',
    dry_run       => false);

COMMIT;
