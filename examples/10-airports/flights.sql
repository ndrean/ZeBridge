-- One flight per tenant, edited together: a departure and an arrival, each a register
-- {v, t, w} — the value, its stamp, its writer — in one jsonb document. Two people can
-- move the two ends at once and both moves survive; on the same end, the later stamp wins
-- on every screen (COOPERATIVE_EDITING.md).
CREATE TABLE IF NOT EXISTS public.flights (
    id          varchar(80) PRIMARY KEY,                -- 'flight-<tenant>': every browser of a tenant names the same row
    tenant_id   varchar(64) NOT NULL,
    doc         jsonb NOT NULL DEFAULT '{}'::jsonb,     -- {"origin": {v, t, w}, "destination": {v, t, w}}
    updated_at  timestamptz NOT NULL DEFAULT now(),     -- the row's version
    last_writer varchar(64),                            -- the tiebreak
    deleted_at  timestamptz                             -- the tombstone
);

SELECT step, status FROM zebridge_enable('public.flights',
    tenant_col    => 'tenant_id',
    writable      => true,
    version_col   => 'updated_at',
    tiebreak_col  => 'last_writer',
    tombstone_col => 'deleted_at',
    publication   => 'my_pub',          -- your bridge's BRIDGE_CDC_PUBLICATION
    dry_run       => false);
