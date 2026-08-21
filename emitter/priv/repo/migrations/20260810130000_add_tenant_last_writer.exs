defmodule Emitter.PgProducer.Repo.AddTenantLastWriter do
  use Ecto.Migration

  # Brings `test_types` in line with what the bridge is configured to expect:
  #
  #   SYNC_RULES=test_types:updated_at,deleted_at,last_writer   ← field 3 needs last_writer
  #   TENANT_RULES=test_types:tenant_id                         ← needs tenant_id
  #
  # ⚠️ `test_types`, not `users`. `users` is the outbound-only fixture: no write grant and a
  # bigserial key no client can mint, so a tenant column there scopes nothing and a tiebreak
  # column has no competing writers to break ties between.
  def up do
    alter table(:test_types) do
      # NOT NULL because the replica-identity index requires it, and because a nullable
      # tenant is a row no policy matches and no subject can carry.
      #
      # ⚠️ The value becomes a NATS subject token — `cdc.<tenant>.<table>.<op>` — so it must
      # be a legal one: no `.`, space, `*` or `>`.
      #
      # No column DEFAULT here on purpose, but NOT for the reason a bare `null: false`
      # suggests: the invariant is enforced by `zebridge_guard_tenant_<table>()`, a BEFORE
      # trigger `zebridge_install_write_guards` attaches. It fires before the NOT NULL check
      # (Postgres runs BEFORE-row triggers first), so an omitted / NULL / empty tenant is
      # coalesced to the OPEN tenant — `OPEN_TENANT` in `.env.admin`, `_default` by default,
      # whose subject every principal may read via the CDC_PUBLIC stream — while a
      # malformed real value (containing a subject metacharacter) is REJECTED rather than
      # silently rerouted. This deployment classifies by opting *sensitive* rows into a real
      # tenant; the safe-by-omission value is therefore the shared one (fail-open, and
      # `check.py` is what asserts a sensitive table did not forget). Putting the rule in the
      # trigger keeps it in one place instead of a DEFAULT clause per table.
      add(:tenant_id, :string, null: false)

      # LWW tiebreak (SYNC_RULES field 3). Holds a client_id like `c-3f9a21b7`; the bridge
      # compares `coalesce(stored,'') < coalesce(incoming,'')`. Nullable — a row written
      # before this column existed simply has no recorded winner.
      add(:last_writer, :string)
    end

    # `init.write.template.sql` already has ONE entry point for turning a tenant column
    # into a properly-scoped one — `zebridge_scope_writes_by_tenant(tbl, tenant_col)` —
    # and it does more than the replica-identity fix this migration used to hand-roll
    # here: it also `ENABLE ROW LEVEL SECURITY`s the table and installs the
    # `zb_tenant_write`/`zb_reader_all` policies. Without it, `test_types` had a correct
    # CDC *read* path (tenant-routed subjects) but an entirely unscoped *write* path —
    # `bridge_writer` could write any tenant's rows regardless of
    # `zebridge_user_tenants`, silently, because nothing had ever enabled RLS on the
    # table. Measured: `relrowsecurity = f`, zero rows in `pg_policies` for
    # `test_types`, after the hand-rolled index/identity fix alone.
    #
    # Idempotent (`CREATE UNIQUE INDEX IF NOT EXISTS`, `DROP POLICY IF EXISTS` before
    # each `CREATE POLICY`), so calling it again — e.g. if the table already has the
    # right replica identity from an earlier manual fix — is a safe no-op on that part.
    #
    # Guarded on the function existing, same reasoning as `zebridge_grant_edge_writes`
    # in the sibling migration: a database brought up without `init.write.template.sql`
    # should skip this rather than fail the whole migration on something it does not
    # need.
    execute("""
    DO $$
    BEGIN
        IF EXISTS (
            SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE p.proname = 'zebridge_scope_writes_by_tenant' AND n.nspname = 'public'
        ) THEN
            PERFORM public.zebridge_scope_writes_by_tenant('public.test_types'::regclass, 'tenant_id');
        END IF;
    END $$;
    """)
  end

  # Coarse on purpose, matching the sibling migration's own `down` (a full
  # `drop_if_exists` on both tables, not a precise per-column revert): the RLS policies
  # and replica identity `zebridge_scope_writes_by_tenant` installs above aren't undone
  # here, only the two columns. `Emitter.Scenario` never calls `Ecto.Migrator.down` —
  # `reset/0` tears down with raw `DROP TABLE` instead — so this exists for
  # completeness against `use Ecto.Migration`'s expectations, not because anything in
  # this codebase exercises it.
  def down do
    alter table(:test_types) do
      remove(:tenant_id)
      remove(:last_writer)
    end
  end
end
