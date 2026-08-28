defmodule Emitter.PgProducer.Repo.AddTenantLastWriter do
  use Ecto.Migration

  # Brings `test_types` in line with what its `zebridge_catalogue` row declares
  # (written by the `zebridge_enable` call below, atomically with the guards):
  #
  #   tombstone_col/tiebreak_col need `deleted_at` / `last_writer`
  #   tenant_col needs `tenant_id`
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

      # LWW tiebreak (`zebridge_catalogue.tiebreak_col`). Holds a client_id like `c-3f9a21b7`; the bridge
      # compares `coalesce(stored,'') < coalesce(incoming,'')`. Nullable — a row written
      # before this column existed simply has no recorded winner.
      add(:last_writer, :string)
    end

    flush()

    # ONE call finishes everything `test_types` still needs, and publishes it for the
    # first time (the previous migration deliberately left it unpublished — see its own
    # comment): `zebridge_enable(..., tenant_col: ..., writable: true, ...)` re-grants
    # edge writes and re-installs the version/tombstone guards (idempotent — both already
    # ran in `SetupCdcTables`), installs the tenant guard trigger for the first time (only
    # happens when `tenant_col` is given), calls `zebridge_scope_writes_by_tenant` (moves
    # the tenant column into the replica identity if needed, enables RLS, installs
    # `zb_tenant_write`/`zb_reader_all`), and only then — now that the preflight considers
    # the table scoped — adds it to the publication.
    #
    # This is the single entry point that replaces three separate manual fixes made by
    # hand earlier in this project's history: the `CREATE UNIQUE INDEX` +
    # `REPLICA IDENTITY USING INDEX` pair, the `zebridge_scope_writes_by_tenant` call, and
    # the missing tenant guard trigger — each found only after the bridge refused the
    # table for a different reason each time. `zebridge_enable()` does all three at once,
    # in the order its own preflight requires, so there is no window where a partial
    # application looks finished.
    execute("""
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'zebridge_enable') THEN
            PERFORM * FROM public.zebridge_enable(
                'public.test_types'::regclass,
                tenant_col => 'tenant_id',
                writable => true,
                version_col => 'updated_at',
                tombstone_col => 'deleted_at',
                tiebreak_col => 'last_writer',
                publication => '#{zb_publication()}',
                dry_run => false
            );
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

  # The publication is an ARGUMENT now, never a default: `zebridge_enable` has no
  # default for it (NOTES §10ad), because that name decides which tables a bridge
  # replicates. Unset stops the migration rather than publishing into a guess.
  defp zb_publication do
    System.get_env("BRIDGE_CDC_PUBLICATION") ||
      raise "BRIDGE_CDC_PUBLICATION is not set: this migration publishes a table and must " <>
              "name the publication (the same name the bridge is given as --pub). " <>
              "Source .env.bridge before running migrations."
  end

end
