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
  def change do
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
  end
end
