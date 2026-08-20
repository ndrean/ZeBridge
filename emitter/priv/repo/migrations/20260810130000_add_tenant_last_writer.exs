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
      # be a legal one: no `.`, space, `*` or `>`. A dot splits it into two tokens and the
      # events route somewhere nobody is subscribed to, silently.
      #
      # ⚠️ No default, deliberately: a default would make an omitted tenant silently become
      # someone's, which for a tenant column is the wrong kind of forgiving. Every writer
      # states it — the scenarios read theirs from `zebridge_user_tenants`.
      add(:tenant_id, :string, null: false)

      # LWW tiebreak (SYNC_RULES field 3). Holds a client_id like `c-3f9a21b7`; the bridge
      # compares `coalesce(stored,'') < coalesce(incoming,'')`. Nullable — a row written
      # before this column existed simply has no recorded winner.
      add(:last_writer, :string)
    end
  end
end
