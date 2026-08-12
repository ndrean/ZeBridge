defmodule Emitter.Chaos do
  @moduledoc """
  Helper functions to manually trigger Chaos Test events from an IEx console.
  """

  alias Emitter.Producer.Repo

  defmodule SchemaEvolutionMigration do
    use Ecto.Migration

    def up do
      alter table(:users) do
        add(:kyc_status, :string, default: "unverified")
      end

      flush()

      # Push a row that utilizes the new schema immediately
      execute(
        "INSERT INTO users (name, email, kyc_status, inserted_at, updated_at) VALUES ('Chaos User', 'chaos@test.com', 'verified', NOW(), NOW());"
      )
    end

    def down do
      alter table(:users) do
        remove(:kyc_status)
      end
    end
  end

  @doc """
  Run this from IEx via `Emitter.Chaos.trigger_schema_evolution()` at T=8m.
  It dynamically applies a migration to add a column and insert a row mid-stream.
  """
  def trigger_schema_evolution do
    IO.puts("🧨 Triggering Mid-Stream Schema Evolution...")

    # Ecto.Migrator.up/3 allows us to run a migration module directly without a file.
    # We pass a fake version number (20260811120000) so it doesn't conflict.
    Ecto.Migrator.up(Repo, 20_260_811_120_000, __MODULE__.SchemaEvolutionMigration)

    IO.puts("✅ Schema evolution complete! Check your clients to see if they survived.")
  end

  @doc """
  Reverts the schema evolution so you can run the test again.
  """
  def revert_schema_evolution do
    IO.puts("⏪ Reverting Schema Evolution...")
    execute_clean_rows()
    Ecto.Migrator.down(Repo, 9_999_999_999, __MODULE__.SchemaEvolutionMigration)
    IO.puts("✅ Reverted!")
  end

  defp execute_clean_rows do
    Ecto.Adapters.SQL.query!(Repo, "DELETE FROM users WHERE email = 'chaos@test.com'")
  end
end
