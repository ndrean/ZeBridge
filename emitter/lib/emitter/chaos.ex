defmodule Emitter.Chaos do
  @moduledoc """
  Helper functions to manually trigger Chaos Test events from an IEx console.

  Run these mid-stream (e.g. while `Emitter.Produce.stream/4` is generating load) to
  check that clients survive schema changes arriving in the middle of CDC traffic.
  """

  alias Emitter.Producer.Repo

  # One constant for both directions. These MUST stay equal: Ecto.Migrator.down/3 looks
  # the version up in schema_migrations before running the down, so a mismatch silently
  # reports :already_down, leaves the column in place, and — because the up version stays
  # recorded — turns every later up into a no-op. The harness then looks fine while doing
  # nothing at all.
  @schema_evolution_version 20_260_811_120_000

  defmodule SchemaEvolutionMigration do
    use Ecto.Migration

    def up do
      alter table(:users) do
        add(:kyc_status, :string, default: "unverified")
      end

      flush()

      # Push a row that uses the new column immediately. Ecto wraps migrations in a
      # transaction, so the DDL event and this INSERT land in the same commit — which is
      # exactly the ordering the bridge must preserve: schema first, then the row that
      # depends on it.
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
  Mid-stream schema evolution: adds a column and immediately writes a row using it.

  Exercises `ddl_command_end` -> zebridge_ddl_events -> KV schema publish, and the
  bridge's guarantee that the schema reaches NATS before the dependent CDC row.
  """
  def trigger_schema_evolution do
    IO.puts("🧨 Triggering Mid-Stream Schema Evolution...")
    Ecto.Migrator.up(Repo, @schema_evolution_version, __MODULE__.SchemaEvolutionMigration)
    IO.puts("✅ Schema evolution complete! Check your clients to see if they survived.")
  end

  @doc """
  Reverts the schema evolution so the test can be run again.
  """
  def revert_schema_evolution do
    IO.puts("⏪ Reverting Schema Evolution...")
    execute_clean_rows()
    Ecto.Migrator.down(Repo, @schema_evolution_version, __MODULE__.SchemaEvolutionMigration)
    IO.puts("✅ Reverted!")
  end

  @doc """
  Drop-table chaos: creates a throwaway table, publishes it, then drops it.

  This is the only path that exercises the `sql_drop` event trigger. DROP TABLE never
  reaches `ddl_command_end` — the object is already gone by the time it fires — so drops
  are captured separately and published as a tombstone `{"dropped": true}` rather than a
  schema, which is what tells a client to drop its local replica.

  `ALTER TABLE ... DROP COLUMN` does NOT cover this: that is an ordinary ALTER on a table
  that still exists, and goes down the normal schema path.
  """
  def trigger_table_drop(table \\ "chaos_ephemeral", pause_ms \\ 3_000) do
    publication = System.get_env("BRIDGE_CDC_PUBLICATION", "my_pub")
    bridge_user = System.get_env("POSTGRES_BRIDGE_USER", "bridge_reader")

    IO.puts("🧨 Creating table #{table}...")

    Ecto.Adapters.SQL.query!(Repo, """
    CREATE TABLE IF NOT EXISTS public.#{table} (
      id SERIAL PRIMARY KEY,
      label TEXT,
      amount NUMERIC(20,8)
    );
    """)

    # Publication membership is what puts this table's rows into the WAL the bridge
    # reads. The DDL event itself flows regardless (it is a row in zebridge_ddl_events),
    # but without this the table's own data would never reach clients.
    Ecto.Adapters.SQL.query!(Repo, """
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM pg_publication_tables
        WHERE pubname = '#{publication}' AND tablename = '#{table}'
      ) THEN
        ALTER PUBLICATION #{publication} ADD TABLE public.#{table};
      END IF;
    END $$;
    """)

    Ecto.Adapters.SQL.query!(Repo, "GRANT SELECT ON TABLE public.#{table} TO #{bridge_user};")

    # NUMERIC here is deliberate: it checks that the sqlite flavour declares TEXT rather
    # than REAL, so the 8 decimal places survive to the client intact.
    Ecto.Adapters.SQL.query!(
      Repo,
      "INSERT INTO public.#{table} (label, amount) VALUES ('before-drop', 1.23456789);"
    )

    IO.puts("⏳ Waiting #{pause_ms}ms so clients can observe the CREATE...")
    Process.sleep(pause_ms)

    IO.puts("🧨 Dropping table #{table}...")
    Ecto.Adapters.SQL.query!(Repo, "DROP TABLE public.#{table};")

    IO.puts("✅ Drop complete! Clients should have received a tombstone for #{table}.")
  end

  @doc """
  Inspect what the DDL triggers actually recorded — useful when a client misbehaves and
  you need to know whether the bridge or the client is at fault.
  """
  def ddl_events do
    %{rows: rows} =
      Ecto.Adapters.SQL.query!(Repo, """
      SELECT id, table_name, command_tag,
             COALESCE(jsonb_array_length(schema_def->'columns'), 0) AS ncols,
             emitted_at
      FROM public.zebridge_ddl_events
      ORDER BY id;
      """)

    Enum.each(rows, fn [id, tbl, tag, ncols, at] ->
      IO.puts(
        "  #{id}  #{String.pad_trailing(tbl, 20)} #{String.pad_trailing(tag, 14)} cols=#{ncols}  #{at}"
      )
    end)

    length(rows)
  end

  defp execute_clean_rows do
    Ecto.Adapters.SQL.query!(Repo, "DELETE FROM users WHERE email = 'chaos@test.com'")
  end
end
