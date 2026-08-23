defmodule Emitter.PgProducer.Repo.TimestamptzVersionColumns do
  use Ecto.Migration

  @moduledoc """
  Move the version and tombstone columns to `timestamptz`.

  `timestamps(type: :utc_datetime_usec)` produces `timestamp without time zone`: Ecto
  coerces to UTC in Elixir, but the column records no zone, and the bridge applies edge
  writes with raw SQL that never passes through Ecto. So two clients in different zones
  can store the same digits for instants hours apart — and those values compare as equal,
  which last-write-wins rejects as a tie (PROTOCOL.md §7.3).

  `USING ... AT TIME ZONE 'UTC'` reads the existing values as UTC, which is what they are:
  everything written so far came through Ecto, which coerced them.

  For databases created before the source migration was corrected. A fresh `ecto.reset`
  produces these types directly, and this is then a no-op.
  """

  def up do
    for {table, cols} <- [{"users", ~w(inserted_at updated_at)},
                          {"test_types", ~w(inserted_at updated_at deleted_at)}],
        col <- cols do
      execute("""
      DO $$
      BEGIN
        IF EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema = 'public' AND table_name = '#{table}'
            AND column_name = '#{col}' AND data_type = 'timestamp without time zone'
        ) THEN
          ALTER TABLE public.#{table}
            ALTER COLUMN #{col} TYPE timestamptz USING #{col} AT TIME ZONE 'UTC';
        END IF;
      END $$;
      """)
    end
  end

  def down do
    for {table, cols} <- [{"users", ~w(inserted_at updated_at)},
                          {"test_types", ~w(inserted_at updated_at deleted_at)}],
        col <- cols do
      execute("ALTER TABLE public.#{table} ALTER COLUMN #{col} TYPE timestamp USING #{col} AT TIME ZONE 'UTC';")
    end
  end
end
