defmodule Emitter.PgProducer.Repo.SetupCdcTables do
  use Ecto.Migration

  @publication_name System.get_env("BRIDGE_CDC_PUBLICATION", "my_pub")
  @cdc_tables ["users", "test_types"]
  @publication_name System.get_env("BRIDGE_CDC_PUBLICATION", "my_pub")
  @bridge_user System.get_env("POSTGRES_BRIDGE_USER", "bridge_reader")

  def up do
    create table(:users) do
      add(:name, :string, null: false)
      add(:email, :string)
      timestamps(type: :utc_datetime_usec)
    end

    create table(:test_types) do
      add(:uid, :uuid, default: fragment("gen_random_uuid()"))
      add(:age, :integer)
      add(:temperature, :float)
      add(:price, :decimal, precision: 20, scale: 8)
      add(:is_true, :boolean)
      add(:some_text, :text)
      add(:tags, {:array, :string})
      add(:matrix, {:array, {:array, :integer}})
      add(:metadata, :map)
      timestamps(type: :utc_datetime_usec)
    end

    flush()

    # Set REPLICA IDENTITY DEFAULT and attach to Publication
    for table <- @cdc_tables do
      execute("ALTER TABLE public.#{table} REPLICA IDENTITY DEFAULT;")

      execute("""
      DO $$
      BEGIN
          IF NOT EXISTS (
              SELECT 1 FROM pg_publication_tables
              WHERE pubname = '#{@publication_name}' AND tablename = '#{table}'
          ) THEN
              ALTER PUBLICATION #{@publication_name} ADD TABLE public.#{table};
          END IF;
      END $$;
      """)

      execute("GRANT SELECT ON TABLE public.#{table} TO #{@bridge_user};")
    end
  end

  def down do
    drop_if_exists(table(:users))
    drop_if_exists(table(:test_types))
  end
end
