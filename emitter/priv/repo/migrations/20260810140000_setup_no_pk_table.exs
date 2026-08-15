defmodule Emitter.PgProducer.Repo.SetupNoPkTable do
  use Ecto.Migration

  @publication_name System.get_env("BRIDGE_CDC_PUBLICATION", "my_pub")
  @cdc_table "users_no_pk"
  @publication_name System.get_env("BRIDGE_CDC_PUBLICATION", "my_pub")
  @bridge_user System.get_env("POSTGRES_BRIDGE_USER", "bridge_reader")

  def up do
    create table(:users_no_pk, primary_key: false) do
      add(:name, :string, null: false)
      add(:email, :string)
      timestamps(type: :utc_datetime_usec)
    end

    flush()

    # Set REPLICA IDENTITY DEFAULT and attach to Publication
    execute("ALTER TABLE public.#{@cdc_table} REPLICA IDENTITY DEFAULT;")

    execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_publication_tables
            WHERE pubname = '#{@publication_name}' AND tablename = '#{@cdc_table}'
        ) THEN
            ALTER PUBLICATION #{@publication_name} ADD TABLE public.#{@cdc_table};
        END IF;
    END $$;
    """)

    execute("GRANT SELECT ON TABLE public.#{@cdc_table} TO #{@bridge_user};")
  end

  def down do
    drop_if_exists(table(:users_no_pk))
  end
end
