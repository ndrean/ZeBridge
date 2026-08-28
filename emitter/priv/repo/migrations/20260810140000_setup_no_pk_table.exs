defmodule Emitter.PgProducer.Repo.SetupNoPkTable do
  use Ecto.Migration

  @cdc_table "users_no_pk"
  @bridge_user System.get_env("POSTGRES_BRIDGE_USER", "bridge_reader")

  def up do
    create table(:users_no_pk, primary_key: false) do
      add(:name, :string, null: false)
      add(:email, :string)
      # ⚠️ :timestamptz, not :utc_datetime_usec — Ecto renders the latter as
      # `timestamp(6) WITHOUT time zone`, which zebridge_timestamp_guard refuses
      # outright. The fixture's point is the missing PRIMARY KEY, not the column
      # type; with the naive type this migration could not apply to a fresh
      # database at all (measured on the compose stack, 2026-08-28).
      timestamps(type: :timestamptz)
    end

    flush()

    # Set REPLICA IDENTITY DEFAULT and attach to Publication
    execute("ALTER TABLE public.#{@cdc_table} REPLICA IDENTITY DEFAULT;")

    # The publication guard refuses an unscoped table (no RLS, no row filter) unless the
    # decision to publish it wide-open is recorded — this table is the deliberate
    # exception: a keyless fixture for preflight/no-PK testing, not a real business
    # table, so there is no tenant to scope it by.
    execute("""
    INSERT INTO zebridge_catalogue (tbl, public_reason)
    VALUES ('#{@cdc_table}', 'keyless preflight/no-PK test fixture, not real business data')
    ON CONFLICT (tbl) DO NOTHING;
    """)

    execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_publication_tables
            WHERE pubname = '#{zb_publication()}' AND tablename = '#{@cdc_table}'
        ) THEN
            ALTER PUBLICATION #{zb_publication()} ADD TABLE public.#{@cdc_table};
        END IF;
    END $$;
    """)

    execute("GRANT SELECT ON TABLE public.#{@cdc_table} TO #{@bridge_user};")
  end

  def down do
    drop_if_exists(table(:users_no_pk))
  end
  # No default. The publication decides which feed carries this table, and
  # `zebridge_enable` / the bridge both refuse to guess one (NOTES §10ad/§10ae) —
  # a migration that fell back to "my_pub" would publish into whichever feed
  # happened to be named that on the machine it ran on.
  defp zb_publication do
    System.get_env("BRIDGE_CDC_PUBLICATION") ||
      raise "BRIDGE_CDC_PUBLICATION is not set: this migration publishes a table and must " <>
              "name the publication (the same name the bridge is given as --pub). " <>
              "Source .env.bridge before running migrations."
  end

end
