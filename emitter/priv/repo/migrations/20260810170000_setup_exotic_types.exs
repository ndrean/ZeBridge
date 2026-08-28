defmodule Emitter.PgProducer.Repo.SetupExoticTypes do
  use Ecto.Migration

  @moduledoc """
  A table of types whose binary COPY representation the bridge does not have a
  compile-time OID for.

  Deliberately **separate from `test_types`**: if decoding fails here it must not take
  the fixture every other scenario depends on down with it. The point of this table is
  to fail, or to prove it does not.

  Three categories, and they are not equivalent:

  - `mood` (ENUM) — a user-defined type, so its OID is assigned per database and can
    never be a constant in our switch. Postgres sends enums as their **text label** in
    binary, so falling through to text is correct here.
  - `attrs` (HSTORE) — an extension type, also dynamically OID'd, but its binary form is
    a length-prefixed count followed by length-prefixed key/value pairs. Treating those
    bytes as text yields a string full of embedded lengths and NULs. This is the one
    expected to expose a silent-corruption path.
  - `price` (NUMERIC(20,8)) and `tags` (TEXT[]) — both have fixed OIDs and are supported;
    they are here as the control, and `price` pins the trailing-zero padding.
  """

  @bridge_user System.get_env("POSTGRES_BRIDGE_USER", "bridge_reader")
  @cdc_table "exotic_types"

  def up do
    execute("CREATE EXTENSION IF NOT EXISTS hstore;")

    # PostgreSQL has no `CREATE TYPE IF NOT EXISTS` — unlike CREATE EXTENSION or
    # CREATE TABLE, which is why only this statement needs the guard. Without it the
    # migration is a one-shot: a type outlives `DROP TABLE`, so any re-run after a
    # partial reset dies with `42710 duplicate_object`, and Ecto rolls the whole
    # migration back leaving nothing created.
    #
    # Same DO-block shape as the publication guard below, for the same reason.
    execute("""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_type
            WHERE typname = 'mood' AND typnamespace = 'public'::regnamespace
        ) THEN
            CREATE TYPE public.mood AS ENUM ('sad', 'ok', 'happy');
        END IF;
    END $$;
    """)

    execute("""
    CREATE TABLE public.#{@cdc_table} (
      id          bigserial PRIMARY KEY,
      feeling     mood NOT NULL,
      attrs       hstore,
      price       numeric(20,8),
      tags        text[],
      inserted_at timestamptz NOT NULL DEFAULT now()
    );
    """)

    execute("ALTER TABLE public.#{@cdc_table} REPLICA IDENTITY DEFAULT;")

    # Same publication guard as `users_no_pk` — this fixture has no tenant to scope it
    # by either, so the decision to publish it wide-open is recorded explicitly.
    execute("""
    INSERT INTO zebridge_catalogue (tbl, public_reason)
    VALUES ('#{@cdc_table}', 'decode-coverage fixture for exotic PG types, not real business data')
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
    execute("DROP TABLE IF EXISTS public.#{@cdc_table};")
    execute("DROP TYPE IF EXISTS mood;")
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
