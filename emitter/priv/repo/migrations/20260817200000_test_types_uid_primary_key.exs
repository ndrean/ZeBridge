defmodule Emitter.PgProducer.Repo.TestTypesUidPrimaryKey do
  use Ecto.Migration

  @moduledoc """
  Promote `uid` to the primary key on databases that predate the fix in
  `20260810120000_setup_tables.exs`.

  That migration originally created `test_types` without `primary_key: false`, so Ecto
  added an implicit `id bigserial primary key` and `uid` became a spare column. A
  `bigserial` key cannot be minted by a client — an explicit id does not advance the
  sequence, so every edge-written key is a future duplicate-key error (PROTOCOL.md §7.2).

  The source migration has since been corrected, but editing an applied migration does not
  touch a database that already ran it. So this exists for the databases in between, and
  is a **no-op on a freshly reset one** — hence the `IF EXISTS` guard rather than a bare
  `ALTER`. Without it, `mix ecto.reset` would fail here trying to drop a column the
  corrected migration never created.
  """

  def up do
    execute("""
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'test_types' AND column_name = 'id'
      ) THEN
        UPDATE public.test_types SET uid = gen_random_uuid() WHERE uid IS NULL;
        ALTER TABLE public.test_types ALTER COLUMN uid SET NOT NULL;
        ALTER TABLE public.test_types DROP CONSTRAINT test_types_pkey;
        ALTER TABLE public.test_types ADD PRIMARY KEY (uid);
        ALTER TABLE public.test_types DROP COLUMN id;
      END IF;
    END $$;
    """)
  end

  def down do
    execute("""
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'test_types' AND column_name = 'id'
      ) THEN
        ALTER TABLE public.test_types ADD COLUMN id bigserial;
        ALTER TABLE public.test_types DROP CONSTRAINT test_types_pkey;
        ALTER TABLE public.test_types ADD PRIMARY KEY (id);
        ALTER TABLE public.test_types ALTER COLUMN uid DROP NOT NULL;
      END IF;
    END $$;
    """)
  end
end
