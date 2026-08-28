defmodule Emitter.PgProducer.Repo.UsersDropPk do
  use Ecto.Migration

  @moduledoc """
  Scenario step 4: the "wrong" migration.

  Runs against `demo_key_migration`, a dedicated fixture — not `users`. It used to
  target `users` directly, which meant this demo's own fix (step 5, composite key)
  permanently overwrote `users`'s other documented role as the plain bigserial
  "majority of a real schema" fixture (`20260810120000_setup_tables.exs`'s own
  comment) — the two purposes conflicted the moment both migrations had run, which
  is exactly why `keys.py` (needs a sequence-backed PK) and a few other scenario
  scripts assuming `users` still had one started failing. Redirected so the demo
  narrative and the plain fixture can both exist at once.

  Creates the table with the same shape `users` had (bigserial PK, name/email), then
  drops the primary key from an already-replicating table with live consumers. The
  `id` column and its sequence stay — only the constraint goes — which is exactly the
  shape of a real mistake: nothing looks broken in PostgreSQL, INSERTs keep working,
  and the table quietly becomes unreplicable.

  Expected effect: the bridge's DDL path refuses `demo_key_migration`, overwrites its
  KV schema with a suspension, and drops every subsequent CDC event for it. A
  connected client keeps the rows it already has, shows the suspension banner, and
  stops updating.
  """

  def up do
    create_if_not_exists table(:demo_key_migration) do
      add(:name, :string, null: false)
      add(:email, :string)
      timestamps(type: :timestamptz)
    end

    flush()

    # Same reasoning as `users` in the base migration: genuinely public, no tenant
    # column, ever.
    execute(zb_enable("""
    'public.demo_key_migration'::regclass,
    public_reason => 'CDC fixture: no-PK-refusal-then-composite-key-fix demo',
    publication => '#{zb_publication()}',
    dry_run => false
    """))

    # The constraint name is what Ecto/Postgres generated in the create table above.
    execute("ALTER TABLE public.demo_key_migration DROP CONSTRAINT demo_key_migration_pkey;")
  end

  def down do
    execute(
      "ALTER TABLE public.demo_key_migration ADD CONSTRAINT demo_key_migration_pkey PRIMARY KEY (id);"
    )
  end

  # The publication is an ARGUMENT now, never a default: `zebridge_enable` has no
  # default for it (NOTES §10ad), because that name decides which tables a bridge
  # replicates. Unset stops the migration rather than publishing into a guess.
  defp zb_publication do
    System.get_env("BRIDGE_CDC_PUBLICATION") ||
      raise "BRIDGE_CDC_PUBLICATION is not set: this migration publishes a table and must " <>
              "name the publication (the same name the bridge is given as --pub). " <>
              "Source .env.bridge before running migrations."
  end


  # ⚠️ NOT `PERFORM * FROM zebridge_enable(...)`. `PERFORM` discards the result, and
  # the result is where the refusals live: zebridge_enable reports a preflight failure
  # as an ERROR ROW and returns, so `mix ecto.migrate` printed "Migrated" while the
  # table was never published. Measured on a fresh database 2026-08-28 — the tombstone
  # gate refused both counter tables and both were silently absent from the
  # publication, with nothing in the migrator output to say so.
  #
  # One evaluation of the function, both aggregates via FILTER. An ERROR aborts the
  # migration, which is safe precisely because the function returns EARLY on a
  # preflight failure — there is no half-applied state to roll back, and the
  # transaction rolls back anyway. A WARNING is surfaced rather than raised: those are
  # the width-budget and second-publication notices, and nobody was reading them
  # either.
  defp zb_enable(args) do
    """
    DO $$
    DECLARE refused text; warned text;
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'zebridge_enable') THEN
            RAISE NOTICE 'zebridge_enable() is not installed — skipping (database predates ZeBridge)';
            RETURN;
        END IF;

        SELECT string_agg(step || ': ' || detail, chr(10)) FILTER (WHERE status = 'ERROR'),
               string_agg(step || ': ' || detail, chr(10)) FILTER (WHERE status = 'WARNING')
          INTO refused, warned
          FROM public.zebridge_enable(#{args});

        IF warned IS NOT NULL THEN
            RAISE WARNING E'zebridge_enable warnings:\n%', warned;
        END IF;
        IF refused IS NOT NULL THEN
            RAISE EXCEPTION E'zebridge_enable REFUSED:\n%', refused;
        END IF;
    END $$;
    """
  end

end
