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
    execute("""
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'zebridge_enable') THEN
            PERFORM * FROM public.zebridge_enable(
                'public.demo_key_migration'::regclass,
                public_reason => 'CDC fixture: no-PK-refusal-then-composite-key-fix demo',
                dry_run => false
            );
        END IF;
    END $$;
    """)

    # The constraint name is what Ecto/Postgres generated in the create table above.
    execute("ALTER TABLE public.demo_key_migration DROP CONSTRAINT demo_key_migration_pkey;")
  end

  def down do
    execute(
      "ALTER TABLE public.demo_key_migration ADD CONSTRAINT demo_key_migration_pkey PRIMARY KEY (id);"
    )
  end
end
