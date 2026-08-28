defmodule Emitter.PgProducer.Repo.AddOrders do
  use Ecto.Migration

  @moduledoc """
  A tiny table whose only job is to carry a real foreign key: `orders.user_id →
  users.id`. It exists to make PROTOCOL.md §4's ordering section observable — both
  tables are public, so both ride CDC_PUBLIC, and one transaction shaped like the
  section's own example (`INSERT orders` for an existing user, then `INSERT users` +
  `INSERT orders` for the new one) publishes the child lane ahead of the parent lane.
  The SQLite replica takes it in stride (no FKs declared, PK upserts); a PG replica
  with declared FKs is exactly the consumer §4's deferral rule is written for.

  `ON DELETE CASCADE` so the demo scripts' `DELETE FROM users` keeps working; the
  benchmark's `TRUNCATE users` becomes `TRUNCATE ... CASCADE` for the same reason.
  Read-only from the edge on purpose — the illustration is outbound CDC ordering,
  and a writable table would drag in SYNC_RULES for no extra teaching value.
  """

  def up do
    create table(:orders, primary_key: false) do
      add(:uid, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:user_id, references(:users, type: :bigint, on_delete: :delete_all), null: false)
      add(:label, :string, null: false)
      timestamps(type: :timestamptz)
    end

    flush()

    execute(zb_enable("""
    'public.orders'::regclass,
    public_reason => 'FK-ordering demo: child of users, PROTOCOL.md §4',
    publication => '#{zb_publication()}',
    dry_run => false
    """))
  end

  def down do
    drop_if_exists(table(:orders))
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
