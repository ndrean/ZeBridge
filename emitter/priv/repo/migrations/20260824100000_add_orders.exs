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

    execute("""
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'zebridge_enable') THEN
            PERFORM * FROM public.zebridge_enable(
                'public.orders'::regclass,
                public_reason => 'FK-ordering demo: child of users, PROTOCOL.md §4',
                dry_run => false
            );
        END IF;
    END $$;
    """)
  end

  def down do
    drop_if_exists(table(:orders))
  end
end
