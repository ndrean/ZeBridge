defmodule Emitter.PgProducer.Repo.AddCounters do
  use Ecto.Migration

  # Two counters, deliberately as small as a writable table can be: no tombstone, no
  # composite key, one integer column that only ever changes by +1/-1. One public (every
  # principal shares the same row), one tenant-scoped (RLS-filtered, one row per tenant).
  # `zebridge_enable()` handles guards/RLS/publication for both in one call each — see its
  # own comment in `20260810130000_add_tenant_last_writer.exs` for what it composes.
  def up do
    create table(:counter_public, primary_key: false) do
      add(:uid, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:value, :integer, null: false, default: 0)
      add(:last_writer, :string)
      timestamps(type: :timestamptz)
    end

    create table(:counter_tenant, primary_key: false) do
      add(:uid, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:value, :integer, null: false, default: 0)
      add(:tenant_id, :string, null: false)
      add(:last_writer, :string)
      timestamps(type: :timestamptz)
    end

    flush()

    execute("""
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'zebridge_enable') THEN
            PERFORM * FROM public.zebridge_enable(
                'public.counter_public'::regclass,
                writable => true,
                version_col => 'updated_at',
                public_reason => 'demo counter — identical content for every tenant',
                publication => '#{zb_publication()}',
                dry_run => false
            );
            PERFORM * FROM public.zebridge_enable(
                'public.counter_tenant'::regclass,
                tenant_col => 'tenant_id',
                writable => true,
                version_col => 'updated_at',
                publication => '#{zb_publication()}',
                dry_run => false
            );
        END IF;
    END $$;
    """)
  end

  def down do
    drop_if_exists(table(:counter_public))
    drop_if_exists(table(:counter_tenant))
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

end
