defmodule Emitter.PgProducer.Repo.SetupCdcTables do
  use Ecto.Migration

  # The two fixtures are a deliberate contrast, and every scenario reads better for it:
  #
  #   users      — bigserial PK, no tombstone, no write grant. **Outbound only.** A client
  #                cannot mint a `bigserial` key (an explicit id does not advance the
  #                sequence, so every edge-written key is a future duplicate — §7.2), and
  #                nothing grants `bridge_writer` on it. This is what the majority of a
  #                real schema looks like, and it replicates perfectly.
  #
  #   test_types — uuid PK the client mints with `uuidv7()`, `deleted_at` tombstone,
  #                `updated_at` version, write grant. **Bidirectional.**
  #
  # ⚠️ `timestamps(type: :timestamptz)`, not `:utc_datetime_usec`. The latter *says* UTC
  # and produces `timestamp without time zone` — a promise Ecto keeps in Elixir, on a
  # column that records no zone. The bridge applies edge writes with raw SQL and never
  # passes through Ecto, so that promise does not reach the write path: a client sending
  # local time is stored as-is and compared against another client's UTC value, and the
  # wrong write wins silently. Same 8 bytes, same microsecond precision, and Ecto still
  # reads it back as a `DateTime`.
  #
  # The difference is visible in the published schema descriptor: `pk` is `id`/INTEGER
  # versus `uid`/TEXT, and `tombstone_column` is null versus `deleted_at`.
  def up do
    create_if_not_exists table(:users) do
      add(:name, :string, null: false)
      add(:email, :string)
      timestamps(type: :timestamptz)
    end

    create_if_not_exists table(:test_types, primary_key: false) do
      add(:uid, :uuid, primary_key: true, default: fragment("gen_random_uuid()"))
      add(:age, :integer)
      add(:temperature, :float)
      add(:price, :decimal, precision: 20, scale: 8)
      add(:is_true, :boolean)
      add(:some_text, :text)
      add(:tags, {:array, :string})
      add(:matrix, {:array, {:array, :integer}})
      add(:metadata, :map)

      # Tombstone. A delete from the edge sets this instead of removing the row, so an
      # offline client's queued edit is overruled rather than resurrecting it (§7.5).
      # Nullable by definition — a set value *is* the tombstone.
      #
      # ⚠️ Declaring the column is only half of it: the bridge learns which column is the
      # tombstone from the table's `zebridge_catalogue` row (written by `zebridge_enable`
      # in the NEXT migration), not from the schema. Without it this is an ordinary
      # nullable timestamp and deletes stay physical.
      add(:deleted_at, :timestamptz)

      timestamps(type: :timestamptz)
    end

    flush()

    # `users` — genuinely public, no tenant column, ever. `zebridge_enable()` (defined in
    # init.core.template.sql — the READ half, no write profile needed for this call) does
    # in one statement what this migration used to hand-roll in four: sets
    # `REPLICA IDENTITY`, records the table in `zebridge_catalogue` with a `public_reason`,
    # and — only once that scoping decision exists, which its own preflight enforces —
    # adds it to the publication. `writable` defaults to false, so nothing here touches
    # the write role at all.
    #
    # Guarded on the function existing: a database whose SQL predates this refactor (or
    # was bootstrapped from an older template) skips this rather than failing the whole
    # migration on something it does not have yet.
    execute(zb_enable("""
    'public.users'::regclass,
    public_reason => 'CDC fixture: no tenant column, readable by every consumer',
    publication => '#{zb_publication()}',
    dry_run => false
    """))

    # `test_types` — writable, but NOT yet tenant-scoped: `tenant_id` does not exist until
    # `AddTenantLastWriter` (the next migration) adds it. `zebridge_enable()` refuses to
    # publish an unscoped table by design (its own preflight), so calling it here with no
    # `tenant_col` and no `public_reason` would just error out rather than doing a partial
    # job. This stage is deliberately partial instead: grants + guards only, no publish.
    # The next migration finishes the job — tenant scoping and the first-ever publish,
    # via the same `zebridge_enable()`, once the column it needs actually exists.
    #
    # Publishing early would buy nothing anyway: the catalogue will declare
    # `tenant_col = tenant_id` for it, so a bridge that saw this table published
    # before the column exists would just suspend it (`no_tenant_column`) the moment it
    # tried to preflight it.
    execute("""
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'zebridge_grant_edge_writes') THEN
            PERFORM public.zebridge_grant_edge_writes('public.test_types'::regclass);
            PERFORM public.zebridge_install_write_guards(
                'public.test_types'::regclass, 'updated_at', 'deleted_at'
            );
        END IF;
    END $$;
    """)
  end

  def down do
    drop_if_exists(table(:users))
    drop_if_exists(table(:test_types))
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
