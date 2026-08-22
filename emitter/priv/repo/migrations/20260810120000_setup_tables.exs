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
      # tombstone from `SYNC_RULES=test_types:updated_at,deleted_at`, not from the schema.
      # Without that line this is an ordinary nullable timestamp and deletes stay physical.
      add(:deleted_at, :timestamptz)

      timestamps(type: :timestamptz)
    end

    flush()

    # `users` — genuinely public, no tenant column, ever. `zebridge_enable()` (defined in
    # init.core.template.sql — the READ half, no write profile needed for this call) does
    # in one statement what this migration used to hand-roll in four: sets
    # `REPLICA IDENTITY`, records the table in `zebridge_public_tables` with a reason,
    # and — only once that scoping decision exists, which its own preflight enforces —
    # adds it to the publication. `writable` defaults to false, so nothing here touches
    # the write role at all.
    #
    # Guarded on the function existing: a database whose SQL predates this refactor (or
    # was bootstrapped from an older template) skips this rather than failing the whole
    # migration on something it does not have yet.
    execute("""
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'zebridge_enable') THEN
            PERFORM * FROM public.zebridge_enable(
                'public.users'::regclass,
                public_reason => 'CDC fixture: no tenant column, readable by every consumer',
                dry_run => false
            );
        END IF;
    END $$;
    """)

    # `test_types` — writable, but NOT yet tenant-scoped: `tenant_id` does not exist until
    # `AddTenantLastWriter` (the next migration) adds it. `zebridge_enable()` refuses to
    # publish an unscoped table by design (its own preflight), so calling it here with no
    # `tenant_col` and no `public_reason` would just error out rather than doing a partial
    # job. This stage is deliberately partial instead: grants + guards only, no publish.
    # The next migration finishes the job — tenant scoping and the first-ever publish,
    # via the same `zebridge_enable()`, once the column it needs actually exists.
    #
    # Publishing early would buy nothing anyway: `.env.bridge` already declares
    # `TENANT_RULES=test_types:tenant_id`, so a bridge that saw this table published
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
end
