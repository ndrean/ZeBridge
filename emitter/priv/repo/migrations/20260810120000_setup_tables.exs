defmodule Emitter.PgProducer.Repo.SetupCdcTables do
  use Ecto.Migration

  @publication_name System.get_env("BRIDGE_CDC_PUBLICATION", "my_pub")
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
  @cdc_tables ["users", "test_types"]
  @bridge_user System.get_env("POSTGRES_BRIDGE_USER", "bridge_reader")
  @bridge_writer System.get_env("POSTGRES_BRIDGE_WRITER", "bridge_writer")

  # Tables that accept writes from the edge. Reading needs only the SELECT granted to
  # every CDC table below; writing needs an explicit grant, because `bridge_writer` is
  # created with **no table privileges at all** — ingress is closed until a DBA opens a
  # table, so that a new table is never silently writable from an untrusted client.
  #
  # ⚠️ Without this the failure is silent, not loud: the bridge classifies
  # `permission denied` as transient (it cannot distinguish it from a dropped connection),
  # so a well-formed mutation is retried to `max_deliver` and the client sees no error, no
  # row, and no CDC echo. See PROTOCOL.md §7.2, "Two failures that look like nothing
  # happening".
  @writable_tables ["test_types"]

  def up do
    create table(:users) do
      add(:name, :string, null: false)
      add(:email, :string)
      timestamps(type: :timestamptz)
    end

    create table(:test_types, primary_key: false) do
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

    # Set REPLICA IDENTITY DEFAULT and attach to Publication
    for table <- @cdc_tables do
      execute("ALTER TABLE public.#{table} REPLICA IDENTITY DEFAULT;")

      # ⚠️ Recorded as deliberately unscoped BEFORE publishing. `zebridge_publication_guard`
      # refuses `ALTER PUBLICATION ... ADD TABLE` for a table that is neither tenant-scoped
      # nor listed here — so "everyone reads this" has to be stated, not assumed.
      #
      # These two fixtures genuinely are public: they carry no tenant column and exist to
      # exercise CDC, not to model a real schema. A multi-tenant table would instead be
      # opened with zebridge_publish_tenant_table/4, which creates the policies and the row
      # filter first and then publishes.
      execute("""
      DO $$
      BEGIN
          IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'zebridge_public_tables') THEN
              INSERT INTO public.zebridge_public_tables (tbl, reason)
              VALUES ('public.#{table}'::regclass,
                      'CDC fixture: no tenant column, readable by every consumer')
              ON CONFLICT (tbl) DO NOTHING;
          END IF;
      END $$;
      """)

      execute("""
      DO $$
      BEGIN
          IF NOT EXISTS (
              SELECT 1 FROM pg_publication_tables
              WHERE pubname = '#{@publication_name}' AND tablename = '#{table}'
          ) THEN
              ALTER PUBLICATION #{@publication_name} ADD TABLE public.#{table};
          END IF;
      END $$;
      """)

      execute("GRANT SELECT ON TABLE public.#{table} TO #{@bridge_user};")
    end

    # Uses `zebridge_grant_edge_writes/1` rather than spelling the GRANT out, so the
    # privilege set has exactly one definition. It also refuses `zebridge_ddl_events` by
    # name — the one table where "can write my own rows" becomes "can publish a
    # fabricated schema to every client" — which a hand-written GRANT here would not.
    #
    # Guarded on the function existing: it comes from `init.sql.template`, not from Ecto,
    # so a database brought up without that bootstrap should skip the grant rather than
    # fail the whole migration on something it does not need.
    for table <- @writable_tables do
      execute("""
      DO $$
      BEGIN
          IF EXISTS (
              SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
              WHERE p.proname = 'zebridge_grant_edge_writes' AND n.nspname = 'public'
          ) THEN
              PERFORM public.zebridge_grant_edge_writes('public.#{table}'::regclass);
          END IF;
      END $$;
      """)
    end
  end

  def down do
    drop_if_exists(table(:users))
    drop_if_exists(table(:test_types))
  end
end
