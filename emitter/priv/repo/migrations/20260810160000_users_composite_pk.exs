defmodule Emitter.PgProducer.Repo.UsersCompositePk do
  use Ecto.Migration

  @moduledoc """
  Scenario step 5: the fix, using a composite key rather than restoring `id`.

  This is the case the bridge must NOT refuse. A composite primary key identifies a
  row exactly, so CDC is fully correct, and snapshot pagination compares the whole key
  as a row value. Refusing here would reject a valid schema.

  Expected effect: the DDL event carries `pk: null` and `pk_columns: ["name","email"]`,
  the bridge lifts the refusal, publishes a live schema over the suspension, and CDC
  resumes. A snapshot of `demo_key_migration` then logs
  `'demo_key_migration': paginating on a 2-column primary key` and succeeds.
  """

  def up do
    # ⚠️ A keyless table in a publication that publishes updates rejects UPDATE and
    # DELETE outright:
    #
    #   ERROR 55000: cannot update table "demo_key_migration" because it does not have
    #   a replica identity and publishes updates
    #
    # This is exactly the `writes_rejected` finding preflight reports, hit for real. So
    # the clean-up below is impossible until replica identity is lifted. FULL makes the
    # whole row the identity, which is enough for PostgreSQL — though not enough for a
    # replica, which is why the bridge still refuses the table until the key exists.
    execute("ALTER TABLE public.demo_key_migration REPLICA IDENTITY FULL;")

    # A primary key requires NOT NULL on every column. `email` is nullable, and any
    # existing NULLs would fail the constraint, so give them a value first.
    execute("UPDATE public.demo_key_migration SET email = 'unknown-' || id::text WHERE email IS NULL;")

    # Duplicate (name, email) pairs would also fail. Keep the lowest id of each group.
    #
    # ⚠️ These DELETEs run while the table is still refused, so their CDC events are
    # dropped — a consumer that was connected keeps rows that no longer exist upstream.
    # That is the suspension contract working as designed (local data is frozen at the
    # suspension LSN, not kept in sync), and it is why a consumer should re-seed from a
    # snapshot after a suspension rather than trusting its frozen copy.
    execute("""
    DELETE FROM public.demo_key_migration a
    USING public.demo_key_migration b
    WHERE a.name = b.name
      AND a.email = b.email
      AND a.id > b.id;
    """)

    execute("ALTER TABLE public.demo_key_migration ALTER COLUMN email SET NOT NULL;")
    execute("ALTER TABLE public.demo_key_migration ADD CONSTRAINT demo_key_migration_pkey PRIMARY KEY (name, email);")

    # Back to DEFAULT now that a key exists: FULL multiplies WAL volume on every update,
    # and with a primary key present DEFAULT already carries what a replica needs.
    execute("ALTER TABLE public.demo_key_migration REPLICA IDENTITY DEFAULT;")
  end

  def down do
    execute("ALTER TABLE public.demo_key_migration DROP CONSTRAINT demo_key_migration_pkey;")
    execute("ALTER TABLE public.demo_key_migration ALTER COLUMN email DROP NOT NULL;")
  end
end
