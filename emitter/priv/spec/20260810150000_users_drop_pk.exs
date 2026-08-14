# defmodule Emitter.PgProducer.Repo.UsersDropPk do
#   use Ecto.Migration

#   @moduledoc """
#   Scenario step 4: the "wrong" migration.

#   Drops the primary key from an already-replicating table with live consumers. The
#   `id` column and its sequence stay — only the constraint goes — which is exactly the
#   shape of a real mistake: nothing looks broken in PostgreSQL, INSERTs keep working,
#   and the table quietly becomes unreplicable.

#   Expected effect: the bridge's DDL path refuses `users`, overwrites its KV schema
#   with a suspension, and drops every subsequent CDC event for it. The web-consumer
#   keeps the rows it already has, shows the suspension banner, and stops updating.
#   """

#   def up do
#     # The constraint name is what Ecto/Postgres generated in the create table above.
#     execute("ALTER TABLE public.users DROP CONSTRAINT users_pkey;")
#   end

#   def down do
#     execute("ALTER TABLE public.users ADD CONSTRAINT users_pkey PRIMARY KEY (id);")
#   end
# end
