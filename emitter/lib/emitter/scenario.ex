defmodule Emitter.Scenario do
  @moduledoc """
  The suspension lifecycle, driven one step at a time.

  Migrations here run **on demand**, not `all: true`, because the whole point is to
  watch the bridge and the web-consumer react between steps. `Ecto.Migrator.up/4` runs
  one specific migration by version and module, so the order below is the order you
  call them in — it does not depend on filename timestamps.

  Run each step from `emitter/`:

      mix run --no-start -e 'Emitter.Scenario.step1()'

  Keep the web-consumer open at http://localhost:5173 and the bridge log visible. What
  to expect at each step is in `TEST_SCENARIOS.md` group F.
  """

  alias Emitter.Producer.Repo

  @migrations_path Path.join(["priv", "repo", "migrations"])

  @doc "Step 1 — a keyless table is refused before it ever replicates."
  def step1, do: migrate(20_260_810_140_000, Emitter.PgProducer.Repo.SetupNoPkTable)

  @doc "Step 2 — well-formed tables (`users`, `test_types`) replicate normally."
  def step2, do: migrate(20_260_810_120_000, Emitter.PgProducer.Repo.SetupCdcTables)

  @doc """
  Step 3 — produce a little traffic on a healthy table.

  Small on purpose: you are watching individual rows land in the consumer, not
  benchmarking. Step 5 also de-duplicates `(name, email)`, so a modest number here
  keeps that visible rather than noisy.
  """
  def step3(batches \\ 3, rows_per_batch \\ 5) do
    # `mix run --no-start` does not boot the app, so the repo has to be started
    # explicitly around any query — including the inserts, not just the migrations.
    with_repo(fn _repo ->
      for i <- 1..batches, do: Produce.bulk_insert_in(rows_per_batch, i, "users")
    end)

    count("after step3")
  end

  @doc "Step 4 — the wrong migration: drop the PK on a live table. `users` is refused."
  def step4, do: migrate(20_260_810_150_000, Emitter.PgProducer.Repo.UsersDropPk)

  @doc """
  Step 4b — prove the events are dropped, not delayed.

  These INSERTs succeed in PostgreSQL and reach the WAL. The bridge drops them, so the
  consumer's row count must not move, and `/status` `refused_events_dropped` must climb
  by exactly the number of rows inserted here.
  """
  def step4b(batches \\ 2, rows_per_batch \\ 5) do
    # `mix run --no-start` does not boot the app, so the repo has to be started
    # explicitly around any query — including the inserts, not just the migrations.
    with_repo(fn _repo ->
      for i <- 100..(99 + batches), do: Produce.bulk_insert_in(rows_per_batch, i, "users")
    end)

    count("after step4b — PostgreSQL has these rows, the consumer must not")
  end

  @doc "Step 5 — fix with a COMPOSITE key on (name, email). Must resume, not be refused."
  def step5, do: migrate(20_260_810_160_000, Emitter.PgProducer.Repo.UsersCompositePk)

  @doc "Step 6 — traffic again. CDC flows once more, now keyed on (name, email)."
  def step6(batches \\ 2, rows_per_batch \\ 5) do
    # `mix run --no-start` does not boot the app, so the repo has to be started
    # explicitly around any query — including the inserts, not just the migrations.
    with_repo(fn _repo ->
      for i <- 200..(199 + batches), do: Produce.bulk_insert_in(rows_per_batch, i, "users")
    end)

    count("after step6")
  end

  @doc """
  Reset to nothing: drops every table this scenario touches and clears the migration
  log, so `step1` starts from a clean slate.

  Does not touch the publication itself — the bridge owns that — but a dropped table
  leaves the publication automatically.
  """
  def reset do
    with_repo(fn repo ->
      # The t_* tables are hand-made fixtures from earlier manual testing, not
      # migrations. Dropped too, so the scenario starts against a clean publication.
      for t <- ~w(users test_types users_no_pk t_nopk t_single t_composite) do
        Ecto.Adapters.SQL.query!(repo, "DROP TABLE IF EXISTS public.#{t} CASCADE;")
      end

      # May not exist yet on a truly fresh database — reset must be safe to call first.
      Ecto.Adapters.SQL.query(repo, "DELETE FROM schema_migrations;")
      IO.puts("🧹 reset: tables dropped, migration log cleared")
    end)
  end

  @doc "Row counts for the tables under observation, to compare against the consumer."
  def count(label \\ "count") do
    with_repo(fn repo ->
      for t <- ~w(users users_no_pk) do
        case Ecto.Adapters.SQL.query(repo, "SELECT count(*) FROM public.#{t};") do
          {:ok, %{rows: [[n]]}} -> IO.puts("📊 #{label}: #{t} = #{n}")
          {:error, _} -> IO.puts("📊 #{label}: #{t} = (does not exist)")
        end
      end
    end)
  end

  # Runs exactly one migration, by version. Unlike `Ecto.Migrator.run/4` with `:all` or
  # `:to`, this never drags earlier pending migrations along with it — which is what
  # lets step1 run before step2 despite its later timestamp.
  #
  # Migration files live under `priv/` and are not compiled into the app, so the module
  # has to be loaded from disk before `Ecto.Migrator.up/4` can see it. The module name
  # comes back from the compile rather than being passed in, so a renamed module cannot
  # silently disagree with the caller.
  defp migrate(version, expected_module) do
    dir = Path.join([File.cwd!(), "priv", "repo", "migrations"])

    file =
      dir
      |> File.ls!()
      |> Enum.find(&String.starts_with?(&1, to_string(version)))
      |> case do
        nil -> raise "no migration file for version #{version} in #{dir}"
        name -> Path.join(dir, name)
      end

    module =
      case Code.compile_file(file) do
        [{mod, _bin} | _] -> mod
        [] -> raise "#{file} defined no module"
      end

    if module != expected_module do
      raise "#{file} defines #{inspect(module)}, expected #{inspect(expected_module)}"
    end

    with_repo(fn repo ->
      case Ecto.Migrator.up(repo, version, module, log: :info) do
        :ok -> IO.puts("✅ applied #{inspect(module)} (#{version})")
        :already_up -> IO.puts("↩️  #{inspect(module)} already applied — skipping")
      end
    end)
  end

  defp with_repo(fun) do
    {:ok, result, _} = Ecto.Migrator.with_repo(Repo, fun)
    result
  end

  @doc false
  def migrations_path, do: @migrations_path
end
