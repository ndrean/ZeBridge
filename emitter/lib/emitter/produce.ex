defmodule Produce do
  alias Emitter.Producer.Repo

  # def init_tables do
  #   create_table_users = """
  #   CREATE TABLE IF NOT EXISTS users (
  #     id SERIAL PRIMARY KEY,
  #     name TEXT NOT NULL,
  #     email TEXT,
  #     inserted_at TIMESTAMPTZ DEFAULT now(),
  #     updated_at TIMESTAMPTZ DEFAULT now()
  #   );
  #   """

  #   create_table_test_types = """
  #   CREATE TABLE IF NOT EXISTS test_types (
  #     id SERIAL PRIMARY KEY,
  #     uid UUID DEFAULT gen_random_uuid(),
  #     age INT,
  #     temperature FLOAT8,
  #     price NUMERIC(20,8),
  #     is_true BOOLEAN,
  #     some_text TEXT,
  #     tags TEXT[],
  #     matrix INT[][],
  #     metadata JSONB,
  #     created_at TIMESTAMPTZ DEFAULT now()
  #   );
  #   """

  #   Ecto.Adapters.SQL.query(Emitter.Producer.Repo, create_table_users)
  #   Ecto.Adapters.SQL.query(Emitter.Producer.Repo, create_table_test_types)
  # end

  def check_table(name \\ "users") do
    case name do
      "users" ->
        Repo.all(User)

      "test_types" ->
        Repo.all(TestType)
    end
  end

  def bulk_insert_in(nb, i, table \\ "users") do
    now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

    values =
      case table do
        "users" ->
          for idx <- 1..nb do
            %{
              name: "User-#{i}-#{idx}",
              email: "user-#{i}-#{idx}@example.com",
              inserted_at: now,
              updated_at: now
            }
          end

        "test_types" ->
          for idx <- 1..nb do
            %{
              age: 30 + idx,
              temperature: 39.0 + idx * 0.1,
              is_true: Enum.random([true, false]),
              some_text: "bulk-#{i}-#{idx}",
              tags: ["a", "b", "c"],
              matrix: [[1, 2], [3, 4]],
              metadata: %{"batch" => i, "idx" => idx},
              price: 10.5 * idx,
              inserted_at: now,
              updated_at: now,
              tenant_id: "_default"
            }
          end
      end

    Repo.insert_all(table, values)
  end

  def stream(every, take, nb, table \\ "users") do
    # Task.start(fn ->
    Stream.interval(every)
    |> Stream.take(take)
    |> Task.async_stream(
      fn i -> bulk_insert_in(nb, i, table) end,
      ordered: false
    )
    |> Stream.run()

    # end)
  end

  def crud(nb, i) do
    for idx <- -1..nb do
      now = DateTime.utc_now() |> DateTime.truncate(:microsecond)

      value =
        %TestType{
          age: 30 + idx,
          temperature: 39.0 + idx * 0.1,
          is_true: Enum.random([true, false]),
          some_text: "bulk-#{i}-#{idx}",
          tags: ["a", "b", "c"],
          matrix: [[1, 2], [3, 4]],
          metadata: %{"batch" => i, "idx" => idx},
          price: 10.5 * idx,
          inserted_at: now,
          updated_at: now
        }

      {:ok, %TestType{} = res} = Repo.insert(value)
      new_value = %TestType{res | price: 10.5 * idx * idx}
      {:ok, %TestType{} = id} = Repo.update(new_value, returning: :id)
      Repo.delete(id)
    end
  end
end

# Stream.interval(1) |> Stream.take(1_000) |> Task.async_stream(fn _ -> PgProducer.run(100, Enum.random(["users", "orders"])) end, ordered: false) |> Stream.run()
# Stream.interval(1) |> Stream.take(1_000) |> Task.async_stream(fn _ -> PgProducer.run(100, Enum.random(["users", "orders"])) end, ordered: false) |> Stream.run()
# Stream.interval(1) |> Stream.take(10_000) |> Task.async_stream(fn _ -> PgProducer.run(140, "users") end, ordered: false) |> Stream.run()
