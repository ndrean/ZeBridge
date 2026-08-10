defmodule Produce do
  alias Emitter.Producer.Repo

  def init_tables do
    create_table_users = """
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      inserted_at TIMESTAMPTZ DEFAULT now(),
      updated_at TIMESTAMPTZ DEFAULT now()
    );
    """

    create_table_test_types = """
    CREATE TABLE IF NOT EXISTS test_types (
      id SERIAL PRIMARY KEY,
      uid UUID DEFAULT gen_random_uuid(),
      age INT,
      temperature FLOAT8,
      price NUMERIC(20,8),
      is_true BOOLEAN,
      some_text TEXT,
      tags TEXT[],
      matrix INT[][],
      metadata JSONB,
      created_at TIMESTAMPTZ DEFAULT now()
    );
    """

    Ecto.Adapters.SQL.query(Emitter.Producer.Repo, create_table_users)
    Ecto.Adapters.SQL.query(Emitter.Producer.Repo, create_table_test_types)
  end

  def check_table(name \\ "users") do
    case name do
      "users" ->
        Repo.all(User)

      "test_types" ->
        Repo.all(TestType)
    end
  end

  def bulk_insert_in(nb, i \\ 1, name \\ "users") do
    values =
      for idx <- 1..nb do
        %{
          name: "User-#{i}-#{idx}",
          email: "user-#{i}-#{idx}@example.com"
        }
      end

    Repo.insert_all(name, values)
  end

  # def crud() do
  #   GenServer.call(__MODULE__, :crud)
  # end

  # def stream(take, nb, int \\ 1) do
  #   Task.start(fn ->
  #     Stream.interval(int)
  #     |> Stream.take(take)
  #     |> Task.async_stream(
  #       fn _ -> PgProducer.bulk_insert_in(nb, "users") end,
  #       ordered: false
  #     )
  #     |> Stream.run()
  #   end)
  # end

  # def snap_request(pid) do
  #   Postgrex.query!(pid, """
  #   INSERT INTO snapshot_requests (table_name, requested_by) VALUES ('users', 'elixir-consumer');
  #   """)
  # end

  defp crud_test(pid) do
    %Postgrex.Result{command: :insert, rows: [[inserted_id]]} =
      Postgrex.query!(
        pid,
        """
        INSERT INTO test_types (age, temperature, is_true, some_text, tags, matrix, metadata, price)
        VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
        RETURNING id;
        """,
        [
          30,
          36.6,
          true,
          "Sample text",
          ["tag1", "tag2"],
          [[1, 2], [3, 4]],
          Jason.encode!(%{
            "key_1" => "value_1",
            "key_2" => [[1, 2], [3, 4], [5, 6]],
            "key_3" => %{"key_4" => "value_4", "key_5" => "value_5"}
          }),
          Decimal.new("123.45")
        ]
      )

    %Postgrex.Result{command: :select, rows: [[^inserted_id | _]]} =
      Postgrex.query!(pid, "SELECT * FROM test_types WHERE id = $1", [inserted_id])

    %Postgrex.Result{command: :update, rows: [[^inserted_id | _]]} =
      Postgrex.query!(
        pid,
        """
        UPDATE test_types
           SET age = $1,
               temperature = $2,
               is_true = $3,
               price = $4
         WHERE id = $5
        RETURNING id;
        """,
        [31, 37.0, false, Decimal.new("122.9905"), inserted_id]
      )

    %Postgrex.Result{command: :delete} =
      Postgrex.query!(pid, "DELETE FROM test_types WHERE id = $1", [inserted_id])
  end
end

# Stream.interval(1) |> Stream.take(1_000) |> Task.async_stream(fn _ -> PgProducer.run(100, Enum.random(["users", "orders"])) end, ordered: false) |> Stream.run()
# Stream.interval(1) |> Stream.take(10_000) |> Task.async_stream(fn _ -> PgProducer.run(140, "users") end, ordered: false) |> Stream.run()
