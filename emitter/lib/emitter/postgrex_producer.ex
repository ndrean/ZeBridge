# defmodule PgProducer do
#   use GenServer
#   require Logger
#   alias Decimal

#   def start_link(opts) do
#     GenServer.start_link(__MODULE__, opts, name: __MODULE__)
#   end

#   def check(name \\ "users") do
#     GenServer.call(__MODULE__, {:check, name})
#   end

#   def drop(name \\ "users") do
#     GenServer.call(__MODULE__, {:drop, name})
#   end

#   # def new_table(name \\ "users") do
#   #   GenServer.call(__MODULE__, {:new_table, name})
#   # end

#   def bulk(nb, name \\ "users") do
#     GenServer.call(__MODULE__, {:bulk, nb, name})
#   end

#   def crud() do
#     GenServer.call(__MODULE__, :crud)
#   end

#   def stream(take, nb, int \\ 1) do
#     GenServer.call(__MODULE__, {:stream, take, nb, int})
#   end

#   @impl GenServer
#   def init(opts) do
#     dbg(opts)
#     tables = Keyword.get(opts, :tables, ["users"])
#     {:ok, pid} = Postgrex.start_link(opts)
#     Enum.each(tables, fn table -> create_table(table, pid) end)
#     {:ok, {pid, 0}}
#   end

#   # def handle_call({:new_table, name}, _, {pid, _} = state) do
#   #   :ok = create_table(name, pid)
#   #   {:reply, :ok, state}
#   # end

#   @impl GenServer
#   def handle_call({:bulk, nb, name}, _, {pid, i} = _state) do
#     # Generate list of maps for bulk insert

#     values =
#       for idx <- 1..nb do
#         %{
#           name: "User-#{i}-#{idx}",
#           email: "user-#{i}-#{idx}@example.com"
#         }
#       end

#     Producer.Repo.insert_all(name, values)

#     {:reply, :ok, {pid, i + 1}}
#   end

#   def handle_call({:check, name}, _, {pid, _} = state) do
#     # try do
#     query =
#       Postgrex.prepare!(pid, "", "SELECT * FROM #{name};", []) |> dbg()

#     len =
#       Postgrex.execute!(pid, query, [])
#       |> Map.get(:num_rows)
#       |> dbg()

#     {:reply, len, state}
#     # rescue
#     # e in Postgrex.Error ->
#     # {:reply, e.postgres.message, state}
#     # end
#     {:reply, len, state}
#   end

#   @impl GenServer
#   def handle_call({:drop, name}, _, {pid, _} = state) do
#     Postgrex.query!(pid, "DROP TABLE IF EXISTS #{name};")
#     {:reply, :ok, state}
#   end

#   def handle_call(:crud, _, {pid, _} = state) do
#     crud_test(pid)
#     {:reply, :ok, state}
#   end

#   def handle_call({:stream, take, nb, int}, _, state) do
#     Task.start(fn ->
#       Stream.interval(int)
#       |> Stream.take(take)
#       |> Task.async_stream(
#         fn _ -> PgProducer.bulk(nb, "users") end,
#         ordered: false
#       )
#       |> Stream.run()
#     end)

#     {:reply, :ok, state}
#   end

#   # def snap_request(pid) do
#   #   Postgrex.query!(pid, """
#   #   INSERT INTO snapshot_requests (table_name, requested_by) VALUES ('users', 'elixir-consumer');
#   #   """)
#   # end

#   defp create_table("test_types" = name, pid) do
#     %Postgrex.Result{} =
#       Postgrex.query!(pid, """
#       CREATE TABLE IF NOT EXISTS #{name} (
#         id SERIAL PRIMARY KEY,
#         uid UUID DEFAULT gen_random_uuid(),
#         age INT,
#         temperature FLOAT8,
#         price NUMERIC(20,8),
#         is_true BOOLEAN,
#         some_text TEXT,
#         tags TEXT[],
#         matrix INT[][],
#         metadata JSONB,
#         created_at TIMESTAMPTZ DEFAULT now()
#       );
#       """)

#     :ok
#   end

#   defp create_table(name, pid) do
#     %Postgrex.Result{} =
#       Postgrex.query!(pid, """
#       CREATE TABLE IF NOT EXISTS #{name} (
#         id SERIAL PRIMARY KEY,
#         name TEXT NOT NULL,
#         email TEXT,
#         create_at TIMESTAMPTZ DEFAULT now()
#       );
#       """)

#     :ok
#   end

#   defp crud_test(pid) do
#     %Postgrex.Result{command: :insert, rows: [[inserted_id]]} =
#       Postgrex.query!(
#         pid,
#         """
#         INSERT INTO test_types (age, temperature, is_true, some_text, tags, matrix, metadata, price)
#         VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
#         RETURNING id;
#         """,
#         [
#           30,
#           36.6,
#           true,
#           "Sample text",
#           ["tag1", "tag2"],
#           [[1, 2], [3, 4]],
#           Jason.encode!(%{
#             "key_1" => "value_1",
#             "key_2" => [[1, 2], [3, 4], [5, 6]],
#             "key_3" => %{"key_4" => "value_4", "key_5" => "value_5"}
#           }),
#           Decimal.new("123.45")
#         ]
#       )

#     %Postgrex.Result{command: :select, rows: [[^inserted_id | _]]} =
#       Postgrex.query!(pid, "SELECT * FROM test_types WHERE id = $1", [inserted_id])

#     %Postgrex.Result{command: :update, rows: [[^inserted_id | _]]} =
#       Postgrex.query!(
#         pid,
#         """
#         UPDATE test_types
#            SET age = $1,
#                temperature = $2,
#                is_true = $3,
#                price = $4
#          WHERE id = $5
#         RETURNING id;
#         """,
#         [31, 37.0, false, Decimal.new("122.9905"), inserted_id]
#       )

#     %Postgrex.Result{command: :delete} =
#       Postgrex.query!(pid, "DELETE FROM test_types WHERE id = $1", [inserted_id])
#   end
# end

# # Stream.interval(1) |> Stream.take(1_000) |> Task.async_stream(fn _ -> PgProducer.run(100, Enum.random(["users", "orders"])) end, ordered: false) |> Stream.run()
# # Stream.interval(1) |> Stream.take(10_000) |> Task.async_stream(fn _ -> PgProducer.run(140, "users") end, ordered: false) |> Stream.run()
