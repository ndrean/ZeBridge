#!/usr/bin/env elixir

# Test PostgreSQL connection
IO.puts("Testing PostgreSQL connection...")
IO.puts("PG_HOST: #{System.get_env("PG_HOST")}")
IO.puts("PG_PORT: #{System.get_env("PG_PORT")}")
IO.puts("PG_USER: #{System.get_env("PG_USER")}")
IO.puts("PG_PASSWORD: #{if System.get_env("PG_PASSWORD"), do: "***SET***", else: "***NOT SET***"}")
IO.puts("PG_DB: #{System.get_env("PG_DB")}")
IO.puts("")

opts = [
  hostname: System.fetch_env!("PG_HOST"),
  port: String.to_integer(System.fetch_env!("PG_PORT")),
  username: System.fetch_env!("PG_USER"),
  password: System.fetch_env!("PG_PASSWORD"),
  database: System.fetch_env!("PG_DB")
]

IO.puts("Connection options:")
IO.inspect(opts, label: "opts")
IO.puts("")

case Postgrex.start_link(opts) do
  {:ok, pid} ->
    IO.puts("✅ Connection successful!")

    case Postgrex.query(pid, "SELECT version();", []) do
      {:ok, result} ->
        IO.puts("✅ Query successful!")
        IO.inspect(result.rows)
      {:error, error} ->
        IO.puts("❌ Query failed!")
        IO.inspect(error)
    end

    GenServer.stop(pid)

  {:error, error} ->
    IO.puts("❌ Connection failed!")
    IO.inspect(error, pretty: true, limit: :infinity)

    # Try to get more details
    case error do
      %Postgrex.Error{} = e ->
        IO.puts("\nError details:")
        IO.puts("  Message: #{e.message}")
        if e.postgres do
          IO.puts("  Postgres code: #{e.postgres.code}")
          IO.puts("  Postgres message: #{e.postgres.message}")
        end
      _ ->
        IO.puts("\nUnexpected error type")
    end
end
