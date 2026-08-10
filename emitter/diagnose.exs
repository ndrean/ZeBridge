IO.puts("=== Environment Variables ===")
IO.puts("PG_HOST: #{inspect(System.get_env("PG_HOST"))}")
IO.puts("PG_PORT: #{inspect(System.get_env("PG_PORT"))}")
IO.puts("PG_USER: #{inspect(System.get_env("PG_USER"))}")
IO.puts("PG_PASSWORD: #{if System.get_env("PG_PASSWORD"), do: "***SET (#{String.length(System.get_env("PG_PASSWORD"))} chars)***", else: "nil"}")
IO.puts("PG_DB: #{inspect(System.get_env("PG_DB"))}")
IO.puts("DATABASE_URL: #{inspect(System.get_env("DATABASE_URL"))}")
IO.puts("")

IO.puts("=== PgProducer opts (from application.ex) ===")
opts = [
  hostname: System.fetch_env!("PG_HOST"),
  port: String.to_integer(System.fetch_env!("PG_PORT")),
  username: System.fetch_env!("PG_USER"),
  password: System.fetch_env!("PG_PASSWORD"),
  database: System.fetch_env!("PG_DB"),
  name: PgEx,
  tables: String.split(System.get_env("TABLES"), ",") |> Enum.map(&String.trim/1)
]
IO.inspect(opts, label: "opts", limit: :infinity)
