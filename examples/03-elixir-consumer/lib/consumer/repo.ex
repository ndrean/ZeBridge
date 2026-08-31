defmodule Producer.Repo do
  use Ecto.Repo,
    otp_app: :consumer,
    adapter: Ecto.Adapters.Postgres,
    database: System.get_env("PG_DB"),
    hostname: System.get_env("PG_HOST"),
    port: String.to_integer(System.get_env("PG_PORT") || "5432"),
    username: System.get_env("PG_USER"),
    password: System.get_env("PG_PASSWORD"),
    pool_size: 10
end
