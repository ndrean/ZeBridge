# This file is responsible for configuring your application
# and its dependencies with the aid of the Config module.
#
# This configuration file is loaded before any dependency and
# is restricted to this project.

# General application configuration
import Config

config :emitter,
  generators: [timestamp_type: :utc_datetime]

# Configures Elixir's Logger
config :logger, :default_formatter,
  format: "$time $metadata[$level] $message\n",
  metadata: [:request_id]

# Import environment specific config. This must remain at the bottom
# of this file so it overrides the configuration defined above.
import_config "#{config_env()}.exs"

config :emitter,
  ecto_repos: [Emitter.Producer.Repo],
  generators: [timestamp_type: :utc_datetime]

config :emitter, Emitter.Producer.Repo,
  url:
    System.get_env("DATABASE_URL") ||
      "postgres://postgres:postgres_password@localhost:5432/postgres",
  pool_size: String.to_integer(System.get_env("POOL_SIZE") || "5")
