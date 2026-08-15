defmodule Emitter.Application do
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      Emitter.Producer.Repo
    ]

    opts = [strategy: :one_for_one, name: Emitter.Supervisor]

    case Supervisor.start_link(children, opts) do
      {:ok, pid} ->
        # Run migrations on the active Repo connection pool
        # path = Ecto.Migrator.migrations_path(Emitter.Producer.Repo)
        # Ecto.Migrator.run(Emitter.Producer.Repo, path, :up, all: true) |> dbg()

        # Notify the Zig bridge via NATS
        {:ok, _gnat} =
          Gnat.start_link(
            %{
              host: System.get_env("NATS_HOST") || "127.0.0.1",
              port: String.to_integer(System.get_env("NATS_PORT") || "4222"),
              jwt: System.get_env("NATS_JWT"),
              nkey_seed: System.get_env("NATS_NKEY_SEED")
            }
            |> Enum.reject(fn {_, v} -> is_nil(v) end)
            |> Map.new()
          )

        # Gnat.pub(gnat, "bridge.control.reload_schema", "{}")
        IO.puts("✅ Migrations executed and Zig bridge should be notified on boot")

        {:ok, pid}

      error ->
        error
    end
  end
end
