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
        path = Ecto.Migrator.migrations_path(Emitter.Producer.Repo) |> dbg()
        Application.app_dir(:emitter, "priv/repo/migrations") |> dbg()
        Ecto.Migrator.run(Emitter.Producer.Repo, path, :up, all: true) |> dbg()

        # Notify the Zig bridge via NATS
        {:ok, gnat} =
          Gnat.start_link(%{
            host: "127.0.0.1",
            port: 4222,
            username: "bridge_user",
            password: "bridge_secure_password",
            token: "bridge_user_bridge_secure_password"
          })

        Gnat.pub(gnat, "bridge.control.reload_schema", "{}")
        IO.puts("✅ Migrations executed and Zig bridge notified on boot!")

        {:ok, pid}

      error ->
        error
    end
  end
end
