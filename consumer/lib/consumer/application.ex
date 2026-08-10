defmodule Consumer.Application do
  @moduledoc false

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    :persistent_term.put(:format, System.get_env("FORMAT") || "msgpack")
    :ets.new(:snapshot_schemas, [:named_table, :public, :set])

    children = [
      Producer.Repo,
      {Task.Supervisor, name: MyTaskSupervisor},
      {Gnat.ConnectionSupervisor, gnat_supervisor_settings()},
      {Gnat.ConsumerSupervisor, schema_snap_settings()},
      {PgProducer, args()},
      # JetStream pull consumer for CDC events
      {Task, fn -> publish_schema_snap() end},
      {Consumer.Cdc, consumer_cdc_settings()},
      {Consumer.Snap, consumer_snap_settings()}
    ]

    opts = [strategy: :one_for_one, name: Consumer.Supervisor]
    Supervisor.start_link(children, opts)
  end

  defp get_tables do
    System.get_env("TABLES")
    |> String.split(",")
    |> Enum.map(&String.trim/1)
  end

  defp args do
    [
      hostname: System.fetch_env!("PG_HOST"),
      port: String.to_integer(System.fetch_env!("PG_PORT")),
      username: System.fetch_env!("PG_USER"),
      password: System.fetch_env!("PG_PASSWORD"),
      database: System.fetch_env!("PG_DB"),
      name: PgEx,
      tables: get_tables()
    ]
  end

  defp schema_snap_settings do
    %{
      connection_name: :gnat,
      consuming_function: {SchemaConsumer, :handle_message},
      subscription_topics: [
        %{topic: "schema.>"}
      ]
    }
  end

  defp consumer_cdc_settings do
    %Gnat.Jetstream.API.Consumer{
      # consumer position tracking is persisted
      durable_name: "ex_cdc_consumer_1",
      stream_name: "CDC",
      ack_policy: :explicit,
      # 60 seconds in nanoseconds
      ack_wait: 60_000_000_000,
      max_deliver: 3,
      max_batch: 100,
      deliver_policy: :all,
      filter_subject: "cdc.>"
    }
  end

  defp consumer_snap_settings do
    %Gnat.Jetstream.API.Consumer{
      # consumer position tracking is persisted
      durable_name: "ex_snap_consumer_1",
      stream_name: "INIT",
      ack_policy: :explicit,
      # 60 seconds in nanoseconds
      ack_wait: 60_000_000_000,
      max_deliver: 3,
      filter_subject: "init.snap.>",
      deliver_policy: :all,
      max_batch: 100
    }
  end

  defp gnat_supervisor_settings do
    %{
      name: :gnat,
      backoff_period: 4_000,
      connection_settings: [
        %{
          host: System.get_env("NATS_HOST") || "localhost",
          port: String.to_integer(System.get_env("NATS_PORT") || "4222"),
          username: System.get_env("NATS_USER", "bridge_user"),
          password: System.get_env("NATS_PASSWORD", "bridge_secure_password"),
          token: "bridge_user_bridge_secure_password"
          #   tls: %{
          #   required: true,
          #   verify: true,
          #   cacertfile: "/path/to/ca.pem"
          # }
        }
      ]
    }
  end

  defp publish_schema_snap do
    # wait loop for NATS connection establishment
    case Process.whereis(:gnat) do
      nil ->
        Logger.debug("[INIT Consumer] Waiting for NATS connection...")
        Process.send_after(self(), :retry, 100)

        receive do
          :retry ->
            Logger.debug("Loop-----")
            publish_schema_snap()
        after
          2_000 ->
            Logger.error("[INIT Consumer] 🔴 Timeout waiting for NATS connection")
            raise "Timeout waiting for NATS connection"
        end

      _pid ->
        # ensure JetStream is enabled by the server
        true = Gnat.server_info(:gnat).jetstream
        Logger.info("[INIT Consumer] ❇️ NATS connection established with JetStream enabled")

        tables = System.get_env("TABLES")
        Gnat.pub(:gnat, "init.schema", tables) |> dbg()

        # receive_messages()
    end
  end
end
