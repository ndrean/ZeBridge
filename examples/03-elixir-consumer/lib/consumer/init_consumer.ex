defmodule Consumer.Init do
  @moduledoc """
  INIT consumer for snapshot data from the Zig bridge.

  Fetches schemas from NATS KV store on startup, then consumes snapshot
  data from the INIT JetStream stream.

  Flow:
  1. Fetch schemas from NATS KV store (schemas bucket)
  2. Check if we need to request fresh snapshots
  3. Consume snapshot chunks from INIT stream
  """
  use Gnat.Jetstream.PullConsumer
  require Logger

  def start_link(consumer_config) do
    Gnat.Jetstream.PullConsumer.start_link(__MODULE__, consumer_config, name: __MODULE__)
  end

  @impl true
  def init(%Gnat.Jetstream.API.Consumer{} = consumer_config) do
    # Get stream name from environment variable or use default
    stream_name = Map.fetch!(consumer_config, :stream_name)
    consumer_name = Map.fetch!(consumer_config, :durable_name)

    # Ensure NATS connection and stream exists before starting consumer
    with :ok <- ensure_jetstream_enabled(),
         :ok <- ensure_stream_exists(consumer_config),
         :ok <- create_consumer(consumer_config),
         # Fetch schemas from KV store
         {:ok, schemas} <- fetch_schemas_from_kv(),
         #  do something with schemas, like run migration if needed
         :ok <- maybe_run_migration(schemas),
         # Check if we need to request snapshots
         :ok <- check_and_request_snapshots_if_needed(stream_name) do
      {:ok, schemas,
       connection_name: :gnat, stream_name: stream_name, consumer_name: consumer_name}
    else
      {:error, reason} ->
        Logger.error("[INIT Consumer] 🔴 Initialization failed: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  @impl true
  def handle_message(message, state) do
    if message.topic == "init.start.users" do
      Logger.info("[Start] init.start.users ---------------> #{System.system_time(:microsecond)}")
    end

    # format = :persistent_term.get(:format)

    try do
      Task.Supervisor.start_child(MyTaskSupervisor, fn ->
        Logger.info(
          "[Chunk] #{message.topic}: #{byte_size(message.body)}-----> #{System.system_time(:microsecond)}"
        )

        #   decoded =
        #     case format do
        #       "json" ->
        #         _decoded = Jason.decode!(message.body)

        #       "msgpack" ->
        #         _decoded = Msgpax.unpack!(message.body)
        #     end

        #   # TODO: insert into DB based on schema in state
        #   data = decoded["data"]

        #   len =
        #     case is_list(data) do
        #       true -> length(data)
        #       _ -> 0
        #     end

        #   Logger.info(
        #     "[INIT Consumer] #{System.system_time(:microsecond)} Processed snapshot chunk with #{len} records"
        #   )

        #   Logger.info("[INIT Consumer] Processed snapshot chunk message:  #{inspect(decoded)}")
      end)

      {:ack, state}
    rescue
      error ->
        Logger.error("[INIT Consumer] ⚠️ Error processing message: #{inspect(error)}")
        # Negative acknowledgment - will be redelivered
        {:nack, state}
    end
  end

  defp maybe_run_migration(_schemas) do
    # Placeholder for migration logic based on fetched schemas
    Logger.info("[INIT Consumer] Maybe run migrations")
    :ok
  end

  defp fetch_schemas_from_kv do
    :ok = Gnat.pub(:gnat, "init.schema", "Hello from Elixir!") |> dbg()
    # Get tables from environment
    tables =
      System.get_env("TABLES")
      |> case do
        nil -> ["users", "test_types"]
        tables_str -> String.split(tables_str, ",") |> Enum.map(&String.trim/1)
      end

    # Fetch schema for each table from KV
    schemas =
      Enum.reduce(tables, [], fn table_name, acc ->
        case Gnat.Jetstream.API.KV.get_value(:gnat, "schemas", table_name) do
          schema_data when is_binary(schema_data) ->
            case :persistent_term.get(:format) do
              "json" ->
                [Jason.decode!(schema_data) | acc]

              "msgpack" ->
                {:ok, schema} = Msgpax.unpack(schema_data)

                Logger.info(
                  "[INIT Consumer] Fetched schema from KV for table '#{table_name}': \n#{inspect(schema)}\n"
                )

                [schema | acc]
            end

          _ ->
            Logger.error("[INIT Consumer] 🔴 Failed to fetch schema from KV for '#{table_name}'")
            [:err | acc]
        end
      end)

    {:ok, schemas}
  end

  defp ensure_jetstream_enabled do
    # wait loop for NATS connection establishment
    case Process.whereis(:gnat) do
      nil ->
        Logger.debug("[INIT Consumer] Waiting for NATS connection...")
        Process.send_after(self(), :retry, 100)

        receive do
          :retry ->
            ensure_jetstream_enabled()
        after
          2_000 ->
            Logger.error("[INIT Consumer] 🔴 Timeout waiting for NATS connection")
            raise "Timeout waiting for NATS connection"
        end

      _pid ->
        # ensure JetStream is enabled by the server
        true = Gnat.server_info(:gnat).jetstream
        Logger.info("[INIT Consumer] ❇️ NATS connection established with JetStream enabled")
        :ok
    end
  end

  defp ensure_stream_exists(%Gnat.Jetstream.API.Consumer{} = consumer_config) do
    stream_name = Map.get(consumer_config, :stream_name, "INIT")

    # Stream is created by Zig bridge, just verify it exists
    case Gnat.Jetstream.API.Stream.info(:gnat, stream_name) do
      {:ok, _stream_info} ->
        Logger.info("[INIT Consumer] ❇️ Using JetStream stream '#{stream_name}'")
        :ok

      {:error, reason} ->
        Logger.warning(
          "[INIT Consumer] 🔴 Stream '#{stream_name}' not found: #{inspect(reason)}. Make sure Zig bridge is running and has created this exact stream."
        )

        raise "JetStream stream '#{stream_name}' not found"
    end
  end

  defp create_consumer(consumer_config) do
    consumer_name = Map.get(consumer_config, :durable_name)

    case Gnat.Jetstream.API.Consumer.create(:gnat, consumer_config) do
      {:ok, %{created: _}} ->
        Logger.info("[INIT Consumer] ❇️ Durable consumer '#{consumer_name}' created")
        :ok

      {:error, %{"code" => 400, "description" => "consumer name already in use"}} ->
        Logger.info("[INIT Consumer] ❇️ Durable consumer '#{consumer_name}' already exists")
        :ok

      {:error, reason} ->
        Logger.error("[INIT Consumer] 🔴 Failed to create consumer: #{inspect(reason)}")
        raise "Failed to setup JetStream consumer"
    end
  end

  def check_and_request_snapshots_if_needed(stream_name) do
    # Get stream info to check if there's data
    {:ok, stream_info} =
      Gnat.Jetstream.API.Stream.info(:gnat, stream_name)

    state = stream_info["state"] || %{}
    stream_messages = state["messages"] || 0

    if stream_messages == 0 do
      Logger.info("[INIT Consumer] Stream is empty, requesting fresh snapshots")
      :ok = request_snapshots_for_tables()
    else
      Logger.info("[INIT Consumer] Stream has #{stream_messages} messages, will consume them")
    end

    :ok
  end

  def request_snapshots_for_tables do
    # Get tables from environment
    tables =
      System.get_env("TABLES")
      |> case do
        nil -> ["users", "test_types"]
        tables_str -> String.split(tables_str, ",") |> Enum.map(&String.trim/1)
      end

    # Request snapshot for each table
    Enum.each(tables, fn table_name ->
      :ok = Gnat.pub(:gnat, "snapshot.request." <> table_name, "")
      Logger.info("[INIT Consumer] ℹ️ Requested snapshot for table #{table_name}")
    end)

    :ok
  end
end
