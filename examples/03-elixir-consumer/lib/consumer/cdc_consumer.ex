defmodule Consumer.Cdc do
  @moduledoc """
  JetStream pull consumer for CDC events from the Zig bridge.

  Consumes CDC events from the CDC_BRIDGE JetStream stream with:
  - Durable consumer tracking
  - Acknowledgment of processed messages
  - Automatic stream/consumer creation
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
         :ok <- create_consumer(consumer_config) do
      {:ok, nil, connection_name: :gnat, stream_name: stream_name, consumer_name: consumer_name}
    end
  end

  @impl true
  def handle_message(%{topic: topic, body: body} = _message, state) do
    dbg(topic)

    try do
      case :persistent_term.get(:format, "msgpack") do
        "json" ->
          decoded = Jason.decode!(body)

          if is_list(decoded) do
            Enum.each(decoded, fn event ->
              data_str = if event["data"], do: " data=#{inspect(event["data"])}", else: ""
              lsn_str = if event["lsn"], do: " lsn=#{event["lsn"]}", else: ""

              Logger.info(
                "[CDC Consumer] Batch event - table: #{event["table"]}, operation: #{event["operation"]}, msg_id: #{event["msg_id"]}#{lsn_str}#{data_str}"
              )
            end)
          else
            data_str = if decoded["data"], do: " data=#{inspect(decoded["data"])}", else: ""
            lsn_str = if decoded["lsn"], do: " lsn=#{decoded["lsn"]}", else: ""

            Logger.info(
              "[CDC Consumer] #{topic}: table=#{decoded["table"]}, operation=#{decoded["operation"]}#{lsn_str}#{data_str}"
            )
          end

        "msgpack" ->
          decoded = Msgpax.unpack!(body)

          if is_list(decoded) do
            # Batch of events - each event has table, operation, subject, msg_id, lsn, and data
            Enum.each(decoded, fn event ->
              data_str = if event["data"], do: " data=#{inspect(event["data"])}", else: ""
              lsn_str = if event["lsn"], do: " lsn=#{event["lsn"]}", else: ""

              Logger.info(
                "[CDC Consumer] Batch event - table: #{event["table"]}, operation: #{event["operation"]}, msg_id: #{event["msg_id"]}#{lsn_str}#{data_str}"
              )
            end)
          else
            # Single event with table, operation, lsn, and data fields
            data_str = if decoded["data"], do: " data=#{inspect(decoded["data"])}", else: ""
            lsn_str = if decoded["lsn"], do: " lsn=#{decoded["lsn"]}", else: ""

            Logger.info(
              "[CDC Consumer] #{topic}: table=#{decoded["table"]}, operation=#{decoded["operation"]}#{lsn_str}#{data_str}"
            )
          end
      end

      # Acknowledge successful processing
      {:ack, state}
    rescue
      error ->
        Logger.error("[CDC Consumer] Error processing message: #{inspect(error)}")
        # Negative acknowledgment - will be redelivered
        {:nack, state}
    end
  end

  defp ensure_jetstream_enabled do
    # wait loop for NATS connection establishment
    case Process.whereis(:gnat) do
      nil ->
        Logger.debug("[CDC Consumer] Waiting for NATS connection...")
        Process.send_after(self(), :retry, 100)

        receive do
          :retry ->
            ensure_jetstream_enabled()
        after
          2_000 ->
            Logger.error("[CDC Consumer] Timeout waiting for NATS connection")
            raise "Timeout waiting for NATS connection"
        end

      _pid ->
        # ensure JetStream is enabled by the server
        true = Gnat.server_info(:gnat).jetstream
        Logger.info("[CDC Consumer] ❇️ NATS connection established with JetStream enabled")
        :ok
    end
  end

  defp ensure_stream_exists(%Gnat.Jetstream.API.Consumer{} = consumer_config) do
    stream_name = Map.get(consumer_config, :stream_name, "CDC_BRIDGE")

    # Stream is created by Zig bridge, just verify it exists
    case Gnat.Jetstream.API.Stream.info(:gnat, stream_name) do
      {:ok, _stream_info} ->
        Logger.info("[CDC Consumer] ❇️ Using JetStream stream '#{stream_name}'")
        :ok

      {:error, reason} ->
        Logger.warning(
          "[CDC Consumer] 🔴 Stream '#{stream_name}' not found: #{inspect(reason)}. Make sure Zig bridge is running and has created this exact stream."
        )

        raise "JetStream stream '#{stream_name}' not found"
    end
  end

  defp create_consumer(consumer_config) do
    consumer_name = Map.get(consumer_config, :durable_name)

    case Gnat.Jetstream.API.Consumer.create(:gnat, consumer_config) do
      {:ok, %{created: _}} ->
        Logger.info("[CDC Consumer] ❇️ Durable consumer '#{consumer_name}' created")
        :ok

      {:error, %{"code" => 400, "description" => "consumer name already in use"}} ->
        Logger.info("[CDC Consumer] ❇️ Durable consumer '#{consumer_name}' already exists")
        :ok

      {:error, reason} ->
        Logger.error("[CDC Consumer] 🔴 Failed to create consumer: #{inspect(reason)}")
        raise "Failed to setup JetStream consumer"
    end
  end
end
