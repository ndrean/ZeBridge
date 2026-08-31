defmodule Consumer.Snap do
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
         :ok <- create_consumer(consumer_config) do
      :ok = request_snapshots_for_tables()
      #  do something with schemas, like run migration if needed
      #  :ok <- maybe_run_migration(schemas),
      # Check if we need to request snapshots
      #  :ok <- check_and_request_snapshots_if_needed(stream_name) do
      {:ok, nil, connection_name: :gnat, stream_name: stream_name, consumer_name: consumer_name}
    else
      {:error, reason} ->
        Logger.error("[INIT Consumer] 🔴 Initialization failed: #{inspect(reason)}")
        {:stop, reason}
    end
  end

  @impl true

  def handle_message(%{topic: <<"init.snap.start.", table_name::binary>>} = message, state) do
    dbg(message.topic)
    {:ok, init_msg} = Msgpax.unpack(message.body)
    dbg({table_name, init_msg})
    {:ack, state}
  end

  def handle_message(%{topic: <<"init.snap.meta", table_name::binary>>} = message, state) do
    {:ok, meta_msg} = Msgpax.unpack(message.body)
    dbg({table_name, meta_msg})
    message.headers |> dbg()

    {:ack, state}
  end

  def handle_message(%{topic: <<"init.snap.", rest::binary>>} = message, state) do
    dbg(message.headers)
    [table_name, snapshot_id, batch] = String.split(rest, ".")
    len = Msgpax.unpack!(message.body) |> length()
    dbg({table_name, snapshot_id, batch, len})
    {:ack, state}
  end

  def handle_message(message, state) do
    dbg(message.topic)
    Msgpax.unpack!(message.body) |> dbg()
    Map.keys(message) |> dbg()
    {:ack, state}
  end

  # def handle_message(%{topic: topic} = message, state) do
  #   # Fallback for unknown messages
  #   Logger.info("#{topic}")
  #   body = Msgpax.unpack!(message.body)
  #   dbg(body)
  #   {:ack, state}
  # end

  # # Handle snapshot start notification
  # defp handle_init(table_name, msgpack_body, headers) do
  #   {:ok, init_msg} = Msgpax.unpack(msgpack_body)

  #   # Extract version info from headers
  #   version = get_header(headers, "X-Snapshot-Version", "unknown")
  #   msg_type = get_header(headers, "X-Message-Type", "unknown")

  #   Logger.info("""
  #   📸 Snapshot starting for #{table_name}
  #      Snapshot ID: #{init_msg["snapshot_id"]}
  #      LSN: #{init_msg["lsn"]}
  #      Compression: #{init_msg["compression_enabled"]}
  #      Format: #{init_msg["format"]}
  #      Version: #{version}
  #      Type: #{msg_type}
  #   """)
  # end

  # # Handle schema message
  # defp handle_schema(table_name, snapshot_id, msgpack_body, headers) do
  #   {:ok, schema_msg} = Msgpax.unpack(msgpack_body)
  #   # schema_msg = %{
  #   #   "table" => "users",
  #   #   "snapshot_id" => "snap-123-abcd",
  #   #   "schema" => ["id", "name", "email", "created_at"],
  #   #   "timestamp" => 1234567890
  #   # }

  #   schema = schema_msg["schema"]
  #   key = {table_name, snapshot_id}
  # end

  # # Handle data chunk (raw MessagePack array of arrays)
  # defp handle_data_chunk(table_name, snapshot_id, batch, body, headers) do
  #   # Extract metadata from headers
  #   content_type = get_header(headers, "Content-Type", "unknown")
  #   content_encoding = get_header(headers, "Content-Encoding", "none")
  #   format = get_header(headers, "X-Format", "unknown")
  #   version = get_header(headers, "X-Snapshot-Version", "unknown")
  #   schema_ref = get_header(headers, "X-Schema-Ref", "unknown")

  #   dbg({content_type, schema_ref})

  #   # Verify format version compatibility
  #   if version != "1.0" do
  #     Logger.warning("⚠️  Unsupported snapshot version: #{version} (expected 1.0)")
  #   end

  #   if format != "array" do
  #     Logger.warning("⚠️  Unexpected format: #{format} (expected 'array')")
  #   end

  #   # Decompress if needed
  #   payload =
  #     if content_encoding == "zstd" do
  #       dict_id = get_header(headers, "X-Zstd-Dict-ID", nil)

  #       if dict_id do
  #         Logger.debug("Decompressing zstd payload with dictionary: #{dict_id}")
  #         # TODO: Fetch dictionary from NATS KV store and decompress
  #         # dictionary = fetch_zstd_dictionary(dict_id)
  #         # :ezstd.decompress_using_dict(body, dictionary)
  #         body
  #       else
  #         Logger.debug("Decompressing zstd payload without dictionary")
  #         # TODO: Add zstd decompression
  #         # :ezstd.decompress(body)
  #         body
  #       end
  #     else
  #       body
  #     end

  #   # Unpack MessagePack - this is an ARRAY of ARRAYS!
  #   case Msgpax.unpack(payload) do
  #     {:ok, rows} when is_list(rows) ->
  #       # rows = [
  #       #   [1, "Alice", "alice@example.com", ~U[2025-01-01 00:00:00Z]],
  #       #   [2, "Bob", "bob@example.com", ~U[2025-01-02 00:00:00Z]],
  #       #   ...
  #       # ]

  #       # Get schema for this snapshot
  #       key = {table_name, snapshot_id}

  #       case :ets.lookup(:snapshot_schemas, key) do
  #         [{^key, schema}] ->
  #           # Convert arrays to maps using schema
  #           records =
  #             Enum.map(rows, fn row_array ->
  #               Enum.zip(schema, row_array) |> Map.new()
  #             end)

  #           # records = [
  #           #   %{"id" => 1, "name" => "Alice", "email" => "alice@example.com", ...},
  #           #   %{"id" => 2, "name" => "Bob", ...},
  #           #   ...
  #           # ]

  #           Logger.info("""
  #           ✅ Batch #{batch} from #{table_name}
  #              Rows: #{length(records)}
  #              Format: #{format} (v#{version})
  #              Encoding: #{content_encoding}
  #              Sample: #{inspect(List.first(records))}
  #           """)

  #         # TODO: Insert into database
  #         # {count, _} = Repo.insert_all(table_name, records)

  #         [] ->
  #           Logger.error(
  #             "❌ No schema found for #{table_name}/#{snapshot_id} - waiting for schema message"
  #           )
  #       end

  #     {:error, reason} ->
  #       Logger.error("❌ Failed to unpack MessagePack: #{inspect(reason)}")

  #       Logger.debug(
  #         "Body (first 100 bytes): #{inspect(binary_part(body, 0, min(100, byte_size(body))))}"
  #       )
  #   end
  # end

  # # Handle metadata (final message)
  # defp handle_metadata(table_name, msgpack_body, headers) do
  #   {:ok, meta} = Msgpax.unpack(msgpack_body)

  #   # Extract version info from headers
  #   version = get_header(headers, "X-Snapshot-Version", "unknown")
  #   msg_type = get_header(headers, "X-Message-Type", "unknown")

  #   Logger.info("""
  #   🏁 Snapshot complete for #{table_name}
  #      Total rows: #{meta["row_count"]}
  #      Batches: #{meta["batch_count"]}
  #      Snapshot ID: #{meta["snapshot_id"]}
  #      Version: #{version}
  #      Type: #{msg_type}
  #   """)

  #   # Clean up schema from ETS
  #   snapshot_id = meta["snapshot_id"]
  #   key = {table_name, snapshot_id}
  #   :ets.delete(:snapshot_schemas, key)
  # end

  # # Helper to safely get header value
  # defp get_header(headers, key, default) do
  #   case headers do
  #     nil -> default
  #     headers when is_map(headers) -> Map.get(headers, key, default)
  #     _ -> default
  #   end
  # end

  # defp maybe_run_migration(_schemas) do
  #   # Placeholder for migration logic based on fetched schemas
  #   Logger.info("[INIT Consumer] Maybe run migrations")
  #   :ok
  # end

  # defp fetch_schemas do
  #   tables =
  #     System.get_env("TABLES", "users, test_types")

  #   # |> String.split(",")
  #   # |> Enum.map(&String.trim/1)

  #   :ok = Gnat.pub(:gnat, "init.schema", tables)

  #   :ok
  # end

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
