defmodule SchemaConsumer do
  require Logger

  def handle_message(message) do
    %{"columns" => columns, "table" => table} = Msgpax.unpack!(message.body)

    Logger.info("[SchemaConsumer]: #{message.topic}, #{table}, col_count: #{length(columns)}")
  end
end
