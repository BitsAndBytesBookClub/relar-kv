defmodule Kvstore.SSTWriter do
  def write(stream) do
    GenServer.call(Kvstore.SSTWriterG, {:write, stream})
  end
end

defmodule Kvstore.SSTWriterG do
  use GenServer

  require Logger

  @path "db/sst"

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def init(_) do
    {:ok, %{}}
  end

  def handle_call({:write, table}, _from, state) do
    {megaseconds, seconds, milli} = :os.timestamp()
    file_name = "#{megaseconds * 1_000_000 + seconds * 1_000 + milli}"
    name = @path <> "/" <> file_name

    Logger.info("Writing SST file: #{name}, table: #{inspect(table)}")
    {:ok, file_descriptor} = :file.open(name, [:raw, :write, :append])

    :ets.foldl(
      fn {key, value}, _ ->
        # Logger.debug("foldl #{inspect(table)}, key: #{key}, value: #{value}")
        :file.write(file_descriptor, "#{key},#{value}\n")
      end,
      nil,
      table
    )

    :ok = :file.sync(file_descriptor)

    :ok = :file.close(file_descriptor)

    Kvstore.SSTList.add(file_name)
    Kvstore.Memetable.done_writing()
    Kvstore.Compaction.add_sst()

    {:reply, :ok, state}
  end
end
