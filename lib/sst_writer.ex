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
    {megaseconds, seconds, _} = :os.timestamp()
    file_name = "#{megaseconds * 1_000_000 + seconds}"
    name = @path <> "/" <> file_name

    Logger.info("Writing SST file: #{name}, table: #{inspect(table)}")
    {:ok, file_descriptor} = File.open(name, [:write, :append])

    :ets.foldl(
      fn {key, value}, _ ->
        Logger.debug("foldl #{inspect(table)}, key: #{key}, value: #{value}")
        IO.write(file_descriptor, "#{key},#{value}\n")
      end,
      nil,
      table
    )

    :ok = File.close(file_descriptor)

    Kvstore.SSTList.add_level(file_name)
    Kvstore.Memetable.done_writing()

    {:reply, :ok, state}
  end
end
