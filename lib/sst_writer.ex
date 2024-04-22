defmodule Kvstore.SSTWriter do
  def write(stream) do
    GenServer.call(Kvstore.SSTWriterG, {:write, stream})
  end
end

defmodule Kvstore.SSTWriterG do
  use GenServer

  require Logger

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: String.to_atom("sst_writer_#{args.node_id}"))
  end

  def init(args) do
    {:ok, %{path: args.path}}
  end

  def handle_call({:write, table}, _from, %{path: path} = state) do
    {megaseconds, seconds, micros} = :os.timestamp()
    file_name = "#{megaseconds * 1_000_000_000_000 + seconds * 1_000_000 + micros}"
    name = path <> "/" <> file_name

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

    :ok = :file.datasync(file_descriptor)

    :ok = :file.close(file_descriptor)

    Kvstore.SSTList.add(file_name)
    Kvstore.Memetable.done_writing()
    Kvstore.Compaction.add_sst()

    {:reply, :ok, state}
  end
end
