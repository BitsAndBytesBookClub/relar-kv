defmodule Kvstore.SSTFile do
  require Logger

  def get(sst, key) do
    try do
      GenServer.call({:global, sst}, {:get, key})
    catch
      ArgumentError ->
        # This error occurs if the pid is not a valid process.
        Logger.error("Invalid SSTFile pid: #{inspect(sst)}")

      :exit, reason ->
        # Handle abrupt termination reasons.
        Logger.error("SSTFile terminated: #{inspect(reason)}")
        nil
    end
  end
end

defmodule Kvstore.SSTFileG do
  use GenServer

  require Logger

  def name(node_id, file) do
    String.to_atom("sst_file_#{node_id}_#{file}")
  end

  def start_link(%{file: file} = args) do
    GenServer.start_link(__MODULE__, args, name: {:global, name(args.node_id, file)})
  end

  def init(%{file: file, path: path}) do
    Process.flag(:trap_exit, true)
    {:ok, fd} = File.open(path <> "/" <> file, [:read])
    {:ok, %{fd: fd, file: file}}
  end

  def handle_call({:get, key}, _from, %{fd: fd, file: file} = state) do
    Logger.debug("Reading SST key: #{key} in #{file}")

    {:ok, _} = :file.position(fd, {:bof, 0})

    line =
      IO.stream(fd, :line)
      |> Stream.map(&String.split(&1, ","))
      |> Enum.find(fn [k, _] -> k == key end)

    case line do
      nil ->
        Logger.debug("Key not found: #{key}")
        {:reply, nil, state}

      line ->
        [_, v] = line
        {:reply, String.trim_trailing(v, "\n"), state}
    end
  end

  def terminate(_reason, %{fd: fd}) do
    # Logger.info("Closing SST file #{inspect(reason)}")
    :ok = File.close(fd)
    :ok
  end
end

defmodule Kvstore.SSTFileSupervisor do
  use DynamicSupervisor

  def name(id) do
    String.to_atom("sst_file_#{id}")
  end

  def start_link(args) do
    DynamicSupervisor.start_link(__MODULE__, args, name: name(args.node_id))
  end

  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
