defmodule Kvstore.SSTList do
  def list(node_id) do
    GenServer.call(Kvstore.SSTListG.name(node_id), {:list})
  end

  def add(node_id, name) do
    GenServer.call(Kvstore.SSTListG.name(node_id), {:add_level, name})
  end

  def remove(node_id, files) do
    GenServer.call(Kvstore.SSTListG.name(node_id), {:remove, files})
  end
end

defmodule Kvstore.SSTListG do
  use GenServer

  require Logger

  def name(id) do
    String.to_atom("sst_list_#{id}")
  end

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: name(args.node_id))
  end

  def init(args) do
    files =
      File.ls!(args.path)
      |> Enum.map(&String.to_integer/1)
      |> Enum.sort()
      |> Enum.reverse()
      |> Enum.map(&Integer.to_string/1)

    pids =
      Enum.map(files, fn file ->
        Logger.info("Starting SSTFileG with file: #{file}")

        {:ok, pid} =
          DynamicSupervisor.start_child(
            Kvstore.SSTFileSupervisor.name(args.node_id),
            {Kvstore.SSTFileG, %{file: file, path: args.path}}
          )

        pid
      end)

    {:ok, %{files: Enum.zip([files, pids]), path: args.path}}
  end

  def handle_call({:list}, _from, %{files: files} = state) do
    Logger.debug("Listing SST files: #{inspect(files)}")

    atom_files =
      files
      |> Enum.map(&elem(&1, 0))
      |> Enum.map(&String.to_atom/1)

    {:reply, atom_files, state}
  end

  def handle_call({:remove, files}, _from, %{files: all_files, path: path} = state) do
    Logger.info("Removing SST files: #{inspect(files)}")

    Enum.each(files, fn file ->
      case Enum.find(all_files, fn {f, _} -> f == file end) do
        nil ->
          Logger.error("File not found: #{file}")

        {_, pid} ->
          :ok = DynamicSupervisor.terminate_child(Kvstore.SSTFileSupervisor, pid)
          File.rm!(path <> "/" <> file)
      end
    end)

    {:reply, :ok,
     %{state | files: Enum.reject(all_files, fn {f, _} -> Enum.member?(files, f) end)}}
  end

  def handle_call({:add_level, name}, _from, state) do
    Logger.info("Adding SST file: #{name}")

    {:ok, pid} =
      DynamicSupervisor.start_child(
        Kvstore.SSTFileSupervisor,
        {Kvstore.SSTFileG, %{file: name, path: state.path}}
      )

    {:reply, :ok, %{state | files: [{name, pid} | state.files]}}
  end
end
