defmodule Kvstore.SSTList do
  def list() do
    GenServer.call(Kvstore.SSTListG, {:list})
  end

  def add(name) do
    GenServer.call(Kvstore.SSTListG, {:add_level, name})
  end

  def remove(files) do
    GenServer.call(Kvstore.SSTListG, {:remove, files})
  end
end

defmodule Kvstore.SSTListG do
  use GenServer

  require Logger

  def start_link(path) do
    GenServer.start_link(__MODULE__, path, name: __MODULE__)
  end

  def init(path) do
    files =
      File.ls!(path)
      |> Enum.map(&String.to_integer/1)
      |> Enum.sort()
      |> Enum.reverse()
      |> Enum.map(&Integer.to_string/1)

    pids =
      Enum.map(files, fn file ->
        Logger.info("Starting SSTFileG with file: #{file}")

        {:ok, pid} =
          DynamicSupervisor.start_child(
            Kvstore.SSTFileSupervisor,
            {Kvstore.SSTFileG, %{file: file, path: path}}
          )

        pid
      end)

    {:ok, %{files: Enum.zip([files, pids]), path: path}}
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
