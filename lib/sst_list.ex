defmodule Kvstore.SSTList do
  def list() do
    GenServer.call(Kvstore.SSTListG, {:list, nil})
  end

  def add(name) do
    GenServer.cast(Kvstore.SSTListG, {:add_level, name})
  end
end

defmodule Kvstore.SSTListG do
  use GenServer

  require Logger

  @path "db/sst"

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def init(_) do
    files =
      File.ls!(@path)
      |> Enum.map(&String.to_integer/1)
      |> Enum.sort()
      |> Enum.reverse()
      |> Enum.map(&Integer.to_string/1)

    Enum.each(files, fn file ->
      Logger.info("Starting SSTFileG with file: #{file}")

      DynamicSupervisor.start_child(
        Kvstore.SSTFileSupervisor,
        {Kvstore.SSTFileG, %{file: file}}
      )
    end)

    {:ok, %{files: files}}
  end

  def handle_call({:list, _}, _from, %{files: files} = state) do
    Logger.debug("Listing SST files: #{inspect(files)}")
    atom_files = Enum.map(files, &String.to_atom/1)

    {:reply, atom_files, state}
  end

  def handle_cast({:add_level, name}, %{files: files}) do
    Logger.info("Adding SST file: #{name}")

    DynamicSupervisor.start_child(
      Kvstore.SSTFileSupervisor,
      {Kvstore.SSTFileG, %{file: name}}
    )

    {:noreply, %{files: [name | files]}}
  end
end
