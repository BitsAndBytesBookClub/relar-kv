defmodule Kvstore.LSMPart do
  def get(part, key) do
    GenServer.call(String.to_atom(part), {:get, key})
  end

  def first(pid) do
    GenServer.call(pid, :first)
  end
end

defmodule Kvstore.LSMPartG do
  use GenServer

  require Logger

  def start_link(%{file: file, path: path}) do
    name = path <> "/" <> file

    Logger.info("Starting LSMPart: #{name}")

    GenServer.start_link(__MODULE__, %{file: file, path: path}, name: String.to_atom(name))
  end

  def init(%{file: file, path: path}) do
    {:ok, fd} = File.open(path <> "/" <> file, [:read])

    first_line =
      IO.stream(fd, :line)
      |> Enum.take(1)
      |> hd

    [first, _] = String.split(first_line, ",")

    Logger.info("Opening LSMPart: #{path}/#{file}, first: #{first}")

    {:ok, %{file: file, path: path, fd: fd, first: first}}
  end

  def handle_call({:get, key}, _from, %{fd: fd} = state) do
    Logger.debug("Reading LSMPart key: #{key}")

    :file.position(fd, {:bof, 0})

    line =
      IO.stream(fd, :line)
      |> Stream.map(&String.split(&1, ","))
      |> Enum.find(fn [k, _] -> k == key end)

    case line do
      nil ->
        {:reply, nil, state}

      line ->
        [_, v] = line
        {:reply, String.trim_trailing(v, "\n"), state}
    end
  end

  def handle_call(:first, _from, %{first: first} = state) do
    {:reply, first, state}
  end
end

defmodule Kvstore.LSMPartSupervisor do
  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
