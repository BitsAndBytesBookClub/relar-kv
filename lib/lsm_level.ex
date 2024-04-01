defmodule Kvstore.LSMLevel do
  def get_part(level, key) do
    GenServer.call(String.to_atom("level" <> level), {:get_part, key})
  end
end

defmodule Kvstore.LSMLevelG do
  use DynamicSupervisor

  require Logger

  @path "db/lsm"

  def start_link(%{level: level} = args) do
    GenServer.start_link(__MODULE__, args, name: String.to_atom("level" <> level))
  end

  def init(%{level: level}) do
    path = "#{@path}/#{level}"
    files = File.ls!(path)

    parts =
      Enum.map(files, fn file ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            Kvstore.LSMPartSupervisor,
            {Kvstore.LSMPartG, %{file: file, path: path}}
          )

        pid
      end)

    bounds =
      Enum.map(parts, fn pid ->
        Logger.info("Getting bounds for LSMLevel file: #{inspect(pid)}")
        Kvstore.LSMPart.first(pid)
      end)

    Logger.info("Files in LSMLevel #{level}: #{inspect(files)}, with bounds: #{inspect(bounds)}")

    {:ok, %{files: files, bounds: bounds, level: level}}
  end

  def handle_call({:get_part, key}, _from, %{files: files, level: level, bounds: bounds} = state) do
    Logger.debug("Reading LSMLevel key: #{key}")

    b1 = Enum.take(bounds, Enum.count(bounds) - 1)
    b2 = tl(bounds)

    pairs = Enum.zip(b1, b2)

    i =
      Enum.reduce_while(
        Enum.with_index(pairs),
        fn [i, {a, b}] ->
          case Enum.sort([a, b, key]) do
            [_, ^key, _] ->
              Logger.debug("Key found in LSMLevel #{level}")
              {:halt, i}

            _ ->
              Logger.debug("Key not found in LSMLevel #{level}")
              {:cont, nil}
          end
        end,
        nil
      )

    IO.inspect(i)

    file =
      case i do
        nil ->
          nil

        i ->
          Enum.at(files, i)
      end

    {:reply, file, state}
  end
end

defmodule Kvstore.LSMLevelSupervisor do
  use DynamicSupervisor

  def start_link(init_arg) do
    DynamicSupervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
