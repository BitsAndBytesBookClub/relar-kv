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

    enumed_pairs = Enum.with_index(pairs)

    IO.inspect(enumed_pairs)

    i =
      Enum.reduce_while(
        enumed_pairs,
        nil,
        fn {{a, b}, i}, _ ->
          case Enum.sort([a, b, key]) do
            [_, ^key, ^key] ->
              {:halt, i + 1}

            [_, ^key, _] ->
              {:halt, i}

            [_, _, ^key] ->
              {:halt, i + 1}

            [^key, _, _] ->
              {:cont, nil}
          end
        end
      )

    case i do
      nil ->
        {:reply, nil, state}

      i ->
        Logger.debug("Key in range for LSMLevel #{level}, file: #{Enum.at(files, i)}")
        {:reply, @path <> "/" <> level <> "/" <> Enum.at(files, i), state}
    end
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
