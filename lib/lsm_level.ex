defmodule Kvstore.LSMLevel do
  require Logger

  def get_part({_, _, pid}, key) do
    try do
      GenServer.call(pid, {:get_part, key})
    catch
      ArgumentError ->
        # This error occurs if the pid is not a valid process.
        Logger.error("Invalid LSMLevel pid: #{inspect(pid)}")

      :exit, reason ->
        # Handle abrupt termination reasons.
        Logger.error("LSMLevel terminated: #{inspect(reason)}")
        nil
    end
  end

  def stop(pid) do
    GenServer.stop(pid)
  end

  def list_parts({_, _, pid}) do
    GenServer.call(pid, {:list_parts})
  end
end

defmodule Kvstore.LSMLevelG do
  use GenServer

  require Logger

  def child_spec(args) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [args]},
      type: :supervisor
    }
  end

  def name(node, level, i) do
    String.to_atom("level_#{node}_#{level}_#{i}")
  end

  def start_link(%{level: level, iteration: i, path: _path} = args) do
    GenServer.start_link(__MODULE__, args, name: {:global, name(args.node_id, level, i)})
  end

  def init(%{level: level, iteration: i, path: path} = args) do
    Process.flag(:trap_exit, true)

    full_path = "#{path}/#{level}"

    files =
      full_path
      |> File.ls!()
      |> Enum.sort()

    parts =
      Enum.map(files, fn file ->
        {:ok, pid} =
          DynamicSupervisor.start_child(
            Kvstore.LSMPartSupervisor.name(args.node_id),
            {Kvstore.LSMPartG, %{file: file, path: full_path, iteration: i}}
          )

        pid
      end)

    bounds =
      Enum.zip(files, parts)
      |> Enum.map(fn {_file, pid} ->
        bound = Kvstore.LSMPart.first(pid)
        # Logger.info("LSMLevel | Bounds for LSMLevel file: #{file}, bound: #{bound}")
        bound
      end)

    Logger.info(
      "LSMLevel | Files in LSMLevel #{level}: #{inspect(files)}, with bounds: #{inspect(bounds)}"
    )

    {:ok, Map.merge(args, %{files: files, parts: parts, bounds: bounds})}
  end

  def terminate(reason, state) do
    Logger.info("LSMLevel | Terminating LSMLevel due to: #{inspect(reason)}")

    Enum.each(state.parts, fn pid ->
      DynamicSupervisor.terminate_child(Kvstore.LSMPartSupervisor.name(state.node_id), pid)
    end)
  end

  def handle_call({:get_part, _key}, _from, %{files: []} = state) do
    {:reply, nil, state}
  end

  def handle_call({:get_part, key}, _from, %{files: ["a"], parts: [pid], bounds: [first]} = state) do
    if key >= first do
      {:reply, pid, state}
    else
      {:reply, nil, state}
    end
  end

  def handle_call(
        {:get_part, key},
        _from,
        %{files: files, level: level, bounds: bounds, parts: parts} = state
      ) do
    # Logger.debug("Reading LSMLevel key: #{key}")

    b1 = Enum.take(bounds, Enum.count(bounds) - 1)
    b2 = tl(bounds)

    pairs = Enum.zip(b1, b2)

    enumed_pairs = Enum.with_index(pairs)

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
              {:cont, i + 1}

            [^key, _, _] ->
              {:cont, nil}
          end
        end
      )

    case i do
      nil ->
        {:reply, nil, state}

      i ->
        # Logger.debug("Key in range for LSMLevel #{level}, file: #{Enum.at(files, i)}")
        {:reply, Enum.at(parts, i), state}
    end
  end

  def handle_call({:list_parts}, _from, %{files: files, bounds: bounds} = state) do
    {:reply, Enum.zip(files, bounds), state}
  end
end

defmodule Kvstore.LSMLevelSupervisor do
  use DynamicSupervisor

  def name(id) do
    String.to_atom("lsm_level_supervisor_#{id}")
  end

  def start_link(args) do
    DynamicSupervisor.start_link(__MODULE__, args, name: name(args.node_id))
  end

  def init(_init_arg) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
