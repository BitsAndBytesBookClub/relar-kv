defmodule Kvstore.LSMTree do
  @compaction_path "db/compacted/lsm"
  @lsm_path "db/lsm"

  def get_levels() do
    GenServer.call(Kvstore.LSMTreeG, {:get_levels})
  end

  def update_level_from_compaction(level) when is_integer(level) do
    GenServer.call(Kvstore.LSMTreeG, {:update_level, Integer.to_string(level), @compaction_path})
  end

  def update_level_from_compaction(level) when is_binary(level) do
    GenServer.call(Kvstore.LSMTreeG, {:update_level, level, @compaction_path})
  end

  def update_level_from_lsm(level) when is_integer(level) do
    GenServer.call(Kvstore.LSMTreeG, {:update_level, Integer.to_string(level), @lsm_path})
  end

  def update_level_from_lsm(level) when is_binary(level) do
    GenServer.call(Kvstore.LSMTreeG, {:update_level, level, @lsm_path})
  end
end

defmodule Kvstore.LSMTreeG do
  use GenServer

  require Logger

  def start_link(path) do
    GenServer.start_link(__MODULE__, path, name: __MODULE__)
  end

  def init(path) do
    Process.flag(:trap_exit, true)

    levels =
      File.ls!(path)
      |> Enum.map(&String.to_integer/1)
      |> Enum.sort()
      |> Enum.map(&Integer.to_string/1)

    pids =
      Enum.map(levels, fn level ->
        # Logger.info("LSMTree | Starting LSMLevel: #{level}")

        {:ok, pid} =
          DynamicSupervisor.start_child(
            Kvstore.LSMLevelSupervisor,
            {Kvstore.LSMLevelG, %{level: level, iteration: 0, path: path}}
          )

        pid
      end)

    iterations =
      Enum.map(pids, fn _ ->
        0
      end)

    {
      :ok,
      %{
        levels: Enum.zip([levels, iterations, pids])
      }
    }
  end

  def handle_call({:get_levels}, _from, %{levels: levels} = state) do
    Logger.debug("LSMTree | Listing LSM levels: #{inspect(levels)}")

    {:reply, levels, state}
  end

  def handle_call({:update_level, level, path}, _from, %{levels: levels} = state) do
    Logger.info("LSMTree | Updating LSMLevel: #{inspect(level)}, with levels: #{inspect(levels)}")

    found =
      levels
      |> Enum.with_index()
      |> Enum.find(levels, fn {{l, _, _}, _} -> l == level end)

    case found do
      nil ->
        Logger.info("LSMTree | Starting LSMLevel: #{level}")

        {:ok, new_pid} =
          DynamicSupervisor.start_child(
            Kvstore.LSMLevelSupervisor,
            {Kvstore.LSMLevelG, %{level: level, iteration: 0, path: path}}
          )

        {:noreply, %{state | levels: levels ++ [{level, 0, new_pid}]}}

      {{_, iteration, pid}, _} ->
        Logger.info("LSMTree | Stopping LSMLevel: #{level}, iteration: #{iteration}")

        args = %{level: level, iteration: iteration + 1, path: path}

        {:ok, new_pid} =
          DynamicSupervisor.start_child(
            Kvstore.LSMLevelSupervisor,
            Kvstore.LSMLevelG.child_spec(args)
          )

        :ok =
          DynamicSupervisor.terminate_child(Kvstore.LSMLevelSupervisor, pid)

        {:reply, :ok,
         %{
           state
           | levels:
               Enum.map(levels, fn {l, i, p} ->
                 if l == level do
                   {l, i + 1, new_pid}
                 else
                   {l, i, p}
                 end
               end)
         }}
    end
  end

  def terminate(_reason, %{levels: levels}) do
    Enum.each(levels, fn {_, _, pid} ->
      DynamicSupervisor.terminate_child(Kvstore.LSMLevelSupervisor, pid)
    end)
  end
end
