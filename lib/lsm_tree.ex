defmodule Kvstore.LSMTree do
  def get_levels() do
    GenServer.call(Kvstore.LSMTreeG, {:get_levels})
  end

  def update_level(level) when is_integer(level) do
    GenServer.call(Kvstore.LSMTreeG, {:update_level, Integer.to_string(level)})
  end

  def update_level(level) when is_binary(level) do
    GenServer.call(Kvstore.LSMTreeG, {:update_level, level})
  end
end

defmodule Kvstore.LSMTreeG do
  use GenServer

  require Logger

  @path "db/lsm"
  @compaction_path "db/compacted"

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def init(_) do
    levels =
      File.ls!(@path)
      |> Enum.map(&String.to_integer/1)
      |> Enum.sort()
      |> Enum.map(&Integer.to_string/1)

    pids =
      Enum.map(levels, fn level ->
        Logger.info("LSMTree | Starting LSMLevel: #{level}")

        {:ok, pid} =
          DynamicSupervisor.start_child(
            Kvstore.LSMLevelSupervisor,
            {Kvstore.LSMLevelG, %{level: level, iteration: 0, path: @path}}
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

  def handle_call({:update_level, level}, _from, %{levels: levels} = state) do
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
            {Kvstore.LSMLevelG, %{level: level, iteration: 0, path: @compaction_path}}
          )

        {:noreply, %{state | levels: levels ++ [{level, 0, new_pid}]}}

      {{_, iteration, pid}, _} ->
        Logger.info("LSMTree | Stopping LSMLevel: #{level}, iteration: #{iteration}")
        DynamicSupervisor.terminate_child(Kvstore.LSMLevelSupervisor, pid)

        {:ok, new_pid} =
          DynamicSupervisor.start_child(
            Kvstore.LSMLevelSupervisor,
            {Kvstore.LSMLevelG, %{level: level, iteration: iteration + 1, path: @compaction_path}}
          )

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
end
