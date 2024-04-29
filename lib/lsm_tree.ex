defmodule Kvstore.LSMTree do
  def get_levels(node_id) do
    GenServer.call({:global, Kvstore.LSMTreeG.name(node_id)}, {:get_levels})
  end

  def update_level_from_compaction(node_id, level) when is_integer(level) do
    GenServer.call(
      {:global, Kvstore.LSMTreeG.name(node_id)},
      {:update_level_compaction, Integer.to_string(level)}
    )
  end

  def update_level_from_compaction(node_id, level) when is_binary(level) do
    GenServer.call({:global, Kvstore.LSMTreeG.name(node_id)}, {:update_level_compaction, level})
  end

  def update_level_from_lsm(node_id, level) when is_integer(level) do
    GenServer.call(
      {:global, Kvstore.LSMTreeG.name(node_id)},
      {:update_level_lsm, Integer.to_string(level)}
    )
  end

  def update_level_from_lsm(node_id, level) when is_binary(level) do
    GenServer.call({:global, Kvstore.LSMTreeG.name(node_id)}, {:update_level_lsm, level})
  end
end

defmodule Kvstore.LSMTreeG do
  use GenServer

  require Logger

  def name(id) do
    String.to_atom("lsm_tree_#{id}")
  end

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: {:global, name(args.node_id)})
  end

  def init(args) do
    Process.flag(:trap_exit, true)

    levels =
      File.ls!(args.lsm_path)
      |> Enum.map(&String.to_integer/1)
      |> Enum.sort()
      |> Enum.map(&Integer.to_string/1)

    pids =
      Enum.map(levels, fn level ->
        # Logger.info("LSMTree | Starting LSMLevel: #{level}")

        {:ok, pid} =
          DynamicSupervisor.start_child(
            Kvstore.LSMLevelSupervisor.name(args.node_id),
            {Kvstore.LSMLevelG,
             %{level: level, iteration: 0, path: args.lsm_path, node_id: args.node_id}}
          )

        pid
      end)

    iterations =
      Enum.map(pids, fn _ ->
        0
      end)

    {
      :ok,
      Map.merge(
        args,
        %{
          levels: Enum.zip([levels, iterations, pids])
        }
      )
    }
  end

  def handle_call({:get_levels}, _from, %{levels: levels} = state) do
    # Logger.debug("LSMTree | Listing LSM levels: #{inspect(levels)}")

    {:reply, levels, state}
  end

  def handle_call({:update_level_compaction, level}, _, state) do
    levels = update_level(level, state.levels, state.compaction_path, state.node_id)

    {:reply, :ok, %{state | levels: levels}}
  end

  def handle_call({:update_level_lsm, level}, _, state) do
    levels = update_level(level, state.levels, state.lsm_path, state.node_id)

    {:reply, :ok, %{state | levels: levels}}
  end

  defp update_level(level, levels, path, node_id) do
    Logger.info("LSMTree | Updating LSMLevel: #{inspect(level)}, with levels: #{inspect(levels)}")

    found =
      levels
      |> Enum.find(fn {l, _, _} -> l == level end)

    Kvstore.Broadcast.broadcast({:update_lsm, level, levels, node_id})

    case found do
      nil ->
        Logger.info("LSMTree | Starting LSMLevel: #{level}")

        {:ok, new_pid} =
          DynamicSupervisor.start_child(
            Kvstore.LSMLevelSupervisor.name(node_id),
            {Kvstore.LSMLevelG, %{level: level, iteration: 0, path: path, node_id: node_id}}
          )

        levels ++ [{level, 0, new_pid}]

      {_, iteration, pid} ->
        Logger.info("LSMTree | Stopping LSMLevel: #{level}, iteration: #{iteration}")

        args = %{level: level, iteration: iteration + 1, path: path, node_id: node_id}

        {:ok, new_pid} =
          DynamicSupervisor.start_child(
            Kvstore.LSMLevelSupervisor.name(node_id),
            Kvstore.LSMLevelG.child_spec(args)
          )

        :ok =
          DynamicSupervisor.terminate_child(Kvstore.LSMLevelSupervisor.name(node_id), pid)

        Enum.map(levels, fn {l, i, p} ->
          if l == level do
            {l, i + 1, new_pid}
          else
            {l, i, p}
          end
        end)
    end
  end

  def terminate(_reason, %{levels: levels, node_id: node_id}) do
    Enum.each(levels, fn {_, _, pid} ->
      DynamicSupervisor.terminate_child(Kvstore.LSMLevelSupervisor.name(node_id), pid)
    end)
  end
end
