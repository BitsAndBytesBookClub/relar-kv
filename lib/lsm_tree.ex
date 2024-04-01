defmodule Kvstore.LSMTree do
  def get_levels() do
    GenServer.call(Kvstore.LSMTreeG, {:get_levels})
  end
end

defmodule Kvstore.LSMTreeG do
  use GenServer

  require Logger

  @path "db/lsm"

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def init(_) do
    levels =
      File.ls!(@path)
      |> Enum.map(&String.to_integer/1)
      |> Enum.sort()
      |> Enum.map(&Integer.to_string/1)

    Enum.each(levels, fn level ->
      Logger.info("Starting LSMLevel: #{level}")

      DynamicSupervisor.start_child(
        Kvstore.LSMLevelSupervisor,
        {Kvstore.LSMLevelG, %{level: level}}
      )
    end)

    {
      :ok,
      %{
        levels: levels
      }
    }
  end

  def handle_call({:get_levels}, _from, %{levels: levels} = state) do
    Logger.debug("Listing LSM levels: #{inspect(levels)}")

    {:reply, levels, state}
  end
end
