defmodule Kvstore.Compaction do
  def add_sst() do
    GenServer.cast(:compaction, :add_sst)
  end
end

defmodule Kvstore.CompactionG do
  use GenServer

  require Logger

  @ssts_path "db/sst"
  @max_ssts 4

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: :compaction)
  end

  def init(_args) do
    ssts = Enum.count(File.ls!(@ssts_path))

    if ssts > @max_ssts do
      GenServer.cast(:compaction, :add_sst)
    end

    {:ok, %{ssts: ssts}}
  end

  def handle_cast(:add_sst, %{ssts: ssts} = state) do
    if ssts < @max_ssts do
      {:noreply, %{state | ssts: ssts + 1}}
    else
      Logger.info("Compacting SSTables")
      compact_ssts_into_level0()
      {:noreply, %{state | ssts: 0}}
    end
  end

  def compact_ssts_into_level0 do
    ssts =
      File.ls!(@ssts_path)
      |> Enum.map(&String.to_integer/1)
      |> Enum.sort()
      |> Enum.map(&Integer.to_string/1)

    file_descriptors =
      ssts
      |> Enum.map(&File.open(@ssts_path <> "/" <> &1, [:read]))
      |> Enum.map(fn {:ok, fd} -> fd end)

    streams = Enum.map(file_descriptors, &IO.stream(&1, :line))

    elements =
      Enum.map(streams, fn stream ->
        stream
        |> Enum.take(1)
        |> hd
        |> String.split(",")
      end)

    data = Enum.zip(elements, streams)

    keys = keys(data)

    IO.inspect(keys)
  end

  defp keys([]), do: []

  defp keys(data) do
    {data, key, _} = next_from_sst(data)
    [key | keys(data)]
  end

  defp next_from_sst(data) do
    {[key, _], _} =
      data
      |> Enum.min_by(fn {[k, _], _} -> k end)

    value =
      data
      |> Enum.filter(fn {[k, _], _} -> k == key end)
      |> List.last()

    data =
      data
      |> Enum.with_index()
      |> Enum.reduce(data, fn {{[k, _], stream}, idx}, acc ->
        case k == key do
          true ->
            case Enum.take(stream, 1) do
              [] -> List.replace_at(acc, idx, nil)
              [item] -> List.replace_at(acc, idx, {String.split(item, ","), stream})
            end

          false ->
            acc
        end
      end)

    data = Enum.filter(data, fn x -> x != nil end)

    {data, key, value}
  end

  defp next_from_level0() do
  end
end
