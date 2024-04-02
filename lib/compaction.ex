defmodule Kvstore.Compaction do
  def add_sst() do
    GenServer.cast(:compaction, :add_sst)
  end
end

defmodule Kvstore.CompactionG do
  use GenServer

  require Logger

  @ssts_path "db/sst"
  @level0_path "db/lsm/0"
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
    sst_data = get_sst_data()
    lsm_data = get_lsm_data()

    combine_sst_and_lsm_keys(sst_data, lsm_data)
  end

  defp combine_sst_and_lsm_keys(sst_data, lsm_data) do
    combine(sst_data, lsm_data, :both, nil, nil)
  end

  defp combine([], [], _, _, _) do
    Logger.info("Finished compacting SSTables")
  end

  defp combine([], lsm_data, _, key, value) do
    Logger.info("Writing remaining lsm data to LSM Level 0, key: #{key}, value: #{value}")
    write_to_level0(key, value)
    {data, new_key, new_value} = next_from_lsm(lsm_data)
    combine([], data, :lsm, new_key, new_value)
  end

  defp combine(sst_data, [], _, _, _) do
    {data, key, value} = next_from_sst(sst_data)
    Logger.info("Writing remaining sst data to LSM Level 0, key: #{key}, value: #{value}")
    write_to_level0(key, value)
    combine(data, [], :sst, nil, nil)
  end

  defp combine(sst_data, lsm_data, pull, prev_key, prev_val) do
    {{sst_data, sst_key, sst_value}, {lsm_data, lsm_key, lsm_value}} =
      case pull do
        :sst ->
          {next_from_sst(sst_data), {lsm_data, prev_key, prev_val}}

        :lsm ->
          {{sst_data, prev_key, prev_val}, next_from_lsm(lsm_data)}

        :both ->
          {next_from_sst(sst_data), next_from_lsm(lsm_data)}
      end

    case {sst_key, lsm_key} do
      {key, key} ->
        Logger.info("Keys are equal, writing sst key: #{sst_key} to LSM Level 0")
        write_to_level0(key, sst_value)
        combine(sst_data, lsm_data, :both, nil, nil)

      {s, l} when s < l ->
        Logger.info("Writing sst key: #{sst_key} to LSM Level 0")
        write_to_level0(sst_key, sst_value)
        combine(sst_data, lsm_data, :sst, lsm_key, lsm_value)

      {s, l} when s > l ->
        Logger.info("Writing lsm key: #{lsm_key} to LSM Level 0")
        write_to_level0(lsm_key, lsm_value)
        combine(sst_data, lsm_data, :lsm, sst_key, sst_value)
    end
  end

  defp write_to_level0(key, value) do
    IO.puts("[[[#{inspect(key)},#{inspect(value)}]]]")
  end

  defp next_from_lsm([]) do
    {[], nil, nil}
  end

  defp next_from_lsm(data) do
    [stream | data_tail] = data

    case Enum.take(stream, 1) do
      [] ->
        case data_tail do
          [] ->
            {[], nil, nil}

          [next_stream | _] ->
            [item] = Enum.take(next_stream, 1)
            [key, value] = String.split(item, ",")
            {data_tail, key, value}
        end

      [item] ->
        [key, value] = String.split(item, ",")
        {data, key, value}
    end
  end

  defp get_lsm_data() do
    files =
      File.ls!(@level0_path)
      |> Enum.sort()

    file_descriptors =
      files
      |> Enum.map(&File.open(@level0_path <> "/" <> &1, [:read]))
      |> Enum.map(fn {:ok, fd} -> fd end)

    Enum.map(file_descriptors, &IO.stream(&1, :line))
  end

  defp get_sst_data() do
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

    Enum.zip(elements, streams)
  end

  defp next_from_sst([]) do
    {[], nil, nil}
  end

  defp next_from_sst(data) do
    {[key, _], _} =
      data
      |> Enum.min_by(fn {[k, _], _} -> k end)

    {[_, value], _} =
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
end
