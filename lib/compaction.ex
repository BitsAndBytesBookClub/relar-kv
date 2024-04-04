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
      Kvstore.Compaction.SSTToLevel0.compact()
      {:noreply, %{state | ssts: 0}}
    end
  end
end

defmodule Kvstore.Compaction.SSTToLevel0 do
  require Logger

  @ssts_path "db/sst"
  @level0_path "db/lsm/0"
  @new_level0_path "db/compacted/0"

  def compact() do
    Logger.info("Compacting SSTables")
    sst_files = File.ls!(@ssts_path)
    do_the_compaction(sst_files)

    Kvstore.LSMTree.update_level(0)

    Kvstore.TrashBin.empty()

    File.rename!(@level0_path, Kvstore.TrashBin.path() <> "/" <> "0")
    File.rename!(@new_level0_path, @level0_path)

    Kvstore.TrashBin.empty()

    Kvstore.SSTList.remove(sst_files)

    Logger.info("Finished compacting SSTables")
  end

  def do_the_compaction(sst_files) do
    sst_data = get_sst_data(sst_files)
    lsm_data = get_lsm_data()
    write_data = Compaction.Writer.data(@new_level0_path)

    combine_sst_and_lsm_keys(sst_data, lsm_data, write_data)
  end

  defp combine_sst_and_lsm_keys(sst_data, lsm_data, write_data) do
    combine(write_data, sst_data, lsm_data, :both, nil, nil)
  end

  defp combine(_, [], [], _, _, _) do
    Logger.info("Finished compacting SSTables")
  end

  defp combine(write_data, [], lsm_data, _, key, value) do
    Logger.info("No more SST data")
    {data, new_key, new_value} = next_from_lsm(lsm_data)

    case {key, new_key} do
      {_, nil} ->
        Logger.info("No more lsm data")

      {^key, ^key} ->
        Logger.info("Last key (#{key}) from SST was equal, writing to LSM Level 0")
        write_data = Compaction.Writer.write(write_data, key, value)
        combine(write_data, [], data, :lsm, new_key, new_value)

      {^key, ^new_key} when key < new_key ->
        Logger.info("Writing old key: #{key}, lost to #{new_key} to LSM Level 0")
        write_data = Compaction.Writer.write(write_data, key, value)
        write_data = Compaction.Writer.write(write_data, new_key, new_value)

        drain_lsm(write_data, data)

      {^key, ^new_key} when key > new_key ->
        Logger.info("Writing lsm key: #{new_key} lost to #{key} to LSM Level 0")
        write_data = Compaction.Writer.write(write_data, new_key, new_value)
        combine(write_data, [], data, :lsm, key, value)
    end
  end

  defp combine(write_data, sst_data, [], _, _, _) do
    {data, key, value} = next_from_sst(sst_data)
    Logger.info("Writing remaining sst data to LSM Level 0, key: #{key}, value: #{value}")
    write_data = Compaction.Writer.write(write_data, key, value)
    combine(write_data, data, [], :sst, nil, nil)
  end

  defp combine(write_data, sst_data, lsm_data, pull, prev_key, prev_val) do
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
        write_data = Compaction.Writer.write(write_data, key, sst_value)
        combine(write_data, sst_data, lsm_data, :both, nil, nil)

      {s, l} when s < l ->
        Logger.info("Writing sst key: #{sst_key}, lost to #{lsm_key} to LSM Level 0")
        write_data = Compaction.Writer.write(write_data, sst_key, sst_value)
        combine(write_data, sst_data, lsm_data, :sst, lsm_key, lsm_value)

      {s, l} when s > l ->
        Logger.info("Writing lsm key: #{lsm_key} lost to #{sst_key} to LSM Level 0")
        write_data = Compaction.Writer.write(write_data, lsm_key, lsm_value)
        combine(write_data, sst_data, lsm_data, :lsm, sst_key, sst_value)
    end
  end

  defp drain_lsm(_, []) do
    nil
  end

  defp drain_lsm(write_data, data) do
    {data, key, value} = next_from_lsm(data)

    case key do
      nil ->
        []

      _ ->
        Logger.info("Draining lsm key: #{key} to LSM Level 0")
        write_data = Compaction.Writer.write(write_data, key, value)
        drain_lsm(write_data, data)
    end
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

  defp get_sst_data(files) do
    ssts =
      files
      |> Enum.map(&String.to_integer/1)
      |> Enum.sort()
      |> Enum.map(&Integer.to_string/1)

    file_descriptors =
      ssts
      |> Enum.map(&File.open(@ssts_path <> "/" <> &1, [:read]))
      |> Enum.map(fn {:ok, fd} -> fd end)
      |> dbg()

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

defmodule Compaction.Writer do
  def data(path) do
    File.mkdir_p!(path)

    {:ok, fd} = File.open(path <> "/a", [:write, :utf8])
    {fd, 0, "a", path}
  end

  def write(data, nil, nil) do
    data
  end

  def write({fd, count, letter, path}, key, value) do
    case count > 10 do
      true ->
        File.close(fd)
        next_letter = NextLetter.get_next_letter(letter)
        {:ok, fd} = File.open(path <> "/#{next_letter}", [:write, :utf8])
        IO.write(fd, "#{key},#{value}")
        {fd, 1, next_letter, path}

      false ->
        IO.write(fd, "#{key},#{value}")
        {fd, count + 1, letter, path}
    end
  end
end

defmodule NextLetter do
  def get_next_letter(letter) when byte_size(letter) == 1 do
    [char_code] = String.to_charlist(letter)
    next_char_code = char_code + 1
    List.to_string([next_char_code])
  end
end
