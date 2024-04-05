defmodule Kvstore.Compaction do
  def add_sst() do
    GenServer.cast(:compaction, :add_sst)
  end

  def add_lsm_file(level) do
    GenServer.cast(:compaction, {:add_lsm_file, level})
  end
end

defmodule Kvstore.CompactionG do
  use GenServer

  require Logger

  @lsm_path "db/lsm"
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

    lsms =
      File.ls!(@lsm_path)
      |> Enum.map(fn dir -> {dir, Enum.count(File.ls!(@lsm_path <> "/" <> dir))} end)

    Enum.each(lsms, fn {level, count} ->
      if count > 10 do
        GenServer.cast(:compaction, {:add_lsm_file, level})
      end
    end)

    {:ok, %{ssts: ssts, lsms: lsms}}
  end

  def handle_cast(:add_sst, %{ssts: ssts} = state) do
    if ssts < @max_ssts do
      {:noreply, %{state | ssts: ssts + 1}}
    else
      Kvstore.Compaction.SSTToLevel0.compact()
      {:noreply, %{state | ssts: 0}}
    end
  end

  def handle_cast({:add_lsm_file, level}, %{lsms: lsms} = state) do
    {_, count} = Enum.find(lsms, fn {l, _} -> l == level end)

    new_count =
      if count > 10 do
        Kvstore.Compaction.LSM.compact(level)
        0
      else
        count + 1
      end

    lsms =
      Enum.map(lsms, fn {l, c} ->
        if l == level do
          {l, new_count}
        else
          {l, c}
        end
      end)

    {:noreply, %{state | lsms: lsms}}
  end
end

defmodule Kvstore.Compaction.LSM do
  require Logger

  @path "db/lsm/"

  def compact(level) do
    Logger.info("Compacting LSM Level #{level}")
    do_the_compaction(level)
  end

  defp do_the_compaction(level) do
    lsm_a = Compaction.LSMReader.stream(@path <> level)

    next_level =
      level
      |> String.to_integer()
      |> Kernel.+(1)
      |> Integer.to_string()

    File.mkdir_p!(@path <> next_level)

    lsm_b = Compaction.LSMReader.stream(@path <> next_level)

    wd = Compaction.Writer.data("db/compacted/lsm/#{next_level}")

    combine(wd, lsm_a, lsm_b, :both, nil, nil)
  end

  def combine(wd, a, b, pull, prev_k, prev_v) do
    {{a_k, a_v}, {b_k, b_v}} =
      case pull do
        :a ->
          {Compaction.LSMReader.next(a), {prev_k, prev_v}}

        :b ->
          {{prev_k, prev_v}, Compaction.LSMReader.next(b)}

        :both ->
          {Compaction.LSMReader.next(a), Compaction.LSMReader.next(b)}
      end

    case {a_k, b_k} do
      {nil, nil} ->
        Logger.info("Finished compacting LSM Level")

      {nil, _} ->
        Logger.info("No more A data")
        wd = Compaction.Writer.write(wd, b_k, b_v)
        combine(wd, a, b, :b, nil, nil)

      {_, nil} ->
        Logger.info("No more B data")
        wd = Compaction.Writer.write(wd, a_k, a_v)
        combine(wd, a, b, :a, nil, nil)

      {key, key} ->
        Logger.info("Keys are equal, writing A key: #{a_k} to LSM Level")
        wd = Compaction.Writer.write(wd, key, a_v)
        combine(wd, a, b, :both, nil, nil)

      {a, b} when a < b ->
        Logger.info("Writing A key: #{a_k}, lost to #{b_k} to LSM Level")
        wd = Compaction.Writer.write(wd, a_k, a_v)
        combine(wd, a, b, :a, b_k, b_v)

      {a, b} when a > b ->
        Logger.info("Writing B key: #{b_k} lost to #{a_k} to LSM Level")
        wd = Compaction.Writer.write(wd, b_k, b_v)
        combine(wd, a, b, :b, a_k, a_v)
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
    sst_data = Compaction.SSTReader.data(sst_files, @ssts_path)
    lsm_data = Compaction.LSMReader.stream(@level0_path)
    write_data = Compaction.Writer.data(@new_level0_path)

    combine_sst_and_lsm_keys(sst_data, lsm_data, write_data)
  end

  defp combine_sst_and_lsm_keys(sst_data, lsm_data, write_data) do
    combine(write_data, sst_data, lsm_data, :both, nil, nil)
  end

  defp combine(write_data, sst_data, lsm_data, pull, prev_key, prev_val) do
    {{sst_data, sst_key, sst_value}, {lsm_key, lsm_value}} =
      case pull do
        :sst ->
          {Compaction.SSTReader.next(sst_data), {prev_key, prev_val}}

        :lsm ->
          {{sst_data, prev_key, prev_val}, Compaction.LSMReader.next(lsm_data)}

        :both ->
          {Compaction.SSTReader.next(sst_data), Compaction.LSMReader.next(lsm_data)}
      end

    case {sst_key, lsm_key} do
      {nil, nil} ->
        Logger.info("Finished compacting SSTables")

      {nil, _} ->
        Logger.info("No more SST data")
        write_data = Compaction.Writer.write(write_data, lsm_key, lsm_value)
        combine(write_data, sst_data, lsm_data, :lsm, nil, nil)

      {_, nil} ->
        Logger.info("No more LSM data")
        write_data = Compaction.Writer.write(write_data, sst_key, sst_value)
        combine(write_data, sst_data, lsm_data, :sst, nil, nil)

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
end

defmodule NextLetter do
  def get_next_letter(letter) when byte_size(letter) == 1 do
    [char_code] = String.to_charlist(letter)
    next_char_code = char_code + 1
    List.to_string([next_char_code])
  end
end
