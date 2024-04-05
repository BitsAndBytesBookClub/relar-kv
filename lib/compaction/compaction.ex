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
    sst_data = Compaction.SSTReader.data(sst_files, @ssts_path)
    lsm_data = Compaction.LSMReader.data(@level0_path)
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
    {data, new_key, new_value} = Compaction.LSMReader.next(lsm_data)

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
    {data, key, value} = Compaction.SSTReader.next(sst_data)
    Logger.info("Writing remaining sst data to LSM Level 0, key: #{key}, value: #{value}")
    write_data = Compaction.Writer.write(write_data, key, value)
    combine(write_data, data, [], :sst, nil, nil)
  end

  defp combine(write_data, sst_data, lsm_data, pull, prev_key, prev_val) do
    {{sst_data, sst_key, sst_value}, {lsm_data, lsm_key, lsm_value}} =
      case pull do
        :sst ->
          {Compaction.SSTReader.next(sst_data), {lsm_data, prev_key, prev_val}}

        :lsm ->
          {{sst_data, prev_key, prev_val}, Compaction.LSMReader.next(lsm_data)}

        :both ->
          {Compaction.SSTReader.next(sst_data), Compaction.LSMReader.next(lsm_data)}
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
    {data, key, value} = Compaction.LSMReader.next(data)

    case key do
      nil ->
        []

      _ ->
        Logger.info("Draining lsm key: #{key} to LSM Level 0")
        write_data = Compaction.Writer.write(write_data, key, value)
        drain_lsm(write_data, data)
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
