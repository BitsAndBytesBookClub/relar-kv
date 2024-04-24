defmodule Kvstore.Compaction do
  def add_sst(node_id) do
    :ok = GenServer.cast(Kvstore.CompactionG.name(node_id), :add_sst)
  end

  def add_lsm_file(node_id, level) do
    :ok = GenServer.cast(Kvstore.CompactionG.name(node_id), {:add_lsm_file, level})
  end
end

defmodule Kvstore.CompactionG do
  use GenServer

  require Logger

  def name(id) do
    String.to_atom("compaction_#{id}")
  end

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: name(args.node_id))
  end

  def init(args) do
    ssts = Enum.count(File.ls!(args.sst_path))

    if ssts > args.max_ssts do
      :ok = GenServer.cast(Kvstore.CompactionG.name(args.node_id), :add_sst)
    end

    lsms =
      File.ls!(args.lsm_path)
      |> Enum.map(fn dir -> {dir, Enum.count(File.ls!(args.lsm_path <> "/" <> dir))} end)

    Enum.each(lsms, fn {level, count} ->
      if count > 10 do
        :ok = GenServer.cast(Kvstore.CompactionG.name(args.node_id), {:add_lsm_file, level})
      end
    end)

    level0_compactor =
      Kvstore.Compaction.SSTToLevel0.init(
        args.trash_path,
        args.sst_path,
        args.level0_path,
        args.new_level0_path
      )

    {:ok,
     Map.merge(
       args,
       %{ssts: ssts, l0_compactor: level0_compactor}
     )}
  end

  def handle_cast(:add_sst, state) do
    ssts = Kvstore.SSTList.list(state.node_id)

    if Enum.count(ssts) < state.max_ssts do
      {:noreply, state}
    else
      Kvstore.Compaction.SSTToLevel0.compact(state.node_id, state.l0_compactor, ssts)
      :ok = GenServer.cast(Kvstore.CompactionG.name(state.node_id), {:add_lsm_file, "0"})
      {:noreply, state}
    end
  end

  def handle_cast({:add_lsm_file, level}, state) do
    count =
      (state.lsm_path <> "/" <> level)
      |> File.ls!()
      |> Enum.count()

    if count > 10 do
      next_level =
        level
        |> String.to_integer()
        |> Kernel.+(1)
        |> Integer.to_string()

      lsm_compactor =
        Kvstore.Compaction.LSM.init(
          state.node_id,
          state.lsm_path,
          state.trash_path,
          state.compacted_lsm_dir
        )

      Kvstore.Compaction.LSM.compact(lsm_compactor, level)
      :ok = GenServer.cast(Kvstore.CompactionG.name(state.node_id), {:add_lsm_file, next_level})
    end

    {:noreply, state}
  end
end

defmodule Kvstore.Compaction.LSM do
  require Logger

  defstruct path: nil, trash_path: nil, new_level_path: nil, node_id: nil

  def init(node_id, path, trash_path, new_level_path) do
    %__MODULE__{
      path: path <> "/",
      trash_path: trash_path,
      new_level_path: new_level_path,
      node_id: node_id
    }
  end

  def compact(cfg, level) do
    Logger.info("Compacting LSM Level #{level}")

    do_the_compaction(cfg, level)

    next_level =
      level
      |> String.to_integer()
      |> Kernel.+(1)
      |> Integer.to_string()

    Kvstore.LSMTree.update_level_from_compaction(cfg.node_id, next_level)

    Kvstore.TrashBin.empty(cfg.trash_path)

    File.rename!(cfg.path <> next_level, cfg.trash_path <> "/" <> next_level)
    File.rename!(cfg.new_level_path <> "/" <> next_level, cfg.path <> next_level)

    File.rename!(cfg.path <> level, cfg.trash_path <> "/" <> level)

    File.mkdir_p!(cfg.path <> level)

    Kvstore.TrashBin.empty(cfg.trash_path)

    Kvstore.LSMTree.update_level_from_lsm(cfg.node_id, level)

    Logger.info("Finished compacting LSM Level #{level}")
  end

  defp do_the_compaction(cfg, level) do
    lsm_a = Compaction.LSMReader.stream(cfg.path <> level)

    next_level =
      level
      |> String.to_integer()
      |> Kernel.+(1)
      |> Integer.to_string()

    File.mkdir_p!(cfg.path <> next_level)

    lsm_b = Compaction.LSMReader.stream(cfg.path <> next_level)

    next_level_i =
      level
      |> String.to_integer()

    max_count = :math.pow(10, next_level_i + 3)

    wd =
      Compaction.Writer.data(
        cfg.new_level_path <> "/#{next_level}",
        max_count
      )

    combine(wd, lsm_a, lsm_b, :both, nil, nil)

    Compaction.Writer.close(wd)
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
        wd = Compaction.Writer.write(wd, b_k, b_v)
        combine(wd, a, b, :b, nil, nil)

      {_, nil} ->
        wd = Compaction.Writer.write(wd, a_k, a_v)
        combine(wd, a, b, :a, nil, nil)

      {key, key} ->
        # Logger.info("Keys are equal, writing A key: #{a_k} to LSM Level")
        wd = Compaction.Writer.write(wd, key, a_v)
        combine(wd, a, b, :both, nil, nil)

      {a_key, b_key} when a_key < b_key ->
        # Logger.info("Writing A key: #{a_k}, lost to #{b_k} to LSM Level")
        wd = Compaction.Writer.write(wd, a_k, a_v)
        combine(wd, a, b, :a, b_k, b_v)

      {a_key, b_key} when a_key > b_key ->
        # Logger.info("Writing B key: #{b_k} lost to #{a_k} to LSM Level")
        wd = Compaction.Writer.write(wd, b_k, b_v)
        combine(wd, a, b, :b, a_k, a_v)
    end
  end
end

defmodule Kvstore.Compaction.SSTToLevel0 do
  require Logger

  defstruct trash_path: nil, sst_path: nil, level0_path: nil, new_level0_path: nil

  def init(trash_path, sst_path, level0_path, new_level0_path) do
    %__MODULE__{
      trash_path: trash_path,
      sst_path: sst_path,
      level0_path: level0_path,
      new_level0_path: new_level0_path
    }
  end

  def compact(node_id, cfg, sst_files) do
    Logger.info("Compacting SSTables")

    sst_files =
      sst_files
      |> Enum.map(&Atom.to_string/1)

    do_the_compaction(cfg, sst_files)

    Kvstore.LSMTree.update_level_from_compaction(node_id, 0)

    Kvstore.TrashBin.empty(cfg.trash_path)

    File.rename!(cfg.level0_path, cfg.trash_path <> "/" <> "0")
    File.rename!(cfg.new_level0_path, cfg.level0_path)

    Kvstore.TrashBin.empty(cfg.trash_path)

    Kvstore.SSTList.remove(node_id, sst_files)

    Logger.info("Finished compacting SSTables")
  end

  def do_the_compaction(cfg, sst_files) do
    sst_data = Compaction.SSTReader.data(sst_files, cfg.sst_path)
    lsm_data = Compaction.LSMReader.stream(cfg.level0_path)
    write_data = Compaction.Writer.data(cfg.new_level0_path)

    combine_sst_and_lsm_keys(sst_data, lsm_data, write_data)

    Compaction.Writer.close(write_data)
  end

  def combine_sst_and_lsm_keys(sst_data, lsm_data, write_data) do
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
        write_data = Compaction.Writer.write(write_data, lsm_key, lsm_value)
        combine(write_data, sst_data, lsm_data, :lsm, nil, nil)

      {_, nil} ->
        write_data = Compaction.Writer.write(write_data, sst_key, sst_value)
        combine(write_data, sst_data, lsm_data, :sst, nil, nil)

      {key, key} ->
        # Logger.info("Keys are equal, writing sst key: #{sst_key} to LSM Level 0")
        write_data = Compaction.Writer.write(write_data, key, sst_value)
        combine(write_data, sst_data, lsm_data, :both, nil, nil)

      {s, l} when s < l ->
        # Logger.info("Writing sst key: #{sst_key}, lost to #{lsm_key} to LSM Level 0")
        write_data = Compaction.Writer.write(write_data, sst_key, sst_value)
        combine(write_data, sst_data, lsm_data, :sst, lsm_key, lsm_value)

      {s, l} when s > l ->
        # Logger.info("Writing lsm key: #{lsm_key} lost to #{sst_key} to LSM Level 0")
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
