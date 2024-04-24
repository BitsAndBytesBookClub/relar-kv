defmodule Kvstore.Node do
  use Supervisor

  @name __MODULE__

  # Starts the supervisor
  def start_link(%{id: id} = args) do
    Supervisor.start_link(__MODULE__, args, name: String.to_atom("node_#{id}"))
  end

  # Initializes the supervisor
  @impl true
  def init(args) do
    db_path = args.db_path

    memetable_path = "#{db_path}/memetable"

    sst_path = "#{db_path}/sst"

    lsm_path = "#{db_path}/lsm"
    lsm0_path = "#{lsm_path}/0"

    compaction_path = "#{db_path}/compacted"
    lsm_compacted_path = "#{compaction_path}/lsm"
    new_lsm0_path = "#{lsm_compacted_path}/0"

    trash_path = "#{db_path}/trash"

    File.mkdir_p!(db_path)
    File.mkdir(lsm_path)
    File.mkdir(lsm0_path)
    File.mkdir(sst_path)
    File.mkdir(compaction_path)
    File.mkdir(trash_path)
    File.rm_rf!("#{compaction_path}/*")

    case Mix.env() do
      :test ->
        File.rm_rf!(memetable_path)

        File.ls!(compaction_path)
        |> Enum.each(fn file -> File.rm_rf!("#{compaction_path}/#{file}") end)

        File.ls!(sst_path)
        |> Enum.each(fn file -> File.rm!("#{sst_path}/#{file}") end)

        File.ls!(lsm_path)
        |> Enum.each(fn dir ->
          File.ls!("#{lsm_path}/#{dir}")
          |> Enum.each(fn file -> File.rm("#{lsm_path}/#{dir}/#{file}") end)
        end)

      _ ->
        :ok
    end

    children = [
      {Kvstore.LSMPartSupervisor, %{node_id: args.id}},
      {Kvstore.LSMLevelSupervisor, %{node_id: args.id}},
      {Kvstore.LSMTreeG,
       %{node_id: args.id, lsm_path: lsm_path, compaction_path: lsm_compacted_path}},
      {Kvstore.SSTFileSupervisor, %{node_id: args.id}},
      {Kvstore.SSTListG, %{node_id: args.id, path: sst_path}},
      {Kvstore.MemetableG,
       %{
         table_prefix: :memetable,
         memetable_path: memetable_path,
         max_size: 100,
         node_id: args.id
       }},
      {Kvstore.CompactionG,
       %{
         max_ssts: 3,
         lsm_path: lsm_path,
         sst_path: sst_path,
         trash_path: trash_path,
         level0_path: lsm0_path,
         new_level0_path: new_lsm0_path,
         compacted_lsm_dir: lsm_compacted_path,
         node_id: args.id
       }},
      {Kvstore.SSTWriterG, %{node_id: args.id, path: sst_path}}
    ]

    opts = [strategy: :one_for_one, name: @name]
    Supervisor.init(children, opts)
  end
end
