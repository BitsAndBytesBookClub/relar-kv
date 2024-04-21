defmodule Kvstore.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    db_path = "db"

    memetable_path = "#{db_path}/memetable"

    sst_path = "#{db_path}/sst"

    lsm_path = "#{db_path}/lsm"
    lsm0_path = "#{lsm_path}/0"

    compaction_path = "#{db_path}/compacted"
    lsm_compacted_path = "#{compaction_path}/lsm"
    new_lsm0_path = "#{lsm_compacted_path}/0"

    trash_path = "#{db_path}/trash"

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

    File.mkdir(db_path)
    File.mkdir(lsm_path)
    File.mkdir(lsm0_path)
    File.mkdir(sst_path)
    File.mkdir(compaction_path)
    File.mkdir(trash_path)
    File.rm_rf!("#{compaction_path}/*")

    children = [
      {Kvstore.LSMPartSupervisor, []},
      {Kvstore.LSMLevelSupervisor, []},
      {Kvstore.LSMTreeG, %{lsm_path: lsm_path, compaction_path: lsm_compacted_path}},
      {Kvstore.SSTFileSupervisor, []},
      {Kvstore.SSTListG, sst_path},
      {Kvstore.MemetableG,
       %{table_prefix: :memetable, memetable_path: memetable_path, max_size: 100}},
      {Kvstore.CompactionG,
       %{
         max_ssts: 3,
         lsm_path: lsm_path,
         sst_path: sst_path,
         trash_path: trash_path,
         level0_path: lsm0_path,
         new_level0_path: new_lsm0_path,
         compacted_lsm_dir: lsm_compacted_path
       }},
      {Kvstore.SSTWriterG, sst_path},
      Supervisor.child_spec({Kvstore.Handler, %{name: :h1}}, id: :h1),
      Supervisor.child_spec({Kvstore.Handler, %{name: :h2}}, id: :h2)
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Kvstore.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
