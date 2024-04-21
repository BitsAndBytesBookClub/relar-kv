defmodule Kvstore.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    case Mix.env() do
      :test ->
        File.rm_rf!("db/memetable")

        File.ls!("db/compacted")
        |> Enum.each(fn file -> File.rm_rf!("db/compacted/#{file}") end)

        File.ls!("db/sst")
        |> Enum.each(fn file -> File.rm!("db/sst/#{file}") end)

        File.ls!("db/lsm")
        |> Enum.each(fn dir ->
          File.ls!("db/lsm/#{dir}")
          |> Enum.each(fn file -> File.rm("db/lsm/#{dir}/#{file}") end)
        end)

      _ ->
        :ok
    end

    File.mkdir("db")
    File.mkdir("db/lsm")
    File.mkdir("db/lsm/0")
    File.mkdir("db/sst")
    File.mkdir("db/compacted")
    File.mkdir("db/trash")
    File.rm_rf!("db/trash/*")

    children = [
      {Kvstore.LSMPartSupervisor, []},
      {Kvstore.LSMLevelSupervisor, []},
      {Kvstore.LSMTreeG, %{lsm_path: "db/lsm", compaction_path: "db/compacted/lsm"}},
      {Kvstore.SSTFileSupervisor, []},
      {Kvstore.SSTListG, "db/sst"},
      {Kvstore.MemetableG,
       %{table_prefix: :memetable, memetable_path: "db/memetable", max_size: 100}},
      {Kvstore.CompactionG,
       %{
         max_ssts: 3,
         lsm_path: "db/lsm",
         sst_path: "db/sst",
         trash_path: "db/trash",
         level0_path: "db/lsm/0",
         new_level0_path: "db/compacted/lsm/0"
       }},
      {Kvstore.SSTWriterG, "db/sst"},
      Supervisor.child_spec({Kvstore.Handler, %{name: :h1}}, id: :h1),
      Supervisor.child_spec({Kvstore.Handler, %{name: :h2}}, id: :h2)
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Kvstore.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
