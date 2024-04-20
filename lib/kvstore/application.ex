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
      {Kvstore.LSMTreeG, []},
      {Kvstore.SSTFileSupervisor, []},
      {Kvstore.SSTListG, "db/sst"},
      {Kvstore.MemetableG, []},
      {Kvstore.CompactionG, []},
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
