defmodule Kvstore.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      # Starts a worker by calling: Kvstore.Worker.start_link(arg)
      {Kvstore.MemetableG, []},
      Supervisor.child_spec({Kvstore.Handler, %{name: :h1}}, id: :h1),
      Supervisor.child_spec({Kvstore.Handler, %{name: :h2}}, id: :h2)
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Kvstore.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
