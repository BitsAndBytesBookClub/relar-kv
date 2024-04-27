defmodule Kvstore.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children =
      case Node.self() do
        :nonode@nohost ->
          [
            Supervisor.child_spec({Kvstore.Node, %{id: 1, db_path: "db/node1"}}, id: :node1),
            Supervisor.child_spec({Kvstore.Node, %{id: 2, db_path: "db/node2"}}, id: :node2),
            Supervisor.child_spec({Kvstore.Node, %{id: 3, db_path: "db/node3"}}, id: :node3),
            Supervisor.child_spec({Kvstore.Handler, %{name: :h1}}, id: :h1),
            Supervisor.child_spec({Kvstore.Handler, %{name: :h2}}, id: :h2)
          ]

        node ->
          "node" <> node_id =
            node
            |> Atom.to_string()
            |> String.split("@")
            |> List.first()

          [
            Supervisor.child_spec(
              {Kvstore.Node, %{id: String.to_integer(node_id), db_path: "db/node#{node_id}"}},
              id: String.to_atom("node#{node_id}")
            ),
            Supervisor.child_spec({Kvstore.Handler, %{name: String.to_atom("h#{node_id}")}},
              id: String.to_atom("h#{node_id}")
            )
          ]
      end

    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Kvstore.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
