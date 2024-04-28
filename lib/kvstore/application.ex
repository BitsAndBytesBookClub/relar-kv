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
          partitions = 3
          nodes = partitions

          [
            Supervisor.child_spec(
              {Kvstore.Node, %{id: 1, db_path: "db/node1", partitions: partitions}},
              id: :node1
            ),
            Supervisor.child_spec(
              {Kvstore.Node, %{id: 2, db_path: "db/node2", partitions: partitions}},
              id: :node2
            ),
            Supervisor.child_spec(
              {Kvstore.Node, %{id: 3, db_path: "db/node3", partitions: partitions}},
              id: :node3
            ),
            Supervisor.child_spec({Kvstore.Handler, %{name: :h1, nodes: nodes, current_node: 1}},
              id: :h1
            ),
            Supervisor.child_spec({Kvstore.Handler, %{name: :h2, nodes: nodes, current_node: 2}},
              id: :h2
            ),
            Supervisor.child_spec({Kvstore.Handler, %{name: :h3, nodes: nodes, current_node: 3}},
              id: :h3
            )
          ]

        node ->
          "node" <> node_id =
            node
            |> Atom.to_string()
            |> String.split("@")
            |> List.first()

          [
            Supervisor.child_spec(
              {Kvstore.Node,
               %{id: String.to_integer(node_id), db_path: "db/node#{node_id}", partitions: 3}},
              id: node_id
            ),
            Supervisor.child_spec(
              {Kvstore.Handler,
               %{name: String.to_atom("h#{node_id}"), nodes: 3, current_node: node_id}},
              id: String.to_atom("h#{node_id}")
            )
          ]
      end

    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: Kvstore.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
