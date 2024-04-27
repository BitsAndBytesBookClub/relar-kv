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
    children =
      Enum.map(1..args.partitions, fn partition_id ->
        partition_args = %{
          node_id: args.id,
          partition_id: partition_id,
          db_path: "#{args.db_path}/partition_#{partition_id}"
        }

        Supervisor.child_spec({Kvstore.Partition, partition_args},
          id: String.to_atom("#{args.id}_#{partition_id}")
        )
      end)

    opts = [strategy: :one_for_one, name: @name]
    Supervisor.init(children, opts)
  end
end
