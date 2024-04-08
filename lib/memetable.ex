defmodule Kvstore.Memetable do
  def set(key, value) do
    GenServer.call(Kvstore.MemetableG, {:set, key, value})
  end

  def get(key) do
    result =
      GenServer.call(Kvstore.MemetableG, {:memtables})
      |> Enum.reduce_while(nil, fn table, _ ->
        case :ets.lookup(table, key) do
          nil -> {:cont, nil}
          v -> {:halt, v}
        end
      end)

    case result do
      [] -> nil
      [{^key, value}] -> value
    end
  end

  def done_writing() do
    GenServer.cast(Kvstore.MemetableG, {:done_writing})
  end
end

defmodule Kvstore.MemetableG do
  use GenServer

  require Logger

  @table_prefix :memetable
  @memetable_path "db/memetable"
  @max_size 100

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def add_integer_to_atom(atom, integer) do
    atom_as_string = Atom.to_string(atom)
    integer_as_string = Integer.to_string(integer)
    new_atom_string = atom_as_string <> integer_as_string
    String.to_atom(new_atom_string)
  end

  def init(_) do
    id = 0

    table = create_new_table(id)

    count =
      case File.read(@memetable_path) do
        {:ok, _} -> load_memetable(table)
        {:error, _} -> 0
      end

    {:ok, file_descriptor} = File.open(@memetable_path, [:write, :append])

    {:ok,
     %{
       f: file_descriptor,
       count: count,
       table: table,
       id: id,
       old_table: nil,
       called_roll: false
     }}
  end

  def handle_call({:memtables}, _from, %{table: table, old_table: ot} = state) do
    case ot do
      nil -> {:reply, [table], state}
      _ -> {:reply, [table, ot], state}
    end
  end

  def handle_call(
        {:set, key, value},
        _from,
        %{f: fd, count: count, table: table, old_table: old_table, called_roll: called_roll} =
          state
      ) do
    :ok = IO.write(fd, "#{key},#{value}\n")
    true = :ets.insert(table, {key, value})

    case [count, old_table, called_roll] do
      [n, nil, false] when n > @max_size ->
        GenServer.cast(__MODULE__, {:roll})
        {:reply, :ok, %{state | count: count + 1, called_roll: true}}

      _ ->
        {:reply, :ok, %{state | count: count + 1}}
    end
  end

  def handle_cast({:roll}, %{f: fd, table: table, id: id}) do
    Logger.info("Rolling memetable #{id}")
    new_table = create_new_table(id + 1)
    write_memetable_to_sst(table)
    :ok = :file.sync(fd)
    :ok = File.close(fd)
    :ok = File.rm(@memetable_path)
    {:ok, file_descriptor} = File.open(@memetable_path, [:write, :append])

    {:noreply,
     %{
       f: file_descriptor,
       count: 0,
       table: new_table,
       old_table: table,
       id: id + 1,
       called_roll: false
     }}
  end

  def handle_cast({:done_writing}, %{old_table: ot} = state) do
    Logger.info("Deleting old memetable")
    true = :ets.delete(ot)
    {:noreply, %{state | old_table: nil}}
  end

  defp load_memetable(table) do
    File.stream!(@memetable_path)
    |> Stream.map(&String.trim(&1, "\n"))
    |> Stream.map(&String.split(&1, ","))
    |> Enum.each(fn [key, value] -> :ets.insert(table, {key, value}) end)

    :ets.info(table, :size)
  end

  defp create_new_table(id) do
    :ets.new(add_integer_to_atom(@table_prefix, id), [:ordered_set, :public, :named_table])
  end

  defp write_memetable_to_sst(table) do
    Kvstore.SSTWriter.write(table)
  end
end
