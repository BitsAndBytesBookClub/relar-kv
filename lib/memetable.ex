defmodule Kvstore.Memetable do
  def set(key, value) do
    GenServer.call(Kvstore.MemetableG, {:set, key, value})
  end

  def get(key) do
    result = :ets.lookup(:memetable, key)

    case result do
      [] -> nil
      [{^key, value}] -> value
    end
  end
end

defmodule Kvstore.MemetableG do
  use GenServer

  @table :memetable
  @memetable_path "db/memetable"
  @max_size 4

  def start_link(_) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def init(_) do
    :ets.new(@table, [:ordered_set, :protected, :named_table])

    count =
      case File.read(@memetable_path) do
        {:ok, _} -> load_memetable()
        {:error, _} -> 0
      end

    {:ok, file_descriptor} = File.open(@memetable_path, [:write, :append])
    {:ok, %{f: file_descriptor, count: count}}
  end

  def handle_call({:set, key, value}, _from, %{f: file, count: count} = state) do
    :ok = IO.write(file, "#{key},#{value}\n")
    true = :ets.insert(@table, {key, value})

    case count do
      @max_size -> GenServer.cast(__MODULE__, {:roll})
      _ -> :ok
    end

    {:reply, :ok, %{state | count: count + 1}}
  end

  def handle_cast({:roll}, %{f: file}) do
    write_memetable_to_file()
    :ok = File.close(file)
    :ok = File.rm(@memetable_path)
    true = :ets.delete_all_objects(@table)
    {:ok, file_descriptor} = File.open(@memetable_path, [:write, :append])
    {:noreply, %{f: file_descriptor, count: 0}}
  end

  defp load_memetable do
    File.stream!(@memetable_path)
    |> Stream.map(&String.trim(&1, "\n"))
    |> Stream.map(&String.split(&1, ","))
    |> Enum.each(fn [key, value] -> :ets.insert(@table, {key, value}) end)

    :ets.info(@table, :size)
  end

  defp write_memetable_to_file do
    IO.puts("Writing memetable to file")
  end
end
