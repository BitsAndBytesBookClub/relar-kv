defmodule KV do
  require Logger

  def set(key, value) do
    GenServer.call({:global, handler()}, {:set, key, value})
  end

  def get(key) do
    GenServer.call({:global, handler()}, {:get, key})
  end

  defp handler() do
    Enum.random(handlers(Node.self()))
  end

  defp handlers(:nonode@nohost), do: [:h1, :h2]

  defp handlers(this_node) do
    [this_node | Node.list()]
    |> Enum.map(fn node ->
      "node" <> node_id =
        node
        |> Atom.to_string()
        |> String.split("@")
        |> List.first()

      "h" <> node_id
    end)
    |> dbg()
  end

  def add_random_keys(n) do
    for _ <- 1..n do
      KV.set(Random.key(), Random.value())
    end
  end

  def write_read_random_keys(n) do
    data =
      for _ <- 1..n do
        key = Random.key()
        value = Random.value()
        {key, value}
      end

    Enum.each(data, fn {key, value} ->
      KV.set(key, value)
    end)

    data
    |> Enum.with_index()
    |> Enum.reduce_while(nil, fn {{key, value}, i}, _ ->
      v = KV.get(key)

      case v do
        ^value ->
          {:cont, "!!!WORKED!!!"}

        _ ->
          {:halt, "Failed #{key}(#{i}), got #{v}, wanted #{value}"}
      end
    end)
  end

  def do_the_test(n, i) do
    keys =
      for nn <- 1..n do
        Integer.to_string(nn)
      end

    for _ <- 1..i do
      increment(keys)
    end

    str_i = Integer.to_string(i)

    keys
    |> Enum.map(fn key ->
      case KV.get(key) do
        nil -> "Key not"
        ^str_i -> "good"
        v -> "#{v}"
      end
    end)
    |> Enum.reduce(%{}, fn v, acc ->
      Map.update(acc, v, 1, &(&1 + 1))
    end)
  end

  def increment(keys) do
    keys
    |> Enum.each(fn key ->
      case KV.get(key) do
        nil ->
          KV.set(key, "1")

        v ->
          KV.set(key, Integer.to_string(String.to_integer(v) + 1))
      end
    end)
  end
end

defmodule Kvstore.Handler do
  use GenServer

  require Logger

  def start_link(%{name: n}) do
    GenServer.start_link(__MODULE__, %{}, name: {:global, n})
  end

  def init(_args) do
    {:ok, %{}}
  end

  def from_sst(key) do
    node = Kvstore.Key.node(key)

    Kvstore.SSTList.list(node)
    |> Enum.reduce_while(nil, fn sst, _ ->
      case Kvstore.SSTFile.get(sst, key) do
        nil -> {:cont, nil}
        v -> {:halt, v}
      end
    end)
  end

  def from_lsm(node_id, key) do
    Kvstore.LSMTree.get_levels(node_id)
    |> Enum.reduce_while(nil, fn level, _ ->
      val =
        level
        |> Kvstore.LSMLevel.get_part(key)
        |> Kvstore.LSMPart.get(key)

      case val do
        nil -> {:cont, nil}
        v -> {:halt, v}
      end
    end)
  end

  def handle_call({:get, key}, _from, state) do
    node = Kvstore.Key.node(key)
    val = Kvstore.Memetable.get(node, key)

    val =
      case val do
        nil ->
          # Logger.info("Key not found in memetable: #{key}")

          from_sst(key)

        v ->
          v
      end

    val =
      case val do
        nil ->
          # Logger.info("Key not found in SSTables: #{key}")

          node = Kvstore.Key.node(key)
          from_lsm(node, key)

        v ->
          v
      end

    {:reply, val, state}
  end

  def handle_call({:set, key, value}, _from, state) do
    node = Kvstore.Key.node(key)
    Kvstore.Memetable.set(node, key, value)
    {:reply, :ok, state}
  end
end

defmodule Random do
  @letters ~w(a b c d e f g h i j k l m n o p q r s t u v w x y z)

  def key() do
    Enum.reduce(1..3, "", fn _i, acc -> acc <> Enum.random(@letters) end)
  end

  def value() do
    Enum.reduce(1..10, "", fn _i, acc -> acc <> Enum.random(@letters) end)
  end
end

defmodule Kvstore.Key do
  def node(key) do
    first = String.at(key, 0)

    case first do
      ch when "0" <= ch and ch <= "2" -> 1
      ch when "3" <= ch and ch <= "5" -> 1
      ch when "6" <= ch and ch <= "9" -> 1
      ch when "a" <= ch and ch <= "h" -> 1
      ch when "i" <= ch and ch <= "p" -> 1
      ch when "q" <= ch and ch <= "z" -> 1
      _ -> 1
    end
  end
end
