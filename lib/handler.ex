defmodule KV do
  require Logger

  def set(key, value) do
    GenServer.call(handler(), {:set, key, value})
  end

  def get(key) do
    GenServer.call(handler(), {:get, key})
  end

  defp handler() do
    Enum.random([:h1, :h2])
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

    Enum.reduce_while(data, nil, fn {key, value}, _ ->
      case KV.get(key) do
        ^value ->
          :timer.sleep(10)
          {:cont, nil}

        _ ->
          {:halt, key}
      end
    end)
  end
end

defmodule Kvstore.Handler do
  use GenServer

  require Logger

  def start_link(%{name: n}) do
    GenServer.start_link(__MODULE__, %{}, name: n)
  end

  def init(_args) do
    {:ok, %{}}
  end

  def from_sst(key) do
    Kvstore.SSTList.list()
    |> Enum.reduce_while(nil, fn sst, _ ->
      try do
        case Kvstore.SSTFile.get(sst, key) do
          nil -> {:cont, nil}
          v -> {:halt, v}
        end
      rescue
        _ ->
          Logger.info("Error reading SSTable: #{sst}")
          {:halt, nil}
      end
    end)
  end

  def from_lsm(key) do
    Kvstore.LSMTree.get_levels()
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
    val = Kvstore.Memetable.get(key)

    val =
      case val do
        nil ->
          Logger.info("Key not found in memetable: #{key}")

          from_sst(key)

        v ->
          v
      end

    val =
      case val do
        nil ->
          Logger.info("Key not found in SSTables: #{key}")

          from_lsm(key)

        v ->
          v
      end

    {:reply, val, state}
  end

  def handle_call({:set, key, value}, _from, state) do
    Kvstore.Memetable.set(key, value)
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
