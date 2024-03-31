defmodule KV do
  def set(key, value) do
    GenServer.call(handler(), {:set, key, value})
  end

  def get(key) do
    GenServer.call(handler(), {:get, key})
  end

  defp handler() do
    Enum.random([:h1, :h2])
  end
end

defmodule Kvstore.Handler do
  use GenServer

  def start_link(%{name: n}) do
    GenServer.start_link(__MODULE__, %{}, name: n)
  end

  def init(_args) do
    {:ok, %{}}
  end

  def handle_call({:get, key}, _from, state) do
    val = Kvstore.Memetable.get(key)
    {:reply, val, state}
  end

  def handle_call({:set, key, value}, _from, state) do
    Kvstore.Memetable.set(key, value)
    {:reply, :ok, state}
  end
end
