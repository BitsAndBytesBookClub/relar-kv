defmodule Kvstore.Broadcast do
  def broadcast(message) do
    case Application.get_env(:kvstore, :broadcast) do
      nil -> :ok
      func -> func.(message)
    end
  end
end
