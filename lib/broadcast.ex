defmodule Kvstore.Broadcast do
  def broadcast(message) do
    send =
      case Application.get_env(:kvstore, :broadcast) do
        nil -> {:error, "No broadcast configured"}
        func -> func.(message)
      end

    dbg({message, send})
  end
end
