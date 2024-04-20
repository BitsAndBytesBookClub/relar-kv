defmodule Kvstore.TrashBin do
  def empty(path) do
    File.ls!(path)
    |> Enum.each(fn file ->
      File.rm_rf!(path <> "/" <> file)
    end)
  end
end
