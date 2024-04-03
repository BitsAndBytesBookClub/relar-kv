defmodule Kvstore.TrashBin do
  def path() do
    "db/trash"
  end

  def empty() do
    File.ls!(path())
    |> Enum.each(fn file ->
      File.rm_rf!(path() <> "/" <> file)
    end)
  end
end
