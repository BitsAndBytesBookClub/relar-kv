defmodule Compaction.LSMReader do
  def stream(path) do
    File.ls!(path)
    |> Enum.sort()
    |> Enum.map(&File.open(path <> "/" <> &1, [:read]))
    |> Enum.map(fn {:ok, fd} -> fd end)
    |> Enum.map(&IO.stream(&1, :line))
    |> Stream.concat()
    |> Stream.map(&String.split(&1, ","))
  end

  def next([]) do
    {[], nil, nil}
  end

  def next(stream) do
    case Enum.take(stream, 1) do
      [] ->
        {nil, nil}

      [[key, value]] ->
        {key, value}
    end
  end
end
