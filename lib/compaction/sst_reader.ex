defmodule Compaction.SSTReader do
  def data(files, path) do
    ssts =
      files
      |> Enum.map(&String.to_integer/1)
      |> Enum.sort()
      |> Enum.map(&Integer.to_string/1)

    file_descriptors =
      ssts
      |> Enum.map(&File.open(path <> "/" <> &1, [:read]))
      |> Enum.map(fn {:ok, fd} -> fd end)

    streams = Enum.map(file_descriptors, &IO.stream(&1, :line))

    elements =
      Enum.map(streams, fn stream ->
        stream
        |> Enum.take(1)
        |> hd
        |> String.split(",")
      end)

    Enum.zip(elements, streams)
  end

  def next([]) do
    {[], nil, nil}
  end

  def next(data) do
    {[key, _], _} =
      data
      |> Enum.min_by(fn {[k, _], _} -> k end)

    {[_, value], _} =
      data
      |> Enum.filter(fn {[k, _], _} -> k == key end)
      |> List.last()

    data =
      data
      |> Enum.with_index()
      |> Enum.reduce(data, fn {{[k, _], stream}, idx}, acc ->
        case k == key do
          true ->
            case Enum.take(stream, 1) do
              [] -> List.replace_at(acc, idx, nil)
              [item] -> List.replace_at(acc, idx, {String.split(item, ","), stream})
            end

          false ->
            acc
        end
      end)

    data = Enum.filter(data, fn x -> x != nil end)

    {data, key, value}
  end
end
