defmodule Compaction.LSMReader do
  def data(path) do
    files =
      File.ls!(path)
      |> Enum.sort()

    file_descriptors =
      files
      |> Enum.map(&File.open(path <> "/" <> &1, [:read]))
      |> Enum.map(fn {:ok, fd} -> fd end)

    Enum.map(file_descriptors, &IO.stream(&1, :line))
  end

  def next([]) do
    {[], nil, nil}
  end

  def next(data) do
    [stream | data_tail] = data

    case Enum.take(stream, 1) do
      [] ->
        case data_tail do
          [] ->
            {[], nil, nil}

          [next_stream | _] ->
            [item] = Enum.take(next_stream, 1)
            [key, value] = String.split(item, ",")
            {data_tail, key, value}
        end

      [item] ->
        [key, value] = String.split(item, ",")
        {data, key, value}
    end
  end
end
