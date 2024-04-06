defmodule Compaction.Writer do
  def data(path, count \\ 10) do
    File.mkdir_p!(path)

    {:ok, fd} = File.open(path <> "/a", [:write, :utf8])
    {fd, 0, "a", path, count}
  end

  def write(data, nil, nil) do
    data
  end

  def write({fd, count, letter, path, max_count}, key, value) do
    case count > max_count do
      true ->
        File.close(fd)
        next_letter = NextLetter.get_next_letter(letter)
        {:ok, fd} = File.open(path <> "/#{next_letter}", [:write, :utf8])
        IO.write(fd, "#{key},#{value}")
        {fd, 1, next_letter, path, max_count}

      false ->
        IO.write(fd, "#{key},#{value}")
        {fd, count + 1, letter, path, max_count}
    end
  end
end
