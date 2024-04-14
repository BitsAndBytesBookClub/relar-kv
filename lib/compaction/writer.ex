defmodule Compaction.Writer do
  require Logger

  def data(path, count \\ 100) do
    File.mkdir_p!(path)

    {:ok, fd} = :file.open(path <> "/a", [:raw, :write, :utf8])
    {fd, 0, "a", path, count}
  end

  def write(data, nil, nil) do
    data
  end

  def write({fd, count, letter, path, max_count}, key, value) do
    case count > max_count do
      true ->
        :ok = :file.datasync(fd)

        :file.close(fd)
        next_letter = NextLetter.get_next_letter(letter)
        {:ok, fd} = :file.open(path <> "/#{next_letter}", [:raw, :write, :utf8])
        :file.write(fd, "#{key},#{value}")
        {fd, 1, next_letter, path, max_count}

      false ->
        :file.write(fd, "#{key},#{value}")
        {fd, count + 1, letter, path, max_count}
    end
  end

  def close({fd, _, _, _, _}) do
    case :file.sync(fd) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("Error syncing file: #{inspect(reason)}")
        :error
    end

    :file.close(fd)
  end
end
