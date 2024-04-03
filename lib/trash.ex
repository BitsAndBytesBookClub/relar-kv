defmodule Kvstore.TrashBin do
  def path() do
    "db/trash"
  end

  # TODO move to gen_server?
  def empty() do
    File.rm_rf!("db/trash/*")
  end
end
