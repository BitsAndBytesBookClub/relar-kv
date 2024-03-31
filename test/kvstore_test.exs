defmodule KvstoreTest do
  use ExUnit.Case
  doctest Kvstore

  test "greets the world" do
    assert Kvstore.hello() == :world
  end
end
