defmodule KvstoreTest do
  use ExUnit.Case
  doctest Kvstore

  test "simple set" do
    KV.set("hello", "world")
    assert KV.get("hello") == "world"

    assert KV.do_the_test(100, 100) == %{"good" => 100}
  end
end
