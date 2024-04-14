defmodule KvstoreTest do
  use ExUnit.Case
  doctest Kvstore

  test "test 100x100" do
    KV.set("hello", "world")
    assert KV.get("hello") == "world"

    assert KV.do_the_test(100, 100) == %{"good" => 100}
  end

  test "sst reading" do
    wd = Compaction.SSTReader.data(~w[1 2 3], "test_data/sst")

    kv_pairs = drain(wd)

    assert kv_pairs ==
             [
               {"1", "3\n"},
               {"2", "3\n"},
               {"3", "3\n"},
               {"4", "3\n"},
               {"5", "3\n"}
             ]
  end

  def drain(wd) do
    case Compaction.SSTReader.next(wd) do
      {_, nil, nil} -> []
      {wd, k, v} -> [{k, v} | drain(wd)]
    end
  end
end
