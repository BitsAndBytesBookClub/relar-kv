defmodule KvstoreTest.End2End do
  use ExUnit.Case
  @tag workingon: true

  test "test 100x100" do
    KV.set("hello", "world")
    assert KV.get("hello") == "world"

    n = 300
    incs = 20

    assert KV.do_the_test(n, incs) == %{"good" => n}
  end
end

defmodule KvstoreTest.SST do
  use ExUnit.Case

  test "sst reading" do
    wd = Compaction.SSTReader.data(~w[1 2 3], "test_data/sst_reading")

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

defmodule KvstoreTest.Compaction do
  use ExUnit.Case

  def setup do
    File.rm_rf!("test_data/compaction_sst/result")
  end

  test "compaction sst to l0" do
    sst_files = File.ls!("test_data/compaction_sst/sst")
    sst_data = Compaction.SSTReader.data(sst_files, "test_data/compaction_sst/sst")
    lsm_data = Compaction.LSMReader.stream("test_data/compaction_sst/lsm/0")
    write_data = Compaction.Writer.data("test_data/compaction_sst/result")

    Kvstore.Compaction.SSTToLevel0.combine_sst_and_lsm_keys(sst_data, lsm_data, write_data)

    Compaction.Writer.close(write_data)

    {:ok, results} = File.ls("test_data/compaction_sst/result")
    {:ok, wants} = File.ls("test_data/compaction_sst/want")

    assert Enum.count(results) == Enum.count(wants)

    Enum.each(wants, fn file ->
      assert File.read!("test_data/compaction_sst/result/#{file}") ==
               File.read!("test_data/compaction_sst/want/#{file}")
    end)
  end
end
