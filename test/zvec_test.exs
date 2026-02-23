defmodule ZvecTest do
  use ExUnit.Case

  setup do
    path = Path.join(System.tmp_dir!(), "zvec_test_#{System.unique_integer([:positive])}")

    schema =
      Zvec.Schema.new("test")
      |> Zvec.Schema.add_vector("vec", 4, index: %{type: :hnsw, metric_type: :cosine})
      |> Zvec.Schema.add_field("text", :string)
      |> Zvec.Schema.add_field("score_val", :int64)

    {:ok, col} = Zvec.Collection.create_and_open(path, schema)

    on_exit(fn ->
      Zvec.Collection.destroy(col)
    end)

    %{col: col, path: path}
  end

  test "create_and_open returns a reference", %{col: col} do
    assert is_reference(col)
  end

  test "insert and fetch documents", %{col: col} do
    vec = Zvec.Query.float_list_to_binary([0.1, 0.2, 0.3, 0.4])
    doc = Zvec.Doc.new("pk1", %{"vec" => vec, "text" => "hello", "score_val" => 42})

    assert :ok = Zvec.Collection.insert(col, [doc])
    assert :ok = Zvec.Collection.flush(col)

    {:ok, docs} = Zvec.Collection.fetch(col, ["pk1"])
    assert length(docs) == 1
    fetched = hd(docs)
    assert fetched.pk == "pk1"
    assert fetched.fields["text"] == "hello"
    assert fetched.fields["score_val"] == 42
  end

  test "vector query returns ranked results", %{col: col} do
    # Insert multiple docs with different vectors
    for i <- 1..5 do
      v = List.duplicate(0.1 * i, 4)
      vec = Zvec.Query.float_list_to_binary(v)
      doc = Zvec.Doc.new("doc#{i}", %{"vec" => vec, "text" => "doc #{i}", "score_val" => i})
      :ok = Zvec.Collection.insert(col, [doc])
    end

    :ok = Zvec.Collection.optimize(col)

    query = Zvec.Query.vector("vec", [0.5, 0.5, 0.5, 0.5], topk: 3)
    {:ok, results} = Zvec.Collection.query(col, query)

    assert length(results) == 3
    assert Enum.all?(results, fn r -> is_binary(r.pk) end)
  end

  test "stats returns doc count", %{col: col} do
    vec = Zvec.Query.float_list_to_binary([1.0, 2.0, 3.0, 4.0])
    doc = Zvec.Doc.new("s1", %{"vec" => vec, "text" => "stats test", "score_val" => 1})
    :ok = Zvec.Collection.insert(col, [doc])
    :ok = Zvec.Collection.optimize(col)

    {:ok, stats} = Zvec.Collection.stats(col)
    assert stats.doc_count == 1
  end

  test "delete removes documents", %{col: col} do
    vec = Zvec.Query.float_list_to_binary([1.0, 1.0, 1.0, 1.0])
    doc = Zvec.Doc.new("del1", %{"vec" => vec, "text" => "to delete", "score_val" => 0})
    :ok = Zvec.Collection.insert(col, [doc])
    :ok = Zvec.Collection.flush(col)

    :ok = Zvec.Collection.delete(col, ["del1"])
    :ok = Zvec.Collection.flush(col)

    {:ok, docs} = Zvec.Collection.fetch(col, ["del1"])
    assert docs == []
  end

  test "schema returns collection schema", %{col: col} do
    {:ok, schema} = Zvec.Collection.schema(col)
    assert schema.name == "test"
    assert length(schema.fields) == 3

    vec_field = Enum.find(schema.fields, &(&1.name == "vec"))
    assert vec_field.type == :vector_fp32
    assert vec_field.dimension == 4
  end

  test "upsert updates existing documents", %{col: col} do
    vec = Zvec.Query.float_list_to_binary([1.0, 1.0, 1.0, 1.0])
    doc1 = Zvec.Doc.new("up1", %{"vec" => vec, "text" => "original", "score_val" => 1})
    :ok = Zvec.Collection.insert(col, [doc1])

    doc2 = Zvec.Doc.new("up1", %{"vec" => vec, "text" => "updated", "score_val" => 2})
    :ok = Zvec.Collection.upsert(col, [doc2])
    :ok = Zvec.Collection.flush(col)

    {:ok, docs} = Zvec.Collection.fetch(col, ["up1"])
    assert hd(docs).fields["text"] == "updated"
  end

  test "open existing collection" do
    path = Path.join(System.tmp_dir!(), "zvec_open_test_#{System.unique_integer([:positive])}")

    schema =
      Zvec.Schema.new("reopen")
      |> Zvec.Schema.add_vector("v", 2, index: %{type: :hnsw, metric_type: :l2})

    # Create, insert, flush, then release the first reference via GC
    {:ok, col1} = Zvec.Collection.create_and_open(path, schema)

    vec = Zvec.Query.float_list_to_binary([1.0, 2.0])
    :ok = Zvec.Collection.insert(col1, [Zvec.Doc.new("r1", %{"v" => vec})])
    :ok = Zvec.Collection.flush(col1)

    # Release the first reference by spawning in a separate process
    # and letting it die (GC collects the resource)
    result =
      Task.async(fn ->
        # col1 is not accessible here - open fresh
        # But first we need col1 to be gone...
        :ok
      end)
      |> Task.await()

    assert result == :ok

    # Force GC to release the first collection reference
    _ = col1
    :erlang.garbage_collect()
    # Small sleep to let the NIF destructor run
    Process.sleep(100)

    {:ok, col2} = Zvec.Collection.open(path)
    {:ok, docs} = Zvec.Collection.fetch(col2, ["r1"])
    assert length(docs) == 1

    Zvec.Collection.destroy(col2)
  end

  test "float_list_to_binary and binary_to_float_list roundtrip" do
    original = [1.0, 2.5, -3.0, 0.0]
    bin = Zvec.Query.float_list_to_binary(original)
    assert is_binary(bin)
    assert byte_size(bin) == 4 * 4

    roundtripped = Zvec.Query.binary_to_float_list(bin)
    assert length(roundtripped) == 4

    Enum.zip(original, roundtripped)
    |> Enum.each(fn {a, b} -> assert_in_delta(a, b, 1.0e-6) end)
  end

  test "error on opening non-existent collection" do
    result = Zvec.Collection.open("/tmp/zvec_nonexistent_#{System.unique_integer([:positive])}")
    assert {:error, {_code, _msg}} = result
  end
end
