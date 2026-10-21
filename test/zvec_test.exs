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

  test "batch insert multiple documents", %{col: col} do
    docs =
      for i <- 1..10 do
        vec = Zvec.Query.float_list_to_binary(List.duplicate(0.1 * i, 4))
        Zvec.Doc.new("batch#{i}", %{"vec" => vec, "text" => "item #{i}", "score_val" => i})
      end

    assert :ok = Zvec.Collection.insert(col, docs)
    assert :ok = Zvec.Collection.flush(col)

    {:ok, fetched} = Zvec.Collection.fetch(col, ["batch1", "batch5", "batch10"])
    assert length(fetched) == 3
    pks = Enum.map(fetched, & &1.pk) |> Enum.sort()
    assert pks == ["batch1", "batch10", "batch5"]
  end

  test "fetch non-existent pk returns empty list", %{col: col} do
    {:ok, docs} = Zvec.Collection.fetch(col, ["no_such_pk"])
    assert docs == []
  end

  test "delete_by_filter removes matching documents", %{col: col} do
    for i <- 1..3 do
      vec = Zvec.Query.float_list_to_binary(List.duplicate(0.1 * i, 4))
      doc = Zvec.Doc.new("filt#{i}", %{"vec" => vec, "text" => "cat_a", "score_val" => i})
      :ok = Zvec.Collection.insert(col, [doc])
    end

    vec = Zvec.Query.float_list_to_binary([0.9, 0.9, 0.9, 0.9])

    :ok =
      Zvec.Collection.insert(col, [
        Zvec.Doc.new("filt_other", %{"vec" => vec, "text" => "cat_b", "score_val" => 99})
      ])

    :ok = Zvec.Collection.flush(col)

    :ok = Zvec.Collection.delete_by_filter(col, "text = 'cat_a'")
    :ok = Zvec.Collection.flush(col)

    {:ok, remaining} = Zvec.Collection.fetch(col, ["filt1", "filt2", "filt3", "filt_other"])
    assert length(remaining) == 1
    assert hd(remaining).pk == "filt_other"
  end

  test "query with filter expression", %{col: col} do
    for i <- 1..5 do
      vec = Zvec.Query.float_list_to_binary(List.duplicate(0.1 * i, 4))
      text = if rem(i, 2) == 0, do: "even", else: "odd"
      doc = Zvec.Doc.new("qf#{i}", %{"vec" => vec, "text" => text, "score_val" => i})
      :ok = Zvec.Collection.insert(col, [doc])
    end

    :ok = Zvec.Collection.optimize(col)

    query = Zvec.Query.vector("vec", [0.5, 0.5, 0.5, 0.5], topk: 10, filter: "text = 'even'")
    {:ok, results} = Zvec.Collection.query(col, query)

    assert length(results) == 2
    assert Enum.all?(results, fn r -> r.fields["text"] == "even" end)
  end

  test "query with output_fields limits returned fields", %{col: col} do
    vec = Zvec.Query.float_list_to_binary([1.0, 1.0, 1.0, 1.0])
    doc = Zvec.Doc.new("of1", %{"vec" => vec, "text" => "hello", "score_val" => 42})
    :ok = Zvec.Collection.insert(col, [doc])
    :ok = Zvec.Collection.optimize(col)

    query = Zvec.Query.vector("vec", [1.0, 1.0, 1.0, 1.0], topk: 1, output_fields: ["text"])
    {:ok, results} = Zvec.Collection.query(col, query)

    assert length(results) == 1
    result = hd(results)
    assert result.fields["text"] == "hello"
    refute Map.has_key?(result.fields, "score_val")
  end

  test "query with include_vector returns vector data", %{col: col} do
    original = [1.0, 2.0, 3.0, 4.0]
    vec = Zvec.Query.float_list_to_binary(original)
    doc = Zvec.Doc.new("iv1", %{"vec" => vec, "text" => "with vec", "score_val" => 1})
    :ok = Zvec.Collection.insert(col, [doc])
    :ok = Zvec.Collection.optimize(col)

    query = Zvec.Query.vector("vec", [1.0, 2.0, 3.0, 4.0], topk: 1, include_vector: true)
    {:ok, results} = Zvec.Collection.query(col, query)

    assert length(results) == 1
    result = hd(results)
    assert is_binary(result.fields["vec"])
    roundtripped = Zvec.Query.binary_to_float_list(result.fields["vec"])

    Enum.zip(original, roundtripped)
    |> Enum.each(fn {a, b} -> assert_in_delta(a, b, 1.0e-6) end)
  end

  test "query with raw binary vector", %{col: col} do
    vec = Zvec.Query.float_list_to_binary([1.0, 1.0, 1.0, 1.0])
    doc = Zvec.Doc.new("rb1", %{"vec" => vec, "text" => "raw binary", "score_val" => 1})
    :ok = Zvec.Collection.insert(col, [doc])
    :ok = Zvec.Collection.optimize(col)

    # Pass binary directly instead of float list
    query_vec = Zvec.Query.float_list_to_binary([1.0, 1.0, 1.0, 1.0])
    query = Zvec.Query.vector("vec", query_vec, topk: 1)
    {:ok, results} = Zvec.Collection.query(col, query)

    assert length(results) == 1
    assert hd(results).pk == "rb1"
  end

  test "query results have scores", %{col: col} do
    for i <- 1..3 do
      vec = Zvec.Query.float_list_to_binary(List.duplicate(0.1 * i, 4))
      doc = Zvec.Doc.new("sc#{i}", %{"vec" => vec, "text" => "scored", "score_val" => i})
      :ok = Zvec.Collection.insert(col, [doc])
    end

    :ok = Zvec.Collection.optimize(col)

    query = Zvec.Query.vector("vec", [0.3, 0.3, 0.3, 0.3], topk: 3)
    {:ok, results} = Zvec.Collection.query(col, query)

    assert Enum.all?(results, fn r -> is_float(r.score) or is_number(r.score) end)
  end

  test "open read_only prevents writes" do
    path = Path.join(System.tmp_dir!(), "zvec_ro_test_#{System.unique_integer([:positive])}")

    schema =
      Zvec.Schema.new("readonly")
      |> Zvec.Schema.add_vector("v", 2, index: %{type: :hnsw, metric_type: :cosine})

    {:ok, col1} = Zvec.Collection.create_and_open(path, schema)
    vec = Zvec.Query.float_list_to_binary([1.0, 2.0])
    :ok = Zvec.Collection.insert(col1, [Zvec.Doc.new("ro1", %{"v" => vec})])
    :ok = Zvec.Collection.flush(col1)

    # Release first reference
    _ = col1
    :erlang.garbage_collect()
    Process.sleep(100)

    {:ok, col2} = Zvec.Collection.open(path, read_only: true)

    # Reading should work
    {:ok, docs} = Zvec.Collection.fetch(col2, ["ro1"])
    assert length(docs) == 1

    # Writing should fail
    vec2 = Zvec.Query.float_list_to_binary([3.0, 4.0])
    result = Zvec.Collection.insert(col2, [Zvec.Doc.new("ro2", %{"v" => vec2})])
    assert {:error, {_code, _msg}} = result

    Zvec.Collection.destroy(col2)
  end

  test "create_index and drop_index on scalar field", %{col: col} do
    # text field has no index initially; add an invert index
    assert :ok = Zvec.Collection.create_index(col, "text", %{type: :invert})
    assert :ok = Zvec.Collection.drop_index(col, "text")
  end

  test "schema with multiple field types" do
    path = Path.join(System.tmp_dir!(), "zvec_types_#{System.unique_integer([:positive])}")

    schema =
      Zvec.Schema.new("types")
      |> Zvec.Schema.add_vector("v", 2, index: %{type: :hnsw, metric_type: :cosine})
      |> Zvec.Schema.add_field("s", :string)
      |> Zvec.Schema.add_field("i32", :int32)
      |> Zvec.Schema.add_field("i64", :int64)
      |> Zvec.Schema.add_field("f", :float)
      |> Zvec.Schema.add_field("d", :double)
      |> Zvec.Schema.add_field("b", :bool)

    {:ok, col} = Zvec.Collection.create_and_open(path, schema)

    vec = Zvec.Query.float_list_to_binary([1.0, 2.0])

    doc =
      Zvec.Doc.new("t1", %{
        "v" => vec,
        "s" => "text",
        "i32" => 32,
        "i64" => 64,
        "f" => 1.5,
        "d" => 2.5,
        "b" => true
      })

    :ok = Zvec.Collection.insert(col, [doc])
    :ok = Zvec.Collection.flush(col)

    {:ok, [fetched]} = Zvec.Collection.fetch(col, ["t1"])
    assert fetched.fields["s"] == "text"
    assert fetched.fields["i32"] == 32
    assert fetched.fields["i64"] == 64
    assert_in_delta fetched.fields["f"], 1.5, 1.0e-6
    assert_in_delta fetched.fields["d"], 2.5, 1.0e-12
    assert fetched.fields["b"] == true

    Zvec.Collection.destroy(col)
  end

  test "schema with nullable field" do
    path = Path.join(System.tmp_dir!(), "zvec_nullable_#{System.unique_integer([:positive])}")

    schema =
      Zvec.Schema.new("nullable")
      |> Zvec.Schema.add_vector("v", 2, index: %{type: :hnsw, metric_type: :cosine})
      |> Zvec.Schema.add_field("opt", :string, nullable: true)

    {:ok, col} = Zvec.Collection.create_and_open(path, schema)

    # Insert doc without the nullable field
    vec = Zvec.Query.float_list_to_binary([1.0, 2.0])
    doc = Zvec.Doc.new("n1", %{"v" => vec})
    :ok = Zvec.Collection.insert(col, [doc])
    :ok = Zvec.Collection.flush(col)

    {:ok, [fetched]} = Zvec.Collection.fetch(col, ["n1"])
    assert fetched.pk == "n1"

    Zvec.Collection.destroy(col)
  end

  test "flat index type for brute-force search" do
    path = Path.join(System.tmp_dir!(), "zvec_flat_#{System.unique_integer([:positive])}")

    schema =
      Zvec.Schema.new("flat")
      |> Zvec.Schema.add_vector("v", 4, index: %{type: :flat, metric_type: :l2})

    {:ok, col} = Zvec.Collection.create_and_open(path, schema)

    for i <- 1..5 do
      vec = Zvec.Query.float_list_to_binary(List.duplicate(0.1 * i, 4))
      :ok = Zvec.Collection.insert(col, [Zvec.Doc.new("f#{i}", %{"v" => vec})])
    end

    :ok = Zvec.Collection.optimize(col)

    query = Zvec.Query.vector("v", [0.3, 0.3, 0.3, 0.3], topk: 2)
    {:ok, results} = Zvec.Collection.query(col, query)
    assert length(results) == 2

    Zvec.Collection.destroy(col)
  end

  test "inner product metric type", %{col: _col} do
    path = Path.join(System.tmp_dir!(), "zvec_ip_#{System.unique_integer([:positive])}")

    schema =
      Zvec.Schema.new("ip_test")
      |> Zvec.Schema.add_vector("v", 4, index: %{type: :hnsw, metric_type: :ip})

    {:ok, col} = Zvec.Collection.create_and_open(path, schema)

    for i <- 1..3 do
      vec = Zvec.Query.float_list_to_binary(List.duplicate(0.1 * i, 4))
      :ok = Zvec.Collection.insert(col, [Zvec.Doc.new("ip#{i}", %{"v" => vec})])
    end

    :ok = Zvec.Collection.optimize(col)

    query = Zvec.Query.vector("v", [1.0, 1.0, 1.0, 1.0], topk: 3)
    {:ok, results} = Zvec.Collection.query(col, query)
    assert length(results) == 3

    Zvec.Collection.destroy(col)
  end

  test "optimize updates stats", %{col: col} do
    vec = Zvec.Query.float_list_to_binary([1.0, 2.0, 3.0, 4.0])

    for i <- 1..5 do
      doc = Zvec.Doc.new("opt#{i}", %{"vec" => vec, "text" => "x", "score_val" => i})
      :ok = Zvec.Collection.insert(col, [doc])
    end

    :ok = Zvec.Collection.optimize(col)

    {:ok, stats} = Zvec.Collection.stats(col)
    assert stats.doc_count == 5
  end

  test "schema with invert index on string field" do
    path = Path.join(System.tmp_dir!(), "zvec_invert_#{System.unique_integer([:positive])}")

    schema =
      Zvec.Schema.new("invert")
      |> Zvec.Schema.add_vector("v", 4, index: %{type: :hnsw, metric_type: :cosine})
      |> Zvec.Schema.add_field("category", :string, index: %{type: :invert})

    {:ok, col} = Zvec.Collection.create_and_open(path, schema)

    for {cat, i} <- [{"electronics", 1}, {"electronics", 2}, {"books", 3}] do
      vec = Zvec.Query.float_list_to_binary(List.duplicate(0.1 * i, 4))
      doc = Zvec.Doc.new("inv#{i}", %{"v" => vec, "category" => cat})
      :ok = Zvec.Collection.insert(col, [doc])
    end

    :ok = Zvec.Collection.optimize(col)

    # Filter using the inverted index
    query =
      Zvec.Query.vector("v", [0.5, 0.5, 0.5, 0.5], topk: 10, filter: "category = 'electronics'")

    {:ok, results} = Zvec.Collection.query(col, query)

    assert length(results) == 2
    assert Enum.all?(results, fn r -> r.fields["category"] == "electronics" end)

    Zvec.Collection.destroy(col)
  end

  test "vamana index type for graph-based ANN search" do
    path = Path.join(System.tmp_dir!(), "zvec_vamana_#{System.unique_integer([:positive])}")

    schema =
      Zvec.Schema.new("vamana")
      |> Zvec.Schema.add_vector("v", 4,
        index: %{
          type: :vamana,
          metric_type: :l2,
          max_degree: 32,
          search_list_size: 50,
          alpha: 1.2
        }
      )

    {:ok, col} = Zvec.Collection.create_and_open(path, schema)

    for i <- 1..10 do
      vec = Zvec.Query.float_list_to_binary(List.duplicate(0.1 * i, 4))
      :ok = Zvec.Collection.insert(col, [Zvec.Doc.new("vm#{i}", %{"v" => vec})])
    end

    :ok = Zvec.Collection.optimize(col)

    query =
      Zvec.Query.vector("v", [0.3, 0.3, 0.3, 0.3],
        topk: 3,
        query_params: %{type: :vamana, ef_search: 64}
      )

    {:ok, results} = Zvec.Collection.query(col, query)
    assert length(results) == 3

    Zvec.Collection.destroy(col)
  end

  test "hnsw with use_contiguous_memory option" do
    path = Path.join(System.tmp_dir!(), "zvec_hnsw_contig_#{System.unique_integer([:positive])}")

    schema =
      Zvec.Schema.new("hnsw_contig")
      |> Zvec.Schema.add_vector("v", 4,
        index: %{type: :hnsw, metric_type: :l2, use_contiguous_memory: true}
      )

    {:ok, col} = Zvec.Collection.create_and_open(path, schema)

    for i <- 1..5 do
      vec = Zvec.Query.float_list_to_binary(List.duplicate(0.1 * i, 4))
      :ok = Zvec.Collection.insert(col, [Zvec.Doc.new("hc#{i}", %{"v" => vec})])
    end

    :ok = Zvec.Collection.optimize(col)

    query = Zvec.Query.vector("v", [0.3, 0.3, 0.3, 0.3], topk: 2)
    {:ok, results} = Zvec.Collection.query(col, query)
    assert length(results) == 2

    Zvec.Collection.destroy(col)
  end

  test "delete multiple pks at once", %{col: col} do
    vec = Zvec.Query.float_list_to_binary([1.0, 1.0, 1.0, 1.0])

    for i <- 1..4 do
      doc = Zvec.Doc.new("dm#{i}", %{"vec" => vec, "text" => "multi del", "score_val" => i})
      :ok = Zvec.Collection.insert(col, [doc])
    end

    :ok = Zvec.Collection.flush(col)
    :ok = Zvec.Collection.delete(col, ["dm1", "dm2", "dm3"])
    :ok = Zvec.Collection.flush(col)

    {:ok, docs} = Zvec.Collection.fetch(col, ["dm1", "dm2", "dm3", "dm4"])
    assert length(docs) == 1
    assert hd(docs).pk == "dm4"
  end

  test "insert of a duplicate pk returns the per-document error", %{col: col} do
    vec = Zvec.Query.float_list_to_binary([1.0, 0.0, 0.0, 0.0])
    doc = Zvec.Doc.new("dup", %{"vec" => vec, "text" => "a", "score_val" => 1})

    assert :ok = Zvec.Collection.insert(col, [doc])
    assert {:error, {:already_exists, _}} = Zvec.Collection.insert(col, [doc])
    assert :ok = Zvec.Collection.upsert(col, [doc])
  end

  test "fetch preserves request order and honors projection options", %{col: col} do
    for i <- 1..3 do
      vec = Zvec.Query.float_list_to_binary([i * 1.0, 0.0, 0.0, 1.0])
      doc = Zvec.Doc.new("f#{i}", %{"vec" => vec, "text" => "t#{i}", "score_val" => i})
      :ok = Zvec.Collection.insert(col, [doc])
    end

    {:ok, docs} =
      Zvec.Collection.fetch(col, ["f3", "missing", "f1"],
        output_fields: ["text"],
        include_vector: false
      )

    assert Enum.map(docs, & &1.pk) == ["f3", "f1"]
    assert Enum.map(docs, & &1.fields) == [%{"text" => "t3"}, %{"text" => "t1"}]
  end

  test "stream iterates a snapshot and blocks DDL until closed", %{col: col} do
    for i <- 1..5 do
      vec = Zvec.Query.float_list_to_binary([i * 1.0, 1.0, 0.0, 0.0])
      doc = Zvec.Doc.new("it#{i}", %{"vec" => vec, "text" => "x", "score_val" => i})
      :ok = Zvec.Collection.insert(col, [doc])
    end

    {:ok, stream} = Zvec.Collection.stream(col, batch_size: 2, include_vector: false)

    assert {:error, {:failed_precondition, _}} =
             Zvec.Collection.create_index(col, "text", %{type: :invert})

    # Docs written after the snapshot are not visible to the iterator.
    vec = Zvec.Query.float_list_to_binary([9.0, 1.0, 0.0, 0.0])

    :ok =
      Zvec.Collection.insert(col, [
        Zvec.Doc.new("late", %{"vec" => vec, "text" => "x", "score_val" => 9})
      ])

    pks = stream |> Enum.map(& &1.pk) |> Enum.sort()
    assert pks == ["it1", "it2", "it3", "it4", "it5"]

    # The iterator is closed once the stream is consumed or halted.
    {:ok, stream} = Zvec.Collection.stream(col)
    assert [_] = Enum.take(stream, 1)
    assert :ok = Zvec.Collection.create_index(col, "text", %{type: :invert})
  end

  test "close rejects further operations" do
    path = Path.join(System.tmp_dir!(), "zvec_close_#{System.unique_integer([:positive])}")
    schema = Zvec.Schema.new("close") |> Zvec.Schema.add_vector("v", 4)
    {:ok, col} = Zvec.Collection.create_and_open(path, schema)

    assert :ok = Zvec.Collection.close(col)
    assert {:error, _} = Zvec.Collection.query(col, Zvec.Query.vector("v", [1.0, 0.0, 0.0, 0.0]))

    {:ok, reopened} = Zvec.Collection.open(path)
    Zvec.Collection.destroy(reopened)
  end

  test "unknown query_params type raises" do
    query = Zvec.Query.vector("vec", [1.0, 0.0, 0.0, 0.0], query_params: %{type: :bogus})
    path = Path.join(System.tmp_dir!(), "zvec_qp_#{System.unique_integer([:positive])}")
    schema = Zvec.Schema.new("qp") |> Zvec.Schema.add_vector("vec", 4)
    {:ok, col} = Zvec.Collection.create_and_open(path, schema)

    assert_raise ArgumentError, ~r/unknown query_params type/, fn ->
      Zvec.Collection.query(col, query)
    end

    Zvec.Collection.destroy(col)
  end

  test "vector_fp16 fields round-trip and are searchable" do
    path = Path.join(System.tmp_dir!(), "zvec_fp16_#{System.unique_integer([:positive])}")

    schema =
      Zvec.Schema.new("fp16")
      |> Zvec.Schema.add_vector("h", 4,
        type: :vector_fp16,
        index: %{type: :flat, metric_type: :l2}
      )

    {:ok, col} = Zvec.Collection.create_and_open(path, schema)
    fp16 = fn list -> for f <- list, into: <<>>, do: <<f::float-16-native>> end

    :ok = Zvec.Collection.insert(col, [Zvec.Doc.new("a", %{"h" => fp16.([1.0, 0.0, 0.0, 0.0])})])
    :ok = Zvec.Collection.insert(col, [Zvec.Doc.new("b", %{"h" => fp16.([0.0, 1.0, 0.0, 0.0])})])

    {:ok, [top | _]} =
      Zvec.Collection.query(
        col,
        Zvec.Query.vector("h", fp16.([0.0, 0.9, 0.0, 0.0]), include_vector: true)
      )

    assert top.pk == "b"
    assert top.fields["h"] == fp16.([0.0, 1.0, 0.0, 0.0])
    assert {:ok, %{fields: [%{type: :vector_fp16}]}} = Zvec.Collection.schema(col)

    Zvec.Collection.destroy(col)
  end

  test "diskann index type" do
    path = Path.join(System.tmp_dir!(), "zvec_diskann_#{System.unique_integer([:positive])}")

    schema =
      Zvec.Schema.new("diskann")
      |> Zvec.Schema.add_vector("v", 4,
        index: %{type: :diskann, metric_type: :l2, max_degree: 32, list_size: 50, pq_chunk_num: 2}
      )

    {:ok, col} = Zvec.Collection.create_and_open(path, schema)

    docs =
      for i <- 1..50 do
        Zvec.Doc.new("p#{i}", %{"v" => Zvec.Query.float_list_to_binary([i * 1.0, 1.0, 0.0, 0.0])})
      end

    :ok = Zvec.Collection.insert(col, docs)
    :ok = Zvec.Collection.optimize(col)

    query =
      Zvec.Query.vector("v", [7.0, 1.0, 0.0, 0.0],
        topk: 3,
        query_params: %{type: :diskann, list_size: 100}
      )

    {:ok, results} = Zvec.Collection.query(col, query)
    assert hd(results).pk == "p7"

    Zvec.Collection.destroy(col)
  end

  describe "full-text search" do
    setup do
      path = Path.join(System.tmp_dir!(), "zvec_fts_#{System.unique_integer([:positive])}")

      schema =
        Zvec.Schema.new("fts")
        |> Zvec.Schema.add_field("content", :string, index: %{type: :fts})
        |> Zvec.Schema.add_field("cat", :string, index: %{type: :invert})
        |> Zvec.Schema.add_vector("v", 4, index: %{type: :hnsw, metric_type: :l2})

      {:ok, col} = Zvec.Collection.create_and_open(path, schema)

      texts = [
        {"d1", "vector database for semantic search", "a", [1.0, 0.0, 0.0, 0.0]},
        {"d2", "full text search with bm25 ranking", "b", [0.0, 1.0, 0.0, 0.0]},
        {"d3", "cooking pasta at home", "a", [0.0, 0.0, 1.0, 0.0]},
        {"d4", "hybrid retrieval combines vector and text search", "b", [0.0, 0.0, 0.0, 1.0]}
      ]

      docs =
        for {pk, content, cat, v} <- texts do
          Zvec.Doc.new(pk, %{
            "content" => content,
            "cat" => cat,
            "v" => Zvec.Query.float_list_to_binary(v)
          })
        end

      :ok = Zvec.Collection.insert(col, docs)
      on_exit(fn -> Zvec.Collection.destroy(col) end)
      %{col: col}
    end

    defp pks(col, query) do
      {:ok, results} = Zvec.Collection.query(col, query)
      results |> Enum.map(& &1.pk) |> Enum.sort()
    end

    test "match syntax combines terms with the default operator", %{col: col} do
      assert pks(col, Zvec.Query.fts("content", "search")) == ["d1", "d2", "d4"]
      assert pks(col, Zvec.Query.fts("content", "pasta vector")) == ["d1", "d3", "d4"]

      assert pks(col, Zvec.Query.fts("content", "vector search", default_operator: :and)) ==
               ["d1", "d4"]
    end

    test "query syntax supports required and excluded terms", %{col: col} do
      assert pks(col, Zvec.Query.fts("content", "+search -vector", syntax: :query)) == ["d2"]
    end

    test "fts honors scalar filters", %{col: col} do
      assert pks(col, Zvec.Query.fts("content", "search", filter: "cat = 'b'")) == ["d2", "d4"]
    end

    test "multi query fuses vector and text results", %{col: col} do
      subqueries = [
        Zvec.Query.vector("v", [0.0, 0.0, 1.0, 0.0], topk: 4),
        Zvec.Query.fts("content", "pasta", topk: 4)
      ]

      for rerank <- [:rrf, {:rrf, 10}, {:weighted, [0.5, 0.5]}] do
        {:ok, [top | _] = results} =
          Zvec.Collection.query(col, Zvec.Query.multi(subqueries, topk: 2, rerank: rerank))

        assert length(results) == 2
        # d3 is ranked first by both sub-queries.
        assert top.pk == "d3"
      end
    end
  end
end
