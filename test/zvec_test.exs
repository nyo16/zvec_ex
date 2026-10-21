defmodule ZvecTest do
  use ExUnit.Case

  setup ctx do
    if ctx[:own_collection] do
      :ok
    else
      path = Path.join(System.tmp_dir!(), "zvec_test_#{System.unique_integer([:positive])}")

      schema =
        Zvec.Schema.new("test")
        |> Zvec.Schema.add_vector("vec", 4, index: %{type: :hnsw, metric_type: :cosine})
        |> Zvec.Schema.add_field("text", :string)
        |> Zvec.Schema.add_field("score_val", :int64)

      {:ok, col} = Zvec.Collection.create_and_open(path, schema)

      on_exit(fn ->
        Zvec.Collection.destroy(col)
        File.rm_rf!(path)
      end)

      %{col: col, path: path}
    end
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

    assert :ok = Zvec.Collection.upsert(col, [%{doc | fields: %{doc.fields | "text" => "b"}}])
    assert {:ok, [%{fields: %{"text" => "b"}}]} = Zvec.Collection.fetch(col, ["dup"])
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

  test "fetch and stream include vectors unless include_vector: false", %{col: col} do
    vec = Zvec.Query.float_list_to_binary([1.0, 0.0, 0.0, 0.0])
    doc = Zvec.Doc.new("v1", %{"vec" => vec, "text" => "x", "score_val" => 1})
    :ok = Zvec.Collection.insert(col, [doc])

    assert {:ok, [%{fields: %{"vec" => ^vec}}]} = Zvec.Collection.fetch(col, ["v1"])
    assert {:ok, [doc]} = Zvec.Collection.fetch(col, ["v1"], include_vector: false)
    refute Map.has_key?(doc.fields, "vec")

    assert [%{fields: %{"vec" => ^vec}}] = Enum.to_list(Zvec.Collection.stream(col))
    assert [doc] = Enum.to_list(Zvec.Collection.stream(col, include_vector: false))
    refute Map.has_key?(doc.fields, "vec")

    # output_fields restricts scalar fields; vectors follow include_vector.
    assert [doc] =
             Enum.to_list(
               Zvec.Collection.stream(col, output_fields: ["text"], include_vector: false)
             )

    assert Map.keys(doc.fields) == ["text"]
  end

  test "stream is lazy and reads a fresh snapshot per enumeration", %{col: col} do
    for i <- 1..5 do
      vec = Zvec.Query.float_list_to_binary([i * 1.0, 1.0, 0.0, 0.0])
      doc = Zvec.Doc.new("it#{i}", %{"vec" => vec, "text" => "x", "score_val" => i})
      :ok = Zvec.Collection.insert(col, [doc])
    end

    stream = Zvec.Collection.stream(col, batch_size: 2, include_vector: false)

    # No iterator is open until the stream is enumerated.
    assert :ok = Zvec.Collection.create_index(col, "text", %{type: :invert})

    first = stream |> Enum.map(& &1.pk) |> Enum.sort()
    assert first == ["it1", "it2", "it3", "it4", "it5"]
    assert stream |> Enum.map(& &1.pk) |> Enum.sort() == first

    vec = Zvec.Query.float_list_to_binary([9.0, 1.0, 0.0, 0.0])

    :ok =
      Zvec.Collection.insert(col, [
        Zvec.Doc.new("late", %{"vec" => vec, "text" => "x", "score_val" => 9})
      ])

    assert stream |> Enum.map(& &1.pk) |> Enum.sort() == first ++ ["late"]
  end

  test "an enumerating stream blocks close and DDL until it halts", %{col: col} do
    vec = Zvec.Query.float_list_to_binary([1.0, 0.0, 0.0, 0.0])

    :ok =
      Zvec.Collection.insert(col, [
        Zvec.Doc.new("a", %{"vec" => vec, "text" => "x", "score_val" => 1}),
        Zvec.Doc.new("b", %{"vec" => vec, "text" => "y", "score_val" => 2})
      ])

    result =
      Enum.reduce_while(Zvec.Collection.stream(col, batch_size: 1), nil, fn _doc, _acc ->
        {:halt,
         {Zvec.Collection.close(col), Zvec.Collection.create_index(col, "text", %{type: :invert})}}
      end)

    assert {{:error, {:failed_precondition, _}}, {:error, {:failed_precondition, _}}} = result

    # Halting closed the iterator.
    assert :ok = Zvec.Collection.create_index(col, "text", %{type: :invert})
  end

  test "stream validates options when called", %{col: col} do
    assert_raise ArgumentError, ~r/batch_size/, fn ->
      Zvec.Collection.stream(col, batch_size: 0)
    end

    assert_raise ArgumentError, ~r/bad_key/, fn -> Zvec.Collection.stream(col, bad_key: 1) end
  end

  describe "standalone collections" do
    @describetag own_collection: true

    test "close rejects further operations" do
      schema = Zvec.Schema.new("close") |> Zvec.Schema.add_vector("v", 4)
      {col, path} = tmp_collection!("close", schema)

      assert :ok = Zvec.Collection.close(col)

      assert {:error, {:invalid_argument, _}} =
               Zvec.Collection.query(col, Zvec.Query.vector("v", [1.0, 0.0, 0.0, 0.0]))

      assert {:error, {:invalid_argument, _}} = Zvec.Collection.destroy(col)

      {:ok, reopened} = Zvec.Collection.open(path)
      assert :ok = Zvec.Collection.destroy(reopened)
    end

    test "close then reopen persists flushed documents" do
      schema = Zvec.Schema.new("persist") |> Zvec.Schema.add_vector("v", 4)
      {col, path} = tmp_collection!("persist", schema)

      vec = Zvec.Query.float_list_to_binary([1.0, 0.0, 0.0, 0.0])
      :ok = Zvec.Collection.insert(col, [Zvec.Doc.new("p1", %{"v" => vec})])
      :ok = Zvec.Collection.flush(col)
      :ok = Zvec.Collection.close(col)

      {:ok, reopened} = Zvec.Collection.open(path)

      assert {:ok, [%{pk: "p1", fields: %{"v" => ^vec}}]} =
               Zvec.Collection.fetch(reopened, ["p1"])

      assert :ok = Zvec.Collection.destroy(reopened)
    end

    test "unsupported vector field types raise at creation" do
      path = Path.join(System.tmp_dir!(), "zvec_sparse_#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf!(path) end)

      schema =
        Zvec.Schema.new("sparse") |> Zvec.Schema.add_vector("s", 4, type: :sparse_vector_fp32)

      assert_raise ArgumentError, ~r/unsupported field type/, fn ->
        Zvec.Collection.create_and_open(path, schema)
      end
    end

    test "unknown query_params type raises" do
      schema = Zvec.Schema.new("qp") |> Zvec.Schema.add_vector("vec", 4)
      {col, _path} = tmp_collection!("qp", schema)
      query = Zvec.Query.vector("vec", [1.0, 0.0, 0.0, 0.0], query_params: %{type: :bogus})

      assert_raise ArgumentError, ~r/unknown query_params type/, fn ->
        Zvec.Collection.query(col, query)
      end
    end

    test "vector_fp16 fields round-trip and are searchable" do
      schema =
        Zvec.Schema.new("fp16")
        |> Zvec.Schema.add_vector("h", 4,
          type: :vector_fp16,
          index: %{type: :flat, metric_type: :l2}
        )

      {col, _path} = tmp_collection!("fp16", schema)
      fp16 = fn list -> for f <- list, into: <<>>, do: <<f::float-16-native>> end

      :ok =
        Zvec.Collection.insert(col, [Zvec.Doc.new("a", %{"h" => fp16.([1.0, 0.0, 0.0, 0.0])})])

      :ok =
        Zvec.Collection.insert(col, [Zvec.Doc.new("b", %{"h" => fp16.([0.0, 1.0, 0.0, 0.0])})])

      {:ok, [top | _]} =
        Zvec.Collection.query(
          col,
          Zvec.Query.vector("h", fp16.([0.0, 0.9, 0.0, 0.0]), include_vector: true)
        )

      assert top.pk == "b"
      assert top.fields["h"] == fp16.([0.0, 1.0, 0.0, 0.0])
      assert {:ok, %{fields: [%{type: :vector_fp16}]}} = Zvec.Collection.schema(col)
    end

    test "diskann index type" do
      schema =
        Zvec.Schema.new("diskann")
        |> Zvec.Schema.add_vector("v", 4,
          index: %{
            type: :diskann,
            metric_type: :l2,
            max_degree: 32,
            list_size: 50,
            pq_chunk_num: 2
          }
        )

      {col, _path} = tmp_collection!("diskann", schema)

      docs =
        for i <- 1..50 do
          Zvec.Doc.new("p#{i}", %{
            "v" => Zvec.Query.float_list_to_binary([i * 1.0, 1.0, 0.0, 0.0])
          })
        end

      :ok = Zvec.Collection.insert(col, docs)
      :ok = Zvec.Collection.optimize(col)

      query =
        Zvec.Query.vector("v", [7.0, 1.0, 0.0, 0.0],
          topk: 3,
          query_params: %{type: :diskann, list_size: 100}
        )

      {:ok, results} = Zvec.Collection.query(col, query)
      assert length(results) == 3
      assert hd(results).pk == "p7"
    end

    test "index and query options find the nearest document" do
      # enable_rotate is exercised on IVF only: with zvec 0.8.0, HNSW + rotation
      # returns near-random neighbours even for exact-match queries.
      specs = [
        {"hnsw_uint4", %{type: :hnsw, quantize_type: :uniform_uint4}, %{type: :hnsw}},
        {"hnsw_uint8", %{type: :hnsw, quantize_type: :uniform_uint8}, %{type: :hnsw}},
        {"hnsw_int8_refine",
         %{
           type: :hnsw,
           quantize_type: :int8,
           use_flat_contiguous_memory: true,
           flat_data_type: :vector_fp16
         },
         %{
           type: :hnsw,
           is_using_refiner: true,
           scale_factor: 2.0,
           prefetch_offset: 4,
           prefetch_lines: 1
         }},
        {"vamana_uint7", %{type: :vamana, two_pass_build: false, quantize_type: :uniform_uint7},
         %{type: :vamana, ef_search: 32, prefetch_lines: 2}},
        {"ivf_int4", %{type: :ivf, quantize_type: :int4, enable_rotate: true, n_list: 4},
         %{type: :ivf, nprobe: 4}},
        {"flat_fp16",
         %{type: :flat, use_contiguous_memory: true, storage_data_type: :vector_fp16},
         %{type: :flat}}
      ]

      for {name, index, query_params} <- specs do
        assert_finds_nearest(name, 8, index, query_params)
      end
    end

    @tag :rabitq
    test "rabitq indexes find the nearest document" do
      assert_finds_nearest("ivf_rabitq", 64, %{type: :ivf_rabitq, n_list: 4}, %{
        type: :ivf_rabitq,
        nprobe: 2
      })

      assert_finds_nearest("hnsw_rabitq", 64, %{type: :hnsw_rabitq, sample_count: 0}, %{
        type: :hnsw_rabitq
      })
    end
  end

  # Inserts 50 random docs into a fresh collection, optimizes, and checks that a
  # query for one doc's own vector returns it first.
  defp assert_finds_nearest(name, dim, index, query_params) do
    schema =
      Zvec.Schema.new(name)
      |> Zvec.Schema.add_vector("v", dim, index: Map.put(index, :metric_type, :l2))

    {col, _path} = tmp_collection!(name, schema)
    :rand.seed(:exsss, {1, 2, 3})

    vectors =
      for i <- 1..50 do
        {"p#{i}", for(_ <- 1..dim, do: :rand.uniform() * 2 - 1)}
      end

    docs =
      for {pk, v} <- vectors, do: Zvec.Doc.new(pk, %{"v" => Zvec.Query.float_list_to_binary(v)})

    :ok = Zvec.Collection.insert(col, docs)
    :ok = Zvec.Collection.optimize(col)

    {target, v} = Enum.at(vectors, 16)
    query = Zvec.Query.vector("v", v, topk: 3, query_params: query_params)

    {:ok, results} = Zvec.Collection.query(col, query)
    assert length(results) == 3, "#{name}: expected 3 results, got #{length(results)}"
    assert hd(results).pk == target, "#{name}: expected #{target} first, got #{hd(results).pk}"
  end

  defp tmp_collection!(name, schema, opts \\ []) do
    path = Path.join(System.tmp_dir!(), "zvec_#{name}_#{System.unique_integer([:positive])}")
    {:ok, col} = Zvec.Collection.create_and_open(path, schema, opts)

    on_exit(fn ->
      Zvec.Collection.destroy(col)
      File.rm_rf!(path)
    end)

    {col, path}
  end

  defp pks(col, query) do
    {:ok, results} = Zvec.Collection.query(col, query)
    results |> Enum.map(& &1.pk) |> Enum.sort()
  end

  describe "full-text search" do
    @describetag own_collection: true

    setup do
      schema =
        Zvec.Schema.new("fts")
        |> Zvec.Schema.add_field("content", :string, index: %{type: :fts})
        |> Zvec.Schema.add_field("cat", :string, index: %{type: :invert})
        |> Zvec.Schema.add_vector("v", 4, index: %{type: :hnsw, metric_type: :l2})

      {col, _path} = tmp_collection!("fts", schema)

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
      %{col: col}
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

    test "topk limits results and BM25 ranks the shorter matching doc first", %{col: col} do
      assert {:ok, [_]} = Zvec.Collection.query(col, Zvec.Query.fts("content", "search", topk: 1))

      assert {:ok, [%{pk: "d1"} | _]} =
               Zvec.Collection.query(col, Zvec.Query.fts("content", "vector"))
    end

    test "fts honors scalar filters", %{col: col} do
      assert pks(col, Zvec.Query.fts("content", "search", filter: "cat = 'b'")) == ["d2", "d4"]
    end

    test "multi query fuses with RRF using 0-based ranks", %{col: col} do
      subqueries = [
        Zvec.Query.vector("v", [0.0, 0.0, 1.0, 0.0], topk: 4),
        Zvec.Query.fts("content", "pasta", topk: 4)
      ]

      # d3 is ranked first (rank 0) by both sub-queries: 2 * 1 / (k + 0 + 1).
      for {rerank, k} <- [{:rrf, 60}, {{:rrf, 10}, 10}] do
        {:ok, [top | _] = results} =
          Zvec.Collection.query(col, Zvec.Query.multi(subqueries, topk: 2, rerank: rerank))

        assert length(results) == 2
        assert top.pk == "d3"
        assert_in_delta top.score, 2 / (k + 1), 1.0e-4
      end
    end

    test "weighted multi query follows the weights", %{col: col} do
      # The vector sub-query favors d1; the text sub-query only matches d3.
      subqueries = [
        Zvec.Query.vector("v", [1.0, 0.0, 0.0, 0.0]),
        Zvec.Query.fts("content", "pasta")
      ]

      for {weights, expected} <- [{[1.0, 0.0], "d1"}, {[0.0, 1.0], "d3"}] do
        assert {:ok, [%{pk: ^expected} | _]} =
                 Zvec.Collection.query(
                   col,
                   Zvec.Query.multi(subqueries, rerank: {:weighted, weights})
                 )
      end
    end

    test "multi query applies filter and output_fields", %{col: col} do
      subqueries = [
        Zvec.Query.vector("v", [1.0, 1.0, 1.0, 1.0]),
        Zvec.Query.fts("content", "search")
      ]

      {:ok, results} =
        Zvec.Collection.query(col, Zvec.Query.multi(subqueries, filter: "cat = 'b'"))

      assert results != []
      assert Enum.all?(results, &(&1.pk in ["d2", "d4"]))

      {:ok, results} =
        Zvec.Collection.query(col, Zvec.Query.multi(subqueries, output_fields: ["cat"]))

      assert results != []
      assert Enum.all?(results, &(Map.keys(&1.fields) == ["cat"]))
    end

    test "invalid builder options raise ArgumentError" do
      subqueries = [Zvec.Query.vector("v", [1.0, 0.0, 0.0, 0.0]), Zvec.Query.fts("content", "x")]

      assert_raise ArgumentError, fn ->
        Zvec.Query.multi(subqueries, rerank: {:weighted, [1.0]})
      end

      assert_raise ArgumentError, fn -> Zvec.Query.multi(subqueries, rerank: :bogus) end
      assert_raise ArgumentError, fn -> Zvec.Query.fts("content", "x", syntax: :x) end
    end
  end
end
