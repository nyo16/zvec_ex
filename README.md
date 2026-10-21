# Zvec

Elixir NIF bindings for [zvec](https://github.com/alibaba/zvec) (v0.8.0), an in-process vector database from Alibaba.

Built with [Fine](https://github.com/elixir-nx/fine) for ergonomic C++ NIF integration. Vectors pass as raw binaries for zero-overhead encoding. All blocking operations run on dirty schedulers.

## Requirements

- Elixir >= 1.16
- CMake >= 3.26
- C++17 compiler (GCC 7+ or Clang 5+)
- Git (to clone zvec source on first build)

## Installation

```elixir
def deps do
  [
    {:zvec, "~> 0.8.0"}
  ]
end
```

Precompiled NIFs are downloaded for `aarch64-apple-darwin`. On other targets the first `mix compile` clones and builds zvec from source (~5 minutes). Subsequent compiles are instant.

Set `ZVEC_BUILD=true` to always compile the NIF from source (required when changing `c_src/zvec_nif.cpp`; CI sets it).

## Quick Start

```elixir
# 1. Define a schema
schema =
  Zvec.Schema.new("products")
  |> Zvec.Schema.add_field("title", :string, index: %{type: :fts})
  |> Zvec.Schema.add_field("category", :string, index: %{type: :invert})
  |> Zvec.Schema.add_vector("embedding", 384, index: %{type: :hnsw, metric_type: :cosine})

# 2. Create a collection
{:ok, col} = Zvec.Collection.create_and_open("/tmp/products_db", schema)

# 3. Insert documents
doc = Zvec.Doc.new("product_1", %{
  "title" => "Wireless Headphones",
  "category" => "electronics",
  "embedding" => Zvec.Query.float_list_to_binary(my_embedding_vector)
})

:ok = Zvec.Collection.insert(col, [doc])
:ok = Zvec.Collection.optimize(col)

# 4. Search
{:ok, results} = Zvec.Collection.query(col,
  Zvec.Query.vector("embedding", query_vector,
    topk: 10,
    filter: "category = 'electronics'"
  )
)

Enum.each(results, fn doc ->
  IO.puts("#{doc.pk} (#{doc.score}) - #{doc.fields["title"]}")
end)
```

## API Overview

### Schema

```elixir
schema =
  Zvec.Schema.new("name")
  |> Zvec.Schema.add_field("text", :string)
  |> Zvec.Schema.add_field("count", :int64, nullable: true)
  |> Zvec.Schema.add_field("tag", :string, index: %{type: :invert})
  |> Zvec.Schema.add_field("body", :string, index: %{type: :fts, filters: ["lowercase", "stemmer"]})
  |> Zvec.Schema.add_vector("vec", 384, index: %{type: :hnsw, metric_type: :cosine})
```

**Field types:** `:string`, `:int32`, `:int64`, `:uint32`, `:uint64`, `:float`, `:double`, `:bool`, `:binary`

**Vector types:** `:vector_fp32` (default), `:vector_fp16`, `:vector_fp64`, `:vector_int8`, `:vector_int16`

**Scalar index types:**

| Type | Use case | Key options |
|------|----------|-------------|
| `:invert` | Filtering | `enable_range_optimization`, `enable_extended_wildcard` |
| `:fts` | Full-text search (BM25) | `tokenizer` (`"standard"`, `"ngram"`, `"jieba"`, `"whitespace"`), `filters`, `extra_params` |

**Vector index types:**

| Type | Use case | Key options |
|------|----------|-------------|
| `:hnsw` | General-purpose ANN | `m`, `ef_construction`, `use_contiguous_memory`, `flat_data_type` |
| `:flat` | Brute-force (exact) | `use_contiguous_memory`, `storage_data_type` |
| `:ivf` | Large-scale datasets | `n_list`, `n_iters`, `use_soar` |
| `:vamana` | Graph ANN | `max_degree`, `search_list_size`, `alpha`, `two_pass_build` |
| `:diskann` | Disk-resident ANN | `max_degree`, `list_size`, `pq_chunk_num` |
| `:hnsw_rabitq` / `:ivf_rabitq` | RaBitQ-compressed ANN (x86_64 Linux/Windows only) | `total_bits`, `num_clusters` / `n_list` |

All vector indexes take `metric_type`: `:cosine`, `:l2`, or `:ip` (inner product).

**Quantization** (`:hnsw`, `:flat`, `:ivf`, `:vamana`, `:diskann`): `quantize_type: :fp16 | :int8 | :int4 | :uniform_uint4 | :uniform_uint7 | :uniform_uint8`, plus `enable_rotate: true` to rotate vectors before INT8/INT4 quantization for better recall.

See `Zvec.Schema.add_field/4` and `Zvec.Schema.add_vector/4` for every option and its default.

### Collection

```elixir
{:ok, col} = Zvec.Collection.create_and_open(path, schema)
{:ok, col} = Zvec.Collection.open(path)
{:ok, col} = Zvec.Collection.open(path, read_only: true)

:ok = Zvec.Collection.insert(col, docs)   # {:error, {:already_exists, _}} on duplicate pk
:ok = Zvec.Collection.upsert(col, docs)
:ok = Zvec.Collection.delete(col, ["pk1", "pk2"])
:ok = Zvec.Collection.delete_by_filter(col, "category = 'old'")
:ok = Zvec.Collection.optimize(col)
:ok = Zvec.Collection.flush(col)

{:ok, results} = Zvec.Collection.query(col, query)
{:ok, docs}    = Zvec.Collection.fetch(col, ["pk1"], output_fields: ["title"], include_vector: false)
{:ok, stats}   = Zvec.Collection.stats(col)
{:ok, schema}  = Zvec.Collection.schema(col)

# Iterate a snapshot of every document
{:ok, stream} = Zvec.Collection.stream(col, include_vector: false)
Enum.each(stream, &IO.inspect/1)

:ok = Zvec.Collection.create_index(col, "field", %{type: :invert})
:ok = Zvec.Collection.drop_index(col, "field")
:ok = Zvec.Collection.close(col)
:ok = Zvec.Collection.destroy(col)
```

### Documents

```elixir
doc = Zvec.Doc.new("primary_key", %{
  "text" => "hello",
  "vec" => Zvec.Query.float_list_to_binary([0.1, 0.2, 0.3])
})
```

### Queries

```elixir
# Basic vector search
query = Zvec.Query.vector("embedding", [0.1, 0.2, ...], topk: 10)

# With filter and options
query = Zvec.Query.vector("embedding", vector,
  topk: 10,
  filter: "category = 'ai' AND price > 10",
  include_vector: true,
  output_fields: ["title", "price"],
  query_params: %{type: :hnsw, ef: 500}
)

# Full-text search (natural language, or query syntax with +required -excluded "phrases")
query = Zvec.Query.fts("body", "vector database", default_operator: :and)
query = Zvec.Query.fts("body", ~s(+vector -slow "exact phrase"), syntax: :query)

# Hybrid search: fuse sub-query results with RRF or weighted scores
query =
  Zvec.Query.multi(
    [
      Zvec.Query.vector("embedding", vector, topk: 50),
      Zvec.Query.fts("body", "vector database", topk: 50)
    ],
    topk: 10,
    rerank: {:rrf, 60}            # or {:weighted, [0.7, 0.3]}
  )
```

**Query params** by index type: `:hnsw` (`ef`, `prefetch_offset`, `prefetch_lines`), `:hnsw_rabitq` (`ef`), `:vamana` (`ef_search`, `prefetch_offset`, `prefetch_lines`), `:ivf` / `:ivf_rabitq` (`nprobe`), `:diskann` (`list_size`), `:flat`. All accept `radius`, `is_linear`, `is_using_refiner`, and `scale_factor`.

### Vector Utilities

```elixir
# Float list <-> raw binary (float-32-native)
binary = Zvec.Query.float_list_to_binary([1.0, 2.0, 3.0])
floats = Zvec.Query.binary_to_float_list(binary)

# Works directly with Nx tensors
binary = Nx.to_binary(tensor)
```

## Error Handling

All functions return `{:error, {code, message}}` on failure:

```elixir
case Zvec.Collection.open("/bad/path") do
  {:ok, col} -> col
  {:error, {code, msg}} -> raise "zvec error (#{code}): #{msg}"
end
```

Error codes: `:not_found`, `:already_exists`, `:invalid_argument`, `:permission_denied`, `:internal_error`, `:not_supported`, `:failed_precondition`, `:resource_exhausted`, `:unavailable`

Malformed arguments (unknown index or query param types, wrong value types) raise `ArgumentError`.

## License

Apache-2.0 — see [LICENSE](LICENSE).
