# Zvec

Elixir NIF bindings for [zvec](https://github.com/alibaba/zvec), an in-process vector database from Alibaba.

Built with [Fine](https://github.com/elixir-nx/fine) for ergonomic C++ NIF integration. Vectors pass as raw binaries for zero-overhead encoding. All blocking operations run on dirty schedulers.

## Requirements

- Elixir >= 1.16
- CMake >= 3.13
- C++17 compiler (GCC 7+ or Clang 5+)
- Git (to clone zvec source on first build)

## Installation

```elixir
def deps do
  [
    {:zvec, "~> 0.4.0"}
  ]
end
```

The first `mix compile` clones and builds zvec from source (~5 minutes). Subsequent compiles are instant.

## Quick Start

```elixir
# 1. Define a schema
schema =
  Zvec.Schema.new("products")
  |> Zvec.Schema.add_field("title", :string)
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
  |> Zvec.Schema.add_vector("vec", 384, index: %{type: :hnsw, metric_type: :cosine})
```

**Field types:** `:string`, `:int32`, `:int64`, `:uint32`, `:uint64`, `:float`, `:double`, `:bool`, `:binary`

**Vector index types:**

| Type | Use case | Key options |
|------|----------|-------------|
| `:hnsw` | General-purpose ANN | `metric_type`, `m`, `ef_construction` |
| `:flat` | Brute-force (exact) | `metric_type` |
| `:ivf` | Large-scale datasets | `metric_type`, `n_list`, `n_iters` |

**Metric types:** `:cosine`, `:l2`, `:ip` (inner product)

### Collection

```elixir
{:ok, col} = Zvec.Collection.create_and_open(path, schema)
{:ok, col} = Zvec.Collection.open(path)
{:ok, col} = Zvec.Collection.open(path, read_only: true)

:ok = Zvec.Collection.insert(col, docs)
:ok = Zvec.Collection.upsert(col, docs)
:ok = Zvec.Collection.delete(col, ["pk1", "pk2"])
:ok = Zvec.Collection.delete_by_filter(col, "category = 'old'")
:ok = Zvec.Collection.optimize(col)
:ok = Zvec.Collection.flush(col)

{:ok, results} = Zvec.Collection.query(col, query)
{:ok, docs}    = Zvec.Collection.fetch(col, ["pk1"])
{:ok, stats}   = Zvec.Collection.stats(col)
{:ok, schema}  = Zvec.Collection.schema(col)

:ok = Zvec.Collection.create_index(col, "field", %{type: :invert})
:ok = Zvec.Collection.drop_index(col, "field")
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
```

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

## License

Apache-2.0 — see [LICENSE](LICENSE).
