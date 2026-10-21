defmodule Zvec.Collection do
  @moduledoc """
  High-level API for zvec collections.

  ## Example

      schema =
        Zvec.Schema.new("test")
        |> Zvec.Schema.add_vector("vec", 4, index: %{type: :hnsw, metric_type: :cosine})

      {:ok, col} = Zvec.Collection.create_and_open("/tmp/mydb", schema)
      :ok = Zvec.Collection.insert(col, [Zvec.Doc.new("pk1", %{"vec" => vec_binary})])
      :ok = Zvec.Collection.optimize(col)
      {:ok, results} = Zvec.Collection.query(col, Zvec.Query.vector("vec", [0.1, 0.2, 0.3, 0.4], topk: 1))
  """

  alias Zvec.{Native, Schema, Doc}

  @type ref :: reference()
  @type error :: {:error, {atom(), String.t()}}

  @doc """
  Create a new collection and open it.

  ## Options

    * `:read_only` - open read-only (default: `false`)
    * `:enable_mmap` - enable memory mapping (default: `true`)
    * `:max_buffer_size` - write buffer size in bytes (default: 64MB)
  """
  @spec create_and_open(String.t(), Schema.t(), keyword()) :: {:ok, ref()} | error()
  def create_and_open(path, %Schema{} = schema, opts \\ []) do
    options = opts_to_map(opts)
    Native.collection_create_and_open(path, Schema.to_nif(schema), options) |> normalize()
  end

  @doc "Open an existing collection. Accepts the same options as `create_and_open/3`."
  @spec open(String.t(), keyword()) :: {:ok, ref()} | error()
  def open(path, opts \\ []) do
    options = opts_to_map(opts)
    Native.collection_open(path, options) |> normalize()
  end

  @doc """
  Close the collection, releasing its files and locks.

  Fails with `:failed_precondition` while an iterator from `stream/2` is open.
  The collection is also closed automatically when its reference is garbage collected.
  """
  @spec close(ref()) :: :ok | error()
  def close(ref), do: Native.collection_close(ref) |> normalize()

  @doc "Destroy the collection (deletes data on disk)."
  @spec destroy(ref()) :: :ok | error()
  def destroy(ref), do: Native.collection_destroy(ref) |> normalize()

  @doc "Flush pending writes to disk."
  @spec flush(ref()) :: :ok | error()
  def flush(ref), do: Native.collection_flush(ref) |> normalize()

  @doc "Get collection statistics."
  @spec stats(ref()) :: {:ok, map()} | error()
  def stats(ref), do: Native.collection_stats(ref) |> normalize()

  @doc "Get the collection schema."
  @spec schema(ref()) :: {:ok, map()} | error()
  def schema(ref), do: Native.collection_schema(ref) |> normalize()

  @doc """
  Insert documents into the collection.

  Returns `:ok` if every document was inserted, otherwise the first
  per-document error (e.g. `{:error, {:already_exists, msg}}` for a duplicate pk).
  Documents before and after a failing one in the same batch may still be written.
  """
  @spec insert(ref(), [Doc.t()]) :: :ok | error()
  def insert(ref, docs) when is_list(docs) do
    nif_docs = Enum.map(docs, &Doc.to_nif/1)
    Native.collection_insert(ref, nif_docs) |> normalize() |> write_result()
  end

  @doc """
  Upsert documents (insert or update if pk exists).

  Returns `:ok` if every document was written, otherwise the first per-document error.
  """
  @spec upsert(ref(), [Doc.t()]) :: :ok | error()
  def upsert(ref, docs) when is_list(docs) do
    nif_docs = Enum.map(docs, &Doc.to_nif/1)
    Native.collection_upsert(ref, nif_docs) |> normalize() |> write_result()
  end

  defp write_result({:ok, results}) do
    Enum.find(results, :ok, &match?({:error, _}, &1))
  end

  defp write_result({:error, _} = err), do: err

  @doc "Delete documents by primary keys."
  @spec delete(ref(), [String.t()]) :: :ok | error()
  def delete(ref, pks) when is_list(pks), do: Native.collection_delete(ref, pks) |> normalize()

  @doc "Delete documents matching a filter expression."
  @spec delete_by_filter(ref(), String.t()) :: :ok | error()
  def delete_by_filter(ref, filter) when is_binary(filter) do
    Native.collection_delete_by_filter(ref, filter) |> normalize()
  end

  @doc """
  Execute a query built with `Zvec.Query.vector/3`, `Zvec.Query.fts/3`, or
  `Zvec.Query.multi/2`.

  Returns `{:ok, results}` where each result is a `Zvec.Doc` with `:pk`, `:score`, and `:fields`.
  """
  @spec query(ref(), map()) :: {:ok, [Doc.t()]} | error()
  def query(ref, %{queries: _} = multi_query) do
    Native.collection_multi_query(ref, multi_query) |> to_docs()
  end

  def query(ref, query_map) when is_map(query_map) do
    Native.collection_query(ref, query_map) |> to_docs()
  end

  @doc """
  Fetch documents by primary keys.

  Documents are returned in the order of `pks`; missing keys are skipped.

  ## Options

    * `:output_fields` - list of field names to return (default: all fields)
    * `:include_vector` - whether to return vector fields (default: `true`)
  """
  @spec fetch(ref(), [String.t()], keyword()) :: {:ok, [Doc.t()]} | error()
  def fetch(ref, pks, opts \\ []) when is_list(pks) do
    options = %{
      output_fields: Keyword.get(opts, :output_fields),
      include_vector: Keyword.get(opts, :include_vector, true)
    }

    Native.collection_fetch(ref, pks, options) |> to_docs()
  end

  @doc """
  Iterate over every document in the collection.

  The iterator reads an isolated snapshot taken when this function is called
  (on a writable collection this seals the current writing segment). Returns
  `{:ok, stream}`; the stream yields `Zvec.Doc` structs and closes the
  underlying iterator when it finishes or is halted.

  While an iterator is open, schema changes (`create_index/3`, `drop_index/2`),
  `optimize/1`, `close/1`, and `destroy/1` return an error. Writes, flushes and
  queries are not affected.

  Iteration errors raise `RuntimeError`.

  ## Options

    * `:output_fields` - list of field names to return (default: all fields)
    * `:include_vector` - whether to return vector fields (default: `true`)
    * `:batch_size` - number of documents read from the NIF per call (default: `256`)
  """
  @spec stream(ref(), keyword()) :: {:ok, Enumerable.t()} | error()
  def stream(ref, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 256)

    options = %{
      output_fields: Keyword.get(opts, :output_fields),
      include_vector: Keyword.get(opts, :include_vector, true)
    }

    case Native.iterator_create(ref, options) |> normalize() do
      {:ok, iterator} ->
        {:ok,
         Stream.resource(
           fn -> iterator end,
           &next_batch(&1, batch_size),
           &Native.iterator_close/1
         )}

      {:error, _} = err ->
        err
    end
  end

  defp next_batch(iterator, batch_size) do
    case Native.iterator_next(iterator, batch_size) |> normalize() do
      {:ok, []} -> {:halt, iterator}
      {:ok, docs} -> {Enum.map(docs, &Doc.from_nif/1), iterator}
      {:error, {code, msg}} -> raise "zvec iterator error (#{code}): #{msg}"
    end
  end

  @doc "Create an index on a column."
  @spec create_index(ref(), String.t(), map()) :: :ok | error()
  def create_index(ref, column, params) when is_binary(column) and is_map(params) do
    Native.collection_create_index(ref, column, params) |> normalize()
  end

  @doc "Drop an index from a column."
  @spec drop_index(ref(), String.t()) :: :ok | error()
  def drop_index(ref, column) when is_binary(column) do
    Native.collection_drop_index(ref, column) |> normalize()
  end

  @doc "Optimize the collection (build ANN indexes, compact segments)."
  @spec optimize(ref()) :: :ok | error()
  def optimize(ref), do: Native.collection_optimize(ref) |> normalize()

  defp opts_to_map(opts) do
    %{
      read_only: Keyword.get(opts, :read_only, false),
      enable_mmap: Keyword.get(opts, :enable_mmap, true),
      max_buffer_size: Keyword.get(opts, :max_buffer_size, 64 * 1024 * 1024)
    }
  end

  defp to_docs(result) do
    case normalize(result) do
      {:ok, docs} -> {:ok, Enum.map(docs, &Doc.from_nif/1)}
      {:error, _} = err -> err
    end
  end

  # Fine's Error<Atom, string> produces {:error, atom, string}
  # Normalize to {:error, {atom, string}}
  defp normalize({:error, code, msg}) when is_atom(code), do: {:error, {code, msg}}
  defp normalize(other), do: other
end
