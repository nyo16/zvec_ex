#include <fine.hpp>

#include <zvec/db/collection.h>
#include <zvec/db/doc.h>
#include <zvec/db/doc_iterator.h>
#include <zvec/db/index_params.h>
#include <zvec/db/options.h>
#include <zvec/db/query.h>
#include <zvec/db/query_params.h>
#include <zvec/db/reranker.h>
#include <zvec/db/schema.h>
#include <zvec/db/stats.h>
#include <zvec/db/status.h>
#include <zvec/db/type.h>

#include <algorithm>
#include <cstring>
#include <mutex>
#include <optional>
#include <string>
#include <variant>
#include <vector>

// ---------------------------------------------------------------------------
// Atoms
// ---------------------------------------------------------------------------
namespace atoms {
// Status codes
static auto ok = fine::Atom("ok");
static auto not_found = fine::Atom("not_found");
static auto already_exists = fine::Atom("already_exists");
static auto invalid_argument = fine::Atom("invalid_argument");
static auto permission_denied = fine::Atom("permission_denied");
static auto failed_precondition = fine::Atom("failed_precondition");
static auto resource_exhausted = fine::Atom("resource_exhausted");
static auto unavailable = fine::Atom("unavailable");
static auto internal_error = fine::Atom("internal_error");
static auto not_supported = fine::Atom("not_supported");
static auto unknown = fine::Atom("unknown");
}  // namespace atoms

// ---------------------------------------------------------------------------
// Resources
// ---------------------------------------------------------------------------
struct CollectionResource {
  zvec::Collection::Ptr ptr;
  CollectionResource(zvec::Collection::Ptr p) : ptr(std::move(p)) {}
};
FINE_RESOURCE(CollectionResource);

// zvec requires the collection to outlive its iterators, so the iterator
// resource keeps its own reference. Members are destroyed in reverse order:
// the iterator is released before the collection.
struct IteratorResource {
  zvec::Collection::Ptr collection;
  zvec::DocIterator::Ptr iter;
  std::mutex mutex;
  bool closed = false;

  IteratorResource(zvec::Collection::Ptr c, zvec::DocIterator::Ptr i)
      : collection(std::move(c)), iter(std::move(i)) {}

  ~IteratorResource() {
    if (!closed) iter->close();
  }
};
FINE_RESOURCE(IteratorResource);

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------
using OkRef = fine::Ok<fine::ResourcePtr<CollectionResource>>;
using OkIter = fine::Ok<fine::ResourcePtr<IteratorResource>>;
using OkTerm = fine::Ok<fine::Term>;
using OkNothing = fine::Ok<>;
using Err = fine::Error<fine::Atom, std::string>;
using ResultRef = std::variant<OkRef, Err>;
using ResultIter = std::variant<OkIter, Err>;
using ResultTerm = std::variant<OkTerm, Err>;
using ResultOk = std::variant<OkNothing, Err>;

// ---------------------------------------------------------------------------
// Helpers: Status -> Err
// ---------------------------------------------------------------------------
static fine::Atom status_code_to_atom(zvec::StatusCode code) {
  switch (code) {
    case zvec::StatusCode::OK:
      return atoms::ok;
    case zvec::StatusCode::NOT_FOUND:
      return atoms::not_found;
    case zvec::StatusCode::ALREADY_EXISTS:
      return atoms::already_exists;
    case zvec::StatusCode::INVALID_ARGUMENT:
      return atoms::invalid_argument;
    case zvec::StatusCode::PERMISSION_DENIED:
      return atoms::permission_denied;
    case zvec::StatusCode::FAILED_PRECONDITION:
      return atoms::failed_precondition;
    case zvec::StatusCode::RESOURCE_EXHAUSTED:
      return atoms::resource_exhausted;
    case zvec::StatusCode::UNAVAILABLE:
      return atoms::unavailable;
    case zvec::StatusCode::INTERNAL_ERROR:
      return atoms::internal_error;
    case zvec::StatusCode::NOT_SUPPORTED:
      return atoms::not_supported;
    default:
      return atoms::unknown;
  }
}

static Err status_to_error(const zvec::Status &s) {
  return Err(status_code_to_atom(s.code()), std::string(s.message()));
}

// ---------------------------------------------------------------------------
// Helpers: Decode Elixir terms -> zvec types
// ---------------------------------------------------------------------------

// Get a map value by atom key, returns 0 on missing
static ERL_NIF_TERM map_get(ErlNifEnv *env, ERL_NIF_TERM map,
                            const char *key) {
  ERL_NIF_TERM k = enif_make_atom(env, key);
  ERL_NIF_TERM v;
  if (enif_get_map_value(env, map, k, &v)) {
    return v;
  }
  return 0;
}

static bool term_is_nil(ErlNifEnv *env, ERL_NIF_TERM term) {
  return enif_is_identical(term, enif_make_atom(env, "nil"));
}

// Get a map value by atom key, returns 0 on missing or nil
static ERL_NIF_TERM map_get_present(ErlNifEnv *env, ERL_NIF_TERM map,
                                    const char *key) {
  auto v = map_get(env, map, key);
  if (v && term_is_nil(env, v)) return 0;
  return v;
}

static std::string term_to_string(ErlNifEnv *env, ERL_NIF_TERM term) {
  ErlNifBinary bin;
  if (enif_inspect_binary(env, term, &bin)) {
    return std::string(reinterpret_cast<const char *>(bin.data), bin.size);
  }
  // Try iolist
  if (enif_inspect_iolist_as_binary(env, term, &bin)) {
    return std::string(reinterpret_cast<const char *>(bin.data), bin.size);
  }
  throw std::invalid_argument("expected binary/string");
}

static std::string term_to_atom_string(ErlNifEnv *env, ERL_NIF_TERM term) {
  char buf[256];
  if (enif_get_atom(env, term, buf, sizeof(buf), ERL_NIF_LATIN1)) {
    return std::string(buf);
  }
  throw std::invalid_argument("expected atom");
}

static int64_t term_to_int(ErlNifEnv *env, ERL_NIF_TERM term) {
  ErlNifSInt64 val;
  if (enif_get_int64(env, term, &val)) return static_cast<int64_t>(val);
  throw std::invalid_argument("expected integer");
}

static double term_to_double(ErlNifEnv *env, ERL_NIF_TERM term) {
  double val;
  if (enif_get_double(env, term, &val)) return val;
  // Accept integer as double
  ErlNifSInt64 ival;
  if (enif_get_int64(env, term, &ival)) return static_cast<double>(ival);
  throw std::invalid_argument("expected number");
}

static bool term_to_bool(ErlNifEnv *env, ERL_NIF_TERM term) {
  char buf[16];
  if (enif_get_atom(env, term, buf, sizeof(buf), ERL_NIF_LATIN1)) {
    if (strcmp(buf, "true") == 0) return true;
    if (strcmp(buf, "false") == 0) return false;
  }
  throw std::invalid_argument("expected boolean atom");
}

static std::vector<std::string> term_to_string_list(ErlNifEnv *env,
                                                    ERL_NIF_TERM term) {
  unsigned len;
  if (!enif_get_list_length(env, term, &len)) {
    throw std::invalid_argument("expected list of strings");
  }
  std::vector<std::string> out;
  out.reserve(len);
  ERL_NIF_TERM head, tail = term;
  while (enif_get_list_cell(env, tail, &head, &tail)) {
    out.push_back(term_to_string(env, head));
  }
  return out;
}

static std::optional<std::vector<std::string>> decode_output_fields(
    ErlNifEnv *env, ERL_NIF_TERM map) {
  auto term = map_get_present(env, map, "output_fields");
  if (!term) return std::nullopt;
  return term_to_string_list(env, term);
}

static zvec::MetricType decode_metric_type(ErlNifEnv *env,
                                           ERL_NIF_TERM term) {
  auto s = term_to_atom_string(env, term);
  if (s == "l2") return zvec::MetricType::L2;
  if (s == "ip") return zvec::MetricType::IP;
  if (s == "cosine") return zvec::MetricType::COSINE;
  throw std::invalid_argument("unknown metric_type: " + s);
}

static zvec::QuantizeType decode_quantize_type(ErlNifEnv *env,
                                               ERL_NIF_TERM term) {
  auto s = term_to_atom_string(env, term);
  if (s == "fp16") return zvec::QuantizeType::FP16;
  if (s == "int8") return zvec::QuantizeType::INT8;
  if (s == "int4") return zvec::QuantizeType::INT4;
  if (s == "rabitq") return zvec::QuantizeType::RABITQ;
  if (s == "uniform_uint4") return zvec::QuantizeType::UNIFORM_UINT4;
  if (s == "uniform_uint7") return zvec::QuantizeType::UNIFORM_UINT7;
  if (s == "uniform_uint8") return zvec::QuantizeType::UNIFORM_UINT8;
  if (s == "undefined") return zvec::QuantizeType::UNDEFINED;
  throw std::invalid_argument("unknown quantize_type: " + s);
}

struct DataTypeName {
  zvec::DataType type;
  const char *name;
};

static const DataTypeName kDataTypeNames[] = {
    {zvec::DataType::BINARY, "binary"},
    {zvec::DataType::STRING, "string"},
    {zvec::DataType::BOOL, "bool"},
    {zvec::DataType::INT32, "int32"},
    {zvec::DataType::INT64, "int64"},
    {zvec::DataType::UINT32, "uint32"},
    {zvec::DataType::UINT64, "uint64"},
    {zvec::DataType::FLOAT, "float"},
    {zvec::DataType::DOUBLE, "double"},
    {zvec::DataType::VECTOR_BINARY32, "vector_binary32"},
    {zvec::DataType::VECTOR_BINARY64, "vector_binary64"},
    {zvec::DataType::VECTOR_FP16, "vector_fp16"},
    {zvec::DataType::VECTOR_FP32, "vector_fp32"},
    {zvec::DataType::VECTOR_FP64, "vector_fp64"},
    {zvec::DataType::VECTOR_INT8, "vector_int8"},
    {zvec::DataType::VECTOR_INT16, "vector_int16"},
    {zvec::DataType::VECTOR_UINT8, "vector_uint8"},
    {zvec::DataType::SPARSE_VECTOR_FP16, "sparse_vector_fp16"},
    {zvec::DataType::SPARSE_VECTOR_FP32, "sparse_vector_fp32"},
};

static zvec::DataType decode_data_type(ErlNifEnv *env, ERL_NIF_TERM term) {
  auto s = term_to_atom_string(env, term);
  for (const auto &entry : kDataTypeNames) {
    if (s == entry.name) return entry.type;
  }
  throw std::invalid_argument("unknown data_type: " + s);
}

static ERL_NIF_TERM encode_data_type(ErlNifEnv *env, zvec::DataType dt) {
  for (const auto &entry : kDataTypeNames) {
    if (entry.type == dt) return enif_make_atom(env, entry.name);
  }
  return enif_make_atom(env, "unknown");
}

static int decode_int_opt(ErlNifEnv *env, ERL_NIF_TERM map, const char *key,
                          int fallback) {
  auto t = map_get_present(env, map, key);
  return t ? static_cast<int>(term_to_int(env, t)) : fallback;
}

// Settings shared by every quantizable vector index: :quantize_type and
// :enable_rotate (random rotation before INT8/INT4 quantization).
static void apply_quantization(ErlNifEnv *env, ERL_NIF_TERM term,
                               zvec::VectorIndexParams &params) {
  if (auto t = map_get_present(env, term, "quantize_type")) {
    params.set_quantize_type(decode_quantize_type(env, t));
  }
  if (auto t = map_get_present(env, term, "enable_rotate")) {
    params.set_quantizer_param(zvec::QuantizerParam(term_to_bool(env, t)));
  }
}

// Decode index params from an Elixir map: %{type: :hnsw, metric_type: :cosine, ...}
static zvec::IndexParams::Ptr decode_index_params(ErlNifEnv *env,
                                                  ERL_NIF_TERM term) {
  if (term_is_nil(env, term)) return nullptr;
  if (!enif_is_map(env, term)) {
    throw std::invalid_argument("index params must be a map");
  }

  auto type_term = map_get(env, term, "type");
  if (!type_term) throw std::invalid_argument("index_params missing :type");

  auto type_str = term_to_atom_string(env, type_term);

  if (type_str == "invert") {
    auto p = std::make_shared<zvec::InvertIndexParams>();
    if (auto t = map_get_present(env, term, "enable_range_optimization")) {
      p->set_enable_range_optimization(term_to_bool(env, t));
    }
    if (auto t = map_get_present(env, term, "enable_extended_wildcard")) {
      p->set_enable_extended_wildcard(term_to_bool(env, t));
    }
    return p;
  }

  if (type_str == "fts") {
    std::string tokenizer = "standard";
    std::vector<std::string> filters = {"lowercase"};
    std::string extra_params;
    if (auto t = map_get_present(env, term, "tokenizer")) {
      tokenizer = term_to_string(env, t);
    }
    if (auto t = map_get_present(env, term, "filters")) {
      filters = term_to_string_list(env, t);
    }
    if (auto t = map_get_present(env, term, "extra_params")) {
      extra_params = term_to_string(env, t);
    }
    return std::make_shared<zvec::FtsIndexParams>(
        std::move(tokenizer), std::move(filters), std::move(extra_params));
  }

  // Vector index types need metric_type
  auto mt_term = map_get(env, term, "metric_type");
  if (!mt_term) throw std::invalid_argument("vector index_params missing :metric_type");
  auto metric = decode_metric_type(env, mt_term);

  if (type_str == "hnsw") {
    auto p = std::make_shared<zvec::HnswIndexParams>(
        metric, decode_int_opt(env, term, "m", 16),
        decode_int_opt(env, term, "ef_construction", 200));
    apply_quantization(env, term, *p);
    if (auto t = map_get_present(env, term, "use_contiguous_memory")) {
      p->set_use_contiguous_memory(term_to_bool(env, t));
    }
    if (auto t = map_get_present(env, term, "use_flat_contiguous_memory")) {
      p->set_use_flat_contiguous_memory(term_to_bool(env, t));
    }
    if (auto t = map_get_present(env, term, "flat_data_type")) {
      p->set_flat_data_type(decode_data_type(env, t));
    }
    return p;
  }

  if (type_str == "flat") {
    // FlatIndexParams exposes these knobs only through its constructor.
    bool use_contig = false;
    zvec::DataType storage = zvec::DataType::UNDEFINED;
    if (auto t = map_get_present(env, term, "use_contiguous_memory")) {
      use_contig = term_to_bool(env, t);
    }
    if (auto t = map_get_present(env, term, "storage_data_type")) {
      storage = decode_data_type(env, t);
    }
    auto p = std::make_shared<zvec::FlatIndexParams>(
        metric, zvec::QuantizeType::UNDEFINED, zvec::QuantizerParam{},
        use_contig, storage);
    apply_quantization(env, term, *p);
    return p;
  }

  if (type_str == "ivf") {
    auto p = std::make_shared<zvec::IVFIndexParams>(metric);
    apply_quantization(env, term, *p);
    if (auto t = map_get_present(env, term, "n_list")) {
      p->set_n_list(static_cast<int>(term_to_int(env, t)));
    }
    if (auto t = map_get_present(env, term, "n_iters")) {
      p->set_n_iters(static_cast<int>(term_to_int(env, t)));
    }
    if (auto t = map_get_present(env, term, "use_soar")) {
      p->set_use_soar(term_to_bool(env, t));
    }
    return p;
  }

  if (type_str == "hnsw_rabitq") {
    auto p = std::make_shared<zvec::HnswRabitqIndexParams>(
        metric, decode_int_opt(env, term, "total_bits", 7),
        decode_int_opt(env, term, "num_clusters", 16),
        decode_int_opt(env, term, "m", 16),
        decode_int_opt(env, term, "ef_construction", 200),
        decode_int_opt(env, term, "sample_count", 0));
    return p;
  }

  if (type_str == "ivf_rabitq") {
    auto p = std::make_shared<zvec::IvfRabitqIndexParams>(metric);
    if (auto t = map_get_present(env, term, "n_list")) {
      p->set_nlist(static_cast<int>(term_to_int(env, t)));
    }
    if (auto t = map_get_present(env, term, "total_bits")) {
      p->set_total_bits(static_cast<int>(term_to_int(env, t)));
    }
    if (auto t = map_get_present(env, term, "sample_count")) {
      p->set_sample_count(static_cast<int>(term_to_int(env, t)));
    }
    return p;
  }

  if (type_str == "diskann") {
    auto p = std::make_shared<zvec::DiskAnnIndexParams>(metric);
    apply_quantization(env, term, *p);
    if (auto t = map_get_present(env, term, "max_degree")) {
      p->set_max_degree(static_cast<int>(term_to_int(env, t)));
    }
    if (auto t = map_get_present(env, term, "list_size")) {
      p->set_list_size(static_cast<int>(term_to_int(env, t)));
    }
    if (auto t = map_get_present(env, term, "pq_chunk_num")) {
      p->set_pq_chunk_num(static_cast<int>(term_to_int(env, t)));
    }
    return p;
  }

  if (type_str == "vamana") {
    auto p = std::make_shared<zvec::VamanaIndexParams>(metric);
    apply_quantization(env, term, *p);
    if (auto t = map_get_present(env, term, "max_degree")) {
      p->set_max_degree(static_cast<int>(term_to_int(env, t)));
    }
    if (auto t = map_get_present(env, term, "search_list_size")) {
      p->set_search_list_size(static_cast<int>(term_to_int(env, t)));
    }
    if (auto t = map_get_present(env, term, "alpha")) {
      p->set_alpha(static_cast<float>(term_to_double(env, t)));
    }
    if (auto t = map_get_present(env, term, "saturate_graph")) {
      p->set_saturate_graph(term_to_bool(env, t));
    }
    if (auto t = map_get_present(env, term, "use_contiguous_memory")) {
      p->set_use_contiguous_memory(term_to_bool(env, t));
    }
    if (auto t = map_get_present(env, term, "use_id_map")) {
      p->set_use_id_map(term_to_bool(env, t));
    }
    if (auto t = map_get_present(env, term, "two_pass_build")) {
      p->set_two_pass_build(term_to_bool(env, t));
    }
    if (auto t = map_get_present(env, term, "use_flat_contiguous_memory")) {
      p->set_use_flat_contiguous_memory(term_to_bool(env, t));
    }
    if (auto t = map_get_present(env, term, "flat_data_type")) {
      p->set_flat_data_type(decode_data_type(env, t));
    }
    return p;
  }

  throw std::invalid_argument("unknown index type: " + type_str);
}

// Decode a single field schema from Elixir map
static zvec::FieldSchema::Ptr decode_field_schema(ErlNifEnv *env,
                                                  ERL_NIF_TERM term) {
  auto name = term_to_string(env, map_get(env, term, "name"));
  auto type_term = map_get(env, term, "type");
  auto data_type = decode_data_type(env, type_term);

  // decode_doc cannot write these types, so a collection with such a field
  // could never be populated. They stay in kDataTypeNames so encode_data_type
  // still names fields of collections created by other bindings.
  switch (data_type) {
    case zvec::DataType::SPARSE_VECTOR_FP16:
    case zvec::DataType::SPARSE_VECTOR_FP32:
    case zvec::DataType::VECTOR_BINARY32:
    case zvec::DataType::VECTOR_BINARY64:
    case zvec::DataType::VECTOR_UINT8:
      throw std::invalid_argument("unsupported field type for field " + name +
                                  ": " + term_to_atom_string(env, type_term));
    default:
      break;
  }

  bool nullable = false;
  if (auto t = map_get_present(env, term, "nullable")) {
    nullable = term_to_bool(env, t);
  }

  zvec::IndexParams::Ptr index_params = nullptr;
  if (auto t = map_get_present(env, term, "index_params")) {
    index_params = decode_index_params(env, t);
  }

  uint32_t dimension = 0;
  if (auto t = map_get_present(env, term, "dimension")) {
    dimension = static_cast<uint32_t>(term_to_int(env, t));
  }

  if (dimension > 0) {
    return std::make_shared<zvec::FieldSchema>(name, data_type, dimension,
                                               nullable, index_params);
  }
  return std::make_shared<zvec::FieldSchema>(name, data_type, nullable,
                                             index_params);
}

// Decode collection schema from Elixir map:
// %{name: "...", fields: [%{name:, type:, ...}]}
static zvec::CollectionSchema decode_schema(ErlNifEnv *env,
                                            ERL_NIF_TERM term) {
  auto name = term_to_string(env, map_get(env, term, "name"));
  zvec::CollectionSchema schema(name);

  if (auto t = map_get_present(env, term, "max_doc_count_per_segment")) {
    schema.set_max_doc_count_per_segment(
        static_cast<uint64_t>(term_to_int(env, t)));
  }

  auto fields_term = map_get(env, term, "fields");
  if (!fields_term) throw std::invalid_argument("schema missing :fields");

  unsigned len;
  if (!enif_get_list_length(env, fields_term, &len)) {
    throw std::invalid_argument("schema :fields must be a list");
  }

  ERL_NIF_TERM head, tail = fields_term;
  while (enif_get_list_cell(env, tail, &head, &tail)) {
    auto field = decode_field_schema(env, head);
    auto status = schema.add_field(field);
    if (!status.ok()) {
      throw std::invalid_argument("add_field failed: " + status.message());
    }
  }

  return schema;
}

// Decode collection options from Elixir map
static zvec::CollectionOptions decode_options(ErlNifEnv *env,
                                              ERL_NIF_TERM term) {
  zvec::CollectionOptions opts;
  if (term_is_nil(env, term)) return opts;

  if (auto t = map_get_present(env, term, "read_only")) {
    opts.read_only_ = term_to_bool(env, t);
  }
  if (auto t = map_get_present(env, term, "enable_mmap")) {
    opts.enable_mmap_ = term_to_bool(env, t);
  }
  if (auto t = map_get_present(env, term, "max_buffer_size")) {
    opts.max_buffer_size_ = static_cast<uint32_t>(term_to_int(env, t));
  }

  return opts;
}

template <typename T>
static std::vector<T> binary_to_vector(ErlNifEnv *env, ERL_NIF_TERM term,
                                       const char *type_name) {
  ErlNifBinary bin;
  if (!enif_inspect_binary(env, term, &bin)) {
    throw std::invalid_argument(std::string(type_name) +
                                " field expects binary");
  }
  if (bin.size % sizeof(T) != 0) {
    throw std::invalid_argument(std::string(type_name) +
                                " binary size is not a multiple of the "
                                "element size");
  }
  std::vector<T> vec(bin.size / sizeof(T));
  std::memcpy(static_cast<void *>(vec.data()), bin.data, bin.size);
  return vec;
}

// Decode a Doc from Elixir: %{pk: "...", fields: %{"name" => value, ...}}
// We need the collection schema to know field types for proper dispatch
static zvec::Doc decode_doc(ErlNifEnv *env, ERL_NIF_TERM term,
                            const zvec::CollectionSchema &schema) {
  zvec::Doc doc;

  auto pk_term = map_get(env, term, "pk");
  if (pk_term) doc.set_pk(term_to_string(env, pk_term));

  auto fields_term = map_get(env, term, "fields");
  if (!fields_term) return doc;

  ErlNifMapIterator iter;
  if (!enif_map_iterator_create(env, fields_term, &iter,
                                ERL_NIF_MAP_ITERATOR_FIRST)) {
    throw std::invalid_argument("doc :fields must be a map");
  }

  // Ensure the iterator is released even if a field fails to decode.
  struct IterGuard {
    ErlNifEnv *env;
    ErlNifMapIterator *iter;
    ~IterGuard() { enif_map_iterator_destroy(env, iter); }
  } guard{env, &iter};

  ERL_NIF_TERM key, value;
  for (; enif_map_iterator_get_pair(env, &iter, &key, &value);
       enif_map_iterator_next(env, &iter)) {
    auto field_name = term_to_string(env, key);
    auto *field_schema = schema.get_field(field_name);
    if (!field_schema) continue;  // Skip unknown fields

    if (term_is_nil(env, value)) {
      doc.set_null(field_name);
      continue;
    }

    switch (field_schema->data_type()) {
      case zvec::DataType::STRING:
      case zvec::DataType::BINARY:
        doc.set<std::string>(field_name, term_to_string(env, value));
        break;
      case zvec::DataType::BOOL:
        doc.set<bool>(field_name, term_to_bool(env, value));
        break;
      case zvec::DataType::INT32:
        doc.set<int32_t>(field_name,
                         static_cast<int32_t>(term_to_int(env, value)));
        break;
      case zvec::DataType::INT64:
        doc.set<int64_t>(field_name, term_to_int(env, value));
        break;
      case zvec::DataType::UINT32:
        doc.set<uint32_t>(field_name,
                          static_cast<uint32_t>(term_to_int(env, value)));
        break;
      case zvec::DataType::UINT64:
        doc.set<uint64_t>(field_name,
                          static_cast<uint64_t>(term_to_int(env, value)));
        break;
      case zvec::DataType::FLOAT:
        doc.set<float>(field_name,
                       static_cast<float>(term_to_double(env, value)));
        break;
      case zvec::DataType::DOUBLE:
        doc.set<double>(field_name, term_to_double(env, value));
        break;
      case zvec::DataType::VECTOR_FP32:
        doc.set<std::vector<float>>(
            field_name, binary_to_vector<float>(env, value, "vector_fp32"));
        break;
      case zvec::DataType::VECTOR_FP64:
        doc.set<std::vector<double>>(
            field_name, binary_to_vector<double>(env, value, "vector_fp64"));
        break;
      case zvec::DataType::VECTOR_FP16:
        doc.set<std::vector<zvec::float16_t>>(
            field_name,
            binary_to_vector<zvec::float16_t>(env, value, "vector_fp16"));
        break;
      case zvec::DataType::VECTOR_INT8:
        doc.set<std::vector<int8_t>>(
            field_name, binary_to_vector<int8_t>(env, value, "vector_int8"));
        break;
      case zvec::DataType::VECTOR_INT16:
        doc.set<std::vector<int16_t>>(
            field_name, binary_to_vector<int16_t>(env, value, "vector_int16"));
        break;
      default:
        throw std::invalid_argument("unsupported field type for field: " +
                                    field_name);
    }
  }

  return doc;
}

static std::vector<zvec::Doc> decode_docs(ErlNifEnv *env, ERL_NIF_TERM term,
                                          const zvec::CollectionSchema &schema) {
  unsigned len;
  if (!enif_get_list_length(env, term, &len)) {
    throw std::invalid_argument("expected list of docs");
  }

  std::vector<zvec::Doc> docs;
  docs.reserve(len);
  ERL_NIF_TERM head, tail = term;
  while (enif_get_list_cell(env, tail, &head, &tail)) {
    docs.push_back(decode_doc(env, head, schema));
  }
  return docs;
}

// Decode query params: %{type: :hnsw, ef: 300, ...}. Unset `ef`/`ef_search`
// default to 200 (this binding's historical default; zvec's own HNSW default
// is 300). Every other unset knob keeps zvec's default for the index type.
static zvec::QueryParams::Ptr decode_query_params(ErlNifEnv *env,
                                                  ERL_NIF_TERM term) {
  if (!enif_is_map(env, term)) {
    throw std::invalid_argument("query_params must be a map");
  }
  auto type_term = map_get(env, term, "type");
  if (!type_term) throw std::invalid_argument("query_params missing :type");
  auto type_str = term_to_atom_string(env, type_term);

  zvec::QueryParams::Ptr params;

  if (type_str == "hnsw") {
    auto p = std::make_shared<zvec::HnswQueryParams>(
        decode_int_opt(env, term, "ef", 200));
    if (auto t = map_get_present(env, term, "prefetch_offset")) {
      p->set_prefetch_offset(static_cast<uint32_t>(term_to_int(env, t)));
    }
    if (auto t = map_get_present(env, term, "prefetch_lines")) {
      p->set_prefetch_lines(static_cast<uint32_t>(term_to_int(env, t)));
    }
    params = p;
  } else if (type_str == "hnsw_rabitq") {
    params = std::make_shared<zvec::HnswRabitqQueryParams>(
        decode_int_opt(env, term, "ef", 200));
  } else if (type_str == "vamana") {
    auto p = std::make_shared<zvec::VamanaQueryParams>(
        decode_int_opt(env, term, "ef_search", 200));
    if (auto t = map_get_present(env, term, "prefetch_offset")) {
      p->set_prefetch_offset(static_cast<uint32_t>(term_to_int(env, t)));
    }
    if (auto t = map_get_present(env, term, "prefetch_lines")) {
      p->set_prefetch_lines(static_cast<uint32_t>(term_to_int(env, t)));
    }
    params = p;
  } else if (type_str == "ivf") {
    auto p = std::make_shared<zvec::IVFQueryParams>();
    if (auto t = map_get_present(env, term, "nprobe")) {
      p->set_nprobe(static_cast<int>(term_to_int(env, t)));
    }
    params = p;
  } else if (type_str == "ivf_rabitq") {
    auto p = std::make_shared<zvec::IvfRabitqQueryParams>();
    if (auto t = map_get_present(env, term, "nprobe")) {
      p->set_nprobe(static_cast<int>(term_to_int(env, t)));
    }
    params = p;
  } else if (type_str == "flat") {
    params = std::make_shared<zvec::FlatQueryParams>();
  } else if (type_str == "diskann") {
    auto p = std::make_shared<zvec::DiskAnnQueryParams>();
    if (auto t = map_get_present(env, term, "list_size")) {
      p->set_list_size(static_cast<int>(term_to_int(env, t)));
    }
    params = p;
  } else if (type_str == "fts") {
    auto p = std::make_shared<zvec::FtsQueryParams>();
    if (auto t = map_get_present(env, term, "default_operator")) {
      auto op = enif_is_atom(env, t) ? term_to_atom_string(env, t)
                                     : term_to_string(env, t);
      p->set_default_operator(op);
    }
    params = p;
  } else {
    throw std::invalid_argument("unknown query_params type: " + type_str);
  }

  // Knobs shared by all query param types (QueryParams base class)
  if (auto t = map_get_present(env, term, "radius")) {
    params->set_radius(static_cast<float>(term_to_double(env, t)));
  }
  if (auto t = map_get_present(env, term, "is_linear")) {
    params->set_is_linear(term_to_bool(env, t));
  }
  if (auto t = map_get_present(env, term, "is_using_refiner")) {
    params->set_is_using_refiner(term_to_bool(env, t));
  }
  if (auto t = map_get_present(env, term, "scale_factor")) {
    params->set_scale_factor(static_cast<float>(term_to_double(env, t)));
  }

  return params;
}

// Decode the target of a query: field name plus either a dense vector
// (:query_vector) or a full-text clause (:fts), and optional :query_params.
static zvec::QueryTarget decode_query_target(ErlNifEnv *env,
                                             ERL_NIF_TERM term) {
  zvec::QueryTarget target;

  auto fn_term = map_get(env, term, "field_name");
  if (!fn_term) throw std::invalid_argument("query missing :field_name");
  target.field_name_ = term_to_string(env, fn_term);

  if (auto fts_term = map_get_present(env, term, "fts")) {
    zvec::FtsClause clause;
    if (auto t = map_get_present(env, fts_term, "query_string")) {
      clause.query_string_ = term_to_string(env, t);
    }
    if (auto t = map_get_present(env, fts_term, "match_string")) {
      clause.match_string_ = term_to_string(env, t);
    }
    target.clause_ = std::move(clause);
  } else if (auto qv_term = map_get_present(env, term, "query_vector")) {
    ErlNifBinary bin;
    if (!enif_inspect_binary(env, qv_term, &bin)) {
      throw std::invalid_argument("query_vector must be binary");
    }
    target.set_vector(
        std::string(reinterpret_cast<const char *>(bin.data), bin.size));
  } else {
    throw std::invalid_argument("query needs :query_vector or :fts");
  }

  if (auto t = map_get_present(env, term, "query_params")) {
    target.query_params_ = decode_query_params(env, t);
  }

  return target;
}

static zvec::SearchQuery decode_search_query(ErlNifEnv *env,
                                             ERL_NIF_TERM term) {
  zvec::SearchQuery q;
  q.target_ = decode_query_target(env, term);
  q.topk_ = decode_int_opt(env, term, "topk", 10);
  if (auto t = map_get_present(env, term, "filter")) {
    q.filter_ = term_to_string(env, t);
  }
  if (auto t = map_get_present(env, term, "include_vector")) {
    q.include_vector_ = term_to_bool(env, t);
  }
  q.output_fields_ = decode_output_fields(env, term);
  return q;
}

static zvec::reranker::RerankParams decode_rerank(ErlNifEnv *env,
                                                  ERL_NIF_TERM term) {
  auto type_term = map_get(env, term, "type");
  if (!type_term) throw std::invalid_argument("rerank missing :type");
  auto type_str = term_to_atom_string(env, type_term);

  if (type_str == "rrf") {
    zvec::reranker::RrfParams rrf;
    rrf.rank_constant = decode_int_opt(env, term, "rank_constant", 60);
    return rrf;
  }
  if (type_str == "weighted") {
    auto w_term = map_get_present(env, term, "weights");
    if (!w_term) throw std::invalid_argument("weighted rerank needs :weights");
    if (!enif_is_list(env, w_term)) {
      throw std::invalid_argument("weighted rerank :weights must be a list");
    }
    zvec::reranker::WeightedParams weighted;
    ERL_NIF_TERM head, tail = w_term;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
      weighted.weights.push_back(term_to_double(env, head));
    }
    return weighted;
  }
  throw std::invalid_argument("unknown rerank type: " + type_str);
}

// Decode %{queries: [sub_query, ...], topk:, filter:, rerank: %{...}, ...}.
// Each sub-query contributes its target and uses its :topk as the number of
// candidates fed into the reranker.
static zvec::MultiQuery decode_multi_query(ErlNifEnv *env, ERL_NIF_TERM term) {
  zvec::MultiQuery mq;

  auto queries_term = map_get(env, term, "queries");
  if (!queries_term) throw std::invalid_argument("multi query missing :queries");
  if (!enif_is_list(env, queries_term)) {
    throw std::invalid_argument("multi query :queries must be a list");
  }
  if (enif_is_empty_list(env, queries_term)) {
    throw std::invalid_argument("multi query :queries must not be empty");
  }
  ERL_NIF_TERM head, tail = queries_term;
  while (enif_get_list_cell(env, tail, &head, &tail)) {
    zvec::SubQuery sub;
    sub.target_ = decode_query_target(env, head);
    sub.num_candidates_ = decode_int_opt(env, head, "topk", 10);
    mq.queries.push_back(std::move(sub));
  }

  mq.topk = decode_int_opt(env, term, "topk", 10);
  if (auto t = map_get_present(env, term, "filter")) {
    mq.filter = term_to_string(env, t);
  }
  if (auto t = map_get_present(env, term, "include_vector")) {
    mq.include_vector = term_to_bool(env, t);
  }
  mq.output_fields = decode_output_fields(env, term);
  if (auto t = map_get_present(env, term, "rerank")) {
    mq.rerank = decode_rerank(env, t);
  }
  return mq;
}

// ---------------------------------------------------------------------------
// Helpers: Encode zvec types -> Elixir terms
// ---------------------------------------------------------------------------

static ERL_NIF_TERM make_binary(ErlNifEnv *env, const void *data,
                                size_t size) {
  ERL_NIF_TERM term;
  auto *p = enif_make_new_binary(env, size, &term);
  if (p && size > 0) std::memcpy(p, data, size);
  return term;
}

template <typename T>
static ERL_NIF_TERM vector_to_binary(ErlNifEnv *env, const std::vector<T> &v) {
  return make_binary(env, v.data(), v.size() * sizeof(T));
}

// Encode a Doc to an Elixir map
static ERL_NIF_TERM encode_doc(ErlNifEnv *env, const zvec::Doc &doc) {
  auto field_names = doc.field_names();
  std::vector<ERL_NIF_TERM> keys, values;
  keys.reserve(field_names.size());
  values.reserve(field_names.size());

  for (const auto &name : field_names) {
    keys.push_back(make_binary(env, name.data(), name.size()));

    ERL_NIF_TERM val = enif_make_atom(env, "nil");

    if (doc.is_null(name)) {
      // nil
    } else if (auto r = doc.get<std::string>(name); r.has_value()) {
      val = make_binary(env, r->data(), r->size());
    } else if (auto r = doc.get<int64_t>(name); r.has_value()) {
      val = enif_make_int64(env, r.value());
    } else if (auto r = doc.get<int32_t>(name); r.has_value()) {
      val = enif_make_int(env, r.value());
    } else if (auto r = doc.get<uint64_t>(name); r.has_value()) {
      val = enif_make_uint64(env, r.value());
    } else if (auto r = doc.get<uint32_t>(name); r.has_value()) {
      val = enif_make_uint(env, r.value());
    } else if (auto r = doc.get<float>(name); r.has_value()) {
      val = enif_make_double(env, static_cast<double>(r.value()));
    } else if (auto r = doc.get<double>(name); r.has_value()) {
      val = enif_make_double(env, r.value());
    } else if (auto r = doc.get<bool>(name); r.has_value()) {
      val = enif_make_atom(env, r.value() ? "true" : "false");
    } else if (auto r = doc.get<std::vector<float>>(name); r.has_value()) {
      val = vector_to_binary(env, r.value());
    } else if (auto r = doc.get<std::vector<double>>(name); r.has_value()) {
      val = vector_to_binary(env, r.value());
    } else if (auto r = doc.get<std::vector<zvec::float16_t>>(name);
               r.has_value()) {
      val = vector_to_binary(env, r.value());
    } else if (auto r = doc.get<std::vector<int8_t>>(name); r.has_value()) {
      val = vector_to_binary(env, r.value());
    } else if (auto r = doc.get<std::vector<int16_t>>(name); r.has_value()) {
      val = vector_to_binary(env, r.value());
    }

    values.push_back(val);
  }

  ERL_NIF_TERM fields_map;
  enif_make_map_from_arrays(env, keys.data(), values.data(), keys.size(),
                            &fields_map);

  ERL_NIF_TERM doc_keys[] = {
      enif_make_atom(env, "pk"),
      enif_make_atom(env, "score"),
      enif_make_atom(env, "fields"),
  };

  const auto &pk = doc.pk();
  ERL_NIF_TERM doc_vals[] = {
      make_binary(env, pk.data(), pk.size()),
      enif_make_double(env, static_cast<double>(doc.score())),
      fields_map,
  };

  ERL_NIF_TERM result;
  enif_make_map_from_arrays(env, doc_keys, doc_vals, 3, &result);
  return result;
}

static ERL_NIF_TERM encode_doc_ptr_list(ErlNifEnv *env,
                                        const zvec::DocPtrList &docs) {
  std::vector<ERL_NIF_TERM> terms;
  terms.reserve(docs.size());
  for (const auto &doc_ptr : docs) {
    terms.push_back(encode_doc(env, *doc_ptr));
  }
  return enif_make_list_from_array(env, terms.data(), terms.size());
}

// Encode WriteResults (vector<Status>)
static ERL_NIF_TERM encode_write_results(ErlNifEnv *env,
                                         const zvec::WriteResults &results) {
  std::vector<ERL_NIF_TERM> terms;
  terms.reserve(results.size());
  for (const auto &s : results) {
    if (s.ok()) {
      terms.push_back(enif_make_atom(env, "ok"));
    } else {
      terms.push_back(enif_make_tuple2(
          env, enif_make_atom(env, "error"),
          enif_make_tuple2(env, fine::encode(env, status_code_to_atom(s.code())),
                           fine::encode(env, std::string(s.message())))));
    }
  }
  return enif_make_list_from_array(env, terms.data(), terms.size());
}

// Encode schema to Elixir map
static ERL_NIF_TERM encode_schema(ErlNifEnv *env,
                                  const zvec::CollectionSchema &schema) {
  auto fields = schema.fields();
  std::vector<ERL_NIF_TERM> field_terms;
  field_terms.reserve(fields.size());

  for (const auto &f : fields) {
    ERL_NIF_TERM fkeys[] = {
        enif_make_atom(env, "name"),
        enif_make_atom(env, "type"),
        enif_make_atom(env, "nullable"),
        enif_make_atom(env, "dimension"),
    };
    ERL_NIF_TERM fvals[] = {
        fine::encode(env, f->name()),
        encode_data_type(env, f->data_type()),
        enif_make_atom(env, f->nullable() ? "true" : "false"),
        enif_make_uint(env, f->dimension()),
    };
    ERL_NIF_TERM field_map;
    enif_make_map_from_arrays(env, fkeys, fvals, 4, &field_map);
    field_terms.push_back(field_map);
  }

  ERL_NIF_TERM skeys[] = {
      enif_make_atom(env, "name"),
      enif_make_atom(env, "fields"),
  };
  ERL_NIF_TERM svals[] = {
      fine::encode(env, schema.name()),
      enif_make_list_from_array(env, field_terms.data(), field_terms.size()),
  };
  ERL_NIF_TERM result;
  enif_make_map_from_arrays(env, skeys, svals, 2, &result);
  return result;
}

// ---------------------------------------------------------------------------
// NIF Functions: collection lifecycle
// ---------------------------------------------------------------------------

ResultRef collection_create_and_open(ErlNifEnv *env, std::string path,
                                     fine::Term schema_term,
                                     fine::Term options_term) {
  auto schema = decode_schema(env, schema_term);
  auto options = decode_options(env, options_term);

  auto result = zvec::Collection::CreateAndOpen(path, schema, options);
  if (!result.has_value()) return status_to_error(result.error());

  return OkRef(
      fine::make_resource<CollectionResource>(std::move(result).value()));
}
FINE_NIF(collection_create_and_open, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultRef collection_open(ErlNifEnv *env, std::string path,
                          fine::Term options_term) {
  auto options = decode_options(env, options_term);

  auto result = zvec::Collection::Open(path, options);
  if (!result.has_value()) return status_to_error(result.error());

  return OkRef(
      fine::make_resource<CollectionResource>(std::move(result).value()));
}
FINE_NIF(collection_open, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultOk collection_close(ErlNifEnv *env,
                          fine::ResourcePtr<CollectionResource> ref) {
  auto status = ref->ptr->close();
  if (!status.ok()) return status_to_error(status);
  return OkNothing();
}
FINE_NIF(collection_close, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultOk collection_destroy(ErlNifEnv *env,
                            fine::ResourcePtr<CollectionResource> ref) {
  auto status = ref->ptr->destroy();
  if (!status.ok()) return status_to_error(status);
  return OkNothing();
}
FINE_NIF(collection_destroy, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultOk collection_flush(ErlNifEnv *env,
                          fine::ResourcePtr<CollectionResource> ref) {
  auto status = ref->ptr->flush();
  if (!status.ok()) return status_to_error(status);
  return OkNothing();
}
FINE_NIF(collection_flush, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultTerm collection_stats(ErlNifEnv *env,
                            fine::ResourcePtr<CollectionResource> ref) {
  auto result = ref->ptr->stats();
  if (!result.has_value()) return status_to_error(result.error());

  auto &stats = result.value();
  // Encode as %{doc_count: N, index_completeness: %{col => float}}
  std::vector<ERL_NIF_TERM> ic_keys, ic_vals;
  for (auto &[col, completeness] : stats.index_completeness) {
    ic_keys.push_back(make_binary(env, col.data(), col.size()));
    ic_vals.push_back(enif_make_double(env, static_cast<double>(completeness)));
  }

  ERL_NIF_TERM ic_map;
  enif_make_map_from_arrays(env, ic_keys.data(), ic_vals.data(),
                            ic_keys.size(), &ic_map);

  ERL_NIF_TERM skeys[] = {
      enif_make_atom(env, "doc_count"),
      enif_make_atom(env, "index_completeness"),
  };
  ERL_NIF_TERM svals[] = {
      enif_make_uint64(env, stats.doc_count),
      ic_map,
  };
  ERL_NIF_TERM result_map;
  enif_make_map_from_arrays(env, skeys, svals, 2, &result_map);
  return OkTerm(fine::Term(result_map));
}
FINE_NIF(collection_stats, ERL_NIF_DIRTY_JOB_CPU_BOUND);

ResultTerm collection_schema(ErlNifEnv *env,
                             fine::ResourcePtr<CollectionResource> ref) {
  auto result = ref->ptr->schema();
  if (!result.has_value()) return status_to_error(result.error());
  return OkTerm(fine::Term(encode_schema(env, result.value())));
}
FINE_NIF(collection_schema, 0);

// ---------------------------------------------------------------------------
// NIF Functions: writes
// ---------------------------------------------------------------------------

ResultTerm collection_insert(ErlNifEnv *env,
                             fine::ResourcePtr<CollectionResource> ref,
                             fine::Term docs_term) {
  auto schema_result = ref->ptr->schema();
  if (!schema_result.has_value())
    return status_to_error(schema_result.error());

  auto docs = decode_docs(env, docs_term, schema_result.value());
  auto result = ref->ptr->insert(docs);
  if (!result.has_value()) return status_to_error(result.error());

  return OkTerm(fine::Term(encode_write_results(env, result.value())));
}
FINE_NIF(collection_insert, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultTerm collection_upsert(ErlNifEnv *env,
                             fine::ResourcePtr<CollectionResource> ref,
                             fine::Term docs_term) {
  auto schema_result = ref->ptr->schema();
  if (!schema_result.has_value())
    return status_to_error(schema_result.error());

  auto docs = decode_docs(env, docs_term, schema_result.value());
  auto result = ref->ptr->upsert(docs);
  if (!result.has_value()) return status_to_error(result.error());

  return OkTerm(fine::Term(encode_write_results(env, result.value())));
}
FINE_NIF(collection_upsert, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultOk collection_delete(ErlNifEnv *env,
                           fine::ResourcePtr<CollectionResource> ref,
                           std::vector<std::string> pks) {
  auto result = ref->ptr->delete_(pks);
  if (!result.has_value()) return status_to_error(result.error());
  for (const auto &s : result.value()) {
    if (!s.ok()) return status_to_error(s);
  }
  return OkNothing();
}
FINE_NIF(collection_delete, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultOk collection_delete_by_filter(ErlNifEnv *env,
                                     fine::ResourcePtr<CollectionResource> ref,
                                     std::string filter) {
  auto status = ref->ptr->delete_by_filter(filter);
  if (!status.ok()) return status_to_error(status);
  return OkNothing();
}
FINE_NIF(collection_delete_by_filter, ERL_NIF_DIRTY_JOB_IO_BOUND);

// ---------------------------------------------------------------------------
// NIF Functions: reads
// ---------------------------------------------------------------------------

ResultTerm collection_query(ErlNifEnv *env,
                            fine::ResourcePtr<CollectionResource> ref,
                            fine::Term query_term) {
  auto query = decode_search_query(env, query_term);
  auto result = ref->ptr->query(query);
  if (!result.has_value()) return status_to_error(result.error());

  return OkTerm(fine::Term(encode_doc_ptr_list(env, result.value())));
}
FINE_NIF(collection_query, ERL_NIF_DIRTY_JOB_CPU_BOUND);

ResultTerm collection_multi_query(ErlNifEnv *env,
                                  fine::ResourcePtr<CollectionResource> ref,
                                  fine::Term query_term) {
  auto query = decode_multi_query(env, query_term);
  auto result = ref->ptr->query(query);
  if (!result.has_value()) return status_to_error(result.error());

  return OkTerm(fine::Term(encode_doc_ptr_list(env, result.value())));
}
FINE_NIF(collection_multi_query, ERL_NIF_DIRTY_JOB_CPU_BOUND);

ResultTerm collection_fetch(ErlNifEnv *env,
                            fine::ResourcePtr<CollectionResource> ref,
                            std::vector<std::string> pks,
                            fine::Term options_term) {
  auto output_fields = decode_output_fields(env, options_term);
  bool include_vector = true;
  if (auto t = map_get_present(env, options_term, "include_vector")) {
    include_vector = term_to_bool(env, t);
  }

  auto result = ref->ptr->fetch(pks, output_fields, include_vector);
  if (!result.has_value()) return status_to_error(result.error());

  // DocPtrMap -> list of docs, in request order
  auto &doc_map = result.value();
  std::vector<ERL_NIF_TERM> terms;
  terms.reserve(doc_map.size());
  for (const auto &pk : pks) {
    auto it = doc_map.find(pk);
    if (it != doc_map.end() && it->second) {
      terms.push_back(encode_doc(env, *it->second));
    }
  }
  return OkTerm(
      fine::Term(enif_make_list_from_array(env, terms.data(), terms.size())));
}
FINE_NIF(collection_fetch, ERL_NIF_DIRTY_JOB_IO_BOUND);

// ---------------------------------------------------------------------------
// NIF Functions: iteration
// ---------------------------------------------------------------------------

ResultIter iterator_create(ErlNifEnv *env,
                           fine::ResourcePtr<CollectionResource> ref,
                           fine::Term options_term) {
  zvec::IteratorOptions options;
  options.output_fields_ = decode_output_fields(env, options_term);
  if (auto t = map_get_present(env, options_term, "include_vector")) {
    options.include_vector_ = term_to_bool(env, t);
  }

  auto result = ref->ptr->create_iterator(options);
  if (!result.has_value()) return status_to_error(result.error());

  return OkIter(fine::make_resource<IteratorResource>(
      ref->ptr, std::move(result).value()));
}
FINE_NIF(iterator_create, ERL_NIF_DIRTY_JOB_IO_BOUND);

// Returns up to `max` docs; an empty list means the iterator is exhausted.
ResultTerm iterator_next(ErlNifEnv *env,
                         fine::ResourcePtr<IteratorResource> it,
                         uint64_t max) {
  std::lock_guard<std::mutex> lock(it->mutex);
  if (it->closed) {
    return Err(atoms::failed_precondition, "iterator is closed");
  }

  std::vector<ERL_NIF_TERM> terms;
  terms.reserve(std::min<uint64_t>(max, 1024));
  while (terms.size() < max) {
    auto result = it->iter->next();
    if (!result.has_value()) return status_to_error(result.error());
    if (!result.value()) break;  // EOF
    terms.push_back(encode_doc(env, *result.value()));
  }
  return OkTerm(
      fine::Term(enif_make_list_from_array(env, terms.data(), terms.size())));
}
FINE_NIF(iterator_next, ERL_NIF_DIRTY_JOB_IO_BOUND);

fine::Atom iterator_close(ErlNifEnv *env,
                          fine::ResourcePtr<IteratorResource> it) {
  std::lock_guard<std::mutex> lock(it->mutex);
  if (!it->closed) {
    it->iter->close();
    it->closed = true;
  }
  return atoms::ok;
}
FINE_NIF(iterator_close, ERL_NIF_DIRTY_JOB_IO_BOUND);

// ---------------------------------------------------------------------------
// NIF Functions: DDL
// ---------------------------------------------------------------------------

ResultOk collection_create_index(ErlNifEnv *env,
                                 fine::ResourcePtr<CollectionResource> ref,
                                 std::string column,
                                 fine::Term params_term) {
  auto index_params = decode_index_params(env, params_term);
  if (!index_params) {
    return Err(atoms::invalid_argument, "index_params required");
  }
  auto status = ref->ptr->create_index(column, index_params);
  if (!status.ok()) return status_to_error(status);
  return OkNothing();
}
FINE_NIF(collection_create_index, ERL_NIF_DIRTY_JOB_CPU_BOUND);

ResultOk collection_drop_index(ErlNifEnv *env,
                               fine::ResourcePtr<CollectionResource> ref,
                               std::string column) {
  auto status = ref->ptr->drop_index(column);
  if (!status.ok()) return status_to_error(status);
  return OkNothing();
}
FINE_NIF(collection_drop_index, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultOk collection_optimize(ErlNifEnv *env,
                             fine::ResourcePtr<CollectionResource> ref) {
  auto status = ref->ptr->optimize();
  if (!status.ok()) return status_to_error(status);
  return OkNothing();
}
FINE_NIF(collection_optimize, ERL_NIF_DIRTY_JOB_CPU_BOUND);

// ---------------------------------------------------------------------------
// Module init
// ---------------------------------------------------------------------------
FINE_INIT("Elixir.Zvec.Native");
