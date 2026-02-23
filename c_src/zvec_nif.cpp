#include <fine.hpp>

#include <zvec/db/collection.h>
#include <zvec/db/doc.h>
#include <zvec/db/index_params.h>
#include <zvec/db/options.h>
#include <zvec/db/query_params.h>
#include <zvec/db/schema.h>
#include <zvec/db/stats.h>
#include <zvec/db/status.h>
#include <zvec/db/type.h>

#include <cstring>
#include <map>
#include <string>
#include <variant>
#include <vector>

// ---------------------------------------------------------------------------
// Atoms
// ---------------------------------------------------------------------------
namespace atoms {
// Status codes
static auto ok = fine::Atom("ok");
static auto error = fine::Atom("error");
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

// Data types
static auto dt_binary = fine::Atom("binary");
static auto dt_string = fine::Atom("string");
static auto dt_bool = fine::Atom("bool");
static auto dt_int32 = fine::Atom("int32");
static auto dt_int64 = fine::Atom("int64");
static auto dt_uint32 = fine::Atom("uint32");
static auto dt_uint64 = fine::Atom("uint64");
static auto dt_float = fine::Atom("float");
static auto dt_double = fine::Atom("double");
static auto dt_vector_fp32 = fine::Atom("vector_fp32");
static auto dt_vector_fp64 = fine::Atom("vector_fp64");
static auto dt_vector_int8 = fine::Atom("vector_int8");

// Index types
static auto idx_hnsw = fine::Atom("hnsw");
static auto idx_flat = fine::Atom("flat");
static auto idx_ivf = fine::Atom("ivf");
static auto idx_invert = fine::Atom("invert");

// Metric types
static auto mt_l2 = fine::Atom("l2");
static auto mt_ip = fine::Atom("ip");
static auto mt_cosine = fine::Atom("cosine");

// Field keys
static auto nil = fine::Atom("nil");
static auto true_atom = fine::Atom("true");
static auto false_atom = fine::Atom("false");
}  // namespace atoms

// ---------------------------------------------------------------------------
// Resource: wraps zvec::Collection::Ptr
// ---------------------------------------------------------------------------
struct CollectionResource {
  zvec::Collection::Ptr ptr;
  CollectionResource(zvec::Collection::Ptr p) : ptr(std::move(p)) {}
};
FINE_RESOURCE(CollectionResource);

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------
using OkRef = fine::Ok<fine::ResourcePtr<CollectionResource>>;
using OkTerm = fine::Ok<fine::Term>;
using OkNothing = fine::Ok<>;
using Err = fine::Error<fine::Atom, std::string>;
using ResultRef = std::variant<OkRef, Err>;
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

// Get a map value by string key, returns 0/NULL term on missing
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
  long val;
  if (enif_get_int64(env, term, &val)) return static_cast<int64_t>(val);
  int ival;
  if (enif_get_int(env, term, &ival)) return static_cast<int64_t>(ival);
  throw std::invalid_argument("expected integer");
}

static double term_to_double(ErlNifEnv *env, ERL_NIF_TERM term) {
  double val;
  if (enif_get_double(env, term, &val)) return val;
  // Accept integer as double
  long ival;
  if (enif_get_int64(env, term, &ival)) return static_cast<double>(ival);
  int i;
  if (enif_get_int(env, term, &i)) return static_cast<double>(i);
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
  if (s == "undefined" || s == "nil") return zvec::QuantizeType::UNDEFINED;
  throw std::invalid_argument("unknown quantize_type: " + s);
}

static zvec::DataType decode_data_type(ErlNifEnv *env, ERL_NIF_TERM term) {
  auto s = term_to_atom_string(env, term);
  if (s == "binary") return zvec::DataType::BINARY;
  if (s == "string") return zvec::DataType::STRING;
  if (s == "bool") return zvec::DataType::BOOL;
  if (s == "int32") return zvec::DataType::INT32;
  if (s == "int64") return zvec::DataType::INT64;
  if (s == "uint32") return zvec::DataType::UINT32;
  if (s == "uint64") return zvec::DataType::UINT64;
  if (s == "float") return zvec::DataType::FLOAT;
  if (s == "double") return zvec::DataType::DOUBLE;
  if (s == "vector_fp32") return zvec::DataType::VECTOR_FP32;
  if (s == "vector_fp64") return zvec::DataType::VECTOR_FP64;
  if (s == "vector_fp16") return zvec::DataType::VECTOR_FP16;
  if (s == "vector_int8") return zvec::DataType::VECTOR_INT8;
  if (s == "vector_int16") return zvec::DataType::VECTOR_INT16;
  if (s == "vector_binary32") return zvec::DataType::VECTOR_BINARY32;
  if (s == "vector_binary64") return zvec::DataType::VECTOR_BINARY64;
  if (s == "sparse_vector_fp32") return zvec::DataType::SPARSE_VECTOR_FP32;
  if (s == "sparse_vector_fp16") return zvec::DataType::SPARSE_VECTOR_FP16;
  throw std::invalid_argument("unknown data_type: " + s);
}

// Decode index params from an Elixir map: %{type: :hnsw, metric_type: :cosine, ...}
static zvec::IndexParams::Ptr decode_index_params(ErlNifEnv *env,
                                                  ERL_NIF_TERM term) {
  if (term_is_nil(env, term)) return nullptr;

  auto type_term = map_get(env, term, "type");
  if (!type_term) throw std::invalid_argument("index_params missing :type");

  auto type_str = term_to_atom_string(env, type_term);

  if (type_str == "invert") {
    bool range_opt = true;
    bool ext_wildcard = false;
    auto ro_term = map_get(env, term, "enable_range_optimization");
    if (ro_term) range_opt = term_to_bool(env, ro_term);
    auto ew_term = map_get(env, term, "enable_extended_wildcard");
    if (ew_term) ext_wildcard = term_to_bool(env, ew_term);
    return std::make_shared<zvec::InvertIndexParams>(range_opt, ext_wildcard);
  }

  // Vector index types need metric_type
  auto mt_term = map_get(env, term, "metric_type");
  if (!mt_term) throw std::invalid_argument("vector index_params missing :metric_type");
  auto metric = decode_metric_type(env, mt_term);

  zvec::QuantizeType quantize = zvec::QuantizeType::UNDEFINED;
  auto qt_term = map_get(env, term, "quantize_type");
  if (qt_term && !term_is_nil(env, qt_term)) {
    quantize = decode_quantize_type(env, qt_term);
  }

  if (type_str == "hnsw") {
    int m = 16;
    int ef = 200;
    auto m_term = map_get(env, term, "m");
    if (m_term) m = static_cast<int>(term_to_int(env, m_term));
    auto ef_term = map_get(env, term, "ef_construction");
    if (ef_term) ef = static_cast<int>(term_to_int(env, ef_term));
    return std::make_shared<zvec::HnswIndexParams>(metric, m, ef, quantize);
  }

  if (type_str == "flat") {
    return std::make_shared<zvec::FlatIndexParams>(metric, quantize);
  }

  if (type_str == "ivf") {
    int n_list = 1024, n_iters = 10;
    bool use_soar = false;
    auto nl_term = map_get(env, term, "n_list");
    if (nl_term) n_list = static_cast<int>(term_to_int(env, nl_term));
    auto ni_term = map_get(env, term, "n_iters");
    if (ni_term) n_iters = static_cast<int>(term_to_int(env, ni_term));
    auto us_term = map_get(env, term, "use_soar");
    if (us_term) use_soar = term_to_bool(env, us_term);
    return std::make_shared<zvec::IVFIndexParams>(metric, n_list, n_iters,
                                                  use_soar, quantize);
  }

  throw std::invalid_argument("unknown index type: " + type_str);
}

// Decode a single field schema from Elixir map
static zvec::FieldSchema::Ptr decode_field_schema(ErlNifEnv *env,
                                                  ERL_NIF_TERM term) {
  auto name = term_to_string(env, map_get(env, term, "name"));
  auto data_type = decode_data_type(env, map_get(env, term, "type"));

  bool nullable = false;
  auto nullable_term = map_get(env, term, "nullable");
  if (nullable_term && !term_is_nil(env, nullable_term)) {
    nullable = term_to_bool(env, nullable_term);
  }

  zvec::IndexParams::Ptr index_params = nullptr;
  auto ip_term = map_get(env, term, "index_params");
  if (ip_term && !term_is_nil(env, ip_term)) {
    index_params = decode_index_params(env, ip_term);
  }

  uint32_t dimension = 0;
  auto dim_term = map_get(env, term, "dimension");
  if (dim_term && !term_is_nil(env, dim_term)) {
    dimension = static_cast<uint32_t>(term_to_int(env, dim_term));
  }

  if (dimension > 0) {
    return std::make_shared<zvec::FieldSchema>(name, data_type, dimension,
                                               nullable, index_params);
  } else {
    return std::make_shared<zvec::FieldSchema>(name, data_type, nullable,
                                               index_params);
  }
}

// Decode collection schema from Elixir map:
// %{name: "...", fields: [%{name:, type:, ...}]}
static zvec::CollectionSchema decode_schema(ErlNifEnv *env,
                                            ERL_NIF_TERM term) {
  auto name = term_to_string(env, map_get(env, term, "name"));
  zvec::CollectionSchema schema(name);

  auto max_docs_term = map_get(env, term, "max_doc_count_per_segment");
  if (max_docs_term && !term_is_nil(env, max_docs_term)) {
    schema.set_max_doc_count_per_segment(
        static_cast<uint64_t>(term_to_int(env, max_docs_term)));
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

  auto ro = map_get(env, term, "read_only");
  if (ro) opts.read_only_ = term_to_bool(env, ro);
  auto mm = map_get(env, term, "enable_mmap");
  if (mm) opts.enable_mmap_ = term_to_bool(env, mm);
  auto bs = map_get(env, term, "max_buffer_size");
  if (bs) opts.max_buffer_size_ = static_cast<uint32_t>(term_to_int(env, bs));

  return opts;
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

  // Iterate the fields map
  ErlNifMapIterator iter;
  if (!enif_map_iterator_create(env, fields_term, &iter,
                                ERL_NIF_MAP_ITERATOR_FIRST)) {
    throw std::invalid_argument("doc :fields must be a map");
  }

  ERL_NIF_TERM key, value;
  while (enif_map_iterator_get_pair(env, &iter, &key, &value)) {
    auto field_name = term_to_string(env, key);
    auto *field_schema = schema.get_field(field_name);

    if (!field_schema) {
      enif_map_iterator_next(env, &iter);
      continue;  // Skip unknown fields
    }

    if (term_is_nil(env, value)) {
      doc.set_null(field_name);
      enif_map_iterator_next(env, &iter);
      continue;
    }

    auto dt = field_schema->data_type();
    switch (dt) {
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
      case zvec::DataType::VECTOR_FP32: {
        // Expect raw binary of float32s
        ErlNifBinary bin;
        if (enif_inspect_binary(env, value, &bin)) {
          std::vector<float> vec(bin.size / sizeof(float));
          std::memcpy(vec.data(), bin.data, bin.size);
          doc.set<std::vector<float>>(field_name, std::move(vec));
        } else {
          throw std::invalid_argument("vector_fp32 field expects binary");
        }
        break;
      }
      case zvec::DataType::VECTOR_FP64: {
        ErlNifBinary bin;
        if (enif_inspect_binary(env, value, &bin)) {
          std::vector<double> vec(bin.size / sizeof(double));
          std::memcpy(vec.data(), bin.data, bin.size);
          doc.set<std::vector<double>>(field_name, std::move(vec));
        } else {
          throw std::invalid_argument("vector_fp64 field expects binary");
        }
        break;
      }
      case zvec::DataType::VECTOR_INT8: {
        ErlNifBinary bin;
        if (enif_inspect_binary(env, value, &bin)) {
          std::vector<int8_t> vec(bin.size);
          std::memcpy(vec.data(), bin.data, bin.size);
          doc.set<std::vector<int8_t>>(field_name, std::move(vec));
        } else {
          throw std::invalid_argument("vector_int8 field expects binary");
        }
        break;
      }
      default:
        // For unsupported types, try string
        doc.set<std::string>(field_name, term_to_string(env, value));
        break;
    }

    enif_map_iterator_next(env, &iter);
  }

  enif_map_iterator_destroy(env, &iter);
  return doc;
}

static std::vector<zvec::Doc> decode_docs(ErlNifEnv *env, ERL_NIF_TERM term,
                                          const zvec::CollectionSchema &schema) {
  std::vector<zvec::Doc> docs;

  unsigned len;
  if (!enif_get_list_length(env, term, &len)) {
    throw std::invalid_argument("expected list of docs");
  }

  docs.reserve(len);
  ERL_NIF_TERM head, tail = term;
  while (enif_get_list_cell(env, tail, &head, &tail)) {
    docs.push_back(decode_doc(env, head, schema));
  }
  return docs;
}

// Decode VectorQuery from Elixir map
static zvec::VectorQuery decode_vector_query(ErlNifEnv *env,
                                             ERL_NIF_TERM term) {
  zvec::VectorQuery q;

  auto topk_term = map_get(env, term, "topk");
  if (topk_term) q.topk_ = static_cast<int>(term_to_int(env, topk_term));

  auto fn_term = map_get(env, term, "field_name");
  if (fn_term) q.field_name_ = term_to_string(env, fn_term);

  auto qv_term = map_get(env, term, "query_vector");
  if (qv_term) {
    ErlNifBinary bin;
    if (enif_inspect_binary(env, qv_term, &bin)) {
      q.query_vector_.assign(reinterpret_cast<const char *>(bin.data),
                             bin.size);
    } else {
      throw std::invalid_argument("query_vector must be binary");
    }
  }

  auto filter_term = map_get(env, term, "filter");
  if (filter_term && !term_is_nil(env, filter_term)) {
    q.filter_ = term_to_string(env, filter_term);
  }

  auto iv_term = map_get(env, term, "include_vector");
  if (iv_term && !term_is_nil(env, iv_term)) {
    q.include_vector_ = term_to_bool(env, iv_term);
  }

  auto of_term = map_get(env, term, "output_fields");
  if (of_term && !term_is_nil(env, of_term)) {
    std::vector<std::string> fields;
    ERL_NIF_TERM head, tail = of_term;
    while (enif_get_list_cell(env, tail, &head, &tail)) {
      fields.push_back(term_to_string(env, head));
    }
    q.output_fields_ = std::move(fields);
  }

  auto qp_term = map_get(env, term, "query_params");
  if (qp_term && !term_is_nil(env, qp_term)) {
    auto type_term = map_get(env, qp_term, "type");
    if (type_term) {
      auto type_str = term_to_atom_string(env, type_term);
      if (type_str == "hnsw") {
        int ef = 200;
        auto ef_term = map_get(env, qp_term, "ef");
        if (ef_term) ef = static_cast<int>(term_to_int(env, ef_term));
        q.query_params_ = std::make_shared<zvec::HnswQueryParams>(ef);
      } else if (type_str == "ivf") {
        int nprobe = 10;
        auto np_term = map_get(env, qp_term, "nprobe");
        if (np_term) nprobe = static_cast<int>(term_to_int(env, np_term));
        q.query_params_ = std::make_shared<zvec::IVFQueryParams>(nprobe);
      } else if (type_str == "flat") {
        q.query_params_ = std::make_shared<zvec::FlatQueryParams>();
      }
    }
  }

  return q;
}

// ---------------------------------------------------------------------------
// Helpers: Encode zvec types -> Elixir terms
// ---------------------------------------------------------------------------

static fine::Atom data_type_to_atom(zvec::DataType dt) {
  switch (dt) {
    case zvec::DataType::BINARY:
      return atoms::dt_binary;
    case zvec::DataType::STRING:
      return atoms::dt_string;
    case zvec::DataType::BOOL:
      return atoms::dt_bool;
    case zvec::DataType::INT32:
      return atoms::dt_int32;
    case zvec::DataType::INT64:
      return atoms::dt_int64;
    case zvec::DataType::UINT32:
      return atoms::dt_uint32;
    case zvec::DataType::UINT64:
      return atoms::dt_uint64;
    case zvec::DataType::FLOAT:
      return atoms::dt_float;
    case zvec::DataType::DOUBLE:
      return atoms::dt_double;
    case zvec::DataType::VECTOR_FP32:
      return atoms::dt_vector_fp32;
    case zvec::DataType::VECTOR_FP64:
      return atoms::dt_vector_fp64;
    case zvec::DataType::VECTOR_INT8:
      return atoms::dt_vector_int8;
    default:
      return atoms::unknown;
  }
}

// Encode a Doc to an Elixir map
static ERL_NIF_TERM encode_doc(ErlNifEnv *env, const zvec::Doc &doc) {
  // Build fields map
  auto field_names = doc.field_names();
  std::vector<ERL_NIF_TERM> keys, values;
  keys.reserve(field_names.size());
  values.reserve(field_names.size());

  for (const auto &name : field_names) {
    ERL_NIF_TERM bin_key;
    auto *ptr = enif_make_new_binary(env, name.size(), &bin_key);
    if (ptr) std::memcpy(ptr, name.data(), name.size());
    keys.push_back(bin_key);

    // Try to get the value using various types
    ERL_NIF_TERM val = enif_make_atom(env, "nil");

    if (doc.is_null(name)) {
      val = enif_make_atom(env, "nil");
    } else if (auto r = doc.get<std::string>(name); r.has_value()) {
      auto &s = r.value();
      ERL_NIF_TERM b;
      auto *p = enif_make_new_binary(env, s.size(), &b);
      if (p) std::memcpy(p, s.data(), s.size());
      val = b;
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
      auto &v = r.value();
      ERL_NIF_TERM b;
      auto *p = enif_make_new_binary(env, v.size() * sizeof(float), &b);
      if (p) std::memcpy(p, v.data(), v.size() * sizeof(float));
      val = b;
    } else if (auto r = doc.get<std::vector<double>>(name); r.has_value()) {
      auto &v = r.value();
      ERL_NIF_TERM b;
      auto *p = enif_make_new_binary(env, v.size() * sizeof(double), &b);
      if (p) std::memcpy(p, v.data(), v.size() * sizeof(double));
      val = b;
    } else if (auto r = doc.get<std::vector<int8_t>>(name); r.has_value()) {
      auto &v = r.value();
      ERL_NIF_TERM b;
      auto *p = enif_make_new_binary(env, v.size(), &b);
      if (p) std::memcpy(p, v.data(), v.size());
      val = b;
    }

    values.push_back(val);
  }

  ERL_NIF_TERM fields_map;
  enif_make_map_from_arrays(env, keys.data(), values.data(), keys.size(),
                            &fields_map);

  // Build doc map: %{pk: ..., score: ..., fields: ...}
  ERL_NIF_TERM doc_keys[] = {
      enif_make_atom(env, "pk"),
      enif_make_atom(env, "score"),
      enif_make_atom(env, "fields"),
  };

  auto pk = doc.pk();
  ERL_NIF_TERM pk_bin;
  auto *pk_ptr = enif_make_new_binary(env, pk.size(), &pk_bin);
  if (pk_ptr) std::memcpy(pk_ptr, pk.data(), pk.size());

  ERL_NIF_TERM doc_vals[] = {
      pk_bin,
      enif_make_double(env, static_cast<double>(doc.score())),
      fields_map,
  };

  ERL_NIF_TERM result;
  enif_make_map_from_arrays(env, doc_keys, doc_vals, 3, &result);
  return result;
}

// Encode DocPtrList
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
      ERL_NIF_TERM err_tuple = enif_make_tuple2(
          env, enif_make_atom(env, "error"),
          fine::encode(env, std::string(s.message())));
      terms.push_back(err_tuple);
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
        fine::encode(env, data_type_to_atom(f->data_type())),
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
// NIF Functions
// ---------------------------------------------------------------------------

ResultRef collection_create_and_open(ErlNifEnv *env, std::string path,
                                     fine::Term schema_term,
                                     fine::Term options_term) {
  auto schema = decode_schema(env, schema_term);
  auto options = decode_options(env, options_term);

  auto result = zvec::Collection::CreateAndOpen(path, schema, options);
  if (!result.has_value()) {
    return status_to_error(result.error());
  }

  auto resource = fine::make_resource<CollectionResource>(std::move(result).value());
  return OkRef(std::move(resource));
}
FINE_NIF(collection_create_and_open, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultRef collection_open(ErlNifEnv *env, std::string path,
                          fine::Term options_term) {
  auto options = decode_options(env, options_term);

  auto result = zvec::Collection::Open(path, options);
  if (!result.has_value()) {
    return status_to_error(result.error());
  }

  auto resource = fine::make_resource<CollectionResource>(std::move(result).value());
  return OkRef(std::move(resource));
}
FINE_NIF(collection_open, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultOk collection_destroy(ErlNifEnv *env,
                            fine::ResourcePtr<CollectionResource> ref) {
  auto status = ref->ptr->Destroy();
  if (!status.ok()) return status_to_error(status);
  return OkNothing();
}
FINE_NIF(collection_destroy, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultOk collection_flush(ErlNifEnv *env,
                          fine::ResourcePtr<CollectionResource> ref) {
  auto status = ref->ptr->Flush();
  if (!status.ok()) return status_to_error(status);
  return OkNothing();
}
FINE_NIF(collection_flush, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultTerm collection_stats(ErlNifEnv *env,
                            fine::ResourcePtr<CollectionResource> ref) {
  auto result = ref->ptr->Stats();
  if (!result.has_value()) return status_to_error(result.error());

  auto &stats = result.value();
  // Encode as %{doc_count: N, index_completeness: %{col => float}}
  std::vector<ERL_NIF_TERM> ic_keys, ic_vals;
  for (auto &[col, completeness] : stats.index_completeness) {
    ERL_NIF_TERM k;
    auto *p = enif_make_new_binary(env, col.size(), &k);
    if (p) std::memcpy(p, col.data(), col.size());
    ic_keys.push_back(k);
    ic_vals.push_back(enif_make_double(env, static_cast<double>(completeness)));
  }

  ERL_NIF_TERM ic_map;
  if (ic_keys.empty()) {
    ic_map = enif_make_new_map(env);
  } else {
    enif_make_map_from_arrays(env, ic_keys.data(), ic_vals.data(),
                              ic_keys.size(), &ic_map);
  }

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
  auto result = ref->ptr->Schema();
  if (!result.has_value()) return status_to_error(result.error());

  auto encoded = encode_schema(env, result.value());
  return OkTerm(fine::Term(encoded));
}
FINE_NIF(collection_schema, 0);

ResultTerm collection_insert(ErlNifEnv *env,
                             fine::ResourcePtr<CollectionResource> ref,
                             fine::Term docs_term) {
  auto schema_result = ref->ptr->Schema();
  if (!schema_result.has_value())
    return status_to_error(schema_result.error());

  auto docs = decode_docs(env, docs_term, schema_result.value());
  auto result = ref->ptr->Insert(docs);
  if (!result.has_value()) return status_to_error(result.error());

  return OkTerm(fine::Term(encode_write_results(env, result.value())));
}
FINE_NIF(collection_insert, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultTerm collection_upsert(ErlNifEnv *env,
                             fine::ResourcePtr<CollectionResource> ref,
                             fine::Term docs_term) {
  auto schema_result = ref->ptr->Schema();
  if (!schema_result.has_value())
    return status_to_error(schema_result.error());

  auto docs = decode_docs(env, docs_term, schema_result.value());
  auto result = ref->ptr->Upsert(docs);
  if (!result.has_value()) return status_to_error(result.error());

  return OkTerm(fine::Term(encode_write_results(env, result.value())));
}
FINE_NIF(collection_upsert, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultOk collection_delete(ErlNifEnv *env,
                           fine::ResourcePtr<CollectionResource> ref,
                           std::vector<std::string> pks) {
  auto result = ref->ptr->Delete(pks);
  if (!result.has_value()) return status_to_error(result.error());
  // Check individual results
  for (const auto &s : result.value()) {
    if (!s.ok()) return status_to_error(s);
  }
  return OkNothing();
}
FINE_NIF(collection_delete, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultOk collection_delete_by_filter(ErlNifEnv *env,
                                     fine::ResourcePtr<CollectionResource> ref,
                                     std::string filter) {
  auto status = ref->ptr->DeleteByFilter(filter);
  if (!status.ok()) return status_to_error(status);
  return OkNothing();
}
FINE_NIF(collection_delete_by_filter, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultTerm collection_query(ErlNifEnv *env,
                            fine::ResourcePtr<CollectionResource> ref,
                            fine::Term query_term) {
  auto query = decode_vector_query(env, query_term);
  auto result = ref->ptr->Query(query);
  if (!result.has_value()) return status_to_error(result.error());

  return OkTerm(fine::Term(encode_doc_ptr_list(env, result.value())));
}
FINE_NIF(collection_query, ERL_NIF_DIRTY_JOB_CPU_BOUND);

ResultTerm collection_fetch(ErlNifEnv *env,
                            fine::ResourcePtr<CollectionResource> ref,
                            std::vector<std::string> pks) {
  auto result = ref->ptr->Fetch(pks);
  if (!result.has_value()) return status_to_error(result.error());

  // DocPtrMap -> list of docs
  auto &doc_map = result.value();
  std::vector<ERL_NIF_TERM> terms;
  terms.reserve(doc_map.size());
  for (const auto &[pk, doc_ptr] : doc_map) {
    if (doc_ptr) {
      terms.push_back(encode_doc(env, *doc_ptr));
    }
  }
  return OkTerm(
      fine::Term(enif_make_list_from_array(env, terms.data(), terms.size())));
}
FINE_NIF(collection_fetch, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultOk collection_create_index(ErlNifEnv *env,
                                 fine::ResourcePtr<CollectionResource> ref,
                                 std::string column,
                                 fine::Term params_term) {
  auto index_params = decode_index_params(env, params_term);
  if (!index_params) {
    return Err(atoms::invalid_argument, "index_params required");
  }
  auto status = ref->ptr->CreateIndex(column, index_params);
  if (!status.ok()) return status_to_error(status);
  return OkNothing();
}
FINE_NIF(collection_create_index, ERL_NIF_DIRTY_JOB_CPU_BOUND);

ResultOk collection_drop_index(ErlNifEnv *env,
                               fine::ResourcePtr<CollectionResource> ref,
                               std::string column) {
  auto status = ref->ptr->DropIndex(column);
  if (!status.ok()) return status_to_error(status);
  return OkNothing();
}
FINE_NIF(collection_drop_index, ERL_NIF_DIRTY_JOB_IO_BOUND);

ResultOk collection_optimize(ErlNifEnv *env,
                             fine::ResourcePtr<CollectionResource> ref) {
  auto status = ref->ptr->Optimize();
  if (!status.ok()) return status_to_error(status);
  return OkNothing();
}
FINE_NIF(collection_optimize, ERL_NIF_DIRTY_JOB_CPU_BOUND);

// ---------------------------------------------------------------------------
// Module init
// ---------------------------------------------------------------------------
FINE_INIT("Elixir.Zvec.Native");
