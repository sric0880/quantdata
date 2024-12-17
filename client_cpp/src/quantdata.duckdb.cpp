#pragma warning(disable : 4996)
#include <iostream>

#include "duckdb/quantdata.h"
#include "quantdata.macros.h"
#include "datetime.h"

duckdb_connection conn_duckdb;
duckdb_database db;

void DuckDBConnect(const char *memory_limit,
                   int threads_limit,
                   bool shared_memory)
{
  duckdb_config config;
  DuckDBGuard guard(config);
  // create the configuration object
  if (duckdb_create_config(&config) == DuckDBError)
  {
    throw DuckDBException("duckdb_create_config error");
  }
  // set some configuration options
  if (threads_limit > 0)
  {
    std::string &&threads_limit_str = std::to_string(threads_limit);
    duckdb_set_config(config, "threads", threads_limit_str.c_str());
  }
  if (memory_limit)
    duckdb_set_config(config, "max_memory", memory_limit);
  const char *path;
  if (shared_memory)
    path = ":memory:lzq01";
  else
    path = ":memory:";

  // open the database using the configuration
  char *err_msg;
  if (duckdb_open_ext(path, &db, config, &err_msg) == DuckDBError)
  {
    DuckDBGuard guard(err_msg);
    throw DuckDBException(err_msg);
  }
  if (duckdb_connect(db, &conn_duckdb) == DuckDBError)
  {
    throw DuckDBException("duckdb_connect error");
  }
}

void DuckDBAttach(const char *uri)
{
  QD_ASSERT_STRING(uri);
  char buf[200]{0};
  std::string _uri(uri);
  auto sep = _uri.rfind('/');
  auto dot = _uri.rfind('.');
  auto dbname = _uri.substr(sep + 1, dot - sep - 1);
  int cx = std::snprintf(buf, 200, "ATTACH IF NOT EXISTS '%s' as %s (READ_ONLY);", uri, dbname.c_str());
  QD_ASSERT((cx >= 0 && cx < 200), "uri too long");
  duckdb_prepared_statement stmt;
  DuckDBGuard guard1(stmt);
  if (duckdb_prepare(conn_duckdb, buf, &stmt) == DuckDBError)
  {
    throw DuckDBException(duckdb_prepare_error(stmt));
  }
  duckdb_result res;
  DuckDBGuard guard2(res);
  if (duckdb_execute_prepared(stmt, &res) == DuckDBError)
  {
    throw DuckDBException(duckdb_result_error(&res));
  }
}

void DuckDBClose()
{
  duckdb_disconnect(&conn_duckdb);
  duckdb_close(&db);
}

DuckDBArrays DuckDBGetArray(const char *db_name,
                            const char *tablename,
                            const char *attrs,
                            const char *filter)
{
  QD_ASSERT_STRING(db_name);
  QD_ASSERT_STRING(tablename);
  char query[200]{};
  int cx = std::snprintf(query, 200, "select %s FROM %s.%s ", attrs, db_name, tablename);
  QD_ASSERT((cx >= 0 && cx < 200), "query statement too long");
  if (filter && filter[0])
  {
    cx += std::snprintf(query + cx, 200 - cx, "WHERE %s", filter);
    QD_ASSERT((cx >= 0 && cx < 200), "query statement too long");
  }
  duckdb_result res;
  if (duckdb_query(conn_duckdb, query, &res) == DuckDBError)
  {
    DuckDBGuard guard(res);
    throw DuckDBException(duckdb_result_error(&res));
  }
  return DuckDBArrays(res);
}

DuckDBArrays DuckDBGetArrayLastRows(const char *db_name,
                                    const char *tablename,
                                    const char *attrs,
                                    const char *filter,
                                    int N)
{
  char query[200]{};
  int cx = std::snprintf(query, 200, "SELECT * FROM (select %s FROM %s.%s ", attrs, db_name, tablename);
  QD_ASSERT((cx >= 0 && cx < 200), "query statement too long");
  if (filter && filter[0])
  {
    cx += std::snprintf(query + cx, 200 - cx, "WHERE %s", filter);
    QD_ASSERT((cx >= 0 && cx < 200), "query statement too long");
  }
  std::snprintf(query + cx, 200 - cx, "ORDER BY dt DESC LIMIT %d) ORDER BY dt ASC;", N);
  QD_ASSERT((cx >= 0 && cx < 200), "query statement too long");
  duckdb_result res;
  if (duckdb_query(conn_duckdb, query, &res) == DuckDBError)
  {
    DuckDBGuard guard(res);
    throw DuckDBException(duckdb_result_error(&res));
  }
  return DuckDBArrays(res);
}

DuckDBArrays::DuckDBArrays(duckdb_result result) : result_(std::move(result))
{
  auto _result = result_.get();
  row_count_ = duckdb_row_count(_result);
  column_count_ = duckdb_column_count(_result);
  chunk_count_ = duckdb_result_chunk_count(*_result);
  for (idx_t i = 0; i < column_count_; i++)
  {
    names.emplace_back(duckdb_column_name(_result, i));
    types.emplace_back(duckdb_column_type(_result, i));
  }
  for (idx_t i = 0; i < chunk_count_; i++)
  {
    chunks_.emplace_back(duckdb_result_get_chunk(*result_, i));
  }
};

const duckdb_data_chunk &DuckDBArrays::FetchChunk(idx_t chunk_idx) const
{
  return *chunks_[chunk_idx];
}

const idx_t DuckDBArrays::ChunkSize(idx_t chunk_idx) const
{
  auto &chunk = FetchChunk(chunk_idx);
  return duckdb_data_chunk_get_size(chunk);
}

std::string ConvertToString(duckdb_result *result, const duckdb_type type, const void *array, idx_t row);
const std::string DuckDBArrays::ToString() const
{
  auto _result = result_.get();
  std::string result;
  for (auto &name : names)
  {
    result += name + "\t";
  }
  result += "\n";
  std::vector<const void *> col_datas(column_count_);
  for (idx_t i = 0; i < chunk_count_; ++i)
  {
    for (idx_t col_idx = 0; col_idx < column_count_; col_idx++)
    {
      col_datas[col_idx] = FetchArray<void>(i, col_idx);
    }
    idx_t size = ChunkSize(i);
    for (idx_t row_idx = 0; row_idx < size; row_idx++)
    {
      for (idx_t col_idx = 0; col_idx < column_count_; col_idx++)
      {
        if (col_idx > 0)
        {
          result += "\t";
        }
        result += duckdb_value_is_null(_result, col_idx, row_idx) ? "NULL" : ConvertToString(_result, types[col_idx], col_datas[col_idx], row_idx);
      }
      result += "\n";
    }
    result += "[ Rows: " + std::to_string(row_count_) + "]\n";
    result += "\n";
  }
  return result;
}

std::string ConvertToString(duckdb_result *result, const duckdb_type type, const void *array, idx_t row)
{
  switch (type)
  {
  case DUCKDB_TYPE_INVALID:
    return "INVALID";
  // bool
  case DUCKDB_TYPE_BOOLEAN:
    return ((bool *)array)[row] ? "true" : "false";
  // int8_t
  case DUCKDB_TYPE_TINYINT:
    return std::to_string(((int8_t *)array)[row]);
    // int16_t
  case DUCKDB_TYPE_SMALLINT:
    return std::to_string(((int16_t *)array)[row]);
  // int32_t
  case DUCKDB_TYPE_INTEGER:
    return std::to_string(((int32_t *)array)[row]);
  // int64_t
  case DUCKDB_TYPE_BIGINT:
    return std::to_string(((int64_t *)array)[row]);
  // uint8_t
  case DUCKDB_TYPE_UTINYINT:
    return std::to_string(((uint8_t *)array)[row]);
  // uint16_t
  case DUCKDB_TYPE_USMALLINT:
    return std::to_string(((uint16_t *)array)[row]);
  // uint32_t
  case DUCKDB_TYPE_UINTEGER:
    return std::to_string(((uint32_t *)array)[row]);
  // uint64_t
  case DUCKDB_TYPE_UBIGINT:
    return std::to_string(((uint64_t *)array)[row]);
  // float
  case DUCKDB_TYPE_FLOAT:
    return std::to_string(((float *)array)[row]);
  // double
  case DUCKDB_TYPE_DOUBLE:
    return std::to_string(((double *)array)[row]);
  // const char*
  case DUCKDB_TYPE_VARCHAR:
  {
    return DuckDBGetString(((duckdb_string_t *)array)[row]);
  }
  // duckdb_timestamp, in microseconds
  case DUCKDB_TYPE_TIMESTAMP:
  {
    return isoformat_microsec(((int64_t *)array)[row]);
  }
  // duckdb_timestamp, in seconds
  case DUCKDB_TYPE_TIMESTAMP_S:
  {
    return isoformat(((int64_t *)array)[row]);
  }
  // duckdb_timestamp, in milliseconds
  case DUCKDB_TYPE_TIMESTAMP_MS:
  {
    return isoformat_millisec(((int64_t *)array)[row]);
  }
  // duckdb_timestamp, in nanoseconds
  case DUCKDB_TYPE_TIMESTAMP_NS:
  {
    return isoformat_nanosec(((int64_t *)array)[row]);
  }
  // duckdb_timestamp
  case DUCKDB_TYPE_TIMESTAMP_TZ:
  // duckdb_interval
  case DUCKDB_TYPE_INTERVAL:
  // duckdb_date
  case DUCKDB_TYPE_DATE:
  // duckdb_time
  case DUCKDB_TYPE_TIME:
  // duckdb_hugeint
  case DUCKDB_TYPE_HUGEINT:
  // duckdb_uhugeint
  case DUCKDB_TYPE_UHUGEINT:
  // duckdb_blob
  case DUCKDB_TYPE_BLOB:
  // decimal
  case DUCKDB_TYPE_DECIMAL:
  // enum type, only useful as logical type
  case DUCKDB_TYPE_ENUM:
  // list type, only useful as logical type
  case DUCKDB_TYPE_LIST:
  // struct type, only useful as logical type
  case DUCKDB_TYPE_STRUCT:
  // map type, only useful as logical type
  case DUCKDB_TYPE_MAP:
  // duckdb_array, only useful as logical type
  case DUCKDB_TYPE_ARRAY:
  // duckdb_hugeint
  case DUCKDB_TYPE_UUID:
  // union type, only useful as logical type
  case DUCKDB_TYPE_UNION:
  // duckdb_bit
  case DUCKDB_TYPE_BIT:
  // duckdb_time_tz
  case DUCKDB_TYPE_TIME_TZ:
  // ANY type
  case DUCKDB_TYPE_ANY:
  // duckdb_varint
  case DUCKDB_TYPE_VARINT:
  // SQLNULL type
  case DUCKDB_TYPE_SQLNULL:
    return "Undefined";
  }
}

std::string DuckDBGetString(const duckdb_string_t &duckdb_string)
{
  auto s = duckdb_string.value.inlined.length;
  const char *p = s <= 12 ? duckdb_string.value.inlined.inlined : duckdb_string.value.pointer.ptr;
  return std::string(p, s);
}