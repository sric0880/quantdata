#pragma once
#include <vector>
#include <string>
#include <stdexcept>

#include "duckdb/duckdb.h"

class DuckDBException : public std::logic_error
{
  using std::logic_error::logic_error;
};
class DuckDBInvalidArgs : public DuckDBException
{
  using DuckDBException::DuckDBException;
};

template <typename>
constexpr bool dependent_false_v = false;

template <class T>
class deleter
{
  typedef void (*deleter_t)(T *);

public:
  deleter(bool is_new_pointer) : is_new_pointer_(is_new_pointer) {}
  void operator()(T *__ptr) const
  {
    get_deleter()(__ptr);
    if (is_new_pointer_)
      delete __ptr;
  }

  constexpr deleter_t get_deleter() const
  {
    if constexpr (std::is_same_v<T, duckdb_config>)
      return duckdb_destroy_config;
    else if constexpr (std::is_same_v<T, char *>)
      return (deleter_t)duckdb_free;
    else if constexpr (std::is_same_v<T, duckdb_prepared_statement>)
      return duckdb_destroy_prepare;
    else if constexpr (std::is_same_v<T, duckdb_result>)
      return duckdb_destroy_result;
    else if constexpr (std::is_same_v<T, duckdb_data_chunk>)
      return duckdb_destroy_data_chunk;
    else
    {
      // https://en.cppreference.com/w/cpp/language/if#Constexpr_if
      static_assert(dependent_false_v<T>, "no duckdb destroy function found");
      return nullptr;
    }
  }

private:
  bool is_new_pointer_;
};

template <class T>
class DuckDBGuard : public std::unique_ptr<T, deleter<T>>
{
public:
  explicit DuckDBGuard(T &obj) : std::unique_ptr<T, deleter<T>>(&obj, false) {};
  explicit DuckDBGuard(T &&obj) : std::unique_ptr<T, deleter<T>>(new T(obj), true) {};
};

class DuckDBArrays
{
public:
  DuckDBArrays(duckdb_result result);
  template <typename T>
  const T *FetchArray(idx_t chunk_idx, idx_t col_idx) const;
  std::vector<std::string> names;
  std::vector<duckdb_type> types;
  const idx_t ColumnCount() const { return column_count_; }
  const idx_t ChunkCount() const { return chunk_count_; }
  const idx_t RowCount() const { return row_count_; }
  const idx_t ChunkSize(idx_t chunk_idx) const;
  const std::string &ColumnName(idx_t index) const { return names[index]; };
  const std::string ToString() const;

protected:
  const duckdb_data_chunk &FetchChunk(idx_t chunk_idx) const;

private:
  idx_t chunk_count_;
  idx_t column_count_;
  idx_t row_count_;
  DuckDBGuard<duckdb_result> result_;
  std::vector<DuckDBGuard<duckdb_data_chunk>> chunks_;
};

// duckdb_column_data 函数拉取string_t数据不正确. [难道是异步，数据还没准备好?]
template <typename T>
const T *DuckDBArrays::FetchArray(idx_t chunk_idx, idx_t col_idx) const
{
  auto &chunk = FetchChunk(chunk_idx);
  duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, col_idx);
  return static_cast<T *>(duckdb_vector_get_data(vec));
}

/**
 * @param
 *  uri: path/to/${file}.db
 * @param
 *  memory_limit: like "10GB"
 * @param
 *  threads_limit: The number of total threads used by the system
 * @param
 *  shared_memory: if true, connect to a shared memory database. default not shared. But file database cannot be shared.

 * more configuration: https://duckdb.org/docs/configuration/overview.html#configuration-reference
 * more about connections: https://duckdb.org/docs/api/python/dbapi.html
 */
void DuckDBConnect(const char *memory_limit = nullptr,
                   int threads_limit = 0,
                   bool shared_memory = false);
/**
 * @param
 *  uri: path/to/${file}.db
 */
void DuckDBAttach(const char *uri);
/**
 * @see https://duckdb.org/docs/api/c/query
 */
DuckDBArrays DuckDBGetArray(const char *db_name,
                            const char *tablename,
                            const char *attrs = "*",
                            const char *filter = nullptr);
DuckDBArrays DuckDBGetArrayLastRows(const char *db_name,
                                    const char *tablename,
                                    const char *attrs = "*",
                                    const char *filter = nullptr,
                                    int N = 1);
void DuckDBClose();

std::string DuckDBGetString(const duckdb_string_t &duckdb_string);