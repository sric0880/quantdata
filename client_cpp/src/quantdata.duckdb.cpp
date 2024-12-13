#include <string>
#include <iostream>
#include "duckdb/quantdata.h"

duckdb_connection conn_duckdb;
duckdb_database db;

#define check_err_return(err)      \
  if (ret == DuckDBError)          \
  {                                \
    std::cerr << err << std::endl; \
    return ret;                    \
  }

#define check_err_release_return(msg, release) \
  if (ret == DuckDBError)                      \
  {                                            \
    std::cerr << msg << std::endl;             \
    release;                                   \
    return ret;                                \
  }

#define check_err_free_return(msg) \
  if (ret == DuckDBError)          \
  {                                \
    std::cerr << msg << std::endl; \
    duckdb_free(msg);              \
    return ret;                    \
  }

#define err_return(err)               \
  {                                   \
    std::cerr << err << std::endl;    \
    return duckdb_state::DuckDBError; \
  }

duckdb_state duckdb_connect(const char *memory_limit,
                            int threads_limit,
                            bool shared_memory)
{
  duckdb_config config;
  // create the configuration object
  duckdb_state ret = duckdb_create_config(&config);
  check_err_return("duckdb_create_config error");
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
  ret = duckdb_open_ext(path, &db, config, &err_msg);
  // cleanup the configuration object
  duckdb_destroy_config(&config);
  check_err_free_return(err_msg);
  ret = duckdb_connect(db, &conn_duckdb);
  check_err_return("duckdb_connect error");
  return duckdb_state::DuckDBSuccess;
}

duckdb_state duckdb_attach(const char *uri)
{
  char buf[200]{0};
  std::string _uri(uri);
  auto sep = _uri.rfind('/');
  auto dot = _uri.rfind('.');
  auto dbname = _uri.substr(sep + 1, dot - sep - 1);
  int cx = std::snprintf(buf, 200, "ATTACH IF NOT EXISTS '%s' as %s (READ_ONLY);", uri, dbname.c_str());
  if (cx < 0 || cx >= 200)
    err_return("error: db path too long");
  duckdb_prepared_statement stmt;
  duckdb_state ret = duckdb_prepare(conn_duckdb, buf, &stmt);
  check_err_release_return(duckdb_prepare_error(stmt), duckdb_destroy_prepare(&stmt));
  duckdb_result res;
  ret = duckdb_execute_prepared(stmt, &res);
  check_err_release_return(" error " << duckdb_result_error(&res), duckdb_destroy_result(&res));
  duckdb_destroy_result(&res);
  duckdb_destroy_prepare(&stmt);
  return duckdb_state::DuckDBSuccess;
}

void duckdb_close()
{
  duckdb_disconnect(&conn_duckdb);
  duckdb_close(&db);
}

duckdb_state duckdb_get_array(duckdb_result *out,
                              const char *db_name,
                              const char *tablename,
                              const char *attrs,
                              const char *filter)
{
  char query[200]{};
  int cx = std::snprintf(query, 200, "select %s FROM %s.%s ", attrs, db_name, tablename);
  if (cx < 0 || cx >= 200)
    err_return("error: query statement too long");
  if (filter && filter[0])
  {
    cx += std::snprintf(query + cx, 200 - cx, "WHERE %s", filter);
    if (cx < 0 || cx >= 200)
      err_return("error: query statement too long");
  }
  duckdb_state ret = duckdb_query(conn_duckdb, query, out);
  check_err_return(query << " duckdb_query error " << duckdb_result_error(out));
  return duckdb_state::DuckDBSuccess;
}

duckdb_state duckdb_get_array_last_rows(duckdb_result *out,
                                        const char *db_name,
                                        const char *tablename,
                                        const char *attrs,
                                        const char *filter,
                                        int N)
{
  char query[200]{};
  int cx = std::snprintf(query, 200, "SELECT * FROM (select %s FROM %s.%s ", attrs, db_name, tablename);
  if (cx < 0 || cx >= 200)
    err_return("error: query statement too long");
  if (filter && filter[0])
  {
    cx += std::snprintf(query + cx, 200 - cx, "WHERE %s", filter);
    if (cx < 0 || cx >= 200)
      err_return("error: query statement too long");
  }
  std::snprintf(query + cx, 200 - cx, "ORDER BY dt DESC LIMIT %d) ORDER BY dt ASC;", N);
  if (cx < 0 || cx >= 200)
    err_return("error: query statement too long");
  duckdb_state ret = duckdb_query(conn_duckdb, query, out);
  check_err_return(query << " duckdb_query error " << duckdb_result_error(out));
  return duckdb_state::DuckDBSuccess;
}

/**
 * @bug duckdb_value_timestamp return 0
 */
void duckdb_print_results(duckdb_result *result)
{
  // print the names of the result
  idx_t row_count = duckdb_row_count(result);
  idx_t column_count = duckdb_column_count(result);
  for (size_t i = 0; i < column_count; i++)
  {
    printf("%s ", duckdb_column_name(result, i));
  }
  printf("\n");
  // print the data of the result
  for (size_t row_idx = 0; row_idx < row_count; row_idx++)
  {
    for (size_t col_idx = 0; col_idx < column_count; col_idx++)
    {
      auto type = duckdb_column_type(result, col_idx);
      switch (type)
      {
      case DUCKDB_TYPE_TIMESTAMP_S:
      {
        auto val = duckdb_value_timestamp(result, col_idx, row_idx);
        // val.micros *= 1000000;
        auto ts_struct = duckdb_from_timestamp(val);
        printf("%ld %d-%02d-%02d %02d:%02d:%02d ", val.micros, ts_struct.date.year, ts_struct.date.month, ts_struct.date.day, ts_struct.time.hour, ts_struct.time.min, ts_struct.time.sec);
        break;
      }
      case DUCKDB_TYPE_VARCHAR:
      {
        auto val = duckdb_value_string(result, col_idx, row_idx);
        printf("%s ", val.data);
        duckdb_free(val.data);
        break;
      }
      case DUCKDB_TYPE_FLOAT:
      {
        auto val = duckdb_value_float(result, col_idx, row_idx);
        printf("%f ", val);
        break;
      }
      }
    }
    printf("\n");
  }
}
