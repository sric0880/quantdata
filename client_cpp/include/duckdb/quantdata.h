#pragma once
#include "duckdb/duckdb.h"

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
duckdb_state duckdb_connect(const char *memory_limit = nullptr,
                            int threads_limit = 0,
                            bool shared_memory = false);
/**
 * @param
 *  uri: path/to/${file}.db
 */
duckdb_state duckdb_attach(const char *uri);
void duckdb_close();
/**
 * @see https://duckdb.org/docs/api/c/query
 */
duckdb_state duckdb_get_array(duckdb_result *out,
                              const char *db_name,
                              const char *tablename,
                              const char *attrs = "*",
                              const char *filter = nullptr);
duckdb_state duckdb_get_array_last_rows(duckdb_result *out,
                                        const char *db_name,
                                        const char *tablename,
                                        const char *attrs = "*",
                                        const char *filter = nullptr,
                                        int N = 1);
void duckdb_print_results(duckdb_result *result);