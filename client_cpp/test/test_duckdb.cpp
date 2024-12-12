#include <iostream>
#include "duckdb/quantdata.h"

void test_connection()
{
  duckdb_state ret;
  ret = duckdb_connect();
  ret = duckdb_attach("../datas/duckdb/test_finance.db");
  duckdb_close();
}

void test_get_array()
{
  duckdb_state ret;
  ret = duckdb_connect();
  ret = duckdb_attach("../datas/duckdb/test_finance.db");
  duckdb_result res;
  duckdb_get_array(&res, "test_finance", "bars_daily_000001_SZ", "dt,name,open");
  duckdb_print_results(&res);
  duckdb_get_array_last_rows(&res, "test_finance", "bars_daily_000001_SZ", "dt,name,open", "dt<'2024-08-01 00:00:00'", 5);
  duckdb_print_results(&res);
  duckdb_destroy_result(&res);
  duckdb_close();
}

int main(int, char **)
{
  test_connection();
  test_get_array();
}
