#include <iostream>
#include <cassert>
#include "quantdata/duckdb.h"

void test_connect()
{
  DuckDBConnect();
  DuckDBConnect();
  DuckDBClose();
  DuckDBClose();
  DuckDBClose();
  DuckDBConnect();
  DuckDBClose();
}

void test_get_array()
{
  auto result1 = DuckDBGetArray("test_finance", "bars_daily_000001_SZ", "dt,name,open");
  std::cerr << result1.ToString() << std::endl;

  auto result2 = DuckDBGetArrayLastRows("test_finance", "bars_daily_000001_SZ", "dt,name,open", "dt<'2024-08-01 00:00:00'", 5);

  auto names = result2.FetchArray<duckdb_string_t>(0, 1);
  idx_t chunk_size = result2.ChunkSize(0);
  std::cerr << result2.ToString() << std::endl;
  assert(DuckDBGetString(names[chunk_size - 1]) == "平安银行");
}

void test_get_array_error()
{
  auto result1 = DuckDBGetArray("test_finance", "not_exists", "dt,name,open");
}

int main(int, char **)
{
  test_connect();
  DuckDBConnect();
  DuckDBAttach("../datas/duckdb/test_finance.db");
  test_get_array();
  try
  {
    test_get_array_error();
  }
  catch (DuckDBException &e)
  {
    // std::cerr << e.what() << std::endl;
  }
  DuckDBClose();

  DuckDBConnect();
  try
  {
    DuckDBAttach("../datas/duckdb/not_exists.db");
  }
  catch (DuckDBException &e)
  {
    // std::cerr << e.what() << std::endl;
  }
  DuckDBClose();
}
