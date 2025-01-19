#include <iostream>
#include "quantdata/mongodb.h"
#include "quantdata/datetime.h"
#include "quantdata/macros.h"

const char *host = "localhost";

void test_connection()
{
  MongoConnect(host);
  MongoConnect(host);
  MongoClose();
  MongoClose();
}

void test_get_array()
{
  options::find opts;
  opts.projection(make_document(kvp("_id", false)));
  auto no_cursor = MongoGetData("finance", "no_exists", opts);
  auto results = MongoFetchArrays<int>(std::move(no_cursor), [](const document::view &view)
                                       { return std::tuple(1); });
  QD_ASSERT(results.size() == 0, "results is not empty");
  auto cursor = MongoGetData("finance", "basic_info_stocks", opts);
  // for (auto &&doc : cursor)
  // {
  //   std::cout << to_json(doc) << std::endl;
  // }
  auto results_ = MongoFetchDict(std::move(cursor));
  auto size = results_["symbol"].size();
  std::cout << results_["symbol"][0].view().get_string().value << "..." << results_["symbol"][size - 1].view().get_string().value << std::endl;
  std::cout << results_["name"][0].view().get_string().value << "..." << results_["name"][size - 1].view().get_string().value << std::endl;
}

void test_get_trade_cal()
{
  const char *db = "quantcalendar";
  const char *tablename = "cn_stock";
  auto cursor = MongoGetTradeCal(db, tablename);
  auto start_date = Datetime<milliseconds>(2024, 8, 19, 23);
  auto end_date = Datetime<milliseconds>(2024, 9, 19);
  auto cursor1 = MongoGetTradeCalBetween(db, tablename, start_date, end_date);
  auto results = MongoFetchArrays<milliseconds, uint8_t>(std::move(cursor1), [](const document::view &view)
                                                         { return std::tuple{view["_id"].get_date().value, view["status"].get_int32().value}; });
  QD_ASSERT(results.size() > 0, "results is empty");
  QD_ASSERT(std::get<0>(results[0]) == Datetime<milliseconds>(2024, 8, 20).to_duration(), "");
  QD_ASSERT(std::get<0>(results[results.size() - 1]) == Datetime<milliseconds>(2024, 9, 19).to_duration(), "");

  auto start_date2 = Datetime<milliseconds>(2024, 8, 18);
  auto cursor2 = MongoGetTradeCalGte(db, tablename, start_date2);
  auto results2 = MongoFetchArrays<milliseconds, uint8_t>(std::move(cursor2), [](const document::view &view)
                                                          { return std::tuple{view["_id"].get_date().value, view["status"].get_int32().value}; });
  QD_ASSERT(results2.size() > 0, "results is empty");
  QD_ASSERT(std::get<0>(results2[0]) == Datetime<milliseconds>(2024, 8, 19).to_duration(), "");

  auto end_date2 = Datetime<milliseconds>(2024, 9, 17);
  auto cursor3 = MongoGetTradeCalLte(db, tablename, end_date2);
  auto results3 = MongoFetchArrays<milliseconds, uint8_t>(std::move(cursor3), [](const document::view &view)
                                                          { return std::tuple{view["_id"].get_date().value, view["status"].get_int32().value}; });
  QD_ASSERT(results3.size() > 0, "results is empty");
  QD_ASSERT(std::get<0>(results3[results3.size() - 1]) == Datetime<milliseconds>(2024, 9, 13).to_duration(), "");

  auto last_dt = MongoGetLastTradeDt(db, tablename, Datetime<milliseconds>(2023, 4, 10, 23));
  std::cout << (*last_dt).count() << std::endl;
  auto next_dt = MongoGetNextTradeDt(db, tablename, Datetime<milliseconds>(2023, 4, 8));
  std::cout << (*next_dt).count() << std::endl;
  QD_ASSERT(*last_dt == *next_dt, "");
}

int main(int, char **)
{
  test_connection();
  MongoConnect(host);
  test_get_array();
  test_get_trade_cal();
  MongoClose();
}