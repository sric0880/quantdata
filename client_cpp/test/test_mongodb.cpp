#include <iostream>
#include "mongodb/quantdata.h"

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
  auto cursor = MongoGetData("cn_stock", "stocks_basic_info");
  for (auto &&doc : cursor)
  {
    std::cout << to_json(doc) << std::endl;
  }
}

void test_get_trade_cal()
{
  // const char *db = "cn_stock";
  // auto cursor = MongoGetTradeCal(db);
  // auto cursor1 = MongoGetTradeCal(
  //     db,
  //     start_date = datetime.datetime(2024, 8, 19, 23),
  //     end_date = datetime.datetime(2024, 9, 19), );
  // assert df["_id"].iloc[0] == datetime.datetime(2024, 8, 20);
  // assert df["_id"].iloc[-1] == datetime.datetime(2024, 9, 19);

  // auto cursor2 = MongoGetTradeCal(db, start_date = datetime.datetime(2024, 8, 18));
  // assert df["_id"].iloc[0] == datetime.datetime(2024, 8, 19);

  // auto cursor3 = MongoGetTradeCal(db, end_date = datetime.datetime(2024, 9, 17));
  // assert df["_id"].iloc[-1] == datetime.datetime(2024, 9, 13);

  // last_dt = qd.mongo_get_last_trade_dt(
  //     db, datetime.datetime(2023, 4, 10, 23, 0, 0)
  // )
  // next_dt = qd.mongo_get_next_trade_dt(db, datetime.datetime(2023, 4, 8, 0, 0, 0))

  // assert last_dt == next_dt
}

int main(int, char **)
{
  test_connection();
  MongoConnect(host);
  test_get_array();
  MongoClose();
}