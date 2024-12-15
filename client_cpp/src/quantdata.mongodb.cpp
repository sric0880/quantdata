#include <iostream>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>

#include "mongodb/quantdata.h"

instance inst;
client *conn_mongodb = nullptr;

#define check_conn                                   \
  if (!conn_mongodb)                                 \
  {                                                  \
    throw MongoDBException("mongodb not connected"); \
  }

#define check_empty_string(arg)                 \
  if (!arg || !arg[0])                          \
  {                                             \
    throw MongoDBInvalidArgs(#arg " is empty"); \
  }

void MongoConnect(const char *host,
                  int port,
                  const char *user,
                  const char *password,
                  const char *authSource,
                  const char *authMechanism)
{
  char buf[200];
  int cx = std::snprintf(buf, 200, "mongodb://%s:%s@%s:%d?authSource=%s&authMechanism=%s", user, password, host, port, authSource, authMechanism);
  if (cx < 0 || cx >= 200)
    throw MongoDBException("mongo connect statement too long");
  if (conn_mongodb)
    return;
  conn_mongodb = new client(uri(buf));
}

void MongoClose()
{
  if (conn_mongodb)
    delete conn_mongodb;
  conn_mongodb = nullptr;
}

cursor MongoGetData(
    const char *db_name,
    const char *collection_name,
    document::view_or_value filter,
    document::view_or_value sort_by,
    document::view_or_value projection,
    int max_count)
{
  check_conn;
  check_empty_string(db_name);
  check_empty_string(collection_name);
  collection coll = (*conn_mongodb)[db_name][collection_name];
  options::find opts;
  if (max_count > 0)
  {
    opts.limit(max_count);
  }
  if (!sort_by.view().empty())
  {
    opts.sort(sort_by);
  }
  if (projection.view().empty())
  {
    opts.projection(projection);
  }
  return coll.find(filter, opts);
}

cursor MongoGetTradeCal(const char *db_name,
                        const char *collection_name,
                        types::b_date start_date,
                        types::b_date end_date)
{
  check_conn;
  check_empty_string(db_name);
  check_empty_string(collection_name);
  auto s = start_date != date_zero;
  auto e = end_date != date_zero;
  if (s && e)
  {
    // filter:
    // {
    //   "$and" : [
    //     {"status" : 1},
    //     {"_id" : {"$gte" : start_date}},
    //     {"_id" : {"$lte" : end_date}},
    //   ]
    // }
    stream_document builder{};
    auto in_array = builder << "$and" << open_array;
    in_array = in_array << open_document << "status" << 1 << close_document;
    in_array = in_array << open_document << "_id" << make_document(kvp("$gte", start_date)) << close_document;
    in_array = in_array << open_document << "_id" << make_document(kvp("$lte", end_date)) << close_document;
    auto doc = in_array << close_array << finalize;
    return MongoGetData(db_name, collection_name, doc.view());
  }
  else if (s)
  {
    // {"$and" : [ {"status" : 1}, {"_id" : {"$gte" : start_date}} ]}
    stream_document builder{};
    auto in_array = builder << "$and" << open_array;
    in_array = in_array << open_document << "status" << 1 << close_document;
    in_array = in_array << open_document << "_id" << make_document(kvp("$gte", start_date)) << close_document;
    auto doc = in_array << close_array << finalize;
    return MongoGetData(db_name, collection_name, doc.view());
  }
  else if (e)
  {
    // {"$and" : [ {"status" : 1}, {"_id" : {"$lte" : end_date}} ]}
    stream_document builder{};
    auto in_array = builder << "$and" << open_array;
    in_array = in_array << open_document << "status" << 1 << close_document;
    in_array = in_array << open_document << "_id" << make_document(kvp("$lte", end_date)) << close_document;
    auto doc = in_array << close_array << finalize;
    return MongoGetData(db_name, collection_name, doc.view());
  }
  else
  {
    // {"status" : 1}
    return MongoGetData(db_name, collection_name, make_document(kvp("status", 1)));
  }
}

// std::time_t mongo_get_last_trade_dt(std::time_t dt){
//     mongo_get_data(db_name, collection_name, {"status" : 1})

//         db["trade_cal"]
//             .find({"$and" : [ {"status" : 1}, {"_id" : {"$lte" : dt}} ]})
//             .sort([("_id", -1)])
//             .limit(1)} std::time_t mongo_get_next_trade_dt(std::time_t datetime)
// {
// }