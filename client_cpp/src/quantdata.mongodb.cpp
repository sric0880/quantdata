#include <iostream>
#include <mongocxx/v_noabi/mongocxx/instance.hpp>
#include <mongocxx/v_noabi/mongocxx/uri.hpp>
#include <mongocxx/v_noabi/mongocxx/client.hpp>
#include <mongocxx/v_noabi/mongocxx/collection.hpp>

#include "mongodb/quantdata.h"

instance inst;
client *conn_mongodb = nullptr;

#define check_conn_return                                     \
  if (!conn_mongodb)                                          \
  {                                                           \
    std::cerr << "error: mongodb not connected" << std::endl; \
    return std::nullopt;                                      \
  }

#define check_empty_str_return(arg)                \
  if (!arg || !arg[0])                             \
  {                                                \
    std::cerr << "error: #arg empty" << std::endl; \
    return std::nullopt;                           \
  }

#define err_return(err)            \
  {                                \
    std::cerr << err << std::endl; \
    return;                        \
  }

void mongo_connect(const char *host,
                   int port,
                   const char *user,
                   const char *password,
                   const char *authSource,
                   const char *authMechanism)
{
  char buf[200];
  int cx = std::snprintf(buf, 200, "mongodb://%s:%s@%s:%d?authSource=%s&authMechanism=%s", user, password, host, port, authSource, authMechanism);
  if (cx < 0 || cx >= 200)
    err_return("error: mongo connect statement too long");
  conn_mongodb = new client(uri(buf));
}

void mongo_close()
{
  if (conn_mongodb)
    delete conn_mongodb;
}

std::optional<cursor> mongo_get_data(
    const char *db_name,
    const char *collection_name,
    document::view_or_value filter,
    document::view_or_value sort_by,
    document::view_or_value projection,
    int max_count)
{
  check_conn_return;
  check_empty_str_return(db_name);
  check_empty_str_return(collection_name);
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
  // if (!filter.view().empty())
  // {
  // }
  // else
  // {
  //   return coll.find(filter, opts);
  // }
}

std::optional<cursor> mongo_get_trade_cal(const char *db_name,
                                          const char *collection_name,
                                          types::b_date start_date,
                                          types::b_date end_date)
{
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
    return mongo_get_data(db_name, collection_name, doc.view());
  }
  else if (s)
  {
    // {"$and" : [ {"status" : 1}, {"_id" : {"$gte" : start_date}} ]}
    stream_document builder{};
    auto in_array = builder << "$and" << open_array;
    in_array = in_array << open_document << "status" << 1 << close_document;
    in_array = in_array << open_document << "_id" << make_document(kvp("$gte", start_date)) << close_document;
    auto doc = in_array << close_array << finalize;
    return mongo_get_data(db_name, collection_name, doc.view());
  }
  else if (e)
  {
    // {"$and" : [ {"status" : 1}, {"_id" : {"$lte" : end_date}} ]}
    stream_document builder{};
    auto in_array = builder << "$and" << open_array;
    in_array = in_array << open_document << "status" << 1 << close_document;
    in_array = in_array << open_document << "_id" << make_document(kvp("$lte", end_date)) << close_document;
    auto doc = in_array << close_array << finalize;
    return mongo_get_data(db_name, collection_name, doc.view());
  }
  else
  {
    // {"status" : 1}
    return mongo_get_data(db_name, collection_name, make_document(kvp("status", 1)));
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