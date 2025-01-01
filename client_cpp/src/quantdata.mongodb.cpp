#include <iostream>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/collection.hpp>
#include <fmt/format.h>

#include "quantdata/mongodb.h"
#include "quantdata/macros.h"

instance inst;
client *conn_mongodb = nullptr;

inline void assert_connected()
{
  if (!conn_mongodb)
  {
    throw MongoDBException("mongodb not connected");
  }
}

void MongoConnect(std::string_view host,
                  int port,
                  std::string_view user,
                  std::string_view password,
                  std::string_view authSource,
                  std::string_view authMechanism)
{
  auto uri_string = fmt::format("mongodb://{}:{}@{}:{}?authSource={}&authMechanism={}", user, password, host, port, authSource, authMechanism);
  if (conn_mongodb)
    return;
  conn_mongodb = new client(uri(uri_string));
}

void MongoClose()
{
  if (conn_mongodb)
    delete conn_mongodb;
  conn_mongodb = nullptr;
}

/**
 * mongodb find
 * options.limit(int)
 * options.sort(view_or_value)
 * options.projection(view_or_value)
 */
cursor MongoGetData(
    std::string_view db_name,
    std::string_view collection_name,
    document::view_or_value filter,
    const options::find &options)
{
  assert_connected();
  QD_ASSERT_STRING(db_name);
  QD_ASSERT_STRING(collection_name);
  collection coll = (*conn_mongodb)[db_name][collection_name];
  return coll.find(filter, options);
}

cursor MongoGetTradeCal(std::string_view db_name, std::string_view collection_name)
{
  assert_connected();
  QD_ASSERT_STRING(db_name);
  // {"status" : 1}
  return MongoGetData(db_name, collection_name, make_document(kvp("status", 1)));
}
cursor MongoGetTradeCalGte(std::string_view db_name,
                           std::string_view collection_name,
                           const milliseconds &start_date)
{
  assert_connected();
  QD_ASSERT_STRING(db_name);
  // {"$and" : [ {"status" : 1}, {"_id" : {"$gte" : start_date}} ]}
  stream_document builder{};
  auto in_array = builder << "$and" << open_array;
  in_array = in_array << open_document << "status" << 1 << close_document;
  in_array = in_array << open_document << "_id" << make_document(kvp("$gte", types::b_date(start_date))) << close_document;
  auto doc = in_array << close_array << finalize;
  return MongoGetData(db_name, collection_name, doc.view());
}
cursor MongoGetTradeCalLte(std::string_view db_name,
                           std::string_view collection_name,
                           const milliseconds &end_date)
{
  assert_connected();
  QD_ASSERT_STRING(db_name);
  // {"$and" : [ {"status" : 1}, {"_id" : {"$lte" : end_date}} ]}
  stream_document builder{};
  auto in_array = builder << "$and" << open_array;
  in_array = in_array << open_document << "status" << 1 << close_document;
  in_array = in_array << open_document << "_id" << make_document(kvp("$lte", types::b_date(end_date))) << close_document;
  auto doc = in_array << close_array << finalize;
  return MongoGetData(db_name, collection_name, doc.view());
}
cursor MongoGetTradeCalBetween(std::string_view db_name,
                               std::string_view collection_name,
                               const milliseconds &start_date,
                               const milliseconds &end_date)
{
  assert_connected();
  QD_ASSERT_STRING(db_name);
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
  in_array = in_array << open_document << "_id" << make_document(kvp("$gte", types::b_date(start_date))) << close_document;
  in_array = in_array << open_document << "_id" << make_document(kvp("$lte", types::b_date(end_date))) << close_document;
  auto doc = in_array << close_array << finalize;
  return MongoGetData(db_name, collection_name, doc.view());
}

std::optional<milliseconds> MongoGetLastTradeDt(std::string_view db_name, std::string_view collection_name, const milliseconds &dt)
{
  assert_connected();
  QD_ASSERT_STRING(db_name);
  options::find opts;
  opts.sort(make_document(kvp("_id", -1)));
  opts.limit(1);
  stream_document builder{};
  auto in_array = builder << "$and" << open_array;
  in_array = in_array << open_document << "_id" << make_document(kvp("$lte", types::b_date(dt))) << close_document;
  in_array = in_array << open_document << "status" << 1 << close_document;
  auto doc = in_array << close_array << finalize;
  collection coll = (*conn_mongodb)[db_name][collection_name];
  auto doc_value = coll.find_one(doc.view(), opts);
  return doc_value.has_value() ? std::optional<milliseconds>(doc_value.value()["_id"].get_date().value) : std::nullopt;
}

std::optional<milliseconds> MongoGetNextTradeDt(std::string_view db_name, std::string_view collection_name, const milliseconds &dt)
{
  assert_connected();
  QD_ASSERT_STRING(db_name);
  options::find opts;
  opts.limit(1);
  stream_document builder{};
  auto in_array = builder << "$and" << open_array;
  in_array = in_array << open_document << "_id" << make_document(kvp("$gt", types::b_date(dt))) << close_document;
  in_array = in_array << open_document << "status" << 1 << close_document;
  auto doc = in_array << close_array << finalize;
  collection coll = (*conn_mongodb)[db_name][collection_name];
  auto doc_value = coll.find_one(doc.view(), opts);
  return doc_value.has_value() ? std::optional<milliseconds>(doc_value.value()["_id"].get_date().value) : std::nullopt;
}

std::unordered_map<std::string, std::vector<types::bson_value::value>> MongoFetchDict(cursor &&cr)
{
  std::unordered_map<std::string, std::vector<types::bson_value::value>> ret;
  for (auto &doc : cr)
  {
    for (auto &elem : doc)
    {
      auto &&[it, ok] = ret.try_emplace(std::string(elem.key()));
      // std::vector只能是types::bson_value::value类型
      // 不能为 types::bson_value::view 或者  document::element 类型
      // 这两种类型只持有底层数据的view，底层数据随时可能被清理。
      // 这种通过emplace_back(view) 减少一次value的构造函数调用
      it->second.emplace_back(elem.get_value());
    }
  }
  return ret;
}
