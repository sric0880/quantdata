#pragma once
#include <string>
#include <optional>
#include <unordered_map>
#include <mongocxx/cursor.hpp>
#include <mongocxx/options/find.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/types/bson_value/value.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/json.hpp>

#include "datetime.h"

using namespace bsoncxx::v_noabi;
using namespace mongocxx::v_noabi;

using builder::basic::kvp;
using builder::basic::make_array;
using builder::basic::make_document;
using stream_array = builder::stream::array;
using stream_document = builder::stream::document;
using builder::stream::close_array;
using builder::stream::close_document;
using builder::stream::finalize;
using builder::stream::open_array;
using builder::stream::open_document;

const static document::view_or_value empty_view;
const static options::find default_find_options;

class MongoDBException : public std::logic_error
{
    using std::logic_error::logic_error;
};

void MongoConnect(const char *host,
                  int port = 27017,
                  const char *user = "root",
                  const char *password = "admin",
                  const char *authSource = "admin",
                  const char *authMechanism = "SCRAM-SHA-1");
void MongoClose();

// cursor 只能遍历一遍
template <class T = document::view>
std::vector<T> MongoFetchArrays(cursor &cr)
{
    if (cr.begin() == cr.end())
    {
        return std::vector<T>();
    }
    else
    {
        std::vector<T> ret;
        for (auto &doc : cr)
        {
            ret.emplace_back(doc);
        }
        return ret;
    }
}

// cursor 只能遍历一遍
// 该函数会复制一遍底层数据，使用 MongoFetchArrays 效率更高
std::unordered_map<std::string, std::vector<types::bson_value::value>> MongoFetchDict(cursor &cr);

cursor MongoGetData(
    const char *db_name,
    const char *collection_name,
    document::view_or_value filter,
    const options::find &options);
inline cursor MongoGetData(
    const char *db_name,
    const char *collection_name)
{
    return MongoGetData(db_name, collection_name, empty_view, default_find_options);
}
inline cursor MongoGetData(
    const char *db_name,
    const char *collection_name,
    document::view_or_value filter)
{
    return MongoGetData(db_name, collection_name, filter, default_find_options);
}
inline cursor MongoGetData(
    const char *db_name,
    const char *collection_name,
    const options::find &options)
{
    return MongoGetData(db_name, collection_name, empty_view, options);
}
/**
 * 返回从start_date到end_date(包括本身)的交易日期
 */
cursor MongoGetTradeCal(const char *db_name, const char *collection_name);
cursor MongoGetTradeCalGte(const char *db_name,
                           const char *collection_name,
                           const milliseconds &start_date);
cursor MongoGetTradeCalLte(const char *db_name,
                           const char *collection_name,
                           const milliseconds &end_date);
cursor MongoGetTradeCalBetween(const char *db_name,
                               const char *collection_name,
                               const milliseconds &start_date,
                               const milliseconds &end_date);
std::optional<milliseconds> MongoGetLastTradeDt(const char *db_name,
                                                const char *collection_name,
                                                const milliseconds &dt);
std::optional<milliseconds> MongoGetNextTradeDt(const char *db_name,
                                                const char *collection_name,
                                                const milliseconds &dt);
