#include <string>
#include <chrono>
#include <optional>
#include <mongocxx/v_noabi/mongocxx/cursor.hpp>
#include <bsoncxx/v_noabi/bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/v_noabi/bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/v_noabi/bsoncxx/builder/basic/array.hpp>
#include <bsoncxx/v_noabi/bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/v_noabi/bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/v_noabi/bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/v_noabi/bsoncxx/document/view_or_value.hpp>
#include <bsoncxx/v_noabi/bsoncxx/types.hpp>

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
const static types::b_date date_zero(std::chrono::milliseconds::zero());

void mongo_connect(const char *host,
                   int port = 27017,
                   const char *user = "root",
                   const char *password = "admin",
                   const char *authSource = "admin",
                   const char *authMechanism = "SCRAM-SHA-1");
void mongo_close();
std::optional<cursor> mongo_get_data(
    const char *db_name,
    const char *collection_name,
    document::view_or_value filter = empty_view,
    document::view_or_value sort_by = empty_view,
    document::view_or_value projection = empty_view,
    int max_count = 0);
/**
 * 返回从start_date到end_date(包括本身)的交易日期
 */
std::optional<cursor> mongo_get_trade_cal(const char *db_name,
                                          const char *collection_name,
                                          types::b_date start_date = date_zero,
                                          types::b_date end_date = date_zero);
// std::time_t mongo_get_last_trade_dt(std::time_t dt);
// std::time_t mongo_get_next_trade_dt(std::time_t datetime);
