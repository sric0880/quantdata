

## cn_stock/trade_cal 交易日历

|字段名|说明|类型|
|--|--|--|
|_id [index] |日期|datetime64[s]|
|status|是否交易：0-不交易 1-交易|int8|

## cn_stock/stocks_basic_info

|字段名|说明|类型|
|--|--|--|
|symbol|股票代码|string|
|name|股票名称|string|
|area|地域|string|
|industry|所属行业|string|
|fullname|股票全称|string|
|enname|英文全称|string|
|cnspell|拼音缩写|string|
|market|市场类型（主板/创业板/科创板/CDR）|string|
|exchange|交易所代码（SSE/SZSE/BSE）|string|
|curr_type|交易货币：CNY|string|
|list_date|上市日期|datetime64[D]|
|delist_date|退市日期|datetime64[D]|
|status|上市状态，其中L上市 D退市 P暂停上市|string|
|is_hs|是否沪深港通标的，N否 H沪股通 S深股通|string|

## cn_stock/bars/daily

|字段名|说明|类型|
|--|--|--|
|dt [index]|交易日期|datetime64[s]|
|name|股票名称|string|
|open|开盘价(前复权)|float32|
|high|最高价(前复权)|float32|
|low|最低价(前复权)|float32|
|close|收盘价(前复权)|float32|
|_open|开盘价|float32|
|_high|最高价|float32|
|_low|最低价|float32|
|_close|收盘价|float32|
|preclose|前收盘价，若当天发生除权，前收盘价为上个交易日复权之后的收盘价|float32|
|volume|成交量（股）|uint32|
|amount|成交额（元）|uint64|
|net_profit_ttm|净利润TTM(不含少数股东损益)|float64|
|cashflow_ttm|现金流TTM, 经营活动产生的现金流量净额|float64|
|equity|净资产|float64|
|asset|总资产|float64|
|debt|总负债|float64|
|debttoasset|资产负债率|float32|
|net_profit_q|净利润(当季)|float64|
|pe_ttm|滚动市盈率|float32|
|pb|市净率|float32|
|mkt_cap|总市值(元)|float64|
|mkt_cap_ashare|流通市值(元)|float64|
|vip_buy_amt|大户资金买入额(万元)|float32|
|vip_sell_amt|大户资金卖出额(万元)|float32|
|inst_buy_amt|机构资金买入额(万元)|float32|
|inst_sell_amt|机构资金卖出额(万元)|float32|
|mid_buy_amt|中户资金买入额(万元)|float32|
|mid_sell_amt|中户资金卖出额(万元)|float32|
|indi_buy_amt|散户资金买入额(万元)|float32|
|indi_sell_amt|散户资金卖出额(万元)|float32|
|master_net_flow_in|主力(机构和大户)净买入(万元)|float32|
|master2_net_flow_in|主力2(机构、大户和中户)净买入(万元)|float32|
|vip_net_flow_in|大户净流入(万元)|float32|
|mid_net_flow_in|中户净流入(万元)|float32|
|inst_net_flow_in|机构净流入(万元)|float32|
|indi_net_flow_in|散户净流入(万元)|float32|
|total_sell_amt|流出资金总额(万元)|float32|
|total_buy_amt|流入资金总额(万元)|float32|
|net_flow_in|资金净流入(万元)|float32|
|turnover|换手率|float32|
|free_shares|流通股本|uint64|
|total_shares|总股本|uint64|
|maxupordown|标记收盘涨停或跌停状态,1-涨停,2-一字板涨停；-1-跌停，-2-一字板跌停；0-未涨跌停|uint8|
|maxupordown_at_open|标记开盘涨停或跌停状态，状态码同上|uint8|
|lb_up_count|连板涨停次数|uint8|
|lb_down_count|连板跌停次数|uint8|

## cn_stock/bars/1min(5min,10min,15min,30min,1h,2h,w,m)

已经是前复权的数据

|字段名|说明|类型|
|--|--|--|
|dt|交易日期|datetime64[s]|
|open|开盘价|float32|
|high|最高价|float32|
|low|最低价|float32|
|close|收盘价|float32|
|volume|成交量|uint64|
|amount|成交额|float64|

## cn_stock/market_stats

排除了ST股

|字段名|说明|类型|
|--|--|--|
|dt [index]|交易日期|datetime64[s]|
|count_of_uplimit|涨停数|uint16|
|count_of_downlimit|跌停数|uint16|
|count_of_yiziup|一字涨停数|uint16|
|count_of_yizidown|一字跌停数|uint16|
|ratio_of_uplimit|涨停数占所有股数比例|float32|
|ratio_of_downlimit|跌停数占所有股数比例|float32|
|ratio_of_yiziup|一字涨停数占所有股数比例|float32|
|ratio_of_yizidown|一字跌停数占所有股数比例|float32|
|lb|连板数量[首板数, 二板数, ...]|[uint8,...]|
