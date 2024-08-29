.open datas/duckdb/finance.db
drop table bars_daily;
CREATE TABLE IF NOT EXISTS bars_daily AS
    SELECT * FROM read_csv('datasources/data.csv', header=true, columns = {
        "dt":"TIMESTAMP_S",
        "name":"VARCHAR",
        "_open":"FLOAT",
        "_high":"FLOAT",
        "_low":"FLOAT",
        "_close":"FLOAT",
        "volume":"UINTEGER",
        "amount":"UBIGINT",
        "preclose":"FLOAT",
        "net_profit_ttm":"FLOAT",
        "cashflow_ttm":"FLOAT",
        "equity":"FLOAT",
        "asset":"FLOAT",
        "debt":"FLOAT",
        "debttoasset":"FLOAT",
        "net_profit_q":"FLOAT",
        "pe_ttm":"FLOAT",
        "pb":"FLOAT",
        "mkt_cap":"DOUBLE",
        "mkt_cap_ashare":"DOUBLE",
        "vip_buy_amt":"FLOAT",
        "vip_sell_amt":"FLOAT",
        "inst_buy_amt":"FLOAT",
        "inst_sell_amt":"FLOAT",
        "mid_buy_amt":"FLOAT",
        "mid_sell_amt":"FLOAT",
        "indi_buy_amt":"FLOAT",
        "indi_sell_amt":"FLOAT",
        "master_net_flow_in":"FLOAT",
        "master2_net_flow_in":"FLOAT",
        "vip_net_flow_in":"FLOAT",
        "mid_net_flow_in":"FLOAT",
        "inst_net_flow_in":"FLOAT",
        "indi_net_flow_in":"FLOAT",
        "total_sell_amt":"FLOAT",
        "total_buy_amt":"FLOAT",
        "net_flow_in":"FLOAT",
        "turnover":"FLOAT",
        "free_shares":"UBIGINT",
        "total_shares":"UBIGINT",
        "maxupordown":"TINYINT",
        "maxupordown_at_open":"TINYINT",
        "lb_up_count":"UTINYINT",
        "lb_down_count":"UTINYINT",
        "close":"FLOAT",
        "open":"FLOAT",
        "high":"FLOAT",
        "low":"FLOAT"
    });
DESCRIBE bars_daily;