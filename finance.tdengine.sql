CREATE DATABASE IF NOT EXISTS finance KEEP 36500 DURATION 3650 VGROUPS 2 MAXROWS 10000 MINROWS 200 CACHEMODEL 'last_row' CACHESIZE 512 BUFFER 1024;
USE finance;
CREATE STABLE IF NOT EXISTS bars_daily (
    dt timestamp,
    name nchar(8),
    _open float,
    _high float,
    _low float,
    _close float,
    volume int unsigned,
    amount bigint unsigned,
    preclose float,
    net_profit_ttm float,
    cashflow_ttm float,
    equity float,
    asset float,
    debt float,
    debttoasset float,
    net_profit_q float,
    pe_ttm float,
    pb float,
    mkt_cap double,
    mkt_cap_ashare double,
    vip_buy_amt float,
    vip_sell_amt float,
    inst_buy_amt float,
    inst_sell_amt float,
    mid_buy_amt float,
    mid_sell_amt float,
    indi_buy_amt float,
    indi_sell_amt float,
    master_net_flow_in float,
    master2_net_flow_in float,
    vip_net_flow_in float,
    mid_net_flow_in float,
    inst_net_flow_in float,
    indi_net_flow_in float,
    total_sell_amt float,
    total_buy_amt float,
    net_flow_in float,
    turnover float,
    free_shares bigint unsigned,
    total_shares bigint unsigned,
    maxupordown tinyint,
    maxupordown_at_open tinyint,
    lb_up_count tinyint unsigned,
    lb_down_count tinyint unsigned,
    close float,
    open float,
    high float,
    low float
) TAGS (symbol binary(9));
CREATE TABLE IF NOT EXISTS bars_daily_000001_sz USING bars_daily TAGS ('000001.SZ');
insert into bars_daily_000001_sz file '/usr/000001.SZ.csv';