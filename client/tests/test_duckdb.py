import logging

import numpy as np

import quantdata as qd

uri = '../datas/duckdb/finance.db'
tablename = 'bars_daily_000001_SZ'

def test_duckdb():
    with qd.duckdb_connect(uri) as conn:
        lastdt = np.datetime64("2024-08-30T08:00:00")
        lastdt01 = qd.duckdb_get_array(conn, tablename).fetchnumpy()['dt'][-1]
        assert lastdt01 == lastdt
        lastdt01 = qd.duckdb_get_array(conn, tablename, attrs=['dt', 'name', 'open']).fetchnumpy()['dt'][-1]
        assert lastdt01 == lastdt
        # logging.info(lastdt01)
        lastdt01= qd.duckdb_get_array(conn, tablename, attrs=['dt', 'name', 'open'], filter="dt>'2024-08-29 08:00:00'").fetchnumpy()['dt'][-1]
        assert lastdt01 == lastdt
        # 最后一行(所有列)
        lastdt01 = qd.duckdb_get_array_last_rows(conn, tablename).fetchnumpy()['dt'][-1]
        assert lastdt01 == lastdt
        # 最后一行(自定义列)
        lastdt01 = qd.duckdb_get_array_last_rows(conn, tablename, attrs=['dt', 'name', 'open']).fetchnumpy()['dt'][-1]
        assert lastdt01 == lastdt
        # 最后5行
        lastdt01 = qd.duckdb_get_array_last_rows(conn, tablename, attrs=['dt', 'name', 'open'], N=5).fetchnumpy()['dt'][-1]
        assert lastdt01 == lastdt
        lastdt01 = qd.duckdb_get_array_last_rows(conn, tablename, N=1000).fetchnumpy()['dt'][-1]
        assert lastdt01 == lastdt