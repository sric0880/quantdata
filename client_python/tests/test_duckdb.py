import numpy as np

import quantdata.databases._duckdb as qd

uri = "../datas/duckdb/test_finance.db"
tablename = "bars_daily_000001_SZ"


def test_duckdb():
    qd.duckdb_connect_attach(uris=[uri])
    lastdt = np.datetime64("2024-08-30T00:00:00")
    lastdt01 = qd.duckdb_get_array("test_finance", tablename).fetchnumpy()["dt"][-1]
    assert lastdt01 == lastdt
    lastdt01 = qd.duckdb_get_array(
        "test_finance", tablename, attrs=["dt", "name", "open"]
    ).fetchnumpy()["dt"][-1]
    assert lastdt01 == lastdt
    # logging.info(lastdt01)
    lastdt01 = qd.duckdb_get_array(
        "test_finance",
        tablename,
        attrs=["dt", "name", "open"],
        filter="dt>'2024-08-29 00:00:00'",
    ).fetchnumpy()["dt"][-1]
    assert lastdt01 == lastdt
    # 最后一行(所有列)
    lastdt01 = qd.duckdb_get_array_last_rows("test_finance", tablename).fetchnumpy()[
        "dt"
    ][-1]
    assert lastdt01 == lastdt
    # 最后一行(自定义列)
    lastdt01 = qd.duckdb_get_array_last_rows(
        "test_finance", tablename, attrs=["dt", "name", "open"]
    ).fetchnumpy()["dt"][-1]
    assert lastdt01 == lastdt
    # 最后5行
    lastdt01 = qd.duckdb_get_array_last_rows(
        "test_finance", tablename, attrs=["dt", "name", "open"], N=5
    ).fetchnumpy()["dt"][-1]
    assert lastdt01 == lastdt
    lastdt01 = qd.duckdb_get_array_last_rows(
        "test_finance", tablename, N=1000
    ).fetchnumpy()["dt"][-1]
    assert lastdt01 == lastdt
