import pytest
from quantdata.databases._filedb import *
from quantdata.databases._mongodb import *


mongo_connect("localhost")

datadir = "daily_factors"

def test_filedb():
    filedb_connect("D://bt_data/duckdb/finance_astock")
    dfs_daily = filedb_get_between(
        datadir,
        fields=["dt", "symbol", "open", "high", "low", "close"],
        start_date="2025-04-30",
        end_date="2025-08-14",
        adj_factor_mongo_params=("finance", "adjust_factors"),
    )
    with pytest.raises(ValueError):
        filedb_get_between(
            datadir,
            fields=["dt", "symbol", "open", "high", "low", "close"],
            start_date="2025-08-16",
            end_date="2025-08-14",
            adj_factor_mongo_params=("finance", "adjust_factors"),
        )

    df = filedb_get_between(
        datadir,
        fields=["dt", "symbol", "open", "high", "low", "close"],
        start_date="2025-08-14",
        end_date="2025-08-14",
        adj_factor_mongo_params=("finance", "adjust_factors"),
        groupby_sybmol=False,
    )
    assert df.select(pl.col("dt").unique().len()).item() == 1

    df1 = filedb_get_at(
        datadir,
        fields=["dt", "symbol", "open", "high", "low", "close"],
        day="2025-08-14",
        adj_factor_mongo_params=("finance", "adjust_factors"),
    )
    assert df.equals(df1)

    df2 = filedb_get_gte(
        datadir,
        day="2025-07-14",
        n=10,
    )
    print(df2["000006.SZ"])
    assert len(df2["000006.SZ"]) == 10

    df3 = filedb_get_lte(
        datadir,
        day="2025-08-14",
        n=10,
    )
    print(df3["000006.SZ"])
    assert len(df3["000006.SZ"]) == 10

    assert not filedb_has_all_columns("daily_factors", "2025-08-14", ["name", "open", "non"])
    assert not filedb_has_any_columns("daily_factors", "2025-08-14", ["abc", "non", "foo"])

    df_mon = filedb_get_between(
        "daily_factors",
        fields=["dt", "symbol", "open", "high", "low", "close", "volume", "amount"],
        start_date="2025-04-30",
        end_date="2025-08-14",
        adj_factor_mongo_params=("finance", "adjust_factors"),
        groupby_freq = "1mo"
    )["000001.SZ"]
    assert 5 == len(df_mon)
    assert dfs_daily["000001.SZ"]["adj_open"][0] == df_mon["adj_open"][0]
    assert dfs_daily["000001.SZ"]["adj_close"][-1] == df_mon["adj_close"][-1]

    df_w = filedb_get_between(
        "daily_factors",
        fields=["dt", "symbol", "open", "high", "low", "close", "volume", "amount"],
        start_date="2025-04-30",
        end_date="2025-08-14",
        adj_factor_mongo_params=("finance", "adjust_factors"),
        groupby_freq = "1w"
    )["000001.SZ"]
    assert 16 == len(df_w)
    assert dfs_daily["000001.SZ"]["adj_open"][0] == df_w["adj_open"][0]
    assert dfs_daily["000001.SZ"]["adj_close"][-1] == df_w["adj_close"][-1]