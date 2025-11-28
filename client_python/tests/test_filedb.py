import pytest
from quantdata.databases._filedb import *


datadir = "daily_factors"


def test_filedb():
    filedb_connect("D://bt_data/duckdb/finance_astock")
    dfs_daily = filedb_get_between(
        datadir,
        "2025-04-30",
        "2025-08-14",
        fields=["dt", "symbol", "open", "high", "low", "close", "preclose"],
        adj=True,
    )
    with pytest.raises(ValueError):
        filedb_get_between(
            datadir,
            "2025-08-16",
            "2025-08-14",
            fields=["dt", "symbol", "open", "high", "low", "close", "preclose"],
            adj=True,
        )

    df = filedb_get_between(
        datadir,
        "2025-08-14",
        "2025-08-14",
        fields=["dt", "symbol", "open", "high", "low", "close", "preclose"],
        adj=True,
        groupby_sybmol=False,
    )
    assert df.select(pl.col("dt").unique().len()).item() == 1

    df1 = filedb_get_at(
        datadir,
        "2025-08-14",
        fields=["dt", "symbol", "open", "high", "low", "close", "preclose"],
        adj=True,
    )
    assert df.equals(df1)

    df2 = filedb_get_gte(
        datadir,
        "2025-07-14",
        n=10,
    )
    print(df2["000006.SZ"])
    assert len(df2["000006.SZ"]) == 10

    df3 = filedb_get_lte(
        datadir,
        "2025-08-14",
        n=10,
    )
    print(df3["000006.SZ"])
    assert len(df3["000006.SZ"]) == 10

    assert not filedb_has_all_columns(
        "daily_factors", "2025-08-14", ["name", "open", "non"]
    )
    assert not filedb_has_any_columns(
        "daily_factors", "2025-08-14", ["abc", "non", "foo"]
    )

    df_mon = filedb_groupby_freq(
        "daily_factors",
        "2025-04-30",
        "2025-08-14",
        "1mo",
        groupby_sybmol=True
    )["000001.SZ"]
    print(df_mon)
    assert 5 == len(df_mon)
    assert dfs_daily["000001.SZ"]["adj_open"][0] == df_mon["open_1mo"][0]
    assert dfs_daily["000001.SZ"]["adj_close"][-1] == df_mon["close_1mo"][-1]

    df_w = filedb_groupby_freq(
        "daily_factors",
        "2025-04-30",
        "2025-08-14",
        "1w",
        groupby_sybmol=True
    )["000001.SZ"]
    print(df_w)
    assert 16 == len(df_w)
    assert dfs_daily["000001.SZ"]["adj_open"][0] == df_w["open_1w"][0]
    assert dfs_daily["000001.SZ"]["adj_close"][-1] == df_w["close_1w"][-1]


if __name__ == "__main__":
    test_filedb()