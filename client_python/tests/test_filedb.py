import pytest
from quantdata.databases._filedb import *
from quantdata.databases._mongodb import *


mongo_connect("localhost")


def test_filedb():
    filedb_connect("D:/bt_data/duckdb/finance_astock")
    filedb_get_between(
        "daily_factors",
        fields=["dt", "symbol", "open", "high", "low", "close"],
        start_date="2025-04-16",
        end_date="2025-08-14",
        adj_factor_mongo_params=("finance", "adjust_factors"),
    )
    with pytest.raises(ValueError):
        filedb_get_between(
            "daily_factors",
            fields=["dt", "symbol", "open", "high", "low", "close"],
            start_date="2025-08-16",
            end_date="2025-08-14",
            adj_factor_mongo_params=("finance", "adjust_factors"),
        )

    df = filedb_get_between(
        "daily_factors",
        fields=["dt", "symbol", "open", "high", "low", "close"],
        start_date="2025-08-14",
        end_date="2025-08-14",
        adj_factor_mongo_params=("finance", "adjust_factors"),
        groupby_sybmol=False,
    )
    assert df.select(pl.col("dt").unique().len()).item() == 1

    df1 = filedb_get_at(
        "daily_factors",
        fields=["dt", "symbol", "open", "high", "low", "close"],
        day="2025-08-14",
        adj_factor_mongo_params=("finance", "adjust_factors"),
    )
    assert df.equals(df1)

    df2 = filedb_get_gte(
        "daily_factors",
        day="2025-07-14",
        n=10,
    )
    print(df2["000006.SZ"])
    assert len(df2["000006.SZ"]) == 10
    df2_ = filedb_get_gte(
        "daily_factors",
        day="2025-08-14",
        n=10,
    )
    print(df2_["000006.SZ"])
    assert len(df2_["000006.SZ"]) == 1

    df3 = filedb_get_lte(
        "daily_factors",
        day="2025-08-14",
        n=10,
    )
    print(df3["000006.SZ"])
    assert len(df3["000006.SZ"]) == 10
