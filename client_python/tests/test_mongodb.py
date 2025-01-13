import datetime
import logging

import pandas as pd

import quantdata.databases._mongodb as qd

host = "localhost"


def test_get_data():
    qd.mongo_connect(host)
    c = qd.mongo_get_data("cn_stock", "stocks_basic_info", projection={"_id": False})
    df = pd.DataFrame(c)

    logging.info(df)


def test_get_trade_cal():
    qd.mongo_connect(host)
    c = qd.mongo_get_trade_cal("cn_stock")
    df = pd.DataFrame(c)

    logging.info(df)

    c = qd.mongo_get_trade_cal(
        "cn_stock",
        start_date=datetime.datetime(2024, 8, 19, 23),
        end_date=datetime.datetime(2024, 9, 19),
    )
    df = pd.DataFrame(c)
    logging.info(df)
    assert df["_id"].iloc[0] == datetime.datetime(2024, 8, 20)
    assert df["_id"].iloc[-1] == datetime.datetime(2024, 9, 19)

    c = qd.mongo_get_trade_cal("cn_stock", start_date=datetime.datetime(2024, 8, 18))
    df = pd.DataFrame(c)
    logging.info(df)
    assert df["_id"].iloc[0] == datetime.datetime(2024, 8, 19)

    c = qd.mongo_get_trade_cal("cn_stock", end_date=datetime.datetime(2024, 9, 17))
    df = pd.DataFrame(c)
    logging.info(df)
    assert df["_id"].iloc[-1] == datetime.datetime(2024, 9, 13)

    last_dt = qd.mongo_get_last_trade_dt(
        "cn_stock", datetime.datetime(2023, 4, 10, 23, 0, 0)
    )
    logging.info(last_dt)

    next_dt = qd.mongo_get_next_trade_dt(
        "cn_stock", datetime.datetime(2023, 4, 8, 0, 0, 0)
    )
    logging.info(next_dt)

    assert last_dt == next_dt
