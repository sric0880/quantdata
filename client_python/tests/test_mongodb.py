import logging

import pandas as pd

import quantdata.databases._mongodb as qd

host = "localhost"


def test_get_data():
    qd.mongo_connect(host)
    c = qd.mongo_get_data("cn_stock", "stocks_basic_info", projection={"_id": False})
    df = pd.DataFrame(c)

    logging.info(df)
