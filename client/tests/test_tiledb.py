import datetime
import logging
from collections import OrderedDict
import numpy as np

import quantdata as qd

tiledb_bucket = "~/WorkingDoc/tiledb/finance-tiledb"
cn_stock = 'cn_stock'
cn_stock_group_name = f"{tiledb_bucket}/{cn_stock}"
array_bars_stock_daily = f"{cn_stock_group_name}/bars/daily"


def test_has_attr():
    assert qd.tiledb_has_attr(array_bars_stock_daily, 'name')


def test_get_attrs():
    logging.info(qd.tiledb_get_attrs(array_bars_stock_daily))


def test_get_symbols():
    logging.info(qd.tiledb_get_symbols(array_bars_stock_daily))


def test_get_datetimes():
    logging.info(qd.tiledb_get_datetimes(array_bars_stock_daily))


def test_get_array001():
    with qd.tiledb_open_array(array_bars_stock_daily) as A:
        np.testing.assert_equal(
            qd.tiledb_get_array(A, query={'attrs':['name']}, indexer=(('symbol', 'dt'), ("000001.SZ", slice(np.datetime64('2024-04-25', 'D'),np.datetime64('2024-04-29', 'D'))))),
            OrderedDict([
                ('dt', np.array(['2024-04-25T08:00:00', '2024-04-26T08:00:00'], dtype='datetime64[s]')), 
                ('symbol', [b'000001.SZ']),
                ('name', [['平安银行', '平安银行']])
            ])
        )


def test_get_array002():
    logging.info("-----------test_get_array002-----------")
    # None 会转换成int, 不能用于 Label indexer
    with qd.tiledb_open_array(array_bars_stock_daily) as A:
        a = qd.tiledb_get_array(A, query={'attrs':['name']}, indexer=(('symbol',), ("000001.SZ", slice(None, None))))
    logging.info(a)
    logging.info("")


def test_get_array003():
    logging.info("-----------test_get_array003-----------")
    # None 会转换成int, 不能用于 Label indexer
    with qd.tiledb_open_array(array_bars_stock_daily) as A:
        a = qd.tiledb_get_array(A, query={'attrs':['name']}, indexer=(('dt',), (slice(None, None), slice(np.datetime64('2024-04-25', 'D'), np.datetime64('2024-04-26', 'D')))))
        logging.info(a)
    logging.info("")


def test_get_array004():
    logging.info("-----------test_get_array004-----------")
    with qd.tiledb_open_array(array_bars_stock_daily) as A:
        a = qd.tiledb_get_array(A, query={'attrs':['name']}, indexer=(('symbol', 'dt'), ("000001.SZ", [(np.datetime64('2024-04-24', 'D'), np.datetime64('2024-04-25', 'D')), np.datetime64('2024-04-25T08:00:00', 's'), np.datetime64('2024-08-15T08:00:00', 's')]))),
        logging.info(a)
    logging.info("")


def test_get_array005():
    logging.info("-----------test_get_array005-----------")
    with qd.tiledb_open_array(array_bars_stock_daily) as A:
        _, dt_len = qd.tiledb_get_array_shape(A)
        a = qd.tiledb_get_array(A, query={'attrs':['name']}, indexer=((slice(None, None), slice(dt_len-1, None)),)),
        logging.info(a)
    logging.info("")


def test_get_array_all():
    logging.info("-----------test_get_array_all-----------")
    with qd.tiledb_open_array(array_bars_stock_daily) as A:
        a = qd.tiledb_get_array(A, query={'attrs':['name']})
        logging.info(a)

        a = qd.tiledb_get_array(A)
        logging.info(a)
    logging.info("")

    
#     logging.info(qd.tiledb_get_array(array_bars_stock_daily))