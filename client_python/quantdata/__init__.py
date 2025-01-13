import contextlib
from datetime import datetime

import yaml

from .databases._duckdb import *
from .databases._mongodb import *


def init_db(config: dict = None, stype: str = None):
    if config is None:
        with open("quantdata_config.yml", "r") as f:
            config = yaml.safe_load(f)
    if stype:
        srv = config["services"][stype]
        cfg = {}
        for db_name, db_cfg_name in srv.items():
            cfg[db_name] = config[db_cfg_name]
        init_db(cfg)
        return
    # 初始化数据库连接，全局共享
    if "duckdb" in config:
        duckdb_connect_attach(**config["duckdb"])
    if "mongodb" in config:
        mongo_connect(**config["mongodb"])


@contextlib.contextmanager
def open_duckdb(**config):
    if conn_duckdb is None:
        duckdb_connect_attach(**config)
        try:
            yield conn_duckdb
        finally:
            duckdb_close()
    else:
        yield conn_duckdb


@contextlib.contextmanager
def open_mongodb(**config):
    if conn_mongodb is None:
        mongo_connect(**config)
        try:
            yield conn_mongodb
        finally:
            mongo_close()
    else:
        yield conn_mongodb


def _get_data(
    db_name,
    tablename,
    fields,
    start_dt: datetime,
    till_dt: datetime,
    side: str,
):
    if side is None:
        gt = ">"
        lt = "<"
    elif side == "both":
        gt = ">="
        lt = "<="
    elif side == "right":
        gt = ">"
        lt = "<="
    elif side == "left":
        gt = ">="
        lt = "<"
    else:
        raise ValueError("side has only 'both'/'right'/'left'/None options")

    if start_dt is not None:
        start_dt = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    if till_dt is not None:
        till_dt = till_dt.strftime("%Y-%m-%d %H:%M:%S")

    if start_dt is not None and till_dt is not None:
        filter = f"dt{gt}'{start_dt}' and dt{lt}'{till_dt}'"
    elif start_dt is not None:
        filter = f"dt{gt}'{start_dt}'"
    elif till_dt is not None:
        filter = f"dt{lt}'{till_dt}'"
    else:
        filter = None

    return duckdb_get_array(
        conn_duckdb, db_name, tablename, attrs=fields, filter=filter
    )


def get_data_last_row(
    db_name, tablename, fields, till_dt: datetime, side: str = "right", N: int = 1
):
    if side is None:
        lt = "<"
    elif side == "right":
        lt = "<="
    else:
        raise ValueError("side has only 'right'/None options")
    if till_dt is not None:
        till_dt = till_dt.strftime("%Y-%m-%d %H:%M:%S")
        filter = f"dt{lt}'{till_dt}'"
    else:
        filter = None
    return duckdb_get_array_last_rows(
        conn_duckdb, db_name, tablename, attrs=fields, filter=filter, N=N
    )


def get_data_recarray_onlyfortest(
    db_name,
    tablename,
    fields: list = None,
    start_dt: datetime = None,
    till_dt: datetime = None,
    side="both",
):
    """少量数据效率高"""
    return duckdb_fetchall_to_recarray(
        _get_data(db_name, tablename, fields, start_dt, till_dt, side)
    )


def get_data_recarray_s(
    db_name,
    tablename,
    fields: list = None,
    start_dt: datetime = None,
    till_dt: datetime = None,
    side="both",
):
    """少量数据效率高"""
    return duckdb_fetchnumpy_to_recarray(
        _get_data(db_name, tablename, fields, start_dt, till_dt, side)
    )


def get_data_recarray_l(
    db_name,
    tablename,
    fields: list = None,
    start_dt: datetime = None,
    till_dt: datetime = None,
    side="both",
):
    """大量数据效率高"""
    return duckdb_fetchdf_to_recarray(
        _get_data(db_name, tablename, fields, start_dt, till_dt, side)
    )


def get_data_ndarray(
    db_name,
    tablename,
    fields: list = None,
    start_dt: datetime = None,
    till_dt: datetime = None,
    side="both",
):
    return _get_data(db_name, tablename, fields, start_dt, till_dt, side).fetchnumpy()


def get_data_df(
    db_name,
    tablename,
    fields: list = None,
    start_dt: datetime = None,
    till_dt: datetime = None,
    side="both",
):
    return _get_data(db_name, tablename, fields, start_dt, till_dt, side).fetchdf()


def get_trade_cal(db_name, **kwargs):
    return mongo_get_trade_cal(db_name, **kwargs)


def get_last_trade_dt(db_name, current_dt):
    return mongo_get_last_trade_dt(db_name, current_dt)
