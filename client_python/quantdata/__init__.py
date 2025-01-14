import contextlib

import yaml

from .databases._duckdb import *
from .databases._mongodb import *


def init_db(config: dict = None, stype: str = None):
    """
    Params:
        - stype: service type
    """
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
def open_dbs(config: dict = None, stype: str = None):
    assert conn_duckdb is None
    assert conn_mongodb is None
    init_db(config, stype)
    try:
        yield
    finally:
        duckdb_close()
        mongo_close()


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
    db_name: str,
    tablename: str,
    fields: list[str],
    start_microsec: int,
    till_microsec: int,
    side: str,
):
    if start_microsec or till_microsec:
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

        if start_microsec and till_microsec:
            filter = f"dt{gt}make_timestamp({start_microsec}) and dt{lt}make_timestamp({till_microsec})"
        elif start_microsec:
            filter = f"dt{gt}make_timestamp({start_microsec})"
        else:
            filter = f"dt{lt}make_timestamp({till_microsec})"
    else:
        filter = None

    return duckdb_get_array(db_name, tablename, attrs=fields, filter=filter)


def get_data_last_row(
    db_name: str,
    tablename: str,
    fields: list[str],
    till_microsec: int,
    side: str = "right",
    N: int = 1,
):
    if side is None:
        lt = "<"
    elif side == "right":
        lt = "<="
    else:
        raise ValueError("side has only 'right'/None options")
    if till_microsec:
        filter = f"dt{lt}make_timestamp({till_microsec})"
    else:
        filter = None
    return duckdb_get_array_last_rows(
        conn_duckdb, db_name, tablename, attrs=fields, filter=filter, N=N
    )


def get_data_recarray_onlyfortest(
    db_name: str,
    tablename: str,
    fields: list[str] = None,
    start_microsec: int = 0,
    till_microsec: int = 0,
    side="both",
):
    """少量数据效率高"""
    return duckdb_fetchall_to_recarray(
        _get_data(db_name, tablename, fields, start_microsec, till_microsec, side)
    )


def get_data_recarray_s(
    db_name: str,
    tablename: str,
    fields: list[str] = None,
    start_microsec: int = 0,
    till_microsec: int = 0,
    side="both",
):
    """少量数据效率高"""
    return duckdb_fetchnumpy_to_recarray(
        _get_data(db_name, tablename, fields, start_microsec, till_microsec, side)
    )


def get_data_recarray_l(
    db_name: str,
    tablename: str,
    fields: list[str] = None,
    start_microsec: int = 0,
    till_microsec: int = 0,
    side="both",
):
    """大量数据效率高"""
    return duckdb_fetchdf_to_recarray(
        _get_data(db_name, tablename, fields, start_microsec, till_microsec, side)
    )


def get_data_ndarray(
    db_name: str,
    tablename: str,
    fields: list[str] = None,
    start_microsec: int = 0,
    till_microsec: int = 0,
    side="both",
):
    return _get_data(
        db_name, tablename, fields, start_microsec, till_microsec, side
    ).fetchnumpy()


def get_data_df(
    db_name: str,
    tablename: str,
    fields: list[str] = None,
    start_microsec: int = 0,
    till_microsec: int = 0,
    side="both",
):
    return _get_data(
        db_name, tablename, fields, start_microsec, till_microsec, side
    ).fetchdf()


def get_trade_cal(db_name: str, **kwargs):
    return mongo_get_trade_cal(db_name, **kwargs)


def get_last_trade_dt(db_name: str, current_dt):
    return mongo_get_last_trade_dt(db_name, current_dt)
