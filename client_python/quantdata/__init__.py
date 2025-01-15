import contextlib

import yaml

from .databases._duckdb import *
from .databases._duckdb_api import *
from .databases._duckdb_api2 import *
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
