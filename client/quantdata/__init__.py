import atexit
import pathlib
from collections import defaultdict
from datetime import datetime
from functools import singledispatch

import numpy as np
import yaml

try:
    import tiledb
    from tiledb.multirange_indexing import LabelIndexer, MultiRangeIndexer
except ImportError:
    pass

try:
    import duckdb
except ImportError:
    pass

from pymongo import MongoClient
from pymongo.database import Database


def mongo_connect(host, port=27017, user="root", password="admin", **kwargs):
    authSource = kwargs.pop("authSource", "admin")
    authMechanism = kwargs.pop("authMechanism", "SCRAM-SHA-1")
    return MongoClient(
        host=host,
        port=port,
        username=user,
        password=password,
        authSource=authSource,
        authMechanism=authMechanism,
        **kwargs,
    )


def mongo_close(client):
    client.close()


@singledispatch
def mongo_get_data(
    db: Database,
    collection_name: str,
    query: dict = None,
    sort_by: dict = None,
    max_count: int = None,
    **find_kwargs,
):
    """
    如果要返回DataFrame，可以用`df = pd.DataFrame(returned cursor)`
    """
    if not collection_name:
        return None
    if query is None:
        docs = db[collection_name].find(**find_kwargs)
    else:
        docs = db[collection_name].find(query, **find_kwargs)
    if sort_by:
        docs = docs.sort(sort_by)
    if max_count:
        docs = docs.limit(max_count)
    return docs


def mongo_fetchnumpy(cursor):
    ret = defaultdict(list)
    for doc in cursor:
        for col, value in doc.items():
            ret[col].append(value)
    for col, lst in ret.items():
        ret[col] = np.array(lst)
    return ret


def mongo_get_trade_cal(
    db: Database,
    collection_name="trade_cal",
    start_date: datetime = None,
    end_date: datetime = None,
):
    """返回从start_date到end_date(包括本身)的交易日期"""
    if start_date and end_date:
        return mongo_get_data(
            db,
            collection_name,
            {
                "$and": [
                    {"status": 1},
                    {"_id": {"$gte": start_date}},
                    {"_id": {"$lte": end_date}},
                ]
            },
        )
    elif start_date:
        return mongo_get_data(
            db,
            collection_name,
            {"$and": [{"status": 1}, {"_id": {"$gte": start_date}}]},
        )
    elif end_date:
        return mongo_get_data(
            db, collection_name, {"$and": [{"status": 1}, {"_id": {"$lte": end_date}}]}
        )
    else:
        return mongo_get_data(db, collection_name, {"status": 1})


def mongo_get_last_trade_dt(db: Database, dt: datetime):
    """从`dt`开始的上一个交易日"""
    result = (
        db["trade_cal"]
        .find({"$and": [{"status": 1}, {"_id": {"$lte": dt}}]})
        .sort([("_id", -1)])
        .limit(1)
    )
    try:
        return result.next()["_id"]
    except StopIteration:
        return None


def mongo_get_next_trade_dt(db: Database, dt: datetime):
    """从`dt`开始的下一个交易日"""
    result = (
        db["trade_cal"].find({"$and": [{"status": 1}, {"_id": {"$gt": dt}}]}).limit(1)
    )
    try:
        return result.next()["_id"]
    except StopIteration:
        return None


_tiledb_default_ctx = None


def _tiledb_default_context():
    global _tiledb_default_ctx
    if _tiledb_default_ctx is None:
        config = tiledb.Config()
        # Set configuration parameters
        config["sm.check_coord_dups"] = "false"
        # turn off Coordinate Out-of-bounds Check
        config["sm.check_coord_oob"] = False
        config["sm.tile_cache_size"] = 10000000

        # Create contex object
        _tiledb_default_ctx = tiledb.Ctx(config)

    return _tiledb_default_ctx


def tiledb_has_attr(uri: str, attr: str):
    schema = tiledb.ArraySchema.load(uri)
    return schema.has_attr(attr)


def tiledb_get_attrs(uri: str) -> list:
    schema = tiledb.ArraySchema.load(uri)
    return [schema.attr(i) for i in range(schema.nattr)]


def tiledb_get_symbols(uri: str) -> list:
    schema = tiledb.ArraySchema.load(uri)
    dim_symbol = schema.dim_label("symbol")
    with tiledb.open(dim_symbol.uri, "r") as symbol_array:
        ned = symbol_array.nonempty_domain()[0]
        return symbol_array[ned[0] : ned[1] + 1][dim_symbol.label_attr_name]


def tiledb_get_datetimes(uri: str) -> list:
    schema = tiledb.ArraySchema.load(uri)
    dim_dt = schema.dim_label("dt")
    with tiledb.open(dim_dt.uri, "r") as dt_arr:
        ned = dt_arr.nonempty_domain()[0]
        return dt_arr[ned[0] : ned[1] + 1][dim_dt.label_attr_name]


def tiledb_connect(uri, ctx=None):
    """
    打开一个文件链接

    ctx 用于参数设置，None使用默认参数
    """
    return tiledb.open(uri, mode="r", ctx=ctx or _tiledb_default_context())


def tiledb_close(A):
    A.close()


def tiledb_get_array_shape(A):
    """默认开头没有空"""
    return tuple(b + 1 for _, b in A.nonempty_domain())


def tiledb_get_array(A, query: dict = None, indexer: dict = None):
    """
    从tiledb返回多维数组，每次打开文件后关闭

    indexer:
        左右两边的边界都包括在内。
        - 切片表示 tuple or slice()，如果开头结尾使用None，表示最后不为空的位置，如果有None必须用slice()否则报错（这是tiledb的BUG）
        - list表示multi range, list中可以嵌套单点和切片
        - 剩下表示单点查询

    注意：返回的array中可能有空cell
    """
    q = None
    if query is not None:
        q = A.query(**query)
    if indexer is not None:
        if len(indexer) > 1:
            return LabelIndexer(A, indexer[0], query=q)[indexer[1]]
        else:
            return MultiRangeIndexer(A, query=q)[indexer[0]]
    elif q is not None:
        return q.multi_index[None:None, None:None]
    else:
        return A.multi_index[None:None, None:None]


def duckdb_connect(
    extensions: tuple = None,
    memory_limit: str = None,
    threads_limit: int = None,
    shared_memory: bool = False,
):
    """
    params:
        uri: path/to/${file}.db
        extensions: like ("httpfs",)
        memory_limit: like "10GB"
        threads_limit: The number of total threads used by the system
        shared_memory: if not None, connect to a shared memory database. default not shared. But file database cannot be shared.
        see more configuration: https://duckdb.org/docs/configuration/overview.html#configuration-reference
        see abort connections: https://duckdb.org/docs/api/python/dbapi.html
    """
    config = {}  # only support global option, local not allowed
    if memory_limit:
        config["memory_limit"] = memory_limit
    if threads_limit:
        config["threads"] = threads_limit
    if shared_memory:
        database = ":memory:lzq01"
    else:
        database = ":memory:"
    # 内存的任何修改在close之后都将丢失
    conn = duckdb.connect(database=database, config=config)
    # local options set here
    conn.execute("SET enable_progress_bar = false;")
    conn.execute("SET enable_progress_bar_print = false;")
    if extensions:
        for ext in extensions:
            conn.load_extension(ext)
    return conn


def duckdb_attach(conn, uri: str):
    """
    共享文件夹的db文件只能通过attach加载，不能通过connect的方式加载
    """
    db_name = pathlib.Path(uri).name.replace(".db", "")
    try:
        conn.execute(f"ATTACH IF NOT EXISTS '{uri}' as {db_name} (READ_ONLY);")
    except:
        conn.close()
        raise


def duckdb_close(conn):
    conn.close()


def duckdb_get_array(
    conn, db_name: str, tablename: str, attrs: list = None, filter: str = None
):
    names = ",".join(attrs) if attrs else "*"
    q = conn.sql(f"select {names} from {db_name}.{tablename}")
    if filter:
        q = q.filter(filter)
    return q


def duckdb_get_array_last_rows(
    conn,
    db_name: str,
    tablename: str,
    attrs: list = None,
    filter: str = None,
    N: int = 1,
):
    q = conn.sql(f"SELECT count(*) from {db_name}.{tablename}")
    if filter:
        q = q.filter(filter)
    count = q.fetchone()[0]
    names = ",".join(attrs) if attrs else "*"
    return conn.sql(
        f"SELECT {names} FROM {db_name}.{tablename} LIMIT {N} OFFSET {count-N};"
    )
    # 返回全部行只要8ms的情况下，常规做法读取最后一行，竟然要16ms
    # if N == 1:
    #     if not attrs:
    #         return conn.sql(f"select last(COLUMNS(*)) from {db_name}.{tablename}")
    #     else:
    #         field_name = "last({a}) as {a}"
    #         names = ",".join(field_name.format(a=attr) for attr in attrs)
    #         return conn.sql(f"select {names} from {db_name}.{tablename}")
    # else:
    #     names = ",".join(attrs) if attrs else "*"
    #     return conn.sql(
    #         f"SELECT * FROM (select {names} from {db_name}.{tablename} ORDER BY dt DESC LIMIT {N}) ORDER BY dt ASC;"
    #     )


def init_db(config: dict = None):
    if config is None:
        with open("quantdata_config.yml", "r") as f:
            config = yaml.safe_load(f)
    # 初始化数据库连接，全局共享
    if "duckdb" in config:
        _init_duckdb(config["duckdb"])
    if "mongodb" in config:
        _init_mongodb(config["mongodb"])


conn_mongodb: MongoClient = None
conn_duckdb = None


def _init_duckdb(config):
    global conn_duckdb
    uris = config.pop("uri")
    if conn_duckdb is None:
        conn_duckdb = duckdb_connect(**config)
    for uri in uris:
        print(f"duckdb attach {uri}...")
        duckdb_attach(conn_duckdb, uri)


def _close_duckdb():
    global conn_duckdb
    if conn_duckdb is None:
        return
    try:
        print(f"duckdb closing...")
        duckdb_close(conn_duckdb)
        print(f"duckdb closed")
    except:
        pass
    conn_duckdb = None


def _init_mongodb(config):
    global conn_mongodb
    if conn_mongodb is not None:
        return
    print("connecting mongodb...")
    conn_mongodb = mongo_connect(**config)


def _close_mongodb():
    global conn_mongodb
    if conn_mongodb is None:
        return
    try:
        print(f"mongodb closing...")
        mongo_close(conn_mongodb)
        print(f"mongodb closed")
    except:
        pass
    conn_mongodb = None


@atexit.register
def close_db():
    _close_mongodb()
    _close_duckdb()


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

    q = duckdb_get_array(
        conn_duckdb,
        db_name,
        tablename,
        attrs=fields,
        filter=filter,
    )
    return q


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


def get_data_recarray(
    db_name,
    tablename,
    fields,
    start_dt: datetime = None,
    till_dt: datetime = None,
    side="both",
):
    """运行效率不高，建议使用 get_data_ndarray"""
    q = _get_data(db_name, tablename, fields, start_dt, till_dt, side)
    return q.fetchdf().to_records(index=False)


def get_data_ndarray(
    db_name,
    tablename,
    fields,
    start_dt: datetime = None,
    till_dt: datetime = None,
    side="both",
):
    q = _get_data(db_name, tablename, fields, start_dt, till_dt, side)
    return q.fetchnumpy()


def get_data_df(
    db_name,
    tablename,
    fields,
    start_dt: datetime = None,
    till_dt: datetime = None,
    side="both",
):
    q = _get_data(db_name, tablename, fields, start_dt, till_dt, side)
    return q.fetchdf()


def get_trade_cal(db_name, **kwargs):
    return mongo_get_trade_cal(conn_mongodb[db_name], **kwargs)


def get_last_trade_dt(db_name, current_dt):
    return mongo_get_last_trade_dt(conn_mongodb[db_name], current_dt)


@mongo_get_data.register
def _(
    db_name: str,
    collection_name: str,
    query: dict = None,
    sort_by: dict = None,
    max_count: int = None,
    **find_kwargs,
):
    return mongo_get_data(
        conn_mongodb[db_name],
        collection_name,
        query=query,
        sort_by=sort_by,
        max_count=max_count,
        **find_kwargs,
    )
