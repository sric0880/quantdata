import datetime

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

__all__ = [
    "mongo_connect",
    "mongo_close",
    "mongo_get_data",
    "mongo_get_last_trade_dt",
    "mongo_get_next_trade_dt",
    "tiledb_has_attr",
    "tiledb_get_attrs",
    "tiledb_get_symbols",
    "tiledb_get_datetimes",
    "tiledb_connect",
    "tiledb_close",
    "tiledb_get_array_shape",
    "tiledb_get_array",
    "duckdb_connect",
    "duckdb_close",
    "duckdb_get_array",
    "duckdb_get_array_last_rows",
]


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


def mongo_get_data(
    db: Database,
    collection_name,
    query: dict = None,
    sort_by: dict = None,
    max_count=None,
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


def mongo_get_trade_cal(
    db: Database,
    collection_name="trade_cal",
    start_date: datetime.datetime = None,
    end_date: datetime.datetime = None,
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


def mongo_get_last_trade_dt(db: Database, dt: datetime.datetime):
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


def mongo_get_next_trade_dt(db: Database, dt: datetime.datetime):
    """从`dt`开始的下一个交易日"""
    result = (
        db["trade_cal"].find({"$and": [{"status": 1}, {"_id": {"$gt": dt}}]}).limit(1)
    )
    try:
        return result.next()["_id"]
    except StopIteration:
        return None


_default_ctx = None


def _tiledb_init_default_context():
    global _default_ctx
    config = tiledb.Config()
    # Set configuration parameters
    config["sm.check_coord_dups"] = "false"
    # turn off Coordinate Out-of-bounds Check
    config["sm.check_coord_oob"] = False
    config["sm.tile_cache_size"] = 10000000

    # Create contex object
    _default_ctx = tiledb.Ctx(config)


_tiledb_init_default_context()


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
    return tiledb.open(uri, mode="r", ctx=ctx or _default_ctx)


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


def duckdb_connect(uri):
    conn = duckdb.connect()
    try:
        conn.execute(f"ATTACH '{uri}' as A (READ_ONLY)")
        return conn
    except:
        conn.close()
        raise


def duckdb_close(conn):
    try:
        conn.execute("DETACH A")
    finally:
        conn.close()


def duckdb_get_array(conn, tablename, attrs: list = None, filter: str = None):
    names = ",".join(attrs) if attrs else "*"
    q = conn.sql(f"select {names} from A.{tablename}")
    if filter:
        q = q.filter(filter)
    return q


def duckdb_get_array_last_rows(conn, tablename: str, attrs: list = None, N: int = 1):
    count = conn.sql(f'SELECT count(*) from A.{tablename}').fetchone()[0]
    names = ",".join(attrs) if attrs else "*"
    return conn.sql(
        f"SELECT {names} FROM A.{tablename} LIMIT {N} OFFSET {count-N};"
    )
    # 返回全部行只要8ms的情况下，常规做法读取最后一行，竟然要16ms
    # if N == 1:
    #     if not attrs:
    #         return conn.sql(f"select last(COLUMNS(*)) from A.{tablename}")
    #     else:
    #         field_name = "last({a}) as {a}"
    #         names = ",".join(field_name.format(a=attr) for attr in attrs)
    #         return conn.sql(f"select {names} from A.{tablename}")
    # else:
    #     names = ",".join(attrs) if attrs else "*"
    #     return conn.sql(
    #         f"SELECT * FROM (select {names} from A.{tablename} ORDER BY dt DESC LIMIT {N}) ORDER BY dt ASC;"
    #     )
