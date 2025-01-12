import atexit
import pathlib

import duckdb
import numpy as np

conn_duckdb = None


def duckdb_connect_attach(
    extensions: tuple = None,
    memory_limit: str = None,
    threads_limit: int = None,
    shared_memory: bool = False,
    uris=[],
):
    """
    共享文件夹的db文件只能通过attach加载，不能通过connect的方式加载

    Params:
        see more configuration: https://duckdb.org/docs/configuration/overview.html#configuration-reference
        see about connections: https://duckdb.org/docs/api/python/dbapi.html
        - extensions: like ("httpfs",)
        - memory_limit: like "10GB"
        - threads_limit: The number of total threads used by the system
        - shared_memory: if True, connect to a shared memory database. default not shared. But file database cannot be shared.
        - uris: path/to/${file}.db
    """
    global conn_duckdb
    if conn_duckdb is None:
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
        conn_duckdb = duckdb.connect(database=database, config=config)
        # local options set here
        conn_duckdb.execute("SET enable_progress_bar = false;")
        conn_duckdb.execute("SET enable_progress_bar_print = false;")
        if extensions:
            for ext in extensions:
                conn_duckdb.load_extension(ext)

    for uri in uris:
        print(f"duckdb attach {uri}...")
        db_name = pathlib.Path(uri).name.replace(".db", "")
        try:
            conn_duckdb.execute(
                f"ATTACH IF NOT EXISTS '{uri}' as {db_name} (READ_ONLY);"
            )
        except:
            conn_duckdb.close()
            raise


@atexit.register
def duckdb_close():
    global conn_duckdb
    if conn_duckdb is None:
        return
    try:
        print(f"duckdb closing...")
        conn_duckdb.close()
        print(f"duckdb closed")
    except:
        pass
    conn_duckdb = None


def duckdb_get_array(
    conn, db_name: str, tablename: str, attrs: list = None, filter: str = None
):
    names = ",".join(attrs) if attrs else "*"
    filter = f"where {filter}" if filter else ""
    q = conn.sql(f"select {names} from {db_name}.{tablename} {filter}")
    # 经测试，下面这种方式效率低
    # if filter:
    #     q = q.filter(filter)
    return q


def duckdb_get_array_last_rows(
    conn,
    db_name: str,
    tablename: str,
    attrs: list = None,
    filter: str = None,
    N: int = 1,
):
    names = ",".join(attrs) if attrs else "*"
    filter = f"where {filter}" if filter else ""
    return conn.sql(
        f"SELECT * FROM (select {names} FROM {db_name}.{tablename} {filter} ORDER BY dt DESC LIMIT {N}) ORDER BY dt ASC;"
    )


duckdbpytype_to_nptype = {
    "BIGINT": np.int64,
    # "BIT": ,
    # "BLOB": ,  # bytearray or bytes
    "BOOLEAN": "?",
    "DATE": "datetime64[D]",
    "DOUBLE": np.float64,
    "FLOAT": np.float32,
    # "HUGEINT": np.int128,
    # "UHUGEINT": np.uint128,
    "INTEGER": np.int32,
    # "INTERVAL":
    "SMALLINT": np.int16,
    # "SQLNULL": ,
    # "TIME": ,
    "TIMESTAMP": "datetime64[us]",  # microsecond
    "TIMESTAMP_MS": "datetime64[ms]",  # millisecond
    "TIMESTAMP_NS": "datetime64[ns]",
    "TIMESTAMP_S": "datetime64[s]",
    # "TIMESTAMP_TZ":
    # "TIME_TZ":
    "TINYINT": np.int8,
    "UBIGINT": np.uint64,
    "UINTEGER": np.uint32,
    "USMALLINT": np.uint16,
    "UTINYINT": np.uint8,
    # "UUID":
    "VARCHAR": "U",
}


def duckdb_fetchall_to_recarray(q):
    """效率低"""
    dtypes = [
        (field, duckdbpytype_to_nptype[str(dtype)])
        for field, dtype in zip(q.columns, q.dtypes)
    ]
    return np.array(q.fetchall(), dtype=dtypes).view(np.recarray)


def duckdb_fetchnumpy_to_recarray(q):
    """少量数据效率高"""
    datas = q.fetchnumpy()
    dtypes = [(col, arr.dtype) for col, arr in datas.items()]
    return np.array(list(zip(*datas.values())), dtype=dtypes).view(np.recarray)


def duckdb_fetchdf_to_recarray(q):
    """大量数据效率高"""
    return q.fetchdf().to_records(index=False)
