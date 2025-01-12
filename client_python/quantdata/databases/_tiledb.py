import tiledb
from tiledb.multirange_indexing import LabelIndexer, MultiRangeIndexer

_tiledb_default_ctx = None


def tiledb_connect(uri, ctx=None):
    """
    打开一个文件链接

    ctx 用于参数设置，None使用默认参数
    """
    return tiledb.open(uri, mode="r", ctx=ctx or _tiledb_default_context())


def tiledb_close(A):
    A.close()


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
