import atexit
import logging

import numpy as np
import pandas as pd
import taos
from taos import TaosResult
from taos.cinterface import *
from taos.error import *

conn_tdengine: taos.TaosConnection = None


def get_conn_tdengine():
    return conn_tdengine


def tdengine_connect(
    host,
    port=6030,
    user="root",
    password="taosdata",
    dbname="finance",
    timezone="UTC",
):
    global conn_tdengine
    if conn_tdengine is not None:
        return
    print("connecting tdengine...")
    conn_tdengine = taos.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=dbname,
        timezone=timezone,
    )


@atexit.register
def tdengine_close():
    global conn_tdengine
    if conn_tdengine is None:
        return
    try:
        print(f"tdengine closing...")
        conn_tdengine.close()
        print(f"tdengine closed")
    except:
        pass
    conn_tdengine = None


def _get_tbname(tablename, stable=None):
    if stable is not None:
        return f"{stable}_{tablename.replace('.', '_')}"
    else:
        return tablename.replace(".", "_")


field_type_strings = {
    # 0: NULL,
    1: "?",  # Bool
    2: np.int8,
    3: np.int16,
    4: np.int32,
    5: np.int64,
    6: np.float32,
    7: np.float64,
    8: "U",
    9: "datetime64[ms]",
    10: "U",
    11: np.uint8,
    12: np.uint16,
    13: np.uint32,
    14: np.uint64,
    # 15: Json,
    16: "U",
    # 17: GEOMETRY
}


class NewTaosResult:
    def __init__(self, taos_result: TaosResult) -> None:
        self._r = taos_result

    def fetch_array_dict(self):
        if self._r._result is None:
            raise OperationalError("Invalid use of fetchall")

        if self._r._fields is None:
            self._r._fields = taos_fetch_fields(self._r._result)
        buffer = [[] for i in range(len(self._r._fields))]
        self._r._row_count = 0
        while True:
            block, num_of_fields = taos_fetch_block(self._r._result, self._r._fields)
            errno = taos_errno(self._r._result)
            if errno != 0:
                raise ProgrammingError(taos_errstr(self._r._result), errno)
            if num_of_fields == 0:
                break
            self._r._row_count += num_of_fields
            for i in range(len(self._r._fields)):
                buffer[i].extend(block[i])
        return dict(zip([field.name for field in self._r.fields], buffer))

    def fetch_df(self):
        data = self._r.fetch_all()
        return pd.DataFrame(data, columns=[field.name for field in self._r.fields])


def _cast_field_to_nptype(field):
    nptype = field_type_strings[field.type]
    if nptype == "U":
        nptype += str(field.bytes)
    return nptype


def _get_df_results(sql, return_type):
    """ """
    result: taos.TaosResult = conn_tdengine.query(sql)
    if return_type == 2:
        return result.fetch_all_into_dict()
    elif return_type == 3:
        names = [field.name for field in result.fields]
        return names, result.fetch_all()
    elif return_type == 4:
        dtype = [(field.name, _cast_field_to_nptype(field)) for field in result.fields]
        return np.array(result.fetch_all(), dtype=dtype).view(np.recarray)
    else:
        new_result = NewTaosResult(result)
        if return_type == 0:
            return new_result.fetch_df()
        elif return_type == 1:
            return new_result.fetch_array_dict()
    return None


def td_get_count_of_rows(tbname):
    counts = _get_df_results(f"SELECT count(dt) FROM {tbname}", 1)
    return counts["count(dt)"][0]


def td_get_data(
    tablename,
    stable=None,
    fields: list = None,
    till_dt=None,
    start_dt=None,
    max_count=None,
    use_df=True,
    side="both",
    adjust_df=None,
    adjust_func="multiply",  # or 'minus'
    return_type=1,
):
    """
    参数：
    return_type (int): 返回数据的格式
        0: Dataframe
        1: dict of arrays. eg. {"dt": [], "open": [], ...}
        2: array of dicts. eg. [{"dt": , "open": 0.1}, {"dt": , "open": 0.2}, ...]
        3: array of tuples with field names. eg. (["open", "high", "volume"], [(0.1, 0.2, 1), (0.11, 0.12, 2), ...])
        4: np.recarray, 时间不含时区（UTC时间）
    """
    if not tablename:
        return None
    if use_df:
        return_type = 0
    if return_type > 0:
        use_df = False
    tablename = _get_tbname(tablename, stable=stable)
    _fields = "*"
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
        return None
    if fields is not None:
        _fields = ",".join(fields)
    if till_dt is not None and start_dt is not None:
        _till_dt = till_dt.isoformat() if not isinstance(till_dt, str) else till_dt
        _start_dt = start_dt.isoformat() if not isinstance(start_dt, str) else start_dt
        _sql = f"SELECT {_fields} FROM {tablename} WHERE dt {gt} '{_start_dt}' AND dt {lt} '{_till_dt}'"
    elif start_dt is not None:
        _start_dt = start_dt.isoformat() if not isinstance(start_dt, str) else start_dt
        _sql = f"SELECT {_fields} FROM {tablename} WHERE dt {gt} '{_start_dt}'"
    elif till_dt is not None:
        _till_dt = till_dt.isoformat() if not isinstance(till_dt, str) else till_dt
        _sql = f"SELECT {_fields} FROM {tablename} WHERE dt {lt} '{_till_dt}'"
    else:
        _sql = f"SELECT {_fields} FROM {tablename}"
    if max_count:
        # ORDER BY dt DESC LIMIT {max_count} 方式查询有BUG：倒数数据有NULL值，正数没有
        if till_dt is not None:  # 从最后往前倒数
            _sql_only_count = _sql.replace(_fields, "count(dt)", 1)
            rows_count = _get_df_results(_sql_only_count, 1)["count(dt)"][0]
            if rows_count > max_count:
                _sql = _sql + f" LIMIT {max_count} OFFSET {rows_count-max_count}"
        else:
            _sql = _sql + f" LIMIT {max_count}"
    ret = _get_df_results(_sql, return_type)
    if adjust_df is not None and not adjust_df.empty:  # 复权或者拼接
        if return_type == 0:
            columns = ret.columns
        elif return_type == 1:
            columns = list(ret.keys())
        elif return_type == 2:
            if not ret:
                return ret
            columns = list(ret[0].keys())
        elif return_type == 3:
            columns = ret[0]
        elif return_type == 4:
            # if isinstance(ret, np.recarray):
            columns = ret.dtype.names

        if "dt" not in columns:
            logging.error(f"数据复权需要dt字段")
            return None
        if use_df:
            indexes = ret["dt"].searchsorted(adjust_df["tradedate"], side="left")
            ret_len = len(ret)
        else:
            if return_type == 1:
                dts = ret["dt"]
            if return_type == 2:
                dts = [row["dt"] for row in ret]
            elif return_type == 3:
                dts = [row[0] for row in ret[1]]
            elif return_type == 4:
                dts = ret.dt  # dts 是不含时区的UTC时间
                adjust_df["tradedate"] = (
                    adjust_df["tradedate"].dt.tz_convert("UTC").dt.tz_localize(None)
                )
            indexes = np.searchsorted(dts, adjust_df["tradedate"], side="left")
            ret_len = len(dts)
        line = np.empty(ret_len)
        line.fill(np.nan)
        multipler = pd.Series(data=line)
        for i, adj in zip(indexes, adjust_df["adjust_factor"]):
            # print(i, adj)
            if i < ret_len:
                multipler[i] = adj
        multipler = multipler.fillna(method="ffill")
        if adjust_func == "minus":
            multipler = multipler.fillna(0)
        elif adjust_func == "multiply":
            multipler = multipler.fillna(1)

        convert_fileds = [
            field
            for field in columns
            if field
            in [
                "last_price",
                "bid_price1",
                "ask_price1",
                "open",
                "high",
                "low",
                "close",
            ]
        ]
        if return_type == 2:
            if adjust_func == "minus":
                for v, factor in zip(ret, multipler):
                    for field in convert_fileds:
                        v[field] -= factor
            elif adjust_func == "multiply":
                for v, factor in zip(ret, multipler):
                    for field in convert_fileds:
                        v[field] *= factor
        elif return_type == 3:
            fieldsmap = [1 if field in convert_fileds else 0 for field in columns]
            _ret = []
            if adjust_func == "minus":
                for v, factor in zip(ret[1], multipler):
                    _ret.append(
                        tuple(
                            vi if m == 0 else vi - factor for vi, m in zip(v, fieldsmap)
                        )
                    )
            elif adjust_func == "multiply":
                for v, factor in zip(ret[1], multipler):
                    _ret.append(
                        tuple(
                            vi if m == 0 else vi * factor for vi, m in zip(v, fieldsmap)
                        )
                    )
            ret = (ret[0], _ret)
        else:
            for field in convert_fileds:
                if return_type == 0 or return_type == 4:
                    if adjust_func == "minus":
                        ret[field] -= multipler
                    elif adjust_func == "multiply":
                        ret[field] *= multipler
                else:  # return_type == 1
                    if adjust_func == "minus":
                        ret[field] = [
                            v - factor for v, factor in zip(ret[field], multipler)
                        ]
                    elif adjust_func == "multiply":
                        ret[field] = [
                            v * factor for v, factor in zip(ret[field], multipler)
                        ]
    return ret


def td_get_data_last_row(tablename, stable=None, fields: list = None, n=1):
    tablename = _get_tbname(tablename, stable=stable)
    rows_count = td_get_count_of_rows(tablename)
    _fields = "*"
    if fields is not None:
        _fields = ", ".join(fields)
    if rows_count > n:
        _sql = f"SELECT {_fields} FROM {tablename} LIMIT {n} OFFSET {rows_count-n}"
    else:
        _sql = f"SELECT {_fields} FROM {tablename}"
    result: taos.TaosResult = conn_tdengine.query(_sql)
    try:
        if n == 1:
            return dict(zip([field.name for field in result.fields], result.next()))
        else:
            return result.fetch_all_into_dict()
    except StopIteration:
        return None


def td_get_klines(
    symbols,
    intervals,
    kBarMaxNum: int = None,
    startTime=None,
    endTime=None,
    stable="bars",
    adjust_df=None,
    adjust_func="multiply",
):
    """
    返回：
        dict of DataFrame
    """
    if startTime is not None and isinstance(startTime, str):
        startTime = pd.to_datetime(startTime)
    if endTime is not None and isinstance(endTime, str):
        endTime = pd.to_datetime(endTime)
    ret = {}
    for symbol in symbols:
        ret[symbol] = {}
        for itv in intervals:
            ret[symbol][itv] = td_get_data(
                f"{symbol}_{itv}",
                stable=stable,
                till_dt=endTime,
                start_dt=startTime,
                max_count=kBarMaxNum,
                adjust_df=adjust_df,
                adjust_func=adjust_func,
            )
    return ret


def td_get_wss_data(
    stable, dt, fields: list = None, tags: dict = None, use_df=True, return_type=1
):
    """
    参数：
    return_type (int): 返回数据的格式
        0: Dataframe
        1: dict of arrays. eg. {"dt": [], "open": [], ...}
        2: array of dicts. eg. [{"dt": , "open": 0.1}, {"dt": , "open": 0.2}, ...]
        3: array of tuples with field names. eg. (["open", "high", "volume"], [(0.1, 0.2, 1), (0.11, 0.12, 2), ...])
        4: np.recarray
    """
    if not stable or not dt:
        return None
    if use_df:
        return_type = 0
    if return_type > 0:
        use_df = False
    _fields = "*" if fields is None else ",".join(fields)
    if tags is None:
        _sql = f"SELECT {_fields} FROM {stable} WHERE dt == '{dt.isoformat()}';"
    else:
        tag_conds = []
        for tag_name, tag_values in tags.items():
            tag_values_str = ", ".join([f"'{tv}'" for tv in tag_values])
            tag_conds.append(f"{tag_name} in ({tag_values_str})")
        tags_condition = " AND ".join(tag_conds)
        _sql = f"SELECT {_fields} FROM {stable} WHERE {tags_condition} AND dt == '{dt.isoformat()}';"
    return _get_df_results(_sql, return_type)
