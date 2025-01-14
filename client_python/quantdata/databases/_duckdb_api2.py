from ._duckdb import *


def get_data_last_row2(
    db_name: str,
    tablename: str,
    fields: list[str],
    till_dt: np.datetime64 = None,
    side: str = "right",
    N: int = 1,
):
    if side is None:
        lt = "<"
    elif side == "right":
        lt = "<="
    else:
        raise ValueError("side has only 'right'/None options")
    if till_dt is not None:
        filter = f"dt{lt}make_timestamp({till_dt.astype(np.int64)})"
    else:
        filter = None
    return duckdb_get_array_last_rows(
        conn_duckdb, db_name, tablename, attrs=fields, filter=filter, N=N
    )


def get_data_recarray_s2(
    db_name: str,
    tablename: str,
    fields: list[str] = None,
    start_dt: np.datetime64 = None,
    till_dt: np.datetime64 = None,
    side="both",
):
    """少量数据效率高"""
    return duckdb_fetchnumpy_to_recarray(
        duckdb_get_timeseries_datetime64(
            db_name, tablename, fields, start_dt, till_dt, side
        )
    )


def get_data_recarray_l2(
    db_name: str,
    tablename: str,
    fields: list[str] = None,
    start_dt: np.datetime64 = None,
    till_dt: np.datetime64 = None,
    side="both",
):
    """大量数据效率高"""
    return duckdb_fetchdf_to_recarray(
        duckdb_get_timeseries_datetime64(
            db_name, tablename, fields, start_dt, till_dt, side
        )
    )


def get_data_ndarray2(
    db_name: str,
    tablename: str,
    fields: list[str] = None,
    start_dt: np.datetime64 = None,
    till_dt: np.datetime64 = None,
    side="both",
):
    return duckdb_get_timeseries_datetime64(
        db_name, tablename, fields, start_dt, till_dt, side
    ).fetchnumpy()


def get_data_df2(
    db_name: str,
    tablename: str,
    fields: list[str] = None,
    start_dt: np.datetime64 = None,
    till_dt: np.datetime64 = None,
    side="both",
):
    return duckdb_get_timeseries_datetime64(
        db_name, tablename, fields, start_dt, till_dt, side
    ).fetchdf()
