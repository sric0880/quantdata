from ._duckdb import *


def get_data_last_row(
    db_name: str,
    tablename: str,
    fields: list[str] = None,
    till_microsec: int = 0,
    side: str = "right",
    N: int = 1,
) -> duckdb.DuckDBPyRelation:
    """
    Return: DuckDBPyRelation
        call `fetchone`, `fetchmany`, `fetchall`, `fetchdf`, `fetchnumpy` on returned obj
    """
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
        db_name, tablename, attrs=fields, filter=filter, N=N
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
        duckdb_get_timeseries(
            db_name, tablename, fields, start_microsec, till_microsec, side
        )
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
        duckdb_get_timeseries(
            db_name, tablename, fields, start_microsec, till_microsec, side
        )
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
        duckdb_get_timeseries(
            db_name, tablename, fields, start_microsec, till_microsec, side
        )
    )


def get_data_ndarray(
    db_name: str,
    tablename: str,
    fields: list[str] = None,
    start_microsec: int = 0,
    till_microsec: int = 0,
    side="both",
):
    return duckdb_get_timeseries(
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
    return duckdb_get_timeseries(
        db_name, tablename, fields, start_microsec, till_microsec, side
    ).fetchdf()
