import os
from datetime import date
from pathlib import Path
from typing import Dict, Union

import polars as pl

from ._mongodb import get_conn_mongodb

_home_dir: Path = None


def filedb_connect(home_dir: str):
    global _home_dir
    _home_dir = Path(home_dir)


def filedb_get_parquet(filepath: str, fields: list[str] = None):
    return pl.read_parquet(_home_dir / filepath, columns=fields)


def filedb_get_at(
    data_dirname: str,
    fields: list[str] = None,
    day: str = 0,
    adj_factor_mongo_params: tuple = None,
):
    """
    Params:
        - fields: 至少要含有["dt", "symbol"]，如果要复权，还应该包括["open", "high", "low", "close"]
        - day: eg. "2025-09-08"
    """
    return filedb_get_between(
        data_dirname,
        fields,
        day,
        day,
        adj_factor_mongo_params,
        False,
    )


def filedb_get_gte(
    data_dirname: str,
    fields: list[str] = None,
    day: str = 0,
    n: int = 1,
    adj_factor_mongo_params: tuple = None,
    groupby_sybmol=True,
    groupby_freq: str = None,
):
    """
    Params:
        - fields: 至少要含有["dt", "symbol"]，如果要复权，还应该包括["open", "high", "low", "close"]
        - day: eg. "2025-09-08"
        - groupby_freq: "1w" for week, "1mo" for month, and default None is for daily. 需要复权数据
    """
    valid_file_list = _get_valid_file_list_after(data_dirname, day, n)
    return _filedb_get(
        data_dirname,
        valid_file_list,
        fields,
        adj_factor_mongo_params,
        groupby_sybmol,
        groupby_freq,
    )


def filedb_get_lte(
    data_dirname: str,
    fields: list[str] = None,
    day: str = 0,
    n: int = 1,
    adj_factor_mongo_params: tuple = None,
    groupby_sybmol=True,
    groupby_freq: str = None,
):
    """
    Params:
        - fields: 至少要含有["dt", "symbol"]，如果要复权，还应该包括["open", "high", "low", "close"]
        - groupby_freq: "1w" for week, "1mo" for month, and default None is for daily. 需要复权数据
    """
    valid_file_list = _get_valid_file_list_before(data_dirname, day, n)
    return _filedb_get(
        data_dirname,
        valid_file_list,
        fields,
        adj_factor_mongo_params,
        groupby_sybmol,
        groupby_freq,
    )


def filedb_get_between(
    data_dirname: str,
    fields: list[str] = None,
    start_date: str = 0,
    end_date: str = 0,
    adj_factor_mongo_params: tuple = None,
    groupby_sybmol=True,
    groupby_freq: str = None,
):
    """
    Params:
        - fields: 至少要含有["dt", "symbol"]，如果要复权，还应该包括["open", "high", "low", "close"]
        - groupby_freq: "1w" for week, "1mo" for month, and default None is for daily. 需要复权数据
    """
    valid_file_list = _get_valid_file_list(data_dirname, start_date, end_date)
    return _filedb_get(
        data_dirname,
        valid_file_list,
        fields,
        adj_factor_mongo_params,
        groupby_sybmol,
        groupby_freq,
    )


def filedb_groupby_freq(df: pl.DataFrame, groupby_freq: str):
    return df.group_by(pl.col("dt").dt.truncate(groupby_freq), maintain_order=True).agg(
        pl.col("adj_open").first(),
        pl.col("adj_high").max(),
        pl.col("adj_low").min(),
        pl.col("adj_close").last(),
        pl.col("volume").sum(),
        pl.col("amount").sum(),
    )


def _filedb_get(
    data_dirname: str,
    sorted_files: list[str],
    fields: list[str] = None,
    adj_factor_mongo_params: tuple = None,
    groupby_sybmol=True,
    groupby_freq: str = None,
) -> Union[pl.DataFrame, Dict[str, pl.DataFrame]]:
    if not sorted_files:
        raise ValueError(f"Empty file list")
    if groupby_freq and not groupby_sybmol:
        raise ValueError(
            f"groupby_freq is {groupby_freq}, param groupby_sybmol must be True"
        )
    start_date, end_date = sorted_files[0][0], sorted_files[-1][0]
    df = pl.scan_parquet(
        [_home_dir / data_dirname / f[1] for f in sorted_files],
        cache=False,
        extra_columns="ignore",
        missing_columns="insert",
    )
    if fields is not None:
        df = df.select(fields)
    df = df.collect()
    if adj_factor_mongo_params:
        db_name, coll_name = adj_factor_mongo_params
        df = apply_adjust_factors(df, db_name, coll_name, start_date, end_date)
    if not groupby_sybmol:
        return df
    elif groupby_freq:
        return {
            symbol: filedb_groupby_freq(sd, groupby_freq)
            for (symbol,), sd in df.group_by(by="symbol")
        }
    else:
        return {symbol: sd for (symbol,), sd in df.group_by(by="symbol")}


def _get_valid_file_list(data_path: str, start_date: str, end_date: str):
    ret = []
    for fname in os.listdir(_home_dir / data_path):
        f_date = fname[:10]
        if f_date >= start_date and f_date <= end_date:
            ret.append((f_date, fname))
    return sorted(ret, key=lambda x: x[0])


def _get_valid_file_list_before(data_path: str, end_date: str, days: int):
    ret = []
    for fname in os.listdir(_home_dir / data_path):
        f_date = fname[:10]
        if f_date <= end_date:
            ret.append((f_date, fname))
    ret = sorted(ret, key=lambda x: x[0])
    return ret[max(0, len(ret) - days) :]


def _get_valid_file_list_after(data_path: str, begin_date: str, days: int):
    ret = []
    for fname in os.listdir(_home_dir / data_path):
        f_date = fname[:10]
        if f_date >= begin_date:
            ret.append((f_date, fname))
    ret = sorted(ret, key=lambda x: x[0])
    return ret[: min(days, len(ret))]


def fetch_adjust_factors(db_name, coll_name, start_date, end_date) -> pl.DataFrame:
    """
    Fetch adjust factors from mongodb that is between [start_date, end_date]
    """
    conn = get_conn_mongodb()
    if conn is None:
        raise RuntimeError("Init mongodb first")
    adjust_factors_collection = conn[db_name][coll_name]
    adjs = adjust_factors_collection.find({})
    adj_df = pl.DataFrame(adjs)
    if adj_df.is_empty():
        raise RuntimeError("Mongodb has no adjust factors")
    adj_df = adj_df.rename(mapping={"tradedate": "dt"}).drop("_id")
    adj_df = adj_df.with_columns(pl.col("dt").cast(pl.Datetime("ms"))).select(
        ["dt", "symbol", "adjust_factor"]
    )
    _start_dt = date.fromisoformat(start_date)
    _end_dt = date.fromisoformat(end_date)
    sub_df = pl.DataFrame(
        {"dt": pl.date_range(_start_dt, _end_dt, interval="1d", eager=True)},
        schema=[("dt", pl.Datetime("ms"))],
    )
    sub_df = sub_df.with_columns(
        adjust_factor=1.0,
        symbol=pl.lit("_temp"),
    ).select(["dt", "symbol", "adjust_factor"])
    # 列与列之间要对齐，否则报错
    adj_df = pl.concat([adj_df, sub_df])
    adj_df = adj_df.sort("dt")
    piv_df = adj_df.pivot("symbol", index="dt", values="adjust_factor").fill_null(
        strategy="forward"
    )
    piv_df = piv_df.filter(pl.col("dt").is_between(_start_dt, _end_dt))
    return piv_df


def apply_adjust_factors(daily_df, db_name, coll_name, start_date, end_date):
    a = fetch_adjust_factors(db_name, coll_name, start_date, end_date)
    a = a.unpivot(index="dt", variable_name="symbol", value_name="adjust_factor")
    return (
        daily_df.join(a, on=["dt", "symbol"], how="left")
        .with_columns(pl.col("adjust_factor").fill_null(1.0))
        .with_columns(
            adj_open=pl.col("open") * pl.col("adjust_factor"),
            adj_high=pl.col("high") * pl.col("adjust_factor"),
            adj_low=pl.col("low") * pl.col("adjust_factor"),
            adj_close=pl.col("close") * pl.col("adjust_factor"),
        )
    )


def filedb_get_schema(data_dirname: str, day: str):
    dest_file = _home_dir / data_dirname / f"{day}.parquet"
    return pl.read_parquet_schema(dest_file)


def filedb_has_columns(data_dirname: str, day: str, fields: list[str]):
    schema = filedb_get_schema(data_dirname, day)
    return [field in schema for field in fields]


def filedb_has_any_columns(data_dirname: str, day: str, fields: list[str]):
    return any(filedb_has_columns(data_dirname, day, fields))


def filedb_has_all_columns(data_dirname: str, day: str, fields: list[str]):
    return all(filedb_has_columns(data_dirname, day, fields))


def filedb_merge(
    data_dirname: str,
    day: str,
    df: pl.DataFrame,
    on="symbol",
    check_integrity=True,
    replace_origin=False,
):
    """
    Params:
        - day: eg. "2025-09-08"
        - check_integrity: ensure origin and df have the same length
        - replace_origin: replace the same columns in both origin and df
    """
    dest_file = _home_dir / data_dirname / f"{day}.parquet"
    origin_df = pl.read_parquet(dest_file)
    if origin_df.is_empty():
        raise ValueError(f"{dest_file} is empty")
    same_cols = set(origin_df.columns) & set(df.columns)
    same_cols -= {"symbol"}
    if replace_origin:
        origin_df = origin_df.drop(same_cols)
    elif same_cols:
        df = df.drop(same_cols)
    dest_df = origin_df.join(df, on=on, how="left")
    if check_integrity:
        if len(dest_df) != len(df):
            raise ValueError(
                f"Cannot merge df to {dest_file} because of different size."
            )
    dest_df.write_parquet(dest_file)
