import os
from pathlib import Path
from typing import Dict, Union
from datetime import datetime, date

import polars as pl

_home_dir: Path = None


def filedb_connect(home_dir: str):
    global _home_dir
    _home_dir = Path(home_dir)


def filedb_get_parquet(filepath: str, fields: list[str] = None):
    return pl.read_parquet(_home_dir / filepath, columns=fields)


def filedb_get_at(
    data_dirname: str,
    day: str,
    fields: list[str] = None,
    adj=False,
):
    """
    Params:
        - fields: 至少要含有["dt", "symbol"]，如果要复权，还应该包括["close", "preclose"]
        - day: eg. "2025-09-08"
    """
    return filedb_get_between(
        data_dirname,
        day,
        day,
        fields,
        adj,
        False,
    )


def filedb_get_gte(
    data_dirname: str,
    day: str,
    fields: list[str] = None,
    n: int = 1,
    adj=False,
    groupby_sybmol=True,
):
    """
    Params:
        - fields: 至少要含有["dt", "symbol"]，如果要复权，还应该包括["close", "preclose"]
        - day: eg. "2025-09-08"
    """
    valid_file_list = _get_valid_file_list_after(data_dirname, day, n)
    return _filedb_get(
        valid_file_list,
        fields,
        adj,
        groupby_sybmol,
    )


def filedb_get_lte(
    data_dirname: str,
    day: str,
    fields: list[str] = None,
    n: int = 1,
    adj=False,
    groupby_sybmol=True,
):
    """
    Params:
        - fields: 至少要含有["dt", "symbol"]，如果要复权，还应该包括["close", "preclose"]
    """
    valid_file_list = _get_valid_file_list_before(data_dirname, day, n)
    return _filedb_get(
        valid_file_list,
        fields,
        adj,
        groupby_sybmol,
    )


def filedb_get_between(
    data_dirname: str,
    start_date: str,
    end_date: str,
    fields: list[str] = None,
    adj=False,
    groupby_sybmol=True,
):
    """
    Params:
        - fields: 至少要含有["dt", "symbol"]，如果要复权，还应该包括["close", "preclose"]
    """
    valid_file_list = _get_valid_file_list(data_dirname, start_date, end_date)
    return _filedb_get(
        valid_file_list,
        fields,
        adj,
        groupby_sybmol,
    )


def filedb_groupby_freq(
    data_dirname: str,
    start_date: str,
    end_date: str,
    freq: str,
    groupby_sybmol=False,
):
    """
    生成周线和月线，字段必须含有[ "dt", "symbol", "open", "high", "low", "close", "preclose", "volume", "amount"]

    Params:
        - freq: "1w" for week, "1mo" for month
    """
    df = filedb_get_between(
        data_dirname,
        start_date,
        end_date,
        [
            "dt",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "preclose",
            "volume",
            "amount",
        ],
        True,
        False,
    )
    df = df.group_by(pl.col("symbol"), pl.col("dt").dt.truncate(freq)).agg(
        pl.col("adj_open").first().alias(f"open_{freq}"),
        pl.col("adj_high").max().alias(f"high_{freq}"),
        pl.col("adj_low").min().alias(f"low_{freq}"),
        pl.col("adj_close").last().alias(f"close_{freq}"),
        pl.col("volume").sum().alias(f"volume_{freq}"),
        pl.col("amount").sum().alias(f"amount_{freq}"),
    )
    df = df.sort("symbol", "dt")
    if groupby_sybmol:
        return {symbol: sd for (symbol,), sd in df.group_by(by="symbol")}
    return df


def filedb_get(
    data_dirname: str,
    sorted_dates: list[Union[datetime, date, str]],
    fields: list[str] = None,
    adj=False,
    groupby_sybmol=True,
):
    if not sorted_dates:
        return None
    if isinstance(sorted_dates[0], (datetime, date)):
        sorted_files = [
            _home_dir / data_dirname / f"{d.isoformat()}.parquet" for d in sorted_dates
        ]
    elif isinstance(sorted_dates[0], str):
        sorted_files = [_home_dir / data_dirname / f"{d}.parquet" for d in sorted_dates]
    else:
        return None
    return _filedb_get(
        sorted_files,
        fields,
        adj,
        groupby_sybmol,
    )


def _filedb_get(
    sorted_files: list[str],
    fields: list[str] = None,
    adj=False,
    groupby_sybmol=True,
) -> Union[pl.DataFrame, Dict[str, pl.DataFrame]]:
    if not sorted_files:
        raise ValueError(f"Empty file list")
    if adj and (
        "close" not in fields or "preclose" not in fields or "symbol" not in fields
    ):
        raise ValueError(
            "fields must contain ['close', 'preclose', 'symbol'] to calculate adjust prices"
        )
    df = pl.scan_parquet(
        sorted_files,
        cache=False,
        extra_columns="ignore",
        missing_columns="insert",
    )
    if fields is not None:
        df = df.select(fields)
    df = df.collect()
    if adj:
        df = apply_adjust_factors(df)
    if groupby_sybmol:
        return {symbol: sd for (symbol,), sd in df.group_by(by="symbol")}
    return df


def _get_valid_file_list(data_path: str, start_date: str, end_date: str):
    ret = []
    p = _home_dir / data_path
    for fname in os.listdir(p):
        f_date = fname[:10]
        if f_date >= start_date and f_date <= end_date:
            ret.append(p / fname)
    return sorted(ret)


def _get_valid_file_list_before(data_path: str, end_date: str, days: int):
    ret = []
    p = _home_dir / data_path
    for fname in os.listdir(p):
        f_date = fname[:10]
        if f_date <= end_date:
            ret.append(p / fname)
    ret = sorted(ret)
    return ret[max(0, len(ret) - days) :]


def _get_valid_file_list_after(data_path: str, begin_date: str, days: int):
    ret = []
    p = _home_dir / data_path
    for fname in os.listdir(p):
        f_date = fname[:10]
        if f_date >= begin_date:
            ret.append(p / fname)
    ret = sorted(ret)
    return ret[: min(days, len(ret))]


def apply_adjust_factors(df: pl.DataFrame):
    ohlc_col_nms = ["open", "high", "low", "close"]
    return df.with_columns(
        adjust_factor=(
            (pl.col("preclose") / pl.col("close").shift(1))
            .cum_prod(reverse=True)
            .shift(-1, fill_value=1.0)
        ).over("symbol")
    ).with_columns(
        [
            (pl.col(col_nm) * pl.col("adjust_factor")).alias("adj_" + col_nm)
            for col_nm in ohlc_col_nms
            if col_nm in df.columns
        ]
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
