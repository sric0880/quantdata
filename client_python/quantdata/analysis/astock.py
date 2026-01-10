from collections import defaultdict
from typing import Union

import polars as pl

from quantdata import mongo_get_data

dbname = "finance"


# 要排除的概率
exclude_concept = [
    "883300.TI",  # 沪深300样本股
    "883301.TI",  # 上证50样本股
    "883302.TI",  # 上证180成份股
    "883303.TI",  # 上证380成份股
    "883304.TI",  # 中证500成份股
    "885338.TI",  # 融资融券
    "885472.TI",  # 上海自贸区
    "885487.TI",  # 天津自贸区
    "885514.TI",  # 京津冀一体化
    "885520.TI",  # 沪股通
    "885521.TI",  # 粤港澳大湾区
    "885587.TI",  # 举牌
    "885591.TI",  # 中韩自贸区
    "885598.TI",  # 新股与次新股
    "885617.TI",  # 福建自贸区
    "885694.TI",  # 深股通
    "885699.TI",  # ST板块
    "885701.TI",  # 杭州亚运会
    "885729.TI",  # 参股新三板
    "885734.TI",  # 广东自贸区
    "885739.TI",  # 股权转让
    "885742.TI",  # 摘帽
    "885796.TI",  # 送转填权
    "885849.TI",  # 黑龙江自贸区
    "885855.TI",  # 创业板重组松绑
    "885867.TI",  # 标普道琼斯A股
    "885873.TI",  # 分拆上市意愿
    "885905.TI",  # 注册制次新股
    "885906.TI",  # 核准制次新股
    "885907.TI",  # 科创次新股
]


def get_ths_concepts_names():
    index_list = mongo_get_data(
        dbname,
        "basic_info_ths_concepts",
        projection={"symbol": 1, "name": 1},
    )
    return {doc["symbol"]: doc["name"] for doc in index_list}


def get_stocks_of_index(collection_name, index_symbol, dt):
    """
    获取截止{dt}时的概念或指数的成分股
    """
    df = pl.DataFrame(
        mongo_get_data(
            dbname,
            collection_name,
            query={"$and": [{"index_code": index_symbol}, {"tradedate": {"$lte": dt}}]},
        )
    )
    stocks = set()
    for row in df.iter_rows(named=True):
        if row["op"] == 1:
            stocks.add(row["stock_code"])
        else:
            try:
                stocks.remove(row["stock_code"])
            except KeyError:
                pass
    return list(stocks)


def _get_indexes_of_stock(sub_df):
    indexes = set()
    for row in sub_df.iter_rows(named=True):
        if row["op"] == 1:
            if row["index_code"] in exclude_concept:
                continue
            indexes.add(row["index_code"])
        else:
            try:
                indexes.remove(row["index_code"])
            except KeyError:
                pass
    return list(indexes)


def get_indexes_of_stock(collection_name, symbol, dt):
    """
    获取截止{dt}时的股票所属的概念板块或指数
    """
    df = pl.DataFrame(
        mongo_get_data(
            dbname,
            collection_name,
            query={"$and": [{"stock_code": symbol}, {"tradedate": {"$lte": dt}}]},
        )
    )
    return _get_indexes_of_stock(df)


def include_in_ths_concepts(stock_list, dt, concept_codes, logger):
    """
    在概念集{concept_codes}中的股票才能被选中
    """
    if not stock_list or not concept_codes:
        return stock_list
    ret = []
    df = pl.DataFrame(mongo_get_data(dbname, "constituent_ths_index"))
    for stock in stock_list:
        sub_df = df.filter(
            (pl.col("tradedate") <= dt) & (pl.col("stock_code") == stock)
        )
        concepts = _get_indexes_of_stock(sub_df)
        found = False
        for one_c in concepts:
            if one_c in concept_codes:
                ret.append(stock)
                found = True
                break
        if not found:
            logger.debug(f"{stock}被排除: 不在概念集中")
    return ret


def top_concepts_of_stocks(stock_list, dt, limit=None):
    """
    统计股票所属概念前{limit}名
    """
    df = pl.DataFrame(mongo_get_data(dbname, "constituent_ths_index"))
    concepts_count = defaultdict(int)
    for stock in stock_list:
        sub_df = df.filter(
            (pl.col("tradedate") <= dt) & (pl.col("stock_code") == stock)
        )
        concepts = _get_indexes_of_stock(sub_df)
        for _c in concepts:
            concepts_count[_c] += 1
    sorted_concepts = sorted(concepts_count.items(), key=lambda x: x[1], reverse=True)
    if limit:
        return sorted_concepts[:limit]
    else:
        return sorted_concepts


def get_indexes_of_stocks(stock_list, dt):
    """
    统计股票所属概念前{limit}名
    """
    df = pl.DataFrame(mongo_get_data(dbname, "constituent_ths_index"))
    all_concepts = {}
    for stock in stock_list:
        sub_df = df.filter(
            (pl.col("tradedate") <= dt) & (pl.col("stock_code") == stock)
        )
        concepts = _get_indexes_of_stock(sub_df)
        all_concepts[stock] = concepts
    return all_concepts


def ths_hot_stocks(top_n, dt):
    """
    获取{dt}当天的同花顺热股前{top_n}名
    """
    hot_stocks = pl.DataFrame(
        mongo_get_data(dbname, "hot_stocks_ths", query={"date": dt})
    )
    return hot_stocks.bottom_k(top_n, by="order")


def get_finance_data(tablename, fields) -> pl.DataFrame:
    projection = {
        "_id": 0,
        "f_ann_date": 1,
        "end_date": 1,
    }
    for f in fields:
        projection[f] = 1
    df = pl.DataFrame(
        mongo_get_data(
            dbname,
            tablename,
            query={},
            projection=projection,
        )
    )
    if df.is_empty():
        return df
    # drop duplicates of finance_data
    df = df.with_columns(
        pl.col("f_ann_date").cast(pl.Datetime("ms")),
        pl.col("end_date").cast(pl.Datetime("ms")),
    )
    df = df.sort(["ts_code", "f_ann_date", "end_date"], maintain_order=True)
    df = df.filter(
        (pl.col("ts_code") != pl.col("ts_code").shift(1))
        | (
            pl.col("end_date")
            >= pl.col("end_date").shift(1, fill_value=pl.datetime(1990, 1, 1))
        )
    )
    return df


def merge_finance_data(*data) -> Union[pl.DataFrame, None]:
    l = len(data)
    if l < 1:
        return None
    elif l == 1:
        return data[0]
    else:
        left = data[0]
        right = merge_finance_data(*data[1:])
        if (
            left is not None
            and not left.is_empty()
            and right is not None
            and not right.is_empty()
        ):
            df = left.join(
                right,
                on=["ts_code", "f_ann_date", "end_date"],
                how="full",
                coalesce=True,
            )
            return df.sort(["ts_code", "f_ann_date", "end_date"], maintain_order=True)
        else:
            return None


if __name__ == "__main__":
    from quantdata import mongo_connect, mongo_close
    from datetime import datetime

    mongo_connect("localhost")
    try:
        dt = datetime(2025, 11, 21)
        print(get_indexes_of_stocks(["002493.SZ", "600605.SH"], dt))
        # print(get_ths_concepts_names())
        hot_stocks = ths_hot_stocks(50, dt)
        hot_stock_codes = set(hot_stocks["code"].to_list())
        top_concepts_of_stocks(hot_stock_codes, dt, limit=6)
    finally:
        mongo_close()
