import atexit
from collections import defaultdict
from datetime import datetime

import numpy as np
from pymongo import MongoClient

conn_mongodb: MongoClient = None


def mongo_connect(host, port=27017, user="root", password="admin", **kwargs):
    global conn_mongodb
    if conn_mongodb is not None:
        return
    authSource = kwargs.pop("authSource", "admin")
    authMechanism = kwargs.pop("authMechanism", "SCRAM-SHA-1")
    print("connecting mongodb...")
    conn_mongodb = MongoClient(
        host=host,
        port=port,
        username=user,
        password=password,
        authSource=authSource,
        authMechanism=authMechanism,
        **kwargs,
    )


@atexit.register
def mongo_close():
    global conn_mongodb
    if conn_mongodb is None:
        return
    try:
        print(f"mongodb closing...")
        conn_mongodb.close()
        print(f"mongodb closed")
    except:
        pass
    conn_mongodb = None


def mongo_get_data(
    db_name: str,
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
        docs = conn_mongodb[db_name][collection_name].find(**find_kwargs)
    else:
        docs = conn_mongodb[db_name][collection_name].find(query, **find_kwargs)
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
    db_name: str,
    collection_name="trade_cal",
    start_date: datetime = None,
    end_date: datetime = None,
):
    """返回从start_date到end_date(包括本身)的交易日期"""
    if start_date and end_date:
        return mongo_get_data(
            db_name,
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
            db_name,
            collection_name,
            {"$and": [{"status": 1}, {"_id": {"$gte": start_date}}]},
        )
    elif end_date:
        return mongo_get_data(
            db_name,
            collection_name,
            {"$and": [{"status": 1}, {"_id": {"$lte": end_date}}]},
        )
    else:
        return mongo_get_data(db_name, collection_name, {"status": 1})


def mongo_get_last_trade_dt(db_name: str, dt: datetime):
    """从`dt`开始的上一个交易日"""
    result = (
        conn_mongodb[db_name]["trade_cal"]
        .find({"$and": [{"status": 1}, {"_id": {"$lte": dt}}]})
        .sort([("_id", -1)])
        .limit(1)
    )
    try:
        return result.next()["_id"]
    except StopIteration:
        return None


def mongo_get_next_trade_dt(db_name: str, dt: datetime):
    """从`dt`开始的下一个交易日"""
    result = (
        conn_mongodb[db_name]["trade_cal"]
        .find({"$and": [{"status": 1}, {"_id": {"$gt": dt}}]})
        .limit(1)
    )
    try:
        return result.next()["_id"]
    except StopIteration:
        return None
