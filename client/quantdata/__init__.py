import datetime
from contextlib import contextmanager

from pymongo import MongoClient
from pymongo.database import Database

__all__ = ["mongo_connect", "mongo_get_data", "mongo_get_last_trade_dt", "mongo_get_next_trade_dt"]


@contextmanager
def mongo_connect(host, port=27017, user="root", password="admin", **kwargs):
    authSource = kwargs.pop('authSource', 'admin')
    authMechanism = kwargs.pop('authMechanism', 'SCRAM-SHA-1')
    try:
        client = MongoClient(host=host, port=port, username=user, password=password, authSource=authSource, authMechanism=authMechanism, **kwargs)
        yield client
    finally:
        client.close()


def mongo_get_data(
    db: Database,
    collection_name,
    query_dict: dict = None,
    sort_by: dict = None,
    max_count=None,
    **find_kwargs
    ):
    """
    如果要返回DataFrame，可以用`df = pd.DataFrame(returned cursor)`
    """
    if not collection_name:
        return None
    if query_dict is None:
        docs = db[collection_name].find(**find_kwargs)
    else:
        docs = db[collection_name].find(query_dict, **find_kwargs)
    if sort_by:
        docs = docs.sort(sort_by)
    if max_count:
        docs = docs.limit(max_count)
    return docs


def mongo_get_trade_cal(db: Database, collection_name="trade_cal", start_date=None, end_date=None):
    if start_date and end_date:
        return mongo_get_data(db, collection_name, {
                    '$and': [
                        {'status': 1},
                        {'_id': {'$gte': start_date}},
                        {'_id': {'$lte': end_date}},
                    ]
                })
    elif start_date:
        return mongo_get_data(db, collection_name, {'$and': [{'status': 1}, {'_id': {'$gte': start_date}}]})
    elif end_date:
        return mongo_get_data(db, collection_name, {'$and': [{'status': 1}, {'_id': {'$lte': end_date}}]})
    else:
        return mongo_get_data(db, collection_name, {'status': 1})


def mongo_get_last_trade_dt(db: Database, dt: datetime.datetime):
    """从`dt`开始的下一个交易日"""
    result = (
        db['trade_cal']
        .find({'$and': [{'status': 1}, {'_id': {'$lte': dt}}]})
        .sort([('_id', -1)])
        .limit(1)
    )
    try:
        return result.next()['_id']
    except StopIteration:
        return None


def mongo_get_next_trade_dt(db: Database, dt: datetime.datetime):
    result = (
        db['trade_cal']
        .find({'$and': [{'status': 1}, {'_id': {'$gt': dt}}]})
        .limit(1)
    )
    try:
        return result.next()['_id']
    except StopIteration:
        return None