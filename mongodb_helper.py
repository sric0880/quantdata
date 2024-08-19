from contextlib import contextmanager

import pandas as pd
from pymongo import MongoClient

__all__ = ["mongo_connect", "insert_many_ignore_nan", "delete_fields"]

@contextmanager
def mongo_connect(host, port=27017, user="root", password="admin", **kwargs):
    authSource = kwargs.pop('authSource', 'admin')
    authMechanism = kwargs.pop('authMechanism', 'SCRAM-SHA-1')
    try:
        client = MongoClient(host=host, port=port, username=user, password=password, authSource=authSource, authMechanism=authMechanism, **kwargs)
        yield client
    finally:
        client.close()

def insert_many_ignore_nan(coll, df):
    data = df.to_dict(orient='records')
    for dct in data:
        del_ks = []
        for k,v in dct.items():
            if pd.isna(v):
                del_ks.append(k)
        for k in del_ks:
            dct.pop(k)
    coll.insert_many(data)

def delete_fields(coll, fields):
    new_ = {'$unset' : {field:'' for field in fields} }
    coll.update_many({}, new_, False)
