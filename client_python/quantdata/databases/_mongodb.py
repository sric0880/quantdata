import atexit
from collections import defaultdict

import numpy as np
from pymongo import MongoClient

conn_mongodb: MongoClient = None


def get_conn_mongodb():
    return conn_mongodb


def mongo_connect(host: str, port=27017, user="root", password="admin", **kwargs):
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
