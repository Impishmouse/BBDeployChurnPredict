import os
import pymongo
from pymongo.errors import ConnectionFailure

# TODO think about move all  credentials to environment variables
# Best birds MongoDB production be careful.
conn_str = os.getenv('DB_URL')


def connect_to_db():
    try:
        # set a 5-second connection timeout
        db_client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000)
        print(db_client.server_info())
        return db_client
    except ConnectionFailure:
        print("Unable to connect to the server.")
        return None


def get_client():
    if client is None:
        print("Unable connect to db")
    else:
        return client


client = connect_to_db()
if client is None:
    print("Unable connect to db")
