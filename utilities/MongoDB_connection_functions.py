import pymongo


def connection_to_mongodb(host: str, port: str, db_name: str, collection_name: str) -> pymongo.collection.Collection:
    """Creates connection with Collection from MongoDB."""
    client = pymongo.MongoClient(f"mongodb://{host}:{port}")
    db = client[db_name]
    offers_collection = db[collection_name]
    return offers_collection
