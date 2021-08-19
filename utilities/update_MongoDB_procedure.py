import pandas
from utilities import MongoDB_connection_functions
import datetime
import pymongo


def check_update_or_create_MongoDB(new_data: pandas.DataFrame,
                                   host: str, port: str,
                                   db_name: str, collection_name: str) -> None:
    """Checks if collection has any records, if not calls create_MongoDB, if yes calls update_MongoDB."""
    #  create connection with MongoDB
    collection = MongoDB_connection_functions.connection_to_mongodb(host, port, db_name, collection_name)
    if collection.estimated_document_count() == 0:
        create_MongoDB(new_data, collection)
    elif collection.estimated_document_count() > 0:
        update_MongoDB(new_data, collection)
    else:
        print("Cannot find existing collection nor create one.")


def create_MongoDB(new_data: pandas.DataFrame, collection: pymongo.collection.Collection) -> None:
    """Creates MongoDB database by inserting pandas DataFrame into it."""
    for row in new_data.to_dict('records'):
        collection.insert_one(row)
    print("Finished creating MongoDB.")


def update_MongoDB(new_data: pandas.DataFrame, collection: pymongo.collection.Collection) -> None:
    """Updates existing database. Checks currently existing records, decides if they already expired or not
    and stamps accordingly."""
    #  get data from DB to later compare it with newly scraped data
    old_data = pandas.DataFrame(list(collection.find()))
    #  intersection of two DataFrames gives rows which are the same in both
    intersection = old_data.merge(new_data[['offer_url']])
    #  expireds DataFrame contains rows which exist in remote but are no longer in fresh data
    #  which means they were taken off justjoin and nofluff meaning they expired
    expireds = old_data[old_data['offer_url'].isin(intersection['offer_url']) == False]
    expireds['expired'] = 'true'
    expireds['expired_at'] = datetime.datetime.today()
    for row in expireds.to_dict('records'):
        collection.delete_one({'offer_url': row['offer_url']})
        collection.insert_one(row)
    #  news DataFrame contains rows which are only in fresh data, they were not found in remote
    #  meaning these are new job offers
    news = new_data[new_data['offer_url'].isin(intersection['offer_url']) == False]
    for row in news.to_dict('records'):
        collection.insert_one(row)
    print("Finished updating MongoDB.")
