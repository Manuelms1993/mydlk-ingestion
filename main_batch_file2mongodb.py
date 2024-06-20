import pandas


def database_exists(client, db_name):
    databases = client.list_database_names()
    return db_name in databases

def collection_exists(db, collection_name):
    collections = db.list_collection_names()
    return collection_name in collections

if __name__ == "__main__":

    import pymongo
    client = pymongo.MongoClient('mongodb://root:example@mongo:27017/')

    db_name = "raw"
    collection_name = "movie_dataset"
    db = client[db_name]
    collection = db[collection_name]

    # Read data from CSV file
    df = pandas.read_csv('/Users/manuelmontero/MM_DLK/MyDatalake/datasets/movie_dataset.csv')
    data = df.to_dict(orient='records')

    # Insert data into the collection
    collection.delete_many({})
    collection.insert_many(data)

    print("ALL OK!")