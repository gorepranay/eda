from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017')



class MongoClient:
    def __init__(self):
        self.db=client.user

    def is_valid(self):
        pass

    def insert_a_record(self):
        pass

    def insert_batch(self):
        pass

    def server_heartbeat(self):
        pass
db = client.test # 'test' is the DB name
collection = db.eda#'coll_1' is the collection name
print(db.list_collection_names())
# db.list_collection_names() # List collection names
# collection.find_one({'age':25}) # Find only one record
# for i in collection.find({'age':{"$gt":20}}): # Find multiple records
#     print(i)

# res=collection.insert_one({'name': 'Ajeet', 'age': 44, 'status': 'married'}) # Inserting a document
# print(res.inserted_id)