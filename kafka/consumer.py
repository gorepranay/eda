import ast
import os
import argparse
from confluent_kafka import Consumer
from pymongo import MongoClient
from encoder import EncoDeco
import cluster as serv

curr_dir = os.path.dirname(os.path.realpath(__file__))
proj_dir = os.path.dirname(os.getcwd())
properties_file=os.path.join(proj_dir,'eda.properties')

mongo_info=serv.get_config(properties_file=properties_file, section='Mongo')
client = MongoClient(mongo_info['mongo_server'], int(mongo_info['port']))

db=client[mongo_info['mongo_db']]
collection=db[mongo_info['collection']]

class Consume:

    def __init__(self):
        pass

    def consume_event(self,topic, brokers, group_id):
        self.cons = Consumer({'bootstrap.servers': brokers, 'group.id': group_id, 'auto.offset.reset': 'earliest'})
        self.cons.subscribe([topic])

        while True:
            msg = self.cons.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue

            # Pushing the event to MongoDB
            try:
                resp=collection.insert_one(ast.literal_eval(EncoDeco.decode(msg.value())))
                print('Object created in event store: ',resp.inserted_id)
            except:
                print('Something went wrong with the event store operations !')
        self.cons.close()

def main(args):
    print('Consumer initiated: Broker-{} | Topic-{} | Group ID-{}'.format(args.bootstrap_servers,args.topic, args.group_id))
    c=Consume()
    c.consume_event(topic=args.topic, brokers=args.bootstrap_servers, group_id=args.group_id)


if __name__ == '__main__':
    msk_configs = serv.get_config(properties_file=properties_file, section='MSK')
    parser = argparse.ArgumentParser(description="Consumer terminal")
    parser.add_argument('-b', dest="bootstrap_servers", default=serv.get_brokers())
    parser.add_argument('-t', dest="topic", default=msk_configs['topic'])
    parser.add_argument('-g', dest="group_id", default=msk_configs['group_id'])

    main(parser.parse_args())