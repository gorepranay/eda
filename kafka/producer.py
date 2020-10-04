import random
import os
import time
import argparse
from faker import Faker
from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from encoder import EncoDeco
import cluster as serv

curr_dir = os.path.dirname(os.path.realpath(__file__))
proj_dir = os.path.dirname(os.getcwd())
properties_file=os.path.join(proj_dir,'eda.properties')

num_of_records = 20

class Produce:
    def __init__(self):
        pass

    @staticmethod
    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
            Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def create_topic(self, topic, admin_client):
        new_topics = [NewTopic(topic, num_partitions=2, replication_factor=2) for topic in topic]
        fs = self.adm_cli.create_topics(new_topics)

        for topic, f in fs.items():
            try:
                f.result()
                print("Topic {} created: ".format(topic))
                os.environ['EDA_TOPIC']=topic

            except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))
        return


    def produce_event(self, topic, brokers, sleep_time=0):

        self.prd = Producer({'bootstrap.servers': brokers})
        self.adm_cli = AdminClient({'bootstrap.servers': brokers})

        topic_list=[topic]
        self.create_topic(topic=topic_list, admin_client=self.adm_cli)

        fdata = Faker()
        action_items = ['debit', 'credit', 'withdrawn', 'deposit']
        data_templ = {'id': None, 'name': None, 'country': None, 'amount': None, 'action': None}

        id = 1
        for _ in range(num_of_records):
            data_templ['id'] = id
            data_templ['name'] = fdata.name()
            data_templ['country'] = fdata.country()
            data_templ['amount'] = random.uniform(1000.0, 3000.0)
            data_templ['action'] = random.choice(action_items)
            # Trigger any available delivery report callbacks from previous produce() calls
            self.prd.poll(0)

            # Asynchronously produce a message, the delivery report callback
            # will be triggered from poll() above, or flush() below, when the message has
            # been successfully delivered or failed permanently.

            try:
                event=EncoDeco.encode(str(data_templ)) # Encode event data
                self.prd.produce(topic=topic, key=str(uuid4()), value=event, callback=self.delivery_report)
            except:
                print('Something wrong with the producer !')
            time.sleep(sleep_time)
            id += 1

        # Wait for any outstanding messages to be delivered and delivery report
        # callbacks to be triggered.
        self.prd.flush()


def main(args):
    print('Producer initiated: Broker-{} | Topic-{} | Sleep-{}'.format(args.bootstrap_servers,args.topic, args.sleep))
    p=Produce()
    p.produce_event(topic=args.topic, brokers=args.bootstrap_servers, sleep_time=args.sleep)
    '''Create MSK clutser using API'''
    #awsserv.create_msk()

if __name__ == '__main__':
    msk_configs = serv.get_config(properties_file=properties_file, section='MSK')
    parser = argparse.ArgumentParser(description="Producer terminal")
    parser.add_argument('-b', dest="bootstrap_servers", default=serv.get_brokers())
    parser.add_argument('-t', dest="topic", default=serv.sk_configs['topic'])
    parser.add_argument('-s', dest="sleep", default=1)
    main(parser.parse_args())