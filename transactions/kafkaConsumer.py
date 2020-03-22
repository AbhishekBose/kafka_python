import json
import threading
import logging
import time
import sys
from confluent_kafka import Consumer,KafkaError
import time


class kafkaConsumer:
    def __init__(self,topic,client_id,group_name,broker,input_db_obj='dummy_obj'):
        self.topic = topic
        self.client_id = client_id
        self.broker = broker

        self.settings = {
        'bootstrap.servers': self.broker,
        'group.id': group_name,
        'client.id': 'client-'+str(self.client_id),

        'enable.auto.commit': True,
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'latest'}
    }
    
        self.consumer = Consumer(self.settings)
        self.consumer.subscribe([self.topic])
        print('Starting Consumer with client id : ',self.client_id)

    def read_from_topic(self,input_db_obj=None):
        if input_db_obj!=None:
            '''do something'''
        time.sleep(1)
        try:
            msg = self.consumer.poll(0.1)
            if msg is None:
                return 0
            elif not msg.error():
                print('Received message: {0}'.format(msg.value()))
                return msg.value()
            elif msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of partition reached {0}/{1}'
                    .format(msg.topic(), msg.partition()))
                return 0
            else:
                print('Error occured: {0}'.format(msg.error().str()))
                return msg.value()

        except Exception as e:
            print('Exception occured in read from topic ',str(e))
            self.consumer.close()

            return 0
        # finally:
            # print('\nShutting consumer')
            # self.consumer.close()

