from confluent_kafka import Producer
import time

class kafkaProducer:
    def __init__(self,broker,topic):
        self.broker = broker
        self.topic = topic
        self.bootstrap_servers = {
                'bootstrap.servers':self.broker
        }
        self.producer = Producer(self.bootstrap_servers)

    def acked(self,err, msg):
        if err is not None:
            print("Failed to deliver message: {0}: {1}"
                .format(msg.value(), err.str()))
        else:
            print("Message produced: {0}".format(msg.value()))

    def push(self,message):
        try:
            self.producer.produce(self.topic,message,callback=self.acked)
            self.producer.poll(0.5)
        except Exception as e:
            print('Exception is :: ',str(e))
        self.producer.flush(30)
    