import json
import time
import sys
sys.path.append("../transactions/")
import time
from kafkaConsumer import kafkaConsumer
from kafkaProducer import kafkaProducer
import yaml
import requests
import json
from datetime import datetime
import os


with open("../config.yml", 'r') as ymlfile:
    topic_list = yaml.load(ymlfile)


CODE = 'PYTHON_2'
BROKER = topic_list[CODE]['BROKER']

CONSUMER_TOPIC = topic_list[CODE]['CONSUMER']


#function to add two numbers
def add(x,y):
    return x+y


#function to take the difference of two numbers
def subtract(x,y):
    if x>y:
        return x-y
    else:
        return y-x


def main(con):
    try:
        while True:
            message = con.read_from_topic()
            if message==0:
                continue
            else:
                print('Going to decode message:: ',message)
                message_string = message.decode('utf-8')
                message_json = json.loads(message_string)
                operation = message_json['operation']
                operator_a = int(message_json['operator_1'])
                operator_b = int(message_json['operator_2'])
                if operation == 'sum':
                    output = add(operator_a,operator_b)
                elif operation == 'sub':
                    output = subtract(operator_a,operator_b)
                print('Result of operation '+operation+' is :::',output)
                #wait for 2 seconds before next message
    finally:
        print('\nShutting consumer')
        # con_object.consumer.close()

if __name__ == "__main__":
    client_id = sys.argv[1]
    con = kafkaConsumer(CONSUMER_TOPIC,client_id,'group1',BROKER)
    main(con)
    


