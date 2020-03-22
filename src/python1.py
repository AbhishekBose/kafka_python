import json
import time
import sys
sys.path.append("../transactions/")
import time
from kafkaConsumer import kafkaConsumer
from kafkaProducer import kafkaProducer
import yaml
import json
from datetime import datetime
import os

with open("../config.yml", 'r') as ymlfile:
    topic_list = yaml.load(ymlfile)

CODE = 'PYTHON_1'
BROKER = topic_list[CODE]['BROKER']
PRODUCER_TOPIC = topic_list[CODE]['PRODUCER']

#%%
def produce_to_topic(prod_object,msg):
    json_message = json.dumps(msg)
    if msg!=None or msg!='':
        prod_object.push(json_message)

#%%
def main(operation,x,y):
    prod = kafkaProducer(BROKER,PRODUCER_TOPIC)
    message = {'operator_1':x, 'operator_2':y, 'operation':operation}
    produce_to_topic(prod,message)
                        
if __name__ == "__main__":
    x = sys.argv[1]
    y = sys.argv[2]
    operation = sys.argv[3]
    main(operation,x,y)



