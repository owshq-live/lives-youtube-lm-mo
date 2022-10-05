#import libs

from confluent_kafka.cimpl import KafkaException, Consumer

import consumer_settings
from dotenv import load_dotenv

import time

import os

# get env
load_dotenv()

# load variable
kafka_topic = 'src-app-py-user-events-json-001'
execution_time = 1000
broker = '167.99.20.155:9094'

# schema


# instance configuration for kafka consumer
c = Consumer(consumer_settings.consumer_settings_json(broker))

# subscribe the topic be consume
c.subscribe([kafka_topic])

# duration of application execution
timeout = time.time() + int(execution_time)

try:
    while timeout >= time.time():
        events = c.poll(0.1)
        if events is None:
            continue
        if events.error():
            raise KafkaException(events.error())
        print(events.topic(), events.partition(), events.offset(), events.value().decode('utf-8'))
        #print(events.value())
except KeyboardInterrupt:
    pass

finally:
    c.close()