# import libs
from object.users import Users
import os
from dotenv import load_dotenv
from producer import Kafka

#load variables

load_dotenv()
# variable
get_dt_rows = os.getenv("EVENTS")
kafka_broker = '167.99.20.155:9094'
users_json_topic = 'src-app-py-user-events-json-001'

# instance of users objects
users_object_name = Users().get_multiple_rows(get_dt_rows)

Kafka().json_producer(broker=kafka_broker, object_name=users_object_name, kafka_topic=users_json_topic)
