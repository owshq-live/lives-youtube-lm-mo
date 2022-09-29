# import python
import json
import producer_settings
from confluent_kafka import Producer
import delivery_reports


# crate kafka class for producer

class Kafka(object):
    
    #producer fuction
    @staticmethod
    def json_producer (broker, object_name,kafka_topic):
        
        # init producer settings
        
        p = Producer(producer_settings.producer_settings_json(broker))
        
        # data from app users
        get_data = object_name
                
        
        #loop to write into apache kafka
        for data in get_data:
            
            try:
                
                p.poll(0)
                
                p.produce(
                    topic=kafka_topic,
                    value=json.dumps(data).encode('utf-8'),
                    callback=delivery_reports.on_delivery_json
                )
                
            except BufferError:
                print("buffer full")
                p.poll(0.1)

            except ValueError:
                print("invalid input")
                raise

            except KeyboardInterrupt:
                raise
        
        p.flush()
        
