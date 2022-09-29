#Create fuction with producer configurations
import json


def producer_settings_json(broker):
    json = {
        'client.id': 'live-python-app-producer-json',
        'bootstrap.servers': broker,
        'enable.idempotence': "true",
        'acks': "all",
        'linger.ms': 100,
        'batch.size': 100,
        'compression.type': 'gzip',
        'max.in.flight.requests.per.connection': 5          
        }
    return dict(json)   