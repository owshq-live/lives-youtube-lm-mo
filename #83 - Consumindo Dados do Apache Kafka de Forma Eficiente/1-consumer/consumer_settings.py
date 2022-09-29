# [json] = consumer config
def consumer_settings_json(broker):

    json = {
        'bootstrap.servers': broker,
        'group.id': 'python-app-consumer-live-83-16-37',
        'client.id': 'live',
        'auto.commit.interval.ms': 6000,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False
        
        }

    # return data
    return dict(json)