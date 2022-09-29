# [json] = consumer config
def consumer_settings_json(broker):

    json = {
        'bootstrap.servers': broker,
        'group.id': 'python-app-consumer',
        'auto.commit.interval.ms': 6000,
        'auto.offset.reset': 'latest'

        }

    # return data
    return dict(json)