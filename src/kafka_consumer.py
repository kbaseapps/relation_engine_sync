"""
Consume workspace update events from kafka.
"""
import json
from confluent_kafka import Consumer, KafkaError
from .utils.config import get_config

_CONFIG = get_config()


def run():
    """Run the main event loop, ie. the Kafka Consumer, dispatching to self._handle_message."""
    topics = [
        _CONFIG['kafka_topics']['workspace_events'],
        _CONFIG['kafka_topics']['re_admin_events']
    ]
    consumer = Consumer({
        'bootstrap.servers': _CONFIG['kafka_server'],
        'group.id': _CONFIG['kafka_clientgroup'],
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    })
    print(f"Subscribing to: {topics}")
    print(f"Client group: {_CONFIG['kafka_clientgroup']}")
    print(f"Kafka server: {_CONFIG['kafka_server']}")
    consumer.subscribe(topics)
    while True:
        msg = consumer.poll(timeout=0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('End of stream.')
            else:
                print(f"Kafka message error: {msg.error()}")
            continue
        val = msg.value().decode('utf-8')
        try:
            data = json.loads(val)
            print('received', data)
        except ValueError as err:
            print(f'JSON parsing error: {err}')
            print(f'Message content: {val}')
    consumer.close()
