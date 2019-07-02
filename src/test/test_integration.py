import unittest
import json
import time
from confluent_kafka import Producer
from src.utils.config import get_config

print("module level test_integration")

_CONFIG = get_config()


class TestIntegration(unittest.TestCase):

    def test_basic(self):
        print("test_basic")
        _produce({'evtype': 'NEW_VERSION', 'wsid': 1, 'objid': 1})
        time.sleep(10)
        self.assertEqual(1, 2)


def _delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed:', err)
    else:
        print('Message delivered to', msg.topic())


def _produce(data, topic=_CONFIG['kafka_topics']['workspace_events']):
    producer = Producer({'bootstrap.servers': _CONFIG['kafka_server']})
    producer.produce(topic, json.dumps(data), callback=_delivery_report)
    producer.poll(60)
