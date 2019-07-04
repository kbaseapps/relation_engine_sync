import unittest
import json
import time
import requests
from confluent_kafka import Producer
from src.utils.config import get_config
from src.utils.re_client import get_doc

_CONFIG = get_config()


class TestIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print('token', _CONFIG['ws_token'])
        # Initialize specs
        print('xyzabc')
        print(_CONFIG['re_api_url'])
        print(_CONFIG['ws_token'])
        print('xyzabc')
        resp = requests.put(
            _CONFIG['re_api_url'] + '/api/v1/specs',
            headers={'Authorization': _CONFIG['ws_token']},
            params={'init_collections': '1'}
        )
        print('xxx', resp.text)
        resp.raise_for_status()

    def test_basic(self):
        print("test_basic")
        _produce({'evtype': 'NEW_VERSION', 'wsid': 43062, 'objid': 2, 'ver': 1})
        time.sleep(30)
        _wait_for_doc('wsfull_object', '43062:2')
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


def _wait_for_doc(coll, key):
    # timeout = int(time.time()) + 60
    results = get_doc(coll, key)
    print("results", results)


def _get_doc(key):
    """Fetch a doc on RE, blocking for it until timeout."""
