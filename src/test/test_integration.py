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
        print('Initializing specs for the RE API..')
        resp = requests.put(
            _CONFIG['re_api_url'] + '/api/v1/specs',
            headers={'Authorization': _CONFIG['ws_token']},
            params={'init_collections': '1'}
        )
        resp.raise_for_status()
        print('Done initializing RE specs.')

    def test_basic(self):
        _produce({'evtype': 'NEW_VERSION', 'wsid': 41347, 'objid': 5, 'ver': 1})
        time.sleep(30)
        doc = _wait_for_doc('wsfull_object', '41347:5')
        self.assertEqual(doc['workspace_id'], 41347)
        self.assertEqual(doc['object_id'], 5)
        self.assertEqual(doc['deleted'], False)


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
    """Fetch a doc with the RE API, waiting for it to become available with a 30s timeout."""
    timeout = int(time.time()) + 30
    while True:
        results = get_doc(coll, key)
        if results['count'] > 0:
            break
        else:
            if int(time.time()) > timeout:
                raise RuntimeError('Timed out trying to fetch', key)
            time.sleep(3)
    return results['results'][0]
