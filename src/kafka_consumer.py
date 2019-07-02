"""
Consume workspace update events from kafka.
"""
import json
import traceback
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
            msg = json.loads(val)
            _handle_msg(msg)
        except ValueError as err:
            print(f'JSON parsing error: {err}')
            print(f'Message content: {val}')
    consumer.close()


def _handle_msg(msg):
    """Receive a kafka message."""
    event_type = msg.get('evtype')
    wsid = msg.get('wsid')
    if not wsid:
        print(f'Invalid wsid in event: {wsid}')
        return
    if not event_type:
        print(f"Missing 'evtype' in event: {msg}")
        return
    print(f'Received {msg["evtype"]} for {wsid}/{msg.get("objid", "?")}')
    try:
        if event_type in ['IMPORT', 'NEW_VERSION', 'COPY_OBJECT', 'RENAME_OBJECT']:
            print('run importer')
        elif event_type == 'IMPORT_NONEXISTENT':
            print('run importer if nonexistent')
        elif event_type == 'OBJECT_DELETE_STATE_CHANGE':
            print('delete obj')
        elif event_type == 'WORKSPACE_DELETE_STATE_CHANGE':
            print('delete obj for all in workspace')
        elif event_type in ['CLONE_WORKSPACE', 'IMPORT_WORKSPACE']:
            print('import whole workspace')
        elif event_type == 'SET_GLOBAL_PERMISSION':
            print('set global permission for a workspace')
        else:
            print(f"Unrecognized event {event_type}.")
            return
    except Exception as err:
        print('=' * 80)
        print(f"Error indexing:\n{type(err)} - {err}")
        print(msg)
        print(err)
        traceback.print_exc()
        print('=' * 80)
