"""
Consume workspace update events from kafka.
"""
import json
import traceback
from confluent_kafka import Consumer, KafkaError

from src.utils.config import get_config
from src.utils.workspace_client import download_info
from src.import_object import import_object

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
        except Exception as err:
            print('=' * 80)
            print(f"Error indexing:\n{type(err)} - {err}")
            print(msg)
            print(err)
            traceback.print_exc()
            print('=' * 80)
    consumer.close()


def _handle_msg(msg):
    """Receive a kafka message."""
    event_type = msg.get('evtype')
    wsid = msg.get('wsid')
    if not wsid:
        raise RuntimeError(f'Invalid wsid in event: {wsid}')
    if not event_type:
        raise RuntimeError(f"Missing 'evtype' in event: {msg}")
    print(f'Received {msg["evtype"]} for {wsid}/{msg.get("objid", "?")}')
    if event_type in ['IMPORT', 'NEW_VERSION', 'COPY_OBJECT', 'RENAME_OBJECT']:
        _import_obj(msg)
    elif event_type == 'IMPORT_NONEXISTENT':
        _import_nonexistent(msg)
    elif event_type == 'OBJECT_DELETE_STATE_CHANGE':
        print('delete obj')
    elif event_type == 'WORKSPACE_DELETE_STATE_CHANGE':
        print('delete obj for all in workspace')
    elif event_type in ['CLONE_WORKSPACE', 'IMPORT_WORKSPACE']:
        print('import whole workspace')
    elif event_type == 'SET_GLOBAL_PERMISSION':
        print('set global permission for a workspace')
    else:
        raise RuntimeError(f"Unrecognized event {event_type}.")


def _import_obj(msg):
    print('Downloading obj')
    obj_info = download_info(msg['wsid'], msg['objid'], msg.get('ver'))
    import_object(obj_info)


def _import_nonexistent(msg):
    print('_import_nonexistent TODO')
    # upa = '/'.join([str(p) for p in [msg['wsid'], msg['objid'], msg['ver']]])
    # exists = check_doc_existence(upa)
    # if not exists:
    #     _import_obj(msg)
