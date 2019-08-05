"""
Consume workspace update events from kafka.
"""
import json
import traceback
from confluent_kafka import Consumer, KafkaError

from src.utils.logger import log
from src.utils.config import get_config
from src.utils.workspace_client import download_info
from src.utils.re_client import check_doc_existence
from src.import_object import import_object

_CONFIG = get_config()


def run():
    """Run the main event loop, ie. the Kafka Consumer, dispatching to self._handle_message."""
    topics = [
        _CONFIG['kafka_topics']['workspace_events'],
        _CONFIG['kafka_topics']['re_admin_events']
    ]
    log('INFO', f"Subscribing to: {topics}")
    log('INFO', f"Client group: {_CONFIG['kafka_clientgroup']}")
    log('INFO', f"Kafka server: {_CONFIG['kafka_server']}")
    consumer = Consumer({
        'bootstrap.servers': _CONFIG['kafka_server'],
        'group.id': _CONFIG['kafka_clientgroup'],
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True
    })
    consumer.subscribe(topics)
    while True:
        msg = consumer.poll(timeout=0.5)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                log('INFO', 'End of stream.')
            else:
                log('ERROR', f"Kafka message error: {msg.error()}")
            continue
        val = msg.value().decode('utf-8')
        try:
            msg = json.loads(val)
            log('INFO', f'New message: {msg}')
            _handle_msg(msg)
        except Exception as err:
            log('ERROR', '=' * 80)
            log('ERROR', f"Error importing:\n{type(err)} - {err}")
            log('ERROR', msg)
            log('ERROR', err)
            # Prints to stderr
            traceback.print_exc()
            log('ERROR', '=' * 80)
    consumer.close()


def _handle_msg(msg):
    """Receive a kafka message."""
    event_type = msg.get('evtype')
    wsid = msg.get('wsid')
    if not wsid:
        raise RuntimeError(f'Invalid wsid in event: {wsid}')
    if not event_type:
        raise RuntimeError(f"Missing 'evtype' in event: {msg}")
    log('INFO', f'Received {msg["evtype"]} for {wsid}/{msg.get("objid", "?")}')
    if event_type in ['IMPORT', 'NEW_VERSION', 'COPY_OBJECT', 'RENAME_OBJECT']:
        _import_obj(msg)
    elif event_type == 'IMPORT_NONEXISTENT':
        _import_nonexistent(msg)
    elif event_type == 'OBJECT_DELETE_STATE_CHANGE':
        _delete_obj(msg)
    elif event_type == 'WORKSPACE_DELETE_STATE_CHANGE':
        _delete_ws(msg)
    elif event_type in ['CLONE_WORKSPACE', 'IMPORT_WORKSPACE']:
        _import_ws(msg)
    elif event_type == 'SET_GLOBAL_PERMISSION':
        _set_global_perms(msg)
    else:
        raise RuntimeError(f"Unrecognized event {event_type}.")


def _import_obj(msg):
    log('INFO', 'Downloading obj')
    obj_info = download_info(msg['wsid'], msg['objid'], msg.get('ver'))
    import_object(obj_info)


def _import_nonexistent(msg):
    """Import an object only if it does not exist in RE already."""
    upa = ':'.join([str(p) for p in [msg['wsid'], msg['objid'], msg['ver']]])
    log('INFO', f'_import_nonexistent on {upa}')  # TODO
    _id = 'wsfull_object_version/' + upa
    exists = check_doc_existence(_id)
    if not exists:
        _import_obj(msg)


def _delete_obj(msg):
    """Handle an object deletion event (OBJECT_DELETE_STATE_CHANGE)"""
    log('INFO', '_delete_obj TODO')  # TODO
    raise NotImplementedError()


def _delete_ws(msg):
    """Handle a workspace deletion event (WORKSPACE_DELETE_STATE_CHANGE)."""
    log('INFO', '_delete_ws TODO')  # TODO
    raise NotImplementedError()


def _import_ws(msg):
    """Import all data for an entire workspace."""
    log('INFO', '_import_ws TODO')  # TODO
    raise NotImplementedError()


def _set_global_perms(msg):
    """Set permissions for an entire workspace (SET_GLOBAL_PERMISSION)."""
    log('INFO', '_set_global_perms TODO')  # TODO
    raise NotImplementedError()
