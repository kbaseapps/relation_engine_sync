"""
Relation Engine API client
"""
import json
import requests
from utils.get_config import get_config
from urllib.parse import urljoin


def save(coll_name, docs):
    """
    Bulk-save documents to the relation engine database
    API docs: https://github.com/kbase/relation_engine_api
    """
    config = get_config()
    url = urljoin(config['relation_engine_url'] + '/', 'api/documents')
    # convert the docs into a string, where each obj is separated by a linebreak
    payload = '\n'.join([json.dumps(d) for d in docs])
    params = {'collection': coll_name, 'on_duplicate': 'update', 'display_errors': True}
    resp = requests.put(
        url,
        data=payload,
        params=params,
        headers={'Authorization': config['relation_engine_token']}
    )
    if resp.status_code != 200:
        raise RuntimeError('Error response from relation engine API: %s' % resp.text)
    return resp.json()
